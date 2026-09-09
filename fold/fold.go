// Package fold is the tail's compaction: it accumulates the mutations of a
// window and emits, per dirty workflow, one merged request plus the assertions
// that request stands on.
//
// Assertions come from the head of the window and data from the tail, because
// only the head was ever evaluated against the cold store. A snapshot-bearing
// request resets the run's accumulator (I8); tasks concatenate across that
// reset, being queue entries rather than workflow state.
//
// This package folds what it is handed and may not name wal.Log or
// wal.Entry: reaching the log is how trim and pacing land here one
// convenience at a time, and both are the apply cycle's policy.
package fold

import (
	"cmp"
	"errors"
	"fmt"
	"iter"
	"slices"

	commonpb "go.temporal.io/api/common/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"

	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/wal"
)

// ErrAfterTombstone reports a mutation on a run the window already deleted.
// [Accumulator.Check] refuses such a mutation before it is acked, so a log that
// still produces one is corrupt: fatal, not folded over.
var ErrAfterTombstone = errors.New("fold: mutation on a run this window already deleted")

// ErrInvalidStream reports a mutation that cannot follow the window's mutations
// in any acked stream: a create of a run the window already holds live, a second
// continue-as-new out of the same run. The single writer would have seen the
// request fail, so the log is corrupt. A double continue-as-new is the one case
// [Accumulator.Check] cannot pre-empt, no assertion standing in its way.
var ErrInvalidStream = errors.New("fold: mutation cannot follow the window's mutations in any acked stream")

// ErrRefused reports a valid window this accumulator cannot express as merged
// requests. The accumulator is left exactly as it was, so the caller recovers
// by draining and starting the refused mutation on a fresh window.
var ErrRefused = errors.New("fold: window not foldable into merged requests")

// RunAssertion is what the head of the window asserted about one run's row in
// the cold store.
type RunAssertion struct {
	// MustNotExist: the head asserted the row absent (a Create heads the
	// window). BaseVersion is meaningless when set.
	MustNotExist bool
	// BaseVersion is the db_record_version the head asserted the row to hold.
	BaseVersion int64
}

// CurrentKind names the shapes a current-execution-row assertion comes in,
// mirroring what the store's write paths assert.
type CurrentKind int

const (
	// CurrentMustNotExist: no current row (a brand-new Create).
	CurrentMustNotExist CurrentKind = iota + 1
	// CurrentEquals: the current row names RunID.
	CurrentEquals
	// CurrentNotEquals: the current row does not name RunID (bypass writes).
	CurrentNotEquals
	// CurrentEqualsWithVersion: the current row names RunID at LastWriteVersion
	// and is COMPLETED (a Create over a finished run).
	CurrentEqualsWithVersion
)

// CurrentAssertion is what the head of the window asserted about the workflow's
// current-execution row. Fixed by the first mutation of the window that touches
// the row and shared by every request emitted for the workflow: apply asserts it
// once per workflow, not once per request.
type CurrentAssertion struct {
	Kind             CurrentKind
	RunID            string
	LastWriteVersion int64 // CurrentEqualsWithVersion only
}

// CurrentWrite is the current-execution row's content as the sequential path
// would have left it: the window's last mutation that writes the row, rendered
// the way the store's own path for that kind renders it. Apply swallows the
// write the store derives from the merged request's kind (the head of the
// window) and registers this one instead.
type CurrentWrite struct {
	RunID            string
	StateBlob        *commonpb.DataBlob
	LastWriteVersion int64
	State            enumsspb.WorkflowExecutionState
}

// BufferedBatch is one batch of buffered events and the run that accumulated
// it. The run travels with the blob because apply keys the row by it, and a
// window whose merged state is a snapshot carries no mutation to read it from.
type BufferedBatch struct {
	RunID string
	Blob  *commonpb.DataBlob
}

// Emitted is one merged request: the unit apply drives through the store.
//
// Its fields are what the request is — whose it is, which slice of the window
// it came from, the request itself. What the fold recorded about it is behind
// the methods below, each of them a claim only [Accumulator.Drain] can make
// true.
type Emitted struct {
	NamespaceID string
	WorkflowID  string
	// HeadSeqno is the seqno of the mutation that opened this request and
	// TailSeqno the last folded into it. A snapshot arriving over a pending
	// update opens a new request at its own seqno and carries the superseded
	// update's tasks and head assertion into it, so a mutation below HeadSeqno
	// can still be part of what is here. Drain returns requests in TailSeqno
	// order and apply must keep that order: a merged request's writes are its
	// folded tail state, so a sequentially interleaved request must land before
	// that tail rather than before its earliest constituent.
	HeadSeqno wal.Seqno
	TailSeqno wal.Seqno
	// Request holds exactly one of the six request kinds a window can emit.
	Request mutation.Mutation
	// BufferedBatches are the window's buffered-event batches in arrival order,
	// one row per batch. They do not merge, so the request's own
	// NewBufferedEvents slot is always nil.
	BufferedBatches []BufferedBatch

	runs          map[string]RunAssertion
	orphanedTasks map[tasks.Category][]p.InternalHistoryTask
	workflow      *WorkflowRecord
	first         bool
}

// RunAssertions is the head-of-window state of each run this request touches,
// by run id, which apply's transaction wrapper substitutes for the assertions
// the store would derive from the versions the request writes. A run with no
// entry came in asserting nothing (a Delete-headed window). The map is the
// batch's own: writing to it rewrites what the drain stands on.
func (e *Emitted) RunAssertions() map[string]RunAssertion { return e.runs }

// OrphanedTasks are tasks from mutations a tombstone collapsed, which only a
// tombstone carries: the Delete that collapsed them has no task slot of its
// own, and losing them would break I7. Writing them is apply's business.
func (e *Emitted) OrphanedTasks() map[tasks.Category][]p.InternalHistoryTask {
	return e.orphanedTasks
}

// Workflow is the current-row facts this request's workflow carries, which are
// the workflow's and not this request's, and never nil. Every request of one
// workflow returns the same pointer, which is what makes "two requests cannot
// disagree about them" a property of the value rather than of an index.
func (e *Emitted) Workflow() *WorkflowRecord { return e.workflow }

// FirstOfWorkflow reports that this is the first request of the batch naming
// its workflow record, which is where that record's own assertions belong. The
// store reports the first failing assertion in registration order, so a
// consumer that hoisted every record's assertions to the front would change
// which failure a mixed batch reports — and one that registered them per
// request would assert the same row several times.
func (e *Emitted) FirstOfWorkflow() bool { return e.first }

// WorkflowRecord is the window's net effect on one workflow's current-execution
// row. A workflow's drain may carry several requests — a tombstone and the run
// created behind it are two — and these facts are the same for all of them, so
// they are stated once here and pointed at by [Emitted.Workflow] rather than
// copied onto each. Apply registers them at the request
// [Emitted.FirstOfWorkflow] marks; two requests cannot disagree about them,
// because there is nothing left to disagree.
type WorkflowRecord struct {
	NamespaceID string
	WorkflowID  string
	// Current is the head-of-window assertion on the current row, nil when the
	// window never touched it.
	Current *CurrentAssertion
	// CurrentWrite is the current-execution row as the window's last writer left
	// it, nil when the window never wrote the row or when a DeleteCurrent
	// removed what it wrote ([WorkflowRecord.CurrentRemoved]).
	CurrentWrite *CurrentWrite
	// CurrentRemoved reports that the window's net effect on the current row is
	// removal: a DeleteCurrent removed the row the window itself had written.
	// Apply must then let the emitted DeleteCurrent remove the row whatever it
	// names (the store's guard asks about the pre-window row) and swallow the
	// store's derived write with nothing in its place. A write and a delete of
	// this row are never emitted together.
	CurrentRemoved bool
}

// Stats are the accumulator's two counters. Fold reports the values and emits no
// metric of its own.
type Stats struct {
	// MutationsIn counts mutations added since the last drain.
	MutationsIn int
	// DirtyWorkflows counts workflows with pending state.
	DirtyWorkflows int
}

// CollapseRatio is mutations in over dirty workflows out, or 0 for an empty
// window.
func (s Stats) CollapseRatio() float64 {
	if s.DirtyWorkflows == 0 {
		return 0
	}
	return float64(s.MutationsIn) / float64(s.DirtyWorkflows)
}

// Accumulator folds one shard's window. Not safe for concurrent use: the shard's
// single-threaded apply loop owns it. Add either folds the mutation in whole or
// returns an error leaving the accumulator exactly as it was, validation running
// before the first write. It takes ownership of what it is handed: requests are
// merged in place, so a caller that needs the mutation afterwards must copy it
// first.
type Accumulator struct {
	shard       wal.ShardID
	lastSeqno   wal.Seqno
	mutationsIn int
	workflows   map[wfKey]*workflowAcc

	// The history-task half of the window (histtasks.go). It sits beside the
	// workflows rather than inside them because the store's task rows are keyed
	// by (shard, category, key) and name no run: addedTasks are the rows an
	// AddHistoryTasks put in, in arrival order and unsorted, and ranges holds the
	// undrained range deletes by category id.
	addedTasks   map[tasks.Category][]p.InternalHistoryTask
	ranges       map[int32]*rangeAcc
	taskTail     wal.Seqno
	tasksDropped map[string]int
}

// New returns an empty accumulator for one shard's window.
func New(shard wal.ShardID) *Accumulator {
	return &Accumulator{
		shard:     shard,
		workflows: make(map[wfKey]*workflowAcc),
		ranges:    make(map[int32]*rangeAcc),
	}
}

type wfKey struct {
	namespaceID string
	workflowID  string
}

type workflowAcc struct {
	key     wfKey
	runs    map[string]*runState
	cur     currentAcc
	pending []*pendingReq
}

// currentAcc is the window's whole state for one workflow's current-execution
// row. [Accumulator.Drain], [Accumulator.decideCurrent] and
// [Accumulator.ViewCurrent] share its single derivation in
// [workflowAcc.currentView].
//
// write and removed are mutually exclusive, and that is a property of the two
// methods below rather than a sentence: they are the only writers of either
// field, and each sets both. A window that wrote the row and then removed it
// emits the removal alone, because handing apply an upsert and a delete of one
// row leaves which wins to the plugin's statement order.
type currentAcc struct {
	// assertion is the head-of-window assertion on the row, nil when the window
	// does not hold it — which is the condition authority's partition
	// ([workflowAcc.assertsCurrent]) and why nothing else may compare it to nil.
	assertion *CurrentAssertion
	// write is the window's last current-row write, overwritten by every
	// mutation that writes the row: a last-writer effect, not a precondition
	// like the assertion above.
	write *CurrentWrite
	// tainted marks that a DeleteCurrent modified the current row with no
	// assertion recorded, so a later mutation asserting on the row cannot fold
	// into this window (currentTaintedRefusal).
	tainted bool
	// removed marks that the window's net effect on the current row is removal.
	// See [WorkflowRecord.CurrentRemoved].
	removed bool
}

// recordWrite takes the window's latest current-row write, which outranks any
// removal before it.
func (c *currentAcc) recordWrite(cw *CurrentWrite) {
	c.write, c.removed = cw, false
}

// remove drops the window's own write and marks the net effect removal.
func (c *currentAcc) remove() {
	c.write, c.removed = nil, true
}

// recordCurrent takes what one mutation says about the current-execution row:
// the head assertion, which only the window's first toucher of the row fixes,
// and the write, which the last one wins. The commit half of a handler calls it
// past its last refusal, cw having been rendered before anything merged.
func (w *workflowAcc) recordCurrent(want asserted, cw *CurrentWrite) {
	if !w.assertsCurrent() {
		w.cur.assertion = want.current
	}
	if cw != nil {
		w.recordCurrentWrite(cw)
	}
}

// recordCurrentWrite records the window's last current-row write and drops any
// delete-current already in the window, the write replacing the row wholesale.
// Keeping both would hand apply an upsert and a delete of one row, leaving which
// of the two wins to the plugin's statement order rather than to the window.
func (w *workflowAcc) recordCurrentWrite(cw *CurrentWrite) {
	w.cur.recordWrite(cw)
	w.pending = slices.DeleteFunc(w.pending, func(pr *pendingReq) bool { return pr.m.DeleteCurrent != nil })
}

// currentTaintedRefusal refuses a mutation standing on the current row when a
// delete-current sits in the window with no assertion above it. The delete is a
// guarded no-op and not an assertion, so an assertion recorded past it would be
// a mid-window claim dressed as a head-of-window one.
//
// Whether the mutation stands on that row at all is this rule's own question and
// not each handler's. A handler that carried the test itself and then dropped it
// would record exactly the claim above, and nothing would say so: the authority
// refuses it before the append, so only a replayed stream — which reaches [Add]
// with no [Accumulator.Check] in front of it — would ever meet the difference.
func currentTaintedRefusal(w *workflowAcc, want asserted) error {
	if want.current == nil {
		return nil
	}
	if w.currentTainted() {
		return fmt.Errorf("%w: a current-row assertion behind a delete-current in the same window", ErrRefused)
	}
	return nil
}

// runState is one run's place in the window. Its assertion is fixed by the
// first mutation that touches the run and never rewritten.
type runState struct {
	// assertion is nil when the run's head asserted nothing (Delete-headed).
	assertion *RunAssertion
	// owner is the pending request whose part carries this run's row state;
	// nil once tombstoned.
	owner *pendingReq
	part  partKind
	// buffered holds the run's buffered-event batches, stripped from the
	// mutations they arrived in.
	buffered   []*commonpb.DataBlob
	tombstoned bool
}

// partKind names which slot of the owning request carries a run's row state. It
// is [mutation.Part] itself so the two numberings cannot drift apart.
type partKind = mutation.Part

const (
	partSnapshot    = mutation.PartSnapshot
	partNewSnapshot = mutation.PartNewSnapshot
	partMutation    = mutation.PartMutation
)

type pendingReq struct {
	headSeqno     wal.Seqno
	tailSeqno     wal.Seqno
	m             mutation.Mutation
	runs          []string
	orphanedTasks map[tasks.Category][]p.InternalHistoryTask
}

func newPending(seqno wal.Seqno, m mutation.Mutation) *pendingReq {
	return &pendingReq{headSeqno: seqno, tailSeqno: seqno, m: m}
}

// Add folds one mutation into the window. seqno must be strictly above every
// seqno already added, across drains — one accumulator follows one log.
func (a *Accumulator) Add(seqno wal.Seqno, m mutation.Mutation) error {
	if seqno <= a.lastSeqno {
		return fmt.Errorf("fold: seqno %d is not above the window's last seqno %d", seqno, a.lastSeqno)
	}
	if got, want := m.ShardID(), int32(a.shard); got != want {
		return fmt.Errorf("fold: mutation belongs to shard %d, this accumulator folds shard %d", got, want)
	}

	var err error
	switch m.Kind() {
	case mutation.KindCreate:
		err = a.addCreate(seqno, m.Create)
	case mutation.KindUpdate:
		err = a.addUpdate(seqno, m.Update)
	case mutation.KindConflictResolve:
		err = a.addConflictResolve(seqno, m.ConflictResolve)
	case mutation.KindSet:
		err = a.addSet(seqno, m.Set)
	case mutation.KindDelete:
		err = a.addDelete(seqno, m.Delete)
	case mutation.KindDeleteCurrent:
		a.addDeleteCurrent(seqno, m.DeleteCurrent)
	case mutation.KindAddTasks:
		a.addTasks(seqno, m.AddTasks)
	case mutation.KindRangeCompleteTasks:
		a.addRangeCompleteTasks(seqno, m.RangeCompleteTasks)
	default:
		return fmt.Errorf("fold: %w", mutation.ErrNotExactlyOneRequest)
	}
	if err != nil {
		return err
	}

	a.lastSeqno = seqno
	a.mutationsIn++
	return nil
}

// Stats reports the window's counters so far.
func (a *Accumulator) Stats() Stats {
	return Stats{MutationsIn: a.mutationsIn, DirtyWorkflows: len(a.workflows)}
}

// Batch is one drain's whole output: these requests, this task work, and the
// seqno a transaction that applied both may acknowledge.
//
// Only [Accumulator.Drain] builds one, and that is what apply's write path
// stands on rather than re-deriving: the requests are in tail-seqno order, they
// belong to the one shard [Batch.Shard] names, the watermark is at or above
// every seqno they carry, every request names a workflow record, and only a
// tombstone carries orphaned tasks. A batch built beside the fold could hold
// none of that; the only one that can still be built is the zero batch, which
// carries nothing and is refused.
type Batch struct {
	shard     wal.ShardID
	requests  []Emitted
	tasks     TaskWork
	stats     Stats
	watermark wal.Seqno
}

// Empty reports a batch no transaction need carry: no merged request and no
// task work. The counters are not consulted.
func (b Batch) Empty() bool { return len(b.requests) == 0 && b.tasks.Empty() }

// Shard is the shard whose window this is: the accumulator's own. It is what a
// caller pairs against the shard it was asked to write, which is the one thing
// above that no batch can know.
func (b Batch) Shard() wal.ShardID { return b.shard }

// Len is how many merged requests the batch carries: usually one per dirty
// workflow, but a workflow whose window tombstoned a run and created the next
// one behind it carries two.
func (b Batch) Len() int { return len(b.requests) }

// Watermark is the seqno the drain's transaction acks: the maximum of the
// requests' tail and the task work's, the two halves being in no shared
// ordering. A batch carrying seqnos above the position it acks would leave
// applied rows above where a replay resumes.
func (b Batch) Watermark() wal.Seqno { return b.watermark }

// Stats describe the window that was drained, not the batch: an empty batch can
// still carry a mutation count, from the one window that folds an entry and
// drains nothing — an AddHistoryTasks that carried no rows.
func (b Batch) Stats() Stats { return b.stats }

// Settles is the position this drain's window acked entries up to, and whether
// it acked any at all. The caller that carries the batch in a transaction
// already has [Batch.Watermark]; this is for the one that carries none, because
// a window can fold entries and still drain an empty batch — in exactly one
// shape, which emptydrain_test.go names and holds every other kind
// against. Those entries are settled off this answer or never.
//
// False is a window that folded nothing at all, whose watermark is zero: a
// position taken from it would report every entry ever acked as unsettled.
func (b Batch) Settles() (wal.Seqno, bool) {
	if b.stats.MutationsIn == 0 {
		return 0, false
	}
	return b.watermark, true
}

// Tasks is the shard-level history-task work beside the requests, which is in
// no workflow's ordering because it is no workflow's ([TaskWork]).
func (b Batch) Tasks() TaskWork { return b.tasks }

// Each iterates the batch's merged requests in tail-seqno order, which is
// apply's own order and the order the assertions must be registered in.
func (b Batch) Each() iter.Seq[*Emitted] {
	return func(yield func(*Emitted) bool) {
		for i := range b.requests {
			if !yield(&b.requests[i]) {
				return
			}
		}
	}
}

// Drain emits the window as one [Batch] — well-formed by construction, in every
// sense [Batch] states — and resets the accumulator. The seqno floor survives
// the drain; the pending task deletion ranges do not, they ride the batch that
// applies them ([TaskWork]).
func (a *Accumulator) Drain() Batch {
	stats := a.Stats()

	// The pending requests and not the workflows: one per dirty workflow is the
	// ordinary case, and the tombstone-and-recreate one appends past it, so the
	// workflow count is a lower bound that regrows a wide struct mid-drain.
	pending := 0
	for _, w := range a.workflows {
		pending += len(w.pending)
	}
	out := make([]Emitted, 0, pending)
	for _, w := range a.workflows {
		rec := &WorkflowRecord{
			NamespaceID: w.key.namespaceID,
			WorkflowID:  w.key.workflowID,
		}
		if w.assertsCurrent() {
			cur := *w.cur.assertion
			rec.Current = &cur
		}
		// From the shape rather than from the two fields, so that "a write and
		// a removal are never emitted together" is read off the same derivation
		// the overlay and the authority read, instead of being a pair of
		// independent ifs that happen never to both fire.
		switch view := w.currentView(); view.Shape {
		case CurrentWritten:
			cw := *view.write
			rec.CurrentWrite = &cw
		case CurrentGone:
			rec.CurrentRemoved = true
		}

		for _, pr := range w.pending {
			e := Emitted{
				NamespaceID:   w.key.namespaceID,
				WorkflowID:    w.key.workflowID,
				HeadSeqno:     pr.headSeqno,
				TailSeqno:     pr.tailSeqno,
				Request:       pr.m,
				orphanedTasks: pr.orphanedTasks,
				workflow:      rec,
				runs:          make(map[string]RunAssertion, len(pr.runs)),
			}
			for _, run := range pr.runs {
				rs := w.runs[run]
				if rs.assertion != nil {
					e.runs[run] = *rs.assertion
				}
				// A tombstoned-then-recreated run's state is shared by two
				// requests; its batches belong to the one that owns it now.
				if rs.owner == pr {
					for _, b := range rs.buffered {
						e.BufferedBatches = append(e.BufferedBatches, BufferedBatch{RunID: run, Blob: b})
					}
				}
			}
			out = append(out, e)
		}
	}
	slices.SortFunc(out, func(a, b Emitted) int { return cmp.Compare(a.TailSeqno, b.TailSeqno) })
	// After the sort, so that "first request naming this record" is a position
	// in the order apply drives rather than in the order the map ranged.
	named := make(map[*WorkflowRecord]struct{}, len(a.workflows))
	for i := range out {
		rec := out[i].workflow
		_, seen := named[rec]
		out[i].first = !seen
		named[rec] = struct{}{}
	}

	work := a.drainTasks()

	// Above both halves: the folded requests, whose last tail seqno is the
	// maximum because out is sorted, and the task work, whose seqnos are not in
	// that ordering. The task work's tail counts even when the work is empty,
	// since a window whose task rows a range delete all dropped still folded
	// those entries and a watermark below them would replay them.
	var watermark wal.Seqno
	if len(out) > 0 {
		watermark = out[len(out)-1].TailSeqno
	}
	watermark = max(watermark, work.TailSeqno)

	a.workflows = make(map[wfKey]*workflowAcc)
	a.mutationsIn = 0
	return Batch{shard: a.shard, requests: out, tasks: work, stats: stats, watermark: watermark}
}

func (a *Accumulator) peek(namespaceID, workflowID string) *workflowAcc {
	return a.workflows[wfKey{namespaceID, workflowID}]
}

// acc returns the workflow's accumulator, creating it. Only the commit half
// of a handler may call it: validation must not leave empty state behind.
func (a *Accumulator) acc(namespaceID, workflowID string) *workflowAcc {
	key := wfKey{namespaceID, workflowID}
	w := a.workflows[key]
	if w == nil {
		w = &workflowAcc{key: key, runs: make(map[string]*runState)}
		a.workflows[key] = w
	}
	return w
}

// heldRun is the window's state for run, nil when the window does not hold it.
//
// This and [workflowAcc.assertsCurrent] are the condition authority's partition: a
// row the window does not hold is a head, so its assertion is recorded rather
// than answered. Both halves ask here, so neither draws a line the other did not.
func (w *workflowAcc) heldRun(run string) *runState {
	if w == nil {
		return nil
	}
	return w.runs[run]
}

// assertsCurrent reports whether the window holds the current-execution row.
func (w *workflowAcc) assertsCurrent() bool { return w != nil && w.cur.assertion != nil }

// currentTainted reports the row a delete-current sits over with no assertion
// above it. Beside [workflowAcc.assertsCurrent] for the same reason: both halves
// of the authority refuse on this, and a half that spelled the test itself would
// answer a claim the other one refuses.
func (w *workflowAcc) currentTainted() bool {
	return w != nil && !w.assertsCurrent() && w.cur.tainted
}

// adopt registers a new pending request as the owner of a run. An existing run
// state keeps its head assertion, which is what makes a Create behind a
// tombstone assert the pre-window row rather than absence; a fresh run gets
// fallback as its head.
func (w *workflowAcc) adopt(pr *pendingReq, run string, part partKind, fallback *RunAssertion) {
	rs := w.heldRun(run)
	if rs == nil {
		rs = &runState{assertion: fallback}
		w.runs[run] = rs
	}
	rs.owner = pr
	rs.part = part
	rs.buffered = nil
	rs.tombstoned = false
	pr.runs = append(pr.runs, run)
}

// drop removes a superseded request from the pending list.
func (w *workflowAcc) drop(pr *pendingReq) {
	if i := slices.Index(w.pending, pr); i >= 0 {
		w.pending = slices.Delete(w.pending, i, i+1)
	}
}

// foldBuffered applies the buffered-events rules for one arriving mutation:
// ClearBufferedEvents drops the batches accumulated before it and marks the
// merged slot to clear the store's pre-window rows; the mutation's own batch is
// stripped into the run's batch list. slot is nil when the run's row state
// lives in a snapshot part, whose own write replaces the rows wholesale.
func (rs *runState) foldBuffered(mut *p.InternalWorkflowMutation, slot *p.InternalWorkflowMutation) {
	if mut.ClearBufferedEvents {
		rs.buffered = nil
		if slot != nil {
			slot.ClearBufferedEvents = true
		}
	}
	if mut.NewBufferedEvents != nil {
		rs.buffered = append(rs.buffered, mut.NewBufferedEvents)
		mut.NewBufferedEvents = nil
	}
}

func (a *Accumulator) addCreate(seqno wal.Seqno, req *p.InternalCreateWorkflowExecutionRequest) error {
	snap := &req.NewWorkflowSnapshot
	want := assertCreate(req)
	w := a.peek(snap.NamespaceID, snap.WorkflowID)

	if rs := w.heldRun(snap.RunID); rs != nil && !rs.tombstoned {
		return fmt.Errorf("%w: create of run %s, which the window already holds live", ErrInvalidStream, snap.RunID)
	}
	if err := currentTaintedRefusal(w, want); err != nil {
		return err
	}

	w = a.acc(snap.NamespaceID, snap.WorkflowID)
	pr := newPending(seqno, mutation.Mutation{Create: req})
	w.adopt(pr, snap.RunID, partSnapshot, want.forRun(snap.RunID))
	w.pending = append(w.pending, pr)

	w.recordCurrent(want, currentWriteOfCreate(req))
	return nil
}

func (a *Accumulator) addUpdate(seqno wal.Seqno, req *p.InternalUpdateWorkflowExecutionRequest) error {
	mut := &req.UpdateWorkflowMutation
	want := assertUpdate(req)
	w := a.peek(mut.NamespaceID, mut.WorkflowID)

	var newRun string
	if req.NewWorkflowSnapshot != nil {
		newRun = req.NewWorkflowSnapshot.RunID
	}

	rs := w.heldRun(mut.RunID)
	if rs != nil && rs.tombstoned {
		return fmt.Errorf("%w: update of run %s", ErrAfterTombstone, mut.RunID)
	}
	if newRun != "" {
		if ns := w.heldRun(newRun); ns != nil && !ns.tombstoned {
			return fmt.Errorf("%w: continue-as-new into run %s, which the window already holds live", ErrInvalidStream, newRun)
		}
	}
	if rs != nil {
		switch rs.part {
		case partMutation:
			if newRun != "" {
				if pr := rs.owner; pr.m.ConflictResolve != nil {
					return fmt.Errorf("%w: continue-as-new folding into a conflict-resolve's current mutation", ErrRefused)
				} else if pr.m.Update.NewWorkflowSnapshot != nil {
					return fmt.Errorf("%w: run %s continued-as-new twice", ErrInvalidStream, mut.RunID)
				}
			}
		case partSnapshot, partNewSnapshot:
			if newRun != "" {
				return fmt.Errorf("%w: continue-as-new out of a run whose window state is a snapshot", ErrRefused)
			}
		}
	}

	if err := currentTaintedRefusal(w, want); err != nil {
		return err
	}

	cw, err := currentWriteOfUpdate(req)
	if err != nil {
		return err
	}

	w = a.acc(mut.NamespaceID, mut.WorkflowID)
	switch {
	case rs == nil:
		// The update heads its run's window.
		pr := newPending(seqno, mutation.Mutation{Update: req})
		w.adopt(pr, mut.RunID, partMutation, want.forRun(mut.RunID))
		w.pending = append(w.pending, pr)
		w.runs[mut.RunID].foldBuffered(mut, mut)
		if newRun != "" {
			w.adopt(pr, newRun, partNewSnapshot, want.forRun(newRun))
		}
	case rs.part == partMutation:
		pr := rs.owner
		pr.tailSeqno = seqno
		dst := pr.mutationPart()
		if pr.m.Update != nil {
			pr.m.Update.Mode = req.Mode
		}
		rs.foldBuffered(mut, dst)
		mergeMutation(dst, mut)
		if newRun != "" {
			pr.m.Update.NewWorkflowSnapshot = req.NewWorkflowSnapshot
			w.adopt(pr, newRun, partNewSnapshot, want.forRun(newRun))
		}
	default:
		// The run's window state is a snapshot: the delta folds into it.
		rs.owner.tailSeqno = seqno
		rs.foldBuffered(mut, nil)
		applyMutationToSnapshot(rs.owner.snapshotPart(rs.part), mut)
	}

	w.recordCurrent(want, cw)
	return nil
}

func (a *Accumulator) addSet(seqno wal.Seqno, req *p.InternalSetWorkflowExecutionRequest) error {
	snap := &req.SetWorkflowSnapshot
	want := assertSet(req)
	w := a.peek(snap.NamespaceID, snap.WorkflowID)

	rs := w.heldRun(snap.RunID)
	if rs != nil {
		if rs.tombstoned {
			return fmt.Errorf("%w: set of run %s", ErrAfterTombstone, snap.RunID)
		}
		if rs.part == partMutation && len(rs.owner.runs) > 1 {
			return fmt.Errorf("%w: set of a run whose pending update also continued-as-new", ErrRefused)
		}
	}

	if err := currentTaintedRefusal(w, want); err != nil {
		return err
	}

	w = a.acc(snap.NamespaceID, snap.WorkflowID)
	switch {
	case rs == nil:
		pr := newPending(seqno, mutation.Mutation{Set: req})
		w.adopt(pr, snap.RunID, partSnapshot, want.forRun(snap.RunID))
		w.pending = append(w.pending, pr)
	case rs.part == partMutation:
		// The snapshot supersedes the pending update; its tasks survive.
		old := rs.owner
		snap.Tasks = mergeTasks(old.m.Update.UpdateWorkflowMutation.Tasks, snap.Tasks)
		w.drop(old)
		pr := newPending(seqno, mutation.Mutation{Set: req})
		w.adopt(pr, snap.RunID, partSnapshot, nil)
		w.pending = append(w.pending, pr)
	default:
		// Already a snapshot: the newer one replaces its content in place,
		// under the owner's own envelope.
		rs.owner.tailSeqno = seqno
		replaceSnapshot(rs.owner.snapshotPart(rs.part), snap)
		rs.buffered = nil
	}
	return nil
}

func (a *Accumulator) addConflictResolve(seqno wal.Seqno, req *p.InternalConflictResolveWorkflowExecutionRequest) error {
	reset := &req.ResetWorkflowSnapshot
	want := assertConflictResolve(req)
	w := a.peek(reset.NamespaceID, reset.WorkflowID)
	parts := 1
	if req.NewWorkflowSnapshot != nil {
		parts++
	}
	if req.CurrentWorkflowMutation != nil {
		parts++
	}

	resetRS := w.heldRun(reset.RunID)
	if resetRS != nil && resetRS.tombstoned {
		return fmt.Errorf("%w: conflict-resolve of run %s", ErrAfterTombstone, reset.RunID)
	}
	if ns := req.NewWorkflowSnapshot; ns != nil {
		if held := w.heldRun(ns.RunID); held != nil && !held.tombstoned {
			return fmt.Errorf("%w: conflict-resolve creates run %s, which the window already holds live",
				ErrInvalidStream, ns.RunID)
		}
	}
	var curRS *runState
	if cur := req.CurrentWorkflowMutation; cur != nil {
		curRS = w.heldRun(cur.RunID)
		if curRS != nil && curRS.tombstoned {
			return fmt.Errorf("%w: conflict-resolve mutates run %s", ErrAfterTombstone, cur.RunID)
		}
		if curRS != nil && (curRS.part != partMutation || curRS.owner.m.Update == nil || len(curRS.owner.runs) > 1) {
			return fmt.Errorf("%w: conflict-resolve's current mutation lands on run %s, whose window state it cannot merge with",
				ErrRefused, cur.RunID)
		}
	}
	if resetRS != nil {
		switch {
		case resetRS.part == partMutation && len(resetRS.owner.runs) > 1:
			return fmt.Errorf("%w: conflict-resolve of a run whose pending update also continued-as-new", ErrRefused)
		case resetRS.part != partMutation && parts > 1:
			// The reset content could replace the snapshot in place, but the
			// request's other parts would be left with no envelope to ride.
			return fmt.Errorf("%w: conflict-resolve with %d parts on a run whose window state is a snapshot", ErrRefused, parts)
		}
	}

	if err := currentTaintedRefusal(w, want); err != nil {
		return err
	}

	cw, err := currentWriteOfConflictResolve(req)
	if err != nil {
		return err
	}

	w = a.acc(reset.NamespaceID, reset.WorkflowID)

	if resetRS != nil && resetRS.part != partMutation {
		// Only the reset part exists (validated above): it replaces the
		// snapshot content under the owner's envelope, like Set does.
		resetRS.owner.tailSeqno = seqno
		replaceSnapshot(resetRS.owner.snapshotPart(resetRS.part), reset)
		resetRS.buffered = nil
		w.recordCurrent(want, cw)
		return nil
	}

	pr := newPending(seqno, mutation.Mutation{ConflictResolve: req})
	if resetRS != nil {
		// The reset supersedes the run's pending update; tasks survive.
		old := resetRS.owner
		reset.Tasks = mergeTasks(old.m.Update.UpdateWorkflowMutation.Tasks, reset.Tasks)
		w.drop(old)
		w.adopt(pr, reset.RunID, partSnapshot, nil)
	} else {
		w.adopt(pr, reset.RunID, partSnapshot, want.forRun(reset.RunID))
	}
	if req.NewWorkflowSnapshot != nil {
		w.adopt(pr, req.NewWorkflowSnapshot.RunID, partNewSnapshot, want.forRun(req.NewWorkflowSnapshot.RunID))
	}
	if cur := req.CurrentWorkflowMutation; cur != nil {
		if curRS != nil {
			// The prior update merges under the conflict-resolve's envelope:
			// its collections head the merge, the resolve's mutation the tail.
			old := curRS.owner
			dst := &old.m.Update.UpdateWorkflowMutation
			curRS.foldBuffered(cur, dst)
			mergeMutation(dst, cur)
			req.CurrentWorkflowMutation = dst
			w.drop(old)
			batches := curRS.buffered // adopt resets the run's batch list
			w.adopt(pr, cur.RunID, partMutation, nil)
			curRS.buffered = batches
		} else {
			w.adopt(pr, cur.RunID, partMutation, want.forRun(cur.RunID))
			w.runs[cur.RunID].foldBuffered(cur, cur)
		}
	}
	w.pending = append(w.pending, pr)

	w.recordCurrent(want, cw)
	return nil
}

func (a *Accumulator) addDelete(seqno wal.Seqno, req *p.DeleteWorkflowExecutionRequest) error {
	w := a.peek(req.NamespaceID, req.WorkflowID)

	rs := w.heldRun(req.RunID)
	if rs != nil && rs.tombstoned {
		// Deleting an absent row succeeds sequentially too: idempotent no-op.
		return nil
	}
	if rs != nil && len(rs.owner.runs) > 1 {
		return fmt.Errorf("%w: delete of run %s, whose pending request also carries other runs", ErrRefused, req.RunID)
	}

	w = a.acc(req.NamespaceID, req.WorkflowID)
	pr := newPending(seqno, mutation.Mutation{Delete: req})
	if rs != nil {
		// The tombstone collapses the run's pending state; its tasks must
		// survive (I7), and the Delete has no slot to carry them in.
		old := rs.owner
		pr.orphanedTasks = old.slotTasks(rs.part)
		w.drop(old)
		rs.owner = nil
		rs.buffered = nil
		rs.tombstoned = true
	} else {
		w.runs[req.RunID] = &runState{tombstoned: true}
	}
	pr.runs = append(pr.runs, req.RunID)
	w.pending = append(w.pending, pr)
	return nil
}

func (a *Accumulator) addDeleteCurrent(seqno wal.Seqno, req *p.DeleteCurrentWorkflowExecutionRequest) {
	// Not a run tombstone: only the current-execution row goes, so the run's
	// window state, if any, stays live.
	w := a.acc(req.NamespaceID, req.WorkflowID)

	// No assertion is recorded: the store's delete-current removes the row only
	// if the row names this run, so there is nothing the window stands on, and
	// synthesising current==run would turn the legal no-op into a false
	// invariant violation. What the delete taints is any later assertion on the
	// row, which currentTaintedRefusal refuses.
	w.cur.tainted = true

	// Upsert-versus-delete, resolved here as for every other key: the guard
	// asks about the row at the delete's position in the stream, which where
	// the window has already written it is the window's own write. Leaving both
	// for apply hands the transaction an upsert and a delete of one row, and
	// the plugin runs every delete first.
	if w.cur.write != nil {
		if w.cur.write.RunID != req.RunID {
			// The row names the window's own write, not this run: sequentially
			// a no-op, so the net effect is the write and nothing is emitted.
			return
		}
		// The delete removes what this window wrote. The net effect is removal
		// whatever the row named before the window, so the write is dropped
		// and the emitted delete carries no guard ([WorkflowRecord.CurrentRemoved]).
		w.cur.remove()
	}

	w.pending = append(w.pending, newPending(seqno, mutation.Mutation{DeleteCurrent: req}))
}

// mutationPart returns the merged mutation that carries a run held as a delta,
// the other half of the question [pendingReq.snapshotPart] answers. Compaction,
// the overlay and the condition authority all ask it, so a request shape added
// beside Update and ConflictResolve is one edit here.
func (pr *pendingReq) mutationPart() *p.InternalWorkflowMutation {
	if pr.m.Update != nil {
		return &pr.m.Update.UpdateWorkflowMutation
	}
	return pr.m.ConflictResolve.CurrentWorkflowMutation
}

// snapshotPart returns the snapshot slot that carries a run's row state.
func (pr *pendingReq) snapshotPart(part partKind) *p.InternalWorkflowSnapshot {
	switch {
	case pr.m.Create != nil:
		return &pr.m.Create.NewWorkflowSnapshot
	case pr.m.Set != nil:
		return &pr.m.Set.SetWorkflowSnapshot
	case pr.m.ConflictResolve != nil && part == partSnapshot:
		return &pr.m.ConflictResolve.ResetWorkflowSnapshot
	case pr.m.ConflictResolve != nil:
		return pr.m.ConflictResolve.NewWorkflowSnapshot
	default:
		return pr.m.Update.NewWorkflowSnapshot
	}
}

// slotTasks returns the tasks of the slot that carried a run's row state, for
// orphaning when a tombstone collapses it.
func (pr *pendingReq) slotTasks(part partKind) map[tasks.Category][]p.InternalHistoryTask {
	slot := pr.m.TaskSlot(part)
	if slot == nil {
		return nil
	}
	return *slot
}
