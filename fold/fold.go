// Package fold compacts the tail: it accumulates a window of mutations and
// emits, per dirty workflow, one merged request plus the assertions that
// request stands on.
//
// Assertions come from the head of the window and data from the tail, because
// only the head was evaluated against the cold store. A snapshot-bearing
// request resets the run's accumulator (I8); tasks are queue entries, not
// state, so they concatenate across that reset.
//
// Event history is not folded (see history.go): a window keeps each record's
// batches in WAL order, and the store dedupes repeated nodes on its own key.
//
// This package may not name wal.Log or wal.Entry: trim and pacing are the
// apply cycle's policy, and reaching the log is how they would leak in.
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

// ErrInvalidStream reports a mutation that no acked stream could put after the
// window's mutations, such as a create of a run the window holds live or a
// second continue-as-new out of one run. The log is corrupt. A double
// continue-as-new is the one case [Accumulator.Check] cannot pre-empt, since no
// assertion stands in its way.
var ErrInvalidStream = errors.New("fold: mutation cannot follow the window's mutations in any acked stream")

// ErrRefused reports a valid window this accumulator cannot express as merged
// requests. The accumulator is left exactly as it was, so the caller recovers
// by draining and starting the refused mutation on a fresh window.
var ErrRefused = errors.New("fold: window not foldable into merged requests")

// ErrForeignPageToken reports a task-page token that [Accumulator.TaskPage] did
// not mint. Paging on the base alone would drop the window from the rest of the
// pagination, and the range completed at its end would delete acked task rows.
var ErrForeignPageToken = errors.New("fold: task-page token was not written by this layer")

// ErrBasePageTooLarge reports a cold store that answered with more rows than
// asked, against [BasePage]'s fourth requirement (also held by
// [HistoryBasePage]). The page cut relies on the count: an extra row is one the
// cursor skips unemitted, and on a task page the range completion deletes it.
var ErrBasePageTooLarge = errors.New("fold: the cold store answered with more rows than the page asked for")

// ErrBaseRowOutsideRange reports a row outside the requested range, against
// [BasePage]'s first requirement. Nothing else filters such a row, and the
// queue reader panics on one with no recover.
var ErrBaseRowOutsideRange = errors.New("fold: the cold store answered with a row outside the range asked for")

// ErrBasePageNotAscending reports a page whose rows do not ascend, or that
// starts at or below a key this pagination already passed ([BasePage]'s second
// requirement). The reader silently skips a row that does not ascend, so that
// task would never be read again. A history page is held only to ascending
// within the page, in the request's direction ([HistoryBasePage]).
var ErrBasePageNotAscending = errors.New("fold: the cold store answered a page that does not ascend")

// ErrBasePageEmptyBesideAToken reports a store answering no rows together with
// a token, against [BasePage]'s third requirement that no rows means the range
// is exhausted. Read as the end, the remaining rows would be skipped, and on a
// task page the range completion would delete them.
var ErrBasePageEmptyBesideAToken = errors.New("fold: the cold store answered no rows beside a token saying it holds more")

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
// current-execution row. The first mutation touching the row fixes it, and
// apply asserts it once per workflow.
type CurrentAssertion struct {
	Kind             CurrentKind
	RunID            string
	LastWriteVersion int64 // CurrentEqualsWithVersion only
}

// CurrentWrite is the current-execution row as the sequential path would have
// left it, rendered from the window's last mutation that writes the row. Apply
// registers it in place of the write the store derives from the merged
// request's kind.
type CurrentWrite struct {
	RunID            string
	StateBlob        *commonpb.DataBlob
	LastWriteVersion int64
	State            enumsspb.WorkflowExecutionState
}

// BufferedBatch is one batch of buffered events and its run. Apply keys the row
// by run, and a snapshot-merged window has no mutation to read the run from.
type BufferedBatch struct {
	RunID string
	Blob  *commonpb.DataBlob
}

// Emitted is one merged request: the unit apply drives through the store.
// Fields describe the request; methods expose what only [Accumulator.Drain]
// records about it.
type Emitted struct {
	NamespaceID string
	WorkflowID  string
	// HeadSeqno is the mutation that opened this request and TailSeqno the last
	// folded into it. A snapshot over a pending update carries that update's
	// tasks and head assertion, so a mutation below HeadSeqno can be included.
	// Drain returns requests in TailSeqno order and apply must keep it, since a
	// request writes its tail state.
	HeadSeqno wal.Seqno
	TailSeqno wal.Seqno
	// Request holds exactly one of the six request kinds a window can emit.
	Request mutation.Mutation
	// BufferedBatches are the window's buffered-event batches in arrival order,
	// one row each. They do not merge; the request's NewBufferedEvents is nil.
	BufferedBatches []BufferedBatch

	runs          map[string]RunAssertion
	orphanedTasks map[tasks.Category][]p.InternalHistoryTask
	workflow      *WorkflowRecord
	first         bool
}

// RunAssertions is the head-of-window state of each run this request touches,
// by run id. The applier substitutes it for the assertions the store would
// derive from the written versions. The map is the batch's own; do not write
// to it.
//
// A run has no entry if it asserted nothing (a Delete-headed window) or if an
// earlier request of the batch already carries its head: the head is placed
// once (see [Accumulator.Drain]).
func (e *Emitted) RunAssertions() map[string]RunAssertion { return e.runs }

// OrphanedTasks are tasks of mutations a tombstone collapsed; only a tombstone
// carries them, as a Delete has no task slot. Apply must write them (I8).
func (e *Emitted) OrphanedTasks() map[tasks.Category][]p.InternalHistoryTask {
	return e.orphanedTasks
}

// Workflow is the request's workflow record, never nil. Every request of one
// workflow returns the same pointer.
func (e *Emitted) Workflow() *WorkflowRecord { return e.workflow }

// FirstOfWorkflow reports that this is the batch's first request naming its
// workflow record, where the record's assertions are registered. The store
// reports the first failing assertion in registration order, so hoisting them
// would change which failure a batch reports.
func (e *Emitted) FirstOfWorkflow() bool { return e.first }

// WorkflowRecord is the window's net effect on one workflow's current-execution
// row. It is shared by all of the workflow's requests (a tombstone and the run
// created behind it are two) through [Emitted.Workflow], and apply registers it
// at the request [Emitted.FirstOfWorkflow] marks.
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
	// CurrentRemoved reports that a DeleteCurrent removed the row the window had
	// written. Apply must let the emitted DeleteCurrent remove the row whatever
	// it names (the store's guard asks about the pre-window row) and drop the
	// store's derived write. A write and a delete of this row never go together.
	CurrentRemoved bool
}

// Stats are the accumulator's counters; fold emits no metric itself.
type Stats struct {
	// MutationsIn counts mutations added since the last drain.
	MutationsIn int
	// DirtyWorkflows counts workflows with pending state.
	DirtyWorkflows int
}

// CollapseRatio is MutationsIn / DirtyWorkflows, or 0 for an empty window.
func (s Stats) CollapseRatio() float64 {
	if s.DirtyWorkflows == 0 {
		return 0
	}
	return float64(s.MutationsIn) / float64(s.DirtyWorkflows)
}

// Accumulator folds one shard's window. Not safe for concurrent use: the shard's
// apply loop owns it. Add either folds the mutation in whole or returns an error
// leaving the accumulator unchanged. Add takes ownership of the mutation and
// merges it in place; a caller that needs it afterwards must copy it first.
type Accumulator struct {
	shard       wal.ShardID
	lastSeqno   wal.Seqno
	mutationsIn int
	workflows   map[wfKey]*workflowAcc

	// The history-task half of the window (histtasks.go), kept per shard because
	// task rows name no run. addedTasks are AddHistoryTasks rows in arrival
	// order, unsorted; ranges are undrained range deletes by category id.
	addedTasks   map[tasks.Category][]p.InternalHistoryTask
	ranges       map[int32]*rangeAcc
	taskTail     wal.Seqno
	tasksDropped map[string]int

	// The event batches this window carries: see history.go.
	historyWrites []*p.InternalAppendHistoryNodesRequest
	historyTail   wal.Seqno
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

// runKey names one run of one workflow inside a drain, by record pointer
// (unique per workflow) rather than by ids.
type runKey struct {
	workflow *WorkflowRecord
	runID    string
}

type workflowAcc struct {
	runs    map[string]*runState
	cur     currentAcc
	pending []*pendingReq
}

// currentAcc is the window's state for one workflow's current-execution row.
// Readers derive from it only through [workflowAcc.currentView].
//
// write and removed are mutually exclusive: the two methods below are their
// only writers and each sets both. An upsert and a delete of one row would
// leave the winner to the plugin's statement order.
type currentAcc struct {
	// assertion is the head-of-window assertion on the row, nil when the window
	// does not hold it. Only [workflowAcc.assertsCurrent] may compare it to nil.
	assertion *CurrentAssertion
	// write is the window's last current-row write (last writer wins).
	write *CurrentWrite
	// tainted marks a DeleteCurrent with no assertion recorded; a later
	// assertion on the row cannot fold in (currentTaintedRefusal).
	tainted bool
	// removed: see [WorkflowRecord.CurrentRemoved].
	removed bool
}

// recordWrite takes the latest current-row write, cancelling an earlier removal.
func (c *currentAcc) recordWrite(cw *CurrentWrite) {
	c.write, c.removed = cw, false
}

// remove drops the window's own write and marks the net effect removal.
func (c *currentAcc) remove() {
	c.write, c.removed = nil, true
}

// recordCurrent records one mutation's current-row assertion (first toucher
// fixes it) and write (last wins). Handlers call it past their last refusal.
func (w *workflowAcc) recordCurrent(want asserted, cw *CurrentWrite) {
	if !w.assertsCurrent() {
		w.cur.assertion = want.current
	}
	if cw != nil {
		w.recordCurrentWrite(cw)
	}
}

// recordCurrentWrite records the last current-row write and drops any pending
// delete-current, since the write replaces the row and an upsert beside a
// delete would leave the winner to the plugin's statement order.
func (w *workflowAcc) recordCurrentWrite(cw *CurrentWrite) {
	w.cur.recordWrite(cw)
	w.pending = slices.DeleteFunc(w.pending, func(pr *pendingReq) bool { return pr.m.DeleteCurrent != nil })
}

// currentTaintedRefusal refuses a mutation asserting on the current row when an
// unasserted delete-current is in the window: an assertion recorded past it
// would be a mid-window claim posing as a head-of-window one.
//
// The "does it assert on the row" test lives here, not in handlers, because
// only a replayed stream (Add without Check) would expose a handler that
// dropped it.
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
// run's first mutation.
type runState struct {
	// assertion is nil when the run's head asserted nothing (Delete-headed).
	assertion *RunAssertion
	// owner is the pending request whose part carries this run's row state;
	// nil once tombstoned.
	owner *pendingReq
	part  partKind
	// buffered holds batches stripped from the run's mutations.
	buffered   []*commonpb.DataBlob
	tombstoned bool
}

// partKind names which slot of the owning request carries a run's row state.
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
	a.addHistory(seqno, m)

	a.lastSeqno = seqno
	a.mutationsIn++
	return nil
}

// Stats reports the window's counters so far.
func (a *Accumulator) Stats() Stats {
	return Stats{MutationsIn: a.mutationsIn, DirtyWorkflows: len(a.workflows)}
}

// Batch is one drain's output: requests, task work, event batches, and the
// seqno a transaction applying them may acknowledge.
//
// Only [Accumulator.Drain] builds one, which guarantees: requests in tail-seqno
// order, all of the shard [Batch.Shard] names, a watermark at or above every
// seqno carried, a workflow record on every request, and orphaned tasks only
// on tombstones. The zero batch carries nothing and is refused.
type Batch struct {
	shard     wal.ShardID
	requests  []Emitted
	tasks     TaskWork
	history   []*p.InternalAppendHistoryNodesRequest
	stats     Stats
	watermark wal.Seqno
}

// Empty reports no requests, task work or history; counters are ignored.
func (b Batch) Empty() bool {
	return len(b.requests) == 0 && b.tasks.Empty() && len(b.history) == 0
}

// Shard is the accumulator's shard, for the caller to check.
func (b Batch) Shard() wal.ShardID { return b.shard }

// Len is the number of merged requests: one per dirty workflow, or two where a
// run was tombstoned and the next created behind it.
func (b Batch) Len() int { return len(b.requests) }

// Watermark is the seqno the drain's transaction acks: the maximum of the
// requests', task work's and history's tails, which share no ordering.
func (b Batch) Watermark() wal.Seqno { return b.watermark }

// Stats describe the drained window, not the batch: an AddHistoryTasks with no
// rows folds an entry and drains an empty batch.
func (b Batch) Stats() Stats { return b.stats }

// Settles is the position this window acked entries up to, and whether it
// acked any. It is for a caller with an empty batch, which can still have
// folded entries (see emptydrain_test.go); those are settled off this or never.
//
// False means nothing was folded; its zero watermark taken as a position would
// mark every acked entry unsettled.
func (b Batch) Settles() (wal.Seqno, bool) {
	if b.stats.MutationsIn == 0 {
		return 0, false
	}
	return b.watermark, true
}

// Tasks is the shard-level history-task work ([TaskWork]).
func (b Batch) Tasks() TaskWork { return b.tasks }

// Each iterates the merged requests in tail-seqno order, the order assertions
// must be registered in.
func (b Batch) Each() iter.Seq[*Emitted] {
	return func(yield func(*Emitted) bool) {
		for i := range b.requests {
			if !yield(&b.requests[i]) {
				return
			}
		}
	}
}

// Drain emits the window as one [Batch] and resets the accumulator. The seqno
// floor survives the drain; pending task deletion ranges ride the batch
// ([TaskWork]).
func (a *Accumulator) Drain() Batch {
	stats := a.Stats()

	// Sized by pending requests, not workflows: a tombstone-and-recreate adds one.
	pending := 0
	for _, w := range a.workflows {
		pending += len(w.pending)
	}
	out := make([]Emitted, 0, pending)
	for key, w := range a.workflows {
		rec := &WorkflowRecord{
			NamespaceID: key.namespaceID,
			WorkflowID:  key.workflowID,
		}
		if w.assertsCurrent() {
			cur := *w.cur.assertion
			rec.Current = &cur
		}
		// The shared derivation never yields both a write and a removal.
		switch view := w.currentView(); view.Shape {
		case CurrentWritten:
			cw := *view.write
			rec.CurrentWrite = &cw
		case CurrentGone:
			rec.CurrentRemoved = true
		}

		for _, pr := range w.pending {
			e := Emitted{
				NamespaceID:   key.namespaceID,
				WorkflowID:    key.workflowID,
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
	// After the sort, so "first" means first in apply's order.
	//
	// A run's head assertion also goes only on the first request naming the run.
	// Two requests for one run means a tombstone then a create; the delete
	// removes the row, so a head at v2 repeated on the create would fail and
	// halt the shard over a valid window.
	named := make(map[*WorkflowRecord]struct{}, len(a.workflows))
	placed := make(map[runKey]struct{}, len(out))
	for i := range out {
		rec := out[i].workflow
		_, seen := named[rec]
		out[i].first = !seen
		named[rec] = struct{}{}

		for run := range out[i].runs {
			key := runKey{rec, run}
			if _, seen := placed[key]; seen {
				delete(out[i].runs, run)
				continue
			}
			placed[key] = struct{}{}
		}
	}

	work := a.drainTasks()

	// The maximum of all three tails (out is sorted). The task tail counts even
	// for empty work: a rowless AddHistoryTasks still folded its entry.
	var watermark wal.Seqno
	if len(out) > 0 {
		watermark = out[len(out)-1].TailSeqno
	}
	watermark = max(watermark, work.TailSeqno, a.historyTail)

	history := a.historyWrites
	a.historyWrites = nil
	a.historyTail = 0
	a.workflows = make(map[wfKey]*workflowAcc)
	a.mutationsIn = 0
	return Batch{shard: a.shard, requests: out, tasks: work, history: history, stats: stats, watermark: watermark}
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
		w = &workflowAcc{runs: make(map[string]*runState)}
		a.workflows[key] = w
	}
	return w
}

// heldRun is the window's state for run, nil when the window does not hold it.
//
// This and [workflowAcc.assertsCurrent] are the condition authority's partition:
// an assertion on a row the window does not hold is recorded, not answered.
// Fold and the check both ask here so they cannot drift.
func (w *workflowAcc) heldRun(run string) *runState {
	if w == nil {
		return nil
	}
	return w.runs[run]
}

// assertsCurrent reports whether the window holds the current-execution row.
func (w *workflowAcc) assertsCurrent() bool { return w != nil && w.cur.assertion != nil }

// currentTainted reports an unasserted delete-current on the row. Both halves
// of the authority refuse on it, so neither may spell the test itself.
func (w *workflowAcc) currentTainted() bool {
	return w != nil && !w.assertsCurrent() && w.cur.tainted
}

// adopt registers a pending request as the owner of a run. A fresh run gets
// fallback as its head; an existing one keeps its head, so a Create behind a
// tombstone stands on the window's head (nothing, if a Delete headed it) rather
// than on absence. [Accumulator.Drain] places that head on the first request.
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
// ClearBufferedEvents drops earlier batches and marks slot to clear the store's
// pre-window rows; the mutation's own batch moves to the run's list. slot is
// nil when the run's state is a snapshot, whose write replaces the rows.
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
		// Already a snapshot: replace its content under the owner's envelope.
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
			// The other parts would have no envelope to ride.
			return fmt.Errorf("%w: conflict-resolve with %d parts on a run whose window state is a snapshot", ErrRefused, parts)
		}
	}

	if err := currentTaintedRefusal(w, want); err != nil {
		return err
	}

	cw := currentWriteOfConflictResolve(req)

	w = a.acc(reset.NamespaceID, reset.WorkflowID)

	if resetRS != nil && resetRS.part != partMutation {
		// Only the reset part exists (validated above); it replaces the
		// snapshot like Set does.
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
			// The prior update heads the merge and the resolve's mutation
			// tails it, under the conflict-resolve's envelope.
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
		// survive (I8) and the Delete has no slot for them.
		old := rs.owner
		pr.orphanedTasks = old.slotTasks(rs.part)
		w.drop(old)
		// Defensive: adopt resets both, and the drain never reaches a dropped
		// owner, but the state should not point at a dropped request.
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

	// No assertion: the delete is guarded (it removes the row only if it names
	// this run), so asserting current==run would turn a legal no-op into a false
	// violation. Instead it taints later assertions (currentTaintedRefusal).
	w.cur.tainted = true

	// Resolve upsert-versus-delete here, as for every key: if the window wrote
	// the row, the guard is asked about that write, not left to the store's
	// statement order.
	if w.cur.write != nil {
		if w.cur.write.RunID != req.RunID {
			// Sequentially a no-op: the write stands, nothing is emitted.
			return
		}
		// Removes the window's own write: the net effect is removal whatever
		// the pre-window row named ([WorkflowRecord.CurrentRemoved]).
		w.cur.remove()
	}

	w.pending = append(w.pending, newPending(seqno, mutation.Mutation{DeleteCurrent: req}))
}

// mutationPart returns the merged mutation carrying a run held as a delta (see
// also [pendingReq.snapshotPart]). A new request shape is one edit here.
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
