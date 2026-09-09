package acceptance

// Both seams real at once, and no server above them: the log is wal/memwal and
// the cold store is cold/memcold — Temporal's own SQL persistence over a SQLite
// database in this process — with one cycle.Manager between them and a
// generated stream driven through it the way acceptance_fold_test drives the
// fold. Every run here is a shard whose writes are acked into a real log,
// folded, and landed in a real database as one transaction per window.
//
// What that buys over the runs against doubles is one thing, and it is the
// thing: a double records what a drain carried, so a suite over it can only
// ever say the layer handed the right batch down. Here the batch is executed —
// against the schema, the row layouts and the condition failures upstream
// wrote — so the claim becomes what the database holds afterwards.
//
// Three claims, and the second case is invariant I2 with both seams real: a
// shard whose epoch moves under a running cycle holds exactly the drains that
// committed before the loss, and the entries acked after it are still in the
// log for the next owner rather than half-applied under a stale epoch.

import (
	"cmp"
	"context"
	"maps"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"

	"github.com/aromanovich/waltz/baserow"
	"github.com/aromanovich/waltz/cold/memcold"
	"github.com/aromanovich/waltz/cycle"
	"github.com/aromanovich/waltz/fold"
	"github.com/aromanovich/waltz/internal/verify/drive"
	"github.com/aromanovich/waltz/internal/verify/mutgen"
	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/wal"
	"github.com/aromanovich/waltz/wal/memwal"
)

// seamsShard is the one shard both cases drive. One shard is one accumulator
// and one apply transaction, so a second would add a second cycle and judge
// nothing new here.
const seamsShard = 1

// seamsMutations is how long a run is. It is a stream length rather than a
// budget: it has to cross enough windows for the trim cadence (16 drains at
// [cycle.Defaults]) to fire while the run is still going, since a trim that
// only ever happens at the end proves nothing about a log being kept short.
const seamsMutations = 6000

// seamsHotSet caps the workflow key space, for the reason the fold acceptance
// caps it: pick() reuses uniformly over the whole pool, so an uncapped pool
// grows past the window and two touches of one workflow almost never land in
// the same drain. A hot set well under the window is a shard that turns over
// faster than it drains, which is the shape the layer exists for.
const seamsHotSet = 32

// seamsSample is how often the run stops to ask the log and the cold store
// about each other. Small enough to land inside every window, since the
// invariant it checks is about the interval between a drain and the trim that
// follows it.
const seamsSample = 64

// seamsBeforeLoss is how far the epoch case runs before the shard changes
// hands. Several windows, so that what the database must hold afterwards is a
// stack of committed drains rather than one, and so the loss lands in the
// middle of a run instead of at its first window.
const seamsBeforeLoss = 2000

// seamsPolicy is the shipped configuration taken whole, and the window in it —
// 256 mutations or 256 KiB, whichever trips first — is the whole reason this
// test means anything. At a window of one, every drain carries one request: nothing is folded, no
// assertion is ever settled against a row an earlier request of the same
// transaction wrote, no workflow's current row is written once for the several
// mutations that touched it, and the deletes never precede inserts they would
// otherwise take away. Such a run is green with the fold path deleted. At 256,
// with 32 workflows hot, every drain is a merge of a hot set that turned over
// several times inside it.
func seamsPolicy() cycle.Config { return cycle.Defaults() }

// TestBothSeamsRealNoServer drives one stream through the layer and asks the
// database what is left.
func TestBothSeamsRealNoServer(t *testing.T) {
	s := newSeams(t, 20260906)

	require.NoError(t, s.drive(t, seamsMutations))
	require.Equal(t, seamsMutations, s.acked, "every mutation of the stream was acked")

	// Read before the shutdown drain: [cycle.Manager.Close] takes the cycles
	// out of the registry, and their counters go with them.
	running := s.mgr.Totals()
	s.mgr.Close(s.ctx)

	require.Greater(t, s.ledger.committed, seamsMutations/seamsPolicy().Mutations,
		"the run drained fewer times than its windows: the stream never reached the cold store")
	require.Greater(t, s.ledger.collapse(), 1.5,
		"the drains carried one request each; nothing was folded and the window is doing no work")
	t.Logf("%d mutations, %d committed drains, %.2f mutations per merged request, %d runs and %d workflows in the database",
		s.acked, s.ledger.committed, s.ledger.collapse(), len(s.ledger.runs), len(s.ledger.current))

	// The claim the doubles cannot make: every row the drains carried, read
	// back out of the database through the store's own reads.
	s.holds(t, s.ledger)

	// The watermark is the applied position and not a number beside it. Every
	// entry was acked and the shutdown drain applied the last window, so the
	// two ends meet at the stream's length.
	seqno, ok, err := s.store.Watermark(s.ctx, seamsShard)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, s.ledger.watermark, seqno, "the store's watermark is the last drain's own")
	require.EqualValues(t, seamsMutations, seqno, "the watermark is the last acked seqno")

	// And the log was trimmed behind that position: its lower end moved while
	// the run was going — the sampling in [seams.drive] is what says it never
	// moved past the watermark — and its upper end is still the last entry
	// acked, so what was trimmed came off the bottom rather than out of the
	// middle.
	require.NotZero(t, running.TrimsCommitted, "no trim reached the log while the run was going")
	require.Empty(t, s.trims.violation())
	require.NotZero(t, s.trims.trims.Load(), "the guard saw no trim, so it judged nothing")
	first, last := s.logEnds(t)
	require.Greater(t, first, wal.FirstSeqno, "nothing was trimmed: the log still holds the whole run")
	require.EqualValues(t, seamsMutations, last, "the log's upper end is the last entry acked")
	t.Logf("log: entries %d..%d of %d survive, %d trims committed",
		first, last, seamsMutations, running.TrimsCommitted)
}

// TestAShardThatLosesItsEpochMidRun is I2 with both seams real. A second owner
// takes the shard in the database — the range id moves, which is all an
// acquire is from underneath — while this node's cycle keeps acking into a log
// nobody fenced away from it. So the loss is discovered where it has to be
// discovered, inside the drain's own transaction, with a window of acked
// mutations riding on it.
//
// What must be true afterwards is the whole of the invariant: the database
// holds every drain that committed before the loss, nothing of the drain that
// found it, and the log still holds every entry those refused drains carried.
// Nothing acked is lost and nothing unapplied is invented.
func TestAShardThatLosesItsEpochMidRun(t *testing.T) {
	s := newSeams(t, 20260907)

	require.NoError(t, s.drive(t, seamsBeforeLoss))
	require.NotZero(t, s.ledger.committed, "the run must commit drains before the loss, or it judges nothing")
	before := s.ledger.snapshot()

	// The new owner. The log is left alone deliberately: fencing it would stop
	// the appends and the cycle would halt without a drain ever reaching the
	// database, which is the other half of fencing and not this one.
	s.anotherNodeTakesTheShard(t)

	err := s.drive(t, seamsMutations)
	require.Error(t, err, "the layer kept accepting writes for a shard it no longer owns")
	require.ErrorAs(t, err, new(*p.ShardOwnershipLostError))
	require.Equal(t, cycle.StateHaltedLost, s.mgr.Shard(seamsShard).State())
	require.NotZero(t, s.ledger.refusedDrains, "no drain ever reached the stale epoch")

	// Exactly the drains that committed, and nothing after them: every row the
	// committed drains left is there at the version they left it, and every run
	// the refused drains would have written — and no committed one did — is not
	// in the database at all.
	s.holds(t, before)
	orphans := s.ledger.refusedOnly()
	require.NotEmpty(t, orphans, "the refused drains carried nothing the committed ones had not already written")
	for _, key := range orphans {
		row, err := s.rows.Run(s.ctx, seamsShard, key.namespaceID, key.workflowID, key.runID)
		require.NoError(t, err)
		require.Nil(t, row, "run %s was written by a drain that lost the shard", key.runID)
	}
	t.Logf("%d committed drains, %d refused, %d runs the refused drains would have written",
		before.committed, s.ledger.refusedDrains, len(orphans))

	// The other half of the invariant: what those drains carried was acked, so
	// it is still in the log, above the watermark, for whoever takes the shard
	// next.
	totals := s.mgr.Totals()
	require.Equal(t, before.watermark, totals.Applied, "the halted cycle applied something after the loss")
	require.Greater(t, totals.Acked, totals.Applied, "nothing was acked after the last committed drain")
	require.Empty(t, s.trims.violation())
	first, last := s.logEnds(t)
	require.LessOrEqual(t, first, totals.Applied+1, "entries the cold store does not hold were trimmed away")
	require.Equal(t, totals.Acked, last, "the log lost an entry it had acked")
}

// ---------------------------------------------------------------------------
// The composition
// ---------------------------------------------------------------------------

// seams is the layer with a real log under one side of it and a real database
// under the other: what a server composes, minus the server.
type seams struct {
	ctx   context.Context
	store *memcold.Store
	log   *memwal.Backend
	// trims judges every Trim the run makes against the watermark the store
	// committed, which is the one caller obligation wal.Log cannot check.
	trims  *trimGuard
	mgr    *cycle.Manager
	rows   *baserow.Rows
	stream *drive.Stream
	ledger *ledger

	epoch wal.Epoch
	acked int
}

func newSeams(t *testing.T, seed int64) *seams {
	t.Helper()
	ctx := context.Background()

	store, release, err := memcold.New("seams")
	require.NoError(t, err)
	t.Cleanup(release)

	rows, err := baserow.Of(store)
	require.NoError(t, err, "the cold store owes the versioned current-row read")

	cfg := mutgen.Default()
	cfg.Seed = seed
	cfg.ShardID = seamsShard
	cfg.Workflows = seamsHotSet
	stream, err := drive.NewStream(cfg)
	require.NoError(t, err)

	s := &seams{
		ctx:    ctx,
		store:  store,
		log:    memwal.New(),
		rows:   rows,
		stream: stream,
		ledger: newLedger(store),
	}

	s.trims = &trimGuard{Log: s.log, mark: store}
	s.mgr, err = cycle.NewManager(cycle.Deps{
		Log:       s.trims,
		Writer:    s.ledger,
		Recoverer: store,
		Registry:  tasks.NewDefaultTaskCategoryRegistry(),
	}, cycle.Fixed(seamsPolicy()))
	require.NoError(t, err)
	t.Cleanup(func() { s.mgr.Close(ctx) })

	s.takeShard(t)
	return s
}

// takeShard takes the shard the way a history node arriving does: the range id
// moves in the database, and the cycle installed for it is fenced onto the log
// at that same number (I11).
func (s *seams) takeShard(t *testing.T) {
	t.Helper()
	s.epoch = wal.Epoch(s.bumpRange(t))
	require.NoError(t, s.mgr.ShardAcquired(s.ctx, seamsShard, s.epoch))
}

// anotherNodeTakesTheShard moves the range id and tells this node nothing,
// which is what losing a shard is: an acquire is observable to the node that
// makes it and to nobody else, so this cycle goes on acking under an epoch that
// is already stale and finds out inside a drain.
func (s *seams) anotherNodeTakesTheShard(t *testing.T) {
	t.Helper()
	require.Greater(t, s.bumpRange(t), int64(s.epoch),
		"the shard has to change hands upwards, or nothing about this node's epoch is stale")
}

func (s *seams) bumpRange(t *testing.T) int64 {
	t.Helper()
	rangeID, err := drive.TakeShardBumpingRangeID(s.ctx, s.store.ShardStore(), seamsShard)
	require.NoError(t, err)
	return rangeID
}

// drive writes the next n mutations of the stream through the layer, stopping
// at the first one it refuses and returning that refusal. The refusal is the
// layer's own error unwrapped, since it is what the history service would
// type-switch on.
//
// The trim invariant is sampled as it goes rather than asserted at the end, and
// the difference is not stylistic: a trim that runs ahead of the watermark
// deletes entries the cold store does not hold yet, and by the end of a run
// that drained everything those entries are applied and the damage is invisible.
// A crash is what would have found it, so the assertion has to be made while
// the run is still going.
func (s *seams) drive(t *testing.T, n int) error {
	t.Helper()
	return s.stream.Drive(n, func(d drive.Delivery) error {
		if err := s.mgr.Write(s.ctx, d.Mutation, s.epoch, s.rows); err != nil {
			return err
		}
		s.acked++
		if s.acked%seamsSample == 0 {
			s.trimStaysBehind(t)
		}
		return nil
	})
}

// trimStaysBehind is the log's half of "nothing acked is lost": every entry the
// cold store has not applied is still in the log. The two reads are in this
// order deliberately — the log's lower end first, the watermark second —
// because the watermark only rises, so a drain committing between them can only
// make the comparison stricter than the moment it is about.
func (s *seams) trimStaysBehind(t *testing.T) {
	t.Helper()
	first := s.logFirst(t)
	applied, _, err := s.store.Watermark(s.ctx, seamsShard)
	require.NoError(t, err)
	require.LessOrEqual(t, first, applied+1,
		"the log was trimmed to %d, past the %d the cold store holds", first, applied)
}

// logFirst is the seqno of the lowest entry the log still holds. The read asks
// from [wal.FirstSeqno], so what comes back is where the trim left the log's
// lower end.
func (s *seams) logFirst(t *testing.T) wal.Seqno {
	t.Helper()
	entries, err := s.log.ReadFrom(s.ctx, seamsShard, wal.FirstSeqno, 1)
	require.NoError(t, err)
	require.NotEmpty(t, entries, "the log is empty, and the mutation just acked into it is gone")
	return entries[0].Seqno
}

// holds asserts the database against a ledger: every run row at the version the
// drains left it or absent where they deleted it, and every workflow's current
// row naming the run they left it naming.
func (s *seams) holds(t *testing.T, led *ledger) {
	t.Helper()

	for _, key := range slices.SortedFunc(maps.Keys(led.runs), byRun) {
		row, err := s.rows.Run(s.ctx, seamsShard, key.namespaceID, key.workflowID, key.runID)
		require.NoError(t, err)
		version := led.runs[key]
		if version == deletedRow {
			require.Nil(t, row, "run %s was deleted by a drain and is still in the database", key.runID)
			continue
		}
		require.NotNil(t, row, "run %s was written by a drain and is not in the database", key.runID)
		require.Equal(t, version, row.DBRecordVersion, "run %s is at the wrong version", key.runID)
	}

	for _, key := range slices.SortedFunc(maps.Keys(led.current), byWorkflow) {
		row, _, err := s.rows.Current(s.ctx, seamsShard, key.namespaceID, key.workflowID)
		require.NoError(t, err)
		run := led.current[key]
		if run == noCurrentRow {
			require.Nil(t, row, "workflow %s still has a current row a drain removed", key.workflowID)
			continue
		}
		require.NotNil(t, row, "workflow %s has no current row", key.workflowID)
		require.Equal(t, run, row.RunID, "workflow %s names the wrong run", key.workflowID)
	}
}

// logEnds is the seqno of the first entry the log still holds and of its last,
// asserting on the way that what lies between them is unbroken: a trimmed log
// is one whose lower end moved, not one with a hole in it.
func (s *seams) logEnds(t *testing.T) (first, last wal.Seqno) {
	t.Helper()
	for entry, err := range wal.Entries(s.ctx, s.log, seamsShard, wal.FirstSeqno, 1000) {
		require.NoError(t, err)
		if first == 0 {
			first, last = entry.Seqno, entry.Seqno
			continue
		}
		require.Equal(t, last+1, entry.Seqno, "the log has a hole below seqno %d", entry.Seqno)
		last = entry.Seqno
	}
	require.NotZero(t, first, "the log is empty")
	return first, last
}

// ---------------------------------------------------------------------------
// The ledger: what the drains carried
// ---------------------------------------------------------------------------

// deletedRow and noCurrentRow are the two absences a drain can leave behind,
// which are answers and not missing entries: a row a drain deleted must be gone
// from the database, exactly as a row it wrote must be there.
const (
	deletedRow   int64 = -1
	noCurrentRow       = ""
)

type runKey struct{ namespaceID, workflowID, runID string }

type wfKey struct{ namespaceID, workflowID string }

// ledger is the cold store with a record kept beside it: the applier the cycle
// drains into, which passes every batch to the database and then reads out of
// the batch alone what the database must hold once it has committed.
//
// The record is built from the batch and never from a readback, which is the
// point — an expectation read out of the store agrees with the store by
// construction and judges nothing. A drain that fails is recorded apart, so
// "what committed" and "what did not" are two answers rather than one.
type ledger struct {
	inner *memcold.Store

	committed, refusedDrains int
	foldedIn, emitted        int
	watermark                wal.Seqno

	runs    map[runKey]int64
	current map[wfKey]string
	refused map[runKey]int64
}

func newLedger(store *memcold.Store) *ledger {
	return &ledger{
		inner:   store,
		runs:    map[runKey]int64{},
		current: map[wfKey]string{},
		refused: map[runKey]int64{},
	}
}

// Apply drains into the database and records what landed. It runs on the
// cycle's own goroutine and nothing here locks, so a read of the record is
// ordered against the writes the reader's own drives provoked and against
// nothing else: the epoch case takes [ledger.snapshot] while the cycle still
// holds the shard, and an age tick drains with no writer asking, so that read
// can be in the maps while this is writing them.
func (l *ledger) Apply(ctx context.Context, shard wal.ShardID, epoch wal.Epoch, batch fold.Batch) error {
	err := l.inner.Apply(ctx, shard, epoch, batch)
	if err != nil {
		l.refusedDrains++
		for e := range batch.Each() {
			for run, version := range runsWritten(e) {
				l.refused[runKey{e.NamespaceID, e.WorkflowID, run}] = version
			}
		}
		return err
	}

	l.committed++
	l.watermark = batch.Watermark()
	l.foldedIn += batch.Stats().MutationsIn
	l.emitted += batch.Len()
	for e := range batch.Each() {
		for run, version := range runsWritten(e) {
			l.runs[runKey{e.NamespaceID, e.WorkflowID, run}] = version
		}
		l.currentRow(e)
	}
	return nil
}

// currentRow follows one request's effect on its workflow's current-execution
// row, in the order apply drives them: the record's own answer where the window
// settled the row, and then the guarded delete where it did not.
//
// The guard is why this models the store instead of copying an answer out of
// the batch. A DeleteCurrent of a row the window itself wrote removes it
// whatever it names, and the record says so; one of a row the window did not
// write removes it only if it names that run, which is a question about what
// the store already holds. The ledger is what holds it — it has watched every
// drain since the database was empty — and a workflow it has no answer for has
// no row, since that is where the database started.
func (l *ledger) currentRow(e *fold.Emitted) {
	key := wfKey{e.NamespaceID, e.WorkflowID}
	if e.FirstOfWorkflow() {
		switch wf := e.Workflow(); {
		case wf.CurrentWrite != nil:
			l.current[key] = wf.CurrentWrite.RunID
		case wf.CurrentRemoved:
			l.current[key] = noCurrentRow
		}
	}
	if e.Request.Kind() == mutation.KindDeleteCurrent &&
		l.current[key] == e.Request.DeleteCurrent.RunID {
		l.current[key] = noCurrentRow
	}
}

// snapshot is the record as it stands, for a caller that goes on driving: the
// maps are the ledger's own and a later drain writes to them.
func (l *ledger) snapshot() *ledger {
	copied := *l
	copied.runs = maps.Clone(l.runs)
	copied.current = maps.Clone(l.current)
	copied.refused = maps.Clone(l.refused)
	return &copied
}

// collapse is what the folding delivered over the whole run: mutations in over
// merged requests out. It is here as a guard rather than as a measurement — a
// run whose windows collapse nothing is one where the seams were composed and
// the mechanism between them was never asked for anything.
func (l *ledger) collapse() float64 {
	if l.emitted == 0 {
		return 0
	}
	return float64(l.foldedIn) / float64(l.emitted)
}

// refusedOnly is the runs a refused drain would have written and no committed
// drain ever did: the rows whose presence in the database would be a drain
// applied under an epoch it had lost.
func (l *ledger) refusedOnly() []runKey {
	var out []runKey
	for key := range l.refused {
		if _, committed := l.runs[key]; !committed {
			out = append(out, key)
		}
	}
	slices.SortFunc(out, byRun)
	return out
}

// The two orders. A failure has to name the same row on every run of a seed,
// and map order would make which row a run reports its own.
func byRun(a, b runKey) int {
	return cmp.Or(cmp.Compare(a.workflowID, b.workflowID), cmp.Compare(a.runID, b.runID))
}

func byWorkflow(a, b wfKey) int { return cmp.Compare(a.workflowID, b.workflowID) }

// runsWritten is every run row one merged request writes and the
// db_record_version it leaves, or [deletedRow] for the row it removes. It walks
// a request's snapshots and mutations the way the store's own write path does,
// off the run id in the execution state rather than the request's own field,
// because that is the one the store keys the row by.
func runsWritten(e *fold.Emitted) map[string]int64 {
	out := map[string]int64{}
	snapshot := func(s *p.InternalWorkflowSnapshot) { out[s.ExecutionState.RunId] = s.DBRecordVersion }
	mutated := func(m *p.InternalWorkflowMutation) { out[m.ExecutionState.RunId] = m.DBRecordVersion }

	switch req := e.Request; req.Kind() {
	case mutation.KindCreate:
		snapshot(&req.Create.NewWorkflowSnapshot)
	case mutation.KindUpdate:
		mutated(&req.Update.UpdateWorkflowMutation)
		if added := req.Update.NewWorkflowSnapshot; added != nil {
			snapshot(added)
		}
	case mutation.KindConflictResolve:
		snapshot(&req.ConflictResolve.ResetWorkflowSnapshot)
		if cur := req.ConflictResolve.CurrentWorkflowMutation; cur != nil {
			mutated(cur)
		}
		if added := req.ConflictResolve.NewWorkflowSnapshot; added != nil {
			snapshot(added)
		}
	case mutation.KindSet:
		snapshot(&req.Set.SetWorkflowSnapshot)
	case mutation.KindDelete:
		out[req.Delete.RunID] = deletedRow
	}
	return out
}
