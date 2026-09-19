package acceptance

// Recovery: one stream driven twice over the same seed, once through a single
// cycle and once through a chain of them, each abandoned the way a process that
// dies abandons one — the range id moves, no drain runs, and the window's
// mutations are in the log and nowhere else. The two databases must agree, row
// for row and blob for blob.
//
// The seams and oracle runs beside it never take an entry out of the log: each
// ends in a shutdown drain, which applies the last window from memory and asks
// the log for nothing. The handover runs do replay, a successor there inheriting
// the window a fenced predecessor was holding; and
// [TestAShardThatLosesItsEpochMidRun] stops one step short of what this run
// does: it proves the entries a refused drain carried are still in the log, not
// that anybody can turn them back into rows. This is that step, and it is the
// one the first rule is stated over — a write acked into the log and never
// applied is lost exactly when a successor cannot recover it.
//
// The comparison is against another database rather than against a model, which
// is what makes it total. The ledger knows what the drains carried, so a
// mutation no drain ever carried is invisible to it; the uninterrupted run's own
// rows are the only expectation that has an entry for something the crashed run
// dropped.

import (
	"context"
	"errors"
	"maps"
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	p "go.temporal.io/server/common/persistence"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/aromanovich/waltz/cold"
	"github.com/aromanovich/waltz/cycle"
	"github.com/aromanovich/waltz/fold"
	"github.com/aromanovich/waltz/wal"
)

// recoveryCrashes is how many times the shard changes hands mid-window. Several,
// because one crash exercises a replay of one tail while a chain of them makes
// each successor recover a tail its own predecessor had partly recovered.
const recoveryCrashes = 5

// TestARecoveredShardHoldsWhatAnUninterruptedOneDoes is the first rule as a run.
func TestARecoveredShardHoldsWhatAnUninterruptedOneDoes(t *testing.T) {
	const seed = 20260909

	control := newSeams(t, seed)
	require.NoError(t, control.drive(t, seamsMutations))
	control.mgr.Close(control.ctx)
	expected := control.ledger.snapshot()

	crashed := newSeams(t, seed)
	leg := seamsMutations / (recoveryCrashes + 1)
	for crash := range recoveryCrashes {
		require.NoError(t, crashed.drive(t, leg))
		totals := crashed.mgr.Totals()
		require.Greater(t, totals.Acked, totals.Applied,
			"crash %d lands on an empty tail, so its successor recovers nothing", crash+1)
		// A crash, in the two things the layer can see of one: the range id has
		// moved and the window this cycle is holding is never drained.
		crashed.takeShard(t)
	}
	require.NoError(t, crashed.drive(t, seamsMutations-crashed.acked))

	// Before the shutdown drain: [cycle.Manager.Close] takes the cycles out of
	// the registry and their counters go with them.
	recovered := crashed.mgr.Totals()
	crashed.mgr.Close(crashed.ctx)

	require.NotZero(t, recovered.Replayed,
		"nothing was ever replayed, so the crashes cost the run nothing and it judges nothing")
	require.Zero(t, recovered.Dropped,
		"a replayed entry was dropped, and no entry of this run was provisional")
	require.Empty(t, recovered.Halted, "a successor halted on the tail it inherited")

	seqno, ok, err := crashed.store.Watermark(crashed.ctx, seamsShard)
	require.NoError(t, err)
	require.True(t, ok)
	require.EqualValues(t, seamsMutations, seqno,
		"the watermark is short of the stream: entries were acked and never applied by anybody")

	// The ledger first, because it names the one row it is unhappy about, and
	// then the whole of both databases against each other.
	crashed.holds(t, expected)
	require.Empty(t, crashed.trims.violation())

	runs := union(expected.runs, crashed.ledger.runs, byRun)
	require.NotEmpty(t, runs, "neither run drained a single row, so there is nothing to compare")
	for _, key := range runs {
		want, got := control.runRow(t, key), crashed.runRow(t, key)
		if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
			t.Fatalf("run %s of workflow %s is not what the uninterrupted stream left (-uninterrupted +recovered):\n%s",
				key.runID, key.workflowID, diff)
		}
	}
	current := union(expected.current, crashed.ledger.current, byWorkflow)
	for _, key := range current {
		require.Equal(t, control.currentRowOf(t, key), crashed.currentRowOf(t, key),
			"workflow %s names a different run after recovery", key.workflowID)
	}
	t.Logf("%d entries replayed across %d crashes, %d runs and %d current rows identical, watermark %d",
		recovered.Replayed, recoveryCrashes, len(runs), len(current), seqno)
}

// TestACrashOnTopOfADrainNobodyCouldReadRecoversEitherWay is the crash the run
// above leaves out. There it falls between two writes, so every entry in the log
// is one whose fate the predecessor knew; here it falls on a shard whose last
// drain has no readable outcome, which is the one state where the entries above
// the watermark may already be rows.
//
// The successor is told nothing about any of it. It reads the watermark, floors
// there and replays what is above — so the same code has to skip entries a
// transaction it cannot see already applied, and apply the ones it did not, with
// the same evidence in both cases.
func TestACrashOnTopOfADrainNobodyCouldReadRecoversEitherWay(t *testing.T) {
	// Whether the transaction ran before the applier lost the ability to say so.
	// Committed, the successor must not apply those entries a second time; not
	// committed, it must find them in the log and apply them.
	const seed = 20260910

	// One uninterrupted owner, driven once: the two subtests differ in what
	// happens to the crashed arm and compare against the same database.
	control := newSeams(t, seed)
	require.NoError(t, control.drive(t, seamsMutations))
	control.mgr.Close(control.ctx)
	expected := control.ledger.snapshot()

	for name, committed := range map[string]bool{
		"the transaction had committed": true,
		"the transaction never ran":     false,
	} {
		t.Run(name, func(t *testing.T) {
			crashed := newSeams(t, seed)
			require.NoError(t, crashed.drive(t, seamsMutations/2))

			// One drain's outcome goes unreadable, and the write carrying it is
			// refused rather than acked: the layer cannot say what happened, so
			// neither may its caller.
			crashed.stage.arm(committed)
			// The stage fires at the next drain, and the window is what decides
			// when that is, so the writes go one at a time until one of them
			// carries it.
			var stalled error
			for range seamsPolicy().Mutations + 1 {
				if stalled = crashed.drive(t, 1); stalled != nil {
					break
				}
			}
			require.Error(t, stalled, "no write in a whole window's worth carried the staged drain")
			require.Equal(t, cycle.StateRunning, crashed.mgr.Shard(seamsShard).State(),
				"an unreadable drain halted the shard, where the tail is meant to stall and heal")

			// And the owner dies before it can ask again, which is what leaves the
			// question to a cycle that never saw the drain.
			crashed.takeShard(t)

			// What the successor inherits, read before it has drained anything:
			// every entry above this watermark and no others. The two cases part
			// here and nowhere else — the committed one leaves a watermark the
			// predecessor never saw move, so the entries below it must not be
			// applied again, while the other leaves the whole window to replay.
			inherited, _, err := crashed.store.Watermark(crashed.ctx, seamsShard)
			require.NoError(t, err)
			acked := wal.Seqno(crashed.acked + 1) // the refused write appended too

			require.NoError(t, crashed.drive(t, seamsMutations-crashed.acked-1))

			recovered := crashed.mgr.Totals()
			crashed.mgr.Close(crashed.ctx)
			require.EqualValues(t, acked-inherited, recovered.Replayed,
				"the successor replayed something other than the %d entries above the watermark %d it found",
				acked-inherited, inherited)
			require.Empty(t, recovered.Halted, "the successor halted on the tail it inherited")

			seqno, ok, err := crashed.store.Watermark(crashed.ctx, seamsShard)
			require.NoError(t, err)
			require.True(t, ok)
			require.EqualValues(t, seamsMutations, seqno,
				"the watermark is short of the stream: entries were acked and never applied by anybody")

			require.Empty(t, crashed.trims.violation())
			for _, key := range union(expected.runs, crashed.ledger.runs, byRun) {
				want, got := control.runRow(t, key), crashed.runRow(t, key)
				if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
					t.Fatalf("run %s of workflow %s is not what the uninterrupted stream left (-uninterrupted +recovered):\n%s",
						key.runID, key.workflowID, diff)
				}
			}
			for _, key := range union(expected.current, crashed.ledger.current, byWorkflow) {
				require.Equal(t, control.currentRowOf(t, key), crashed.currentRowOf(t, key),
					"workflow %s names a different run after recovery", key.workflowID)
			}
			t.Logf("%d entries replayed, watermark %d", recovered.Replayed, seqno)
		})
	}
}

// stagedDrain is the fault the other cases cannot stage: one drain whose outcome
// nothing can read. Disarmed it is the cold seam unchanged, which is how every
// case but the one above sees it.
//
// Both halves are needed. The applier's error is what the cycle classifies as an
// unknown outcome, and the watermark read is what it then asks — a read that
// answers resolves the ambiguity on the spot and leaves nothing for a crash to
// land on, so exactly one read fails and the successor's floor is read normally.
type stagedDrain struct {
	cold.Applier
	mark cold.Watermarker

	armed   bool
	commits bool
	blind   bool
}

// arm stages the next drain. commits says whether its transaction runs first,
// which is the whole of the difference between the two cases.
func (s *stagedDrain) arm(commits bool) { s.armed, s.commits, s.blind = true, commits, true }

func (s *stagedDrain) Apply(ctx context.Context, shard wal.ShardID, epoch wal.Epoch, batch fold.Batch) error {
	if !s.armed {
		return s.Applier.Apply(ctx, shard, epoch, batch)
	}
	s.armed = false
	if s.commits {
		if err := s.Applier.Apply(ctx, shard, epoch, batch); err != nil {
			return err
		}
	}
	return errors.New("staged: the drain's transaction has no readable outcome")
}

func (s *stagedDrain) Watermark(ctx context.Context, shard wal.ShardID) (wal.Seqno, bool, error) {
	if s.blind {
		s.blind = false
		return 0, false, errors.New("staged: the watermark cannot be read")
	}
	return s.mark.Watermark(ctx, shard)
}

// runRow is the mutable state the database holds for one run, nil where it holds
// none. The keys are supplied by the caller rather than scanned because the
// sqlite plugin implements no scan, and the union of the two ledgers is the whole
// of what either database can hold: a row no drain ever carried is a row nobody
// wrote.
func (s *seams) runRow(t *testing.T, key runKey) *p.InternalGetWorkflowExecutionResponse {
	t.Helper()
	row, err := s.rows.Run(s.ctx, seamsShard, key.namespaceID, key.workflowID, key.runID)
	require.NoError(t, err)
	return row
}

// currentRowOf is the workflow's current-execution row as a comparison sees it:
// the run it names *and the content the window left there*. Comparing the run
// alone leaves a whole class of divergence invisible — the row naming the right
// run with the wrong state or the wrong last-write-version — and that content is
// what every later condition on the workflow is judged against, so a drift in it
// is not a stale answer but a condition decided against the wrong value from then
// on. [noCurrentRow] where there is no such row.
func (s *seams) currentRowOf(t *testing.T, key wfKey) currentRowState {
	t.Helper()
	row, version, err := s.rows.Current(s.ctx, seamsShard, key.namespaceID, key.workflowID)
	require.NoError(t, err)
	if row == nil {
		return currentRowState{RunID: noCurrentRow}
	}
	return currentRowState{
		RunID:            row.RunID,
		State:            row.ExecutionState.GetState(),
		Status:           row.ExecutionState.GetStatus(),
		LastWriteVersion: version,
	}
}

// currentRowState is the comparable shape of that row. A struct rather than the
// response, so a divergence names the field.
type currentRowState struct {
	RunID            string
	State            enumsspb.WorkflowExecutionState
	Status           enumspb.WorkflowExecutionStatus
	LastWriteVersion int64
}

// union is the keys of both ledgers in one order, so a divergence names the same
// row on every run of a seed. A key only one of them has is the interesting one:
// a row one run wrote and the other did not.
func union[K comparable, A, B any](a map[K]A, b map[K]B, order func(K, K) int) []K {
	seen := make(map[K]struct{}, len(a)+len(b))
	for k := range a {
		seen[k] = struct{}{}
	}
	for k := range b {
		seen[k] = struct{}{}
	}
	return slices.SortedFunc(maps.Keys(seen), order)
}
