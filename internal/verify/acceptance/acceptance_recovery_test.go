package acceptance

// Recovery: one stream driven twice over the same seed, once through a single
// cycle and once through a chain of them, each abandoned the way a process that
// dies abandons one — the range id moves, no drain runs, and the window's
// mutations are in the log and nowhere else. The two databases must agree, row
// for row and blob for blob.
//
// It is the only run in this package that takes an entry out of the log. Every
// other one ends in a shutdown drain, which applies the last window from memory
// and never asks the log for anything, and [TestAShardThatLosesItsEpochMidRun]
// stops one step short: it proves the entries a refused drain carried are still
// in the log, not that anybody can turn them back into rows. This is that step,
// and it is the one the first rule is stated over — a write acked into the log
// and never applied is lost exactly when a successor cannot recover it.
//
// The comparison is against another database rather than against a model, which
// is what makes it total. The ledger knows what the drains carried, so a
// mutation no drain ever carried is invisible to it; the uninterrupted run's own
// rows are the only expectation that has an entry for something the crashed run
// dropped.

import (
	"maps"
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	p "go.temporal.io/server/common/persistence"
	"google.golang.org/protobuf/testing/protocmp"
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
		// moved and this cycle never drained.
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
		require.Equal(t, control.currentRun(t, key), crashed.currentRun(t, key),
			"workflow %s names a different run after recovery", key.workflowID)
	}
	t.Logf("%d entries replayed across %d crashes, %d runs and %d current rows identical, watermark %d",
		recovered.Replayed, recoveryCrashes, len(runs), len(current), seqno)
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

// currentRun is the run a workflow's current-execution row names, and
// [noCurrentRow] where there is no such row.
func (s *seams) currentRun(t *testing.T, key wfKey) string {
	t.Helper()
	row, _, err := s.rows.Current(s.ctx, seamsShard, key.namespaceID, key.workflowID)
	require.NoError(t, err)
	if row == nil {
		return noCurrentRow
	}
	return row.RunID
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
