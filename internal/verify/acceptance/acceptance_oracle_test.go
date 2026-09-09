package acceptance

// The fold against not folding: one seed driven twice, once at the shipped
// window and once at a window of one mutation, into two databases that have to
// come out identical.
//
// It is the judge the fold has never had. Every other run in this package holds
// the folded path against a record of what its own drains carried, which agrees
// with the fold by construction, or against another folded path: the recovery
// run cuts one stream into two different sets of windows, so it catches a merge
// rule that depends on where a window ended. A rule that is wrong the same way
// at every window size survives all of that.
//
// At a window of one nothing is ever merged. Each drain carries the request one
// mutation makes, which is the shape Temporal's own write path has without this
// layer, so the two arms differ in the merge and in nothing else — and the
// comparison is between two real databases rather than against a model, which is
// what makes it total.
//
// What it cannot see is a defect the two arms share: the codec, the encoding of
// a request, an assertion neither arm makes. Those are the codec guards' and the
// condition authority's own.

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"
)

// TestFoldingChangesNothingButTheNumberOfTransactions is that comparison.
func TestFoldingChangesNothingButTheNumberOfTransactions(t *testing.T) {
	const seed = 20260916

	folded := newSeams(t, seed)
	require.NoError(t, folded.drive(t, seamsMutations))
	folded.mgr.Close(folded.ctx)

	// A window of one mutation: the accumulator holds one request when the drain
	// takes it, so no two mutations of a run ever meet. Everything else is the
	// shipped configuration, this being the one field the comparison is about.
	sequential := seamsPolicy()
	sequential.Mutations = 1
	one := newSeamsWith(t, seed, sequential)
	require.NoError(t, one.drive(t, seamsMutations))
	one.mgr.Close(one.ctx)

	// The two arms have to differ in transactions, or they are one run driven
	// twice and the comparison is with itself.
	require.Greater(t, folded.ledger.collapse(), 1.5,
		"the folded arm merged nothing, so both arms are the sequential path")
	require.Greater(t, one.ledger.committed, 4*folded.ledger.committed,
		"the two arms committed comparable numbers of transactions, so the window is not what separates them")

	// And in nothing else. Read back through the store's own reads, whole, over
	// the union of both ledgers' keys: a row one arm wrote and the other did not
	// is the interesting one, and neither ledger has an entry for it.
	runs := union(folded.ledger.runs, one.ledger.runs, byRun)
	require.NotEmpty(t, runs, "neither arm wrote a row, so there is nothing to compare")
	for _, key := range runs {
		want, got := one.runRow(t, key), folded.runRow(t, key)
		if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
			t.Fatalf("run %s of workflow %s came out of the fold different (-sequential +folded):\n%s",
				key.runID, key.workflowID, diff)
		}
	}
	current := union(folded.ledger.current, one.ledger.current, byWorkflow)
	for _, key := range current {
		require.Equal(t, one.currentRun(t, key), folded.currentRun(t, key),
			"workflow %s names a different run once its mutations are folded", key.workflowID)
	}

	// Both ends of the log meet the stream's length in both arms, so neither
	// comparison above was made over a run that stopped early.
	for name, s := range map[string]*seams{"folded": folded, "sequential": one} {
		seqno, ok, err := s.store.Watermark(s.ctx, seamsShard)
		require.NoError(t, err)
		require.True(t, ok)
		require.EqualValues(t, seamsMutations, seqno, "the %s arm's watermark is short of the stream", name)
		require.Empty(t, s.trims.violation())
	}
	t.Logf("%d mutations: %d transactions folded at a window of %d, %d sequential; %d runs and %d current rows identical",
		seamsMutations, folded.ledger.committed, seamsPolicy().Mutations,
		one.ledger.committed, len(runs), len(current))
}
