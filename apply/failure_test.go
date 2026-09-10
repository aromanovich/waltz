package apply

// The failure classes without a cold store: what Classify says about each way
// an Apply can end, and what the attribution readback reports when the store
// rejects an assertion — a run row's version or the current row. The cause is
// handed to [Attribute] the way an applier hands over what its failing
// transaction returned.

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	enumsspb "go.temporal.io/server/api/enums/v1"
	p "go.temporal.io/server/common/persistence"

	"github.com/aromanovich/waltz/fold"
	"github.com/aromanovich/waltz/internal/verify/basetest"
	"github.com/aromanovich/waltz/internal/verify/mutbuild"
	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/wal"
)

const testShard wal.ShardID = 7

// Requests the store's own code accepts, built by [mutbuild] so that what makes
// one well-formed is Temporal's answer rather than this file's.

var build = mutbuild.For(int32(testShard))

func mkCreate(ns, wf, run string) mutation.Mutation { return build.Create(ns, wf, run) }

func mkUpdate(ns, wf, run string, version int64) mutation.Mutation {
	return build.Update(ns, wf, run, version)
}

// mkCreateBypassingCurrent is a create that leaves the current row alone, which
// is how one workflow's window can hold a second request standing on nothing
// the first one stands on.
func mkCreateBypassingCurrent(ns, wf, run string) mutation.Mutation {
	m := mkCreate(ns, wf, run)
	m.Create.Mode = p.CreateWorkflowModeBypassCurrent
	return m
}

// drainOf folds the mutations under seqnos 1..n and drains, so the batch's
// watermark is n.
func drainOf(t *testing.T, ms ...mutation.Mutation) fold.Batch {
	t.Helper()
	acc := fold.New(testShard)
	for i, m := range ms {
		require.NoError(t, acc.Add(wal.Seqno(i+1), m))
	}
	return acc.Drain()
}

// reqs is the drain's merged requests, in the order an applier drives them.
func reqs(b fold.Batch) []*fold.Emitted { return slices.Collect(b.Each()) }

func TestClassify(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want Class
	}{
		{"nil is a commit", nil, ClassCommitted},
		{"a refusal", Refuse(errors.New("bad drain")), ClassRefused},
		{"a wrapped refusal", fmt.Errorf("outer: %w", Refuse(errors.New("no assertions"))), ClassRefused},
		{"the epoch CAS", &p.ShardOwnershipLostError{ShardID: 1, Msg: "fenced"}, ClassShardLost},
		{"a version assertion", &p.WorkflowConditionFailedError{Msg: "stale"}, ClassInvariantViolated},
		{"a current-row assertion", &p.CurrentWorkflowConditionFailedError{Msg: "moved"}, ClassInvariantViolated},
		{"the attributed wrapper", &InvariantViolationError{Cause: &p.WorkflowConditionFailedError{Msg: "stale"}}, ClassInvariantViolated},
		{"anything else is unknown", errors.New("transport went dark"), ClassUnknownOutcome},
		{"a context deadline is unknown", context.DeadlineExceeded, ClassUnknownOutcome},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, Classify(tc.err), "Classify(%v)", tc.err)
		})
	}
}

// TestAttributeNamesAVersionFailure: the store's error reports the first
// failing assertion and names no workflow; the readback must name exactly the
// diverged ones, and the cut point must sit one entry before the first of
// them.
func TestAttributeNamesAVersionFailure(t *testing.T) {
	ns := uuid.NewString()
	wfA, runA := uuid.NewString(), uuid.NewString()
	wfB, runB := uuid.NewString(), uuid.NewString()

	// A at seqno 1 stands on base 1 and the cold store agrees; B at seqno 2
	// stands on base 2 and the cold store holds 7.
	drain := drainOf(t,
		mkUpdate(ns, wfA, runA, 2),
		mkUpdate(ns, wfB, runB, 3),
	)
	reader := basetest.New()
	reader.Holds(wfA, runA, 1)
	reader.Holds(wfB, runB, 7)
	cause := &p.WorkflowConditionFailedError{Msg: "condition failed"}

	viol := Attribute(context.Background(), reader.Rows(), cause, testShard, drain)
	require.Equal(t, ClassInvariantViolated, Classify(viol))

	require.Len(t, viol.Diverged, 1, "one workflow diverged, one must be named")
	d := viol.Diverged[0]
	require.Equal(t, wfB, d.WorkflowID)
	require.Equal(t, runB, d.RunID)
	require.EqualValues(t, 2, d.AssertedBase)
	require.EqualValues(t, 7, d.ActualBase)
	require.EqualValues(t, 1, viol.CutSeqno,
		"the only legal partial drain is the prefix before B's first entry (seqno 2)")

	var got *p.WorkflowConditionFailedError
	require.ErrorAs(t, error(viol), &got, "the store's own error must stay reachable under the wrapper")
}

// TestAttributeNamesAMustNotExistViolation: a create-headed window asserts
// the run absent and no current row; when the cold store holds both, the
// readback names both divergences, and nothing before the window's first
// entry means nothing may be acknowledged (CutSeqno 0).
func TestAttributeNamesAMustNotExistViolation(t *testing.T) {
	ns, wf, run := uuid.NewString(), uuid.NewString(), uuid.NewString()
	drain := drainOf(t, mkCreate(ns, wf, run))

	reader := basetest.New()
	reader.Holds(wf, run, 5)
	cause := &p.WorkflowConditionFailedError{Msg: "condition failed"}

	viol := Attribute(context.Background(), reader.Rows(), cause, testShard, drain)
	require.Len(t, viol.Diverged, 2, "the run's row and the current row both diverged")
	require.Zero(t, viol.CutSeqno, "the first entry diverged, so nothing may be acknowledged")

	byRun := map[string]Diverged{}
	for _, d := range viol.Diverged {
		byRun[d.RunID] = d
	}
	require.Contains(t, byRun[run].Detail, "asserted absent")
	require.Contains(t, byRun[""].Detail, "asserted no current row",
		"an empty RunID marks the current-execution row")
}

// TestAnIncompleteReadbackAcknowledgesNothing: a cut derived from the prefix
// the readback managed to scan would trust the rows nobody read. Any of them
// can answer for an entry below the lowest divergence that was seen, so that
// cut can sit too high, and a partial re-drain would then acknowledge an entry
// standing on a row that had diverged. An incomplete scan may only say
// "nothing".
func TestAnIncompleteReadbackAcknowledgesNothing(t *testing.T) {
	ns := uuid.NewString()
	wfA, runA := uuid.NewString(), uuid.NewString()
	wfB, runB := uuid.NewString(), uuid.NewString()
	wfC, runC := uuid.NewString(), uuid.NewString()

	// A at seqno 1 agrees with the cold store, B at seqno 2 diverges, and C's
	// read at seqno 3 never answers.
	drain := drainOf(t,
		mkUpdate(ns, wfA, runA, 2),
		mkUpdate(ns, wfB, runB, 2),
		mkUpdate(ns, wfC, runC, 2),
	)
	reader := basetest.New()
	reader.Holds(wfA, runA, 1)
	reader.Holds(wfB, runB, 7)
	reader.SetRunning(wfC, runC)
	reader.FailRun(runC, errors.New("session went dark"))
	cause := &p.WorkflowConditionFailedError{Msg: "condition failed"}

	viol := Attribute(context.Background(), reader.Rows(), cause, testShard, drain)
	require.Error(t, viol.ReadbackErr)
	require.Len(t, viol.Diverged, 1, "what was seen before the failure is still reported")
	require.Equal(t, wfB, viol.Diverged[0].WorkflowID)
	require.Zero(t, viol.CutSeqno,
		"B's divergence would cut at 1, but C's rows were never read: nothing may be acknowledged")
}

// TestACurrentRowDivergenceCutsAtTheWorkflowsLowestHead: the current-row
// assertion is the workflow's head-of-window fact, so a divergence of that row
// is answered for by every entry of that workflow — including those in a
// request other than the one the drain marked first, which is first by tail.
func TestACurrentRowDivergenceCutsAtTheWorkflowsLowestHead(t *testing.T) {
	ns := uuid.NewString()
	other, runOther := uuid.NewString(), uuid.NewString()
	wf, runA, runB := uuid.NewString(), uuid.NewString(), uuid.NewString()

	// The window's workflow drains as two requests: seqno 2 opens one and fixes
	// the current-row assertion, seqno 4 merges into it; the bypassing create at
	// seqno 3 stands on no current row and opens the other.
	drain := drainOf(t,
		mkUpdate(ns, other, runOther, 2),
		mkUpdate(ns, wf, runA, 2),
		mkCreateBypassingCurrent(ns, wf, runB),
		mkUpdate(ns, wf, runA, 3),
	)
	require.Equal(t, 3, drain.Len())
	first := reqs(drain)[1]
	require.Equal(t, wf, first.WorkflowID)
	require.True(t, first.FirstOfWorkflow())
	require.EqualValues(t, 3, first.HeadSeqno,
		"tail order puts the create first, and its head is not the workflow's lowest")

	// Both runs read back clean; only the current row moved.
	reader := basetest.New()
	reader.Holds(other, runOther, 1)
	reader.SetRun(runA, 1)
	reader.SetRunning(wf, uuid.NewString())
	cause := &p.CurrentWorkflowConditionFailedError{Msg: "moved"}

	viol := Attribute(context.Background(), reader.Rows(), cause, testShard, drain)
	require.NoError(t, viol.ReadbackErr)
	require.Len(t, viol.Diverged, 1)
	require.Empty(t, viol.Diverged[0].RunID, "the current-execution row is what diverged")
	require.EqualValues(t, 1, viol.CutSeqno,
		"seqno 2 stood on the diverged row, so a cut at 2 would acknowledge it")
}

// TestTheCurrentRowIsAttributedByTheAuthoritysOwnPredicate: the readback names
// the row diverged exactly when the condition the accumulator vouched for does
// not hold in the cold store. Attribution reads last_write_version back with the
// row, so the reuse assertion is judged on all three of its conditions here as
// well — a row at another version is a divergence, not a match on the run id.
func TestTheCurrentRowIsAttributedByTheAuthoritysOwnPredicate(t *testing.T) {
	ns, wf := uuid.NewString(), uuid.NewString()
	runA, runB := uuid.NewString(), uuid.NewString()

	// Each case stages its row onto the store, or stages nothing where the case
	// is "and there is no row".
	completed := func(run string, version int64) func(*basetest.Store) {
		return func(s *basetest.Store) { s.SetCompleted(wf, run, version) }
	}
	running := func(run string, version int64) func(*basetest.Store) {
		return func(s *basetest.Store) {
			s.SetCurrent(wf, run, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, version)
		}
	}
	reuse := fold.CurrentAssertion{Kind: fold.CurrentEqualsWithVersion, RunID: runA, LastWriteVersion: 11}

	cases := []struct {
		name     string
		want     fold.CurrentAssertion
		row      func(*basetest.Store)
		diverged bool
	}{
		{"must not exist, and there is no row", fold.CurrentAssertion{Kind: fold.CurrentMustNotExist}, nil, false},
		{"must not exist, and a row is in the way", fold.CurrentAssertion{Kind: fold.CurrentMustNotExist}, running(runA, 0), true},

		{"equals, and the row names that run", fold.CurrentAssertion{Kind: fold.CurrentEquals, RunID: runA}, running(runA, 0), false},
		{"equals, and the row names another run", fold.CurrentAssertion{Kind: fold.CurrentEquals, RunID: runA}, running(runB, 0), true},
		{"equals, and there is no row", fold.CurrentAssertion{Kind: fold.CurrentEquals, RunID: runA}, nil, true},

		{"not-equals, and the row names another run", fold.CurrentAssertion{Kind: fold.CurrentNotEquals, RunID: runA}, running(runB, 0), false},
		{"not-equals, and the row names that very run", fold.CurrentAssertion{Kind: fold.CurrentNotEquals, RunID: runA}, running(runA, 0), true},
		{"not-equals, and there is no row", fold.CurrentAssertion{Kind: fold.CurrentNotEquals, RunID: runA}, nil, true},

		{"reuse, and the row is that run, completed, at that version", reuse, completed(runA, 11), false},
		{"reuse, and the row is at another version", reuse, completed(runA, 12), true},
		{"reuse, and the run has not finished", reuse, running(runA, 11), true},
		{"reuse, and the row names another run", reuse, completed(runB, 11), true},
		{"reuse, and there is no row", reuse, nil, true},
	}

	ctx := context.Background()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reader := basetest.New()
			if tc.row != nil {
				tc.row(reader)
			}
			e := &fold.Emitted{NamespaceID: ns, WorkflowID: wf, HeadSeqno: 3, TailSeqno: 4}

			d, err := currentDiverged(ctx, reader.Rows(), testShard, e, &tc.want,
				wfSlice{head: e.HeadSeqno, tail: e.TailSeqno})
			require.NoError(t, err)
			diverged := d != nil
			require.Equal(t, tc.diverged, diverged)

			if diverged {
				require.Equal(t, wf, d.WorkflowID)
				require.Empty(t, d.RunID, "an empty RunID marks the current-execution row")
				require.NotEmpty(t, d.Detail, "a divergence with no detail names nothing an operator can read")
			}
		})
	}
}
