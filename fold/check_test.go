package fold_test

// The condition authority: what the window answers about a mutation before it
// is acked, what it hands on, and what it refuses. A failed assertion must
// produce the plugin's own error value down to the message and the payload,
// because the caller type-switches on it and reads its fields; where a test
// names a condition of the plugin, the plugin's own code is the source.
//
// A check that is subtly too strict refuses a legal write, which these tables
// cannot see; that is check_corpus_test.go's job.

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"

	"github.com/aromanovich/waltz/fold"
	"github.com/aromanovich/waltz/mutation"
)

// check is the accumulator's answer with the delegation dropped.
func check(t *testing.T, a *fold.Accumulator, m mutation.Mutation) error {
	t.Helper()
	_, err := a.Check(m)
	return err
}

// ---------------------------------------------------------------------------
// The run rows.
// ---------------------------------------------------------------------------

// TestTheWindowAnswersAStaleVersion: a caller that read the run before the
// window wrote it and asserts what it read. Without the authority the assertion
// is discarded and the write folds in silently, the database never seeing the
// condition it violated.
func TestTheWindowAnswersAStaleVersion(t *testing.T) {
	a := fold.New(shard)
	add(t, a, mkCreate(runX), mkUpdate(runX, 2))

	// The window writes version 2, so a second update asserting base 1 is the
	// same writer sending twice, or two writers racing on one read.
	err := check(t, a, mkUpdate(runX, 2))

	failed, ok := err.(*p.WorkflowConditionFailedError) //nolint:errorlint // the concrete type is the assertion
	require.True(t, ok, "the store answers a version mismatch with its own type, got %T: %v", err, err)
	require.EqualValues(t, 2, failed.DBRecordVersion,
		"the version the window will write, which is what the store's own readback would have reported")
	require.Equal(t, "Encounter workflow db version mismatch, request db version: 1, actual db version: 2",
		failed.Msg, "the plugin's own message")

	require.NoError(t, check(t, a, mkUpdate(runX, 3)), "and the write that does follow the window is taken")
}

// TestTheWindowAnswersACreateOfARunItHolds: must-not-exist against the window's
// own state. Set-headed so that the run rule answers; over a Create the window's
// current-row write would raise a current-row conflict instead.
func TestTheWindowAnswersACreateOfARunItHolds(t *testing.T) {
	a := fold.New(shard)
	add(t, a, mkSet(runX, 4))

	err := check(t, a, mkCreate(runX))
	failed, ok := err.(*p.WorkflowConditionFailedError) //nolint:errorlint // the concrete type is the assertion
	require.True(t, ok, "expected the store's own type, got %T: %v", err, err)
	require.Equal(t, "Workflow "+wfID+" must not exist", failed.Msg)
}

// TestTheWindowAnswersAWriteToARunItDeleted is the one run failure the plugin
// reports with the bare ConditionFailedError rather than the workflow one.
func TestTheWindowAnswersAWriteToARunItDeleted(t *testing.T) {
	// Set-headed again, so the tombstone answers rather than a current-row
	// conflict.
	a := fold.New(shard)
	add(t, a, mkSet(runX, 4), mkDelete(runX))

	err := check(t, a, mkUpdate(runX, 2))
	failed, ok := err.(*p.ConditionFailedError) //nolint:errorlint // the concrete type is the assertion
	require.True(t, ok, "expected the store's own type, got %T: %v", err, err)
	require.Equal(t, "Workflow execution "+wfID+" must exist", failed.Msg)

	require.NoError(t, check(t, a, mkCreate(runX)),
		"a create behind the tombstone is the run's next life, and asserts absence — which holds")
}

// TestACreateOfAFreshRunPassesTheWindow: a window holding one run must not
// refuse another.
func TestACreateOfAFreshRunPassesTheWindow(t *testing.T) {
	a := fold.New(shard)
	add(t, a, mkSet(runX, 4))
	require.NoError(t, check(t, a, mkSet(runY, 9)))
}

// ---------------------------------------------------------------------------
// The current-execution row.
// ---------------------------------------------------------------------------

// realState is an execution state that survives a round trip; the current-row
// answer deserialises the blob the window wrote, so a stand-in will not do.
func realState(t *testing.T, run string, state enumsspb.WorkflowExecutionState) *persistencespb.WorkflowExecutionState {
	t.Helper()
	return &persistencespb.WorkflowExecutionState{
		RunId:           run,
		CreateRequestId: "request-" + run,
		State:           state,
		Status:          enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		RequestIds: map[string]*persistencespb.RequestIDInfo{
			"request-" + run: {EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED},
		},
	}
}

// withCurrentRow makes a snapshot write the current-execution row the way the
// store's create does: the state blob passed through, at the snapshot's
// last-write version.
func withCurrentRow(t *testing.T, run string, state enumsspb.WorkflowExecutionState, lastWriteVersion int64,
) func(*p.InternalWorkflowSnapshot) {
	t.Helper()
	return func(s *p.InternalWorkflowSnapshot) {
		st := realState(t, run, state)
		b, err := serialization.WorkflowExecutionStateToBlob(st)
		require.NoError(t, err)
		s.ExecutionState, s.ExecutionStateBlob, s.LastWriteVersion = st, b, lastWriteVersion
	}
}

// mkCreateOver is a create over a previous run — the workflow-id reuse path,
// whose current-row assertion is the three-condition one.
func mkCreateOver(run, previousRun string, previousLastWriteVersion int64) mutation.Mutation {
	m := mkCreate(run)
	m.Create.Mode = p.CreateWorkflowModeUpdateCurrent
	m.Create.PreviousRunID = previousRun
	m.Create.PreviousLastWriteVersion = previousLastWriteVersion
	return m
}

// TestTheCurrentRowConflictCarriesThePluginsPayload: the plugin answers a
// current-row conflict by deserialising the execution-state blob it read out of
// the row, and [fold.CurrentWrite] holds that blob, so the answer carries the
// RequestIDs api/startworkflow's dedup reads.
func TestTheCurrentRowConflictCarriesThePluginsPayload(t *testing.T) {
	a := fold.New(shard)
	add(t, a, mkCreate(runX, withCurrentRow(t, runX, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, 11)))

	err := check(t, a, mkCreate(runY))
	failed, ok := err.(*p.CurrentWorkflowConditionFailedError) //nolint:errorlint // the concrete type is the assertion
	require.True(t, ok, "a brand-new create over a workflow the window holds is a current-row conflict, got %T: %v", err, err)
	require.Equal(t, "must not exist", failed.Msg)
	require.Equal(t, runX, failed.RunID)
	require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, failed.State)
	require.EqualValues(t, 11, failed.LastWriteVersion)
	require.Contains(t, failed.RequestIDs, "request-"+runX,
		"the request ids come out of the window's own blob: the start path deduplicates on them")
}

// The same claim for the other evaluator, and it is a different test rather
// than a case of the one above: the window builds its payload out of a blob it
// wrote, while this one has only what the store's read returned.
// [p.InternalGetCurrentExecutionResponse] carries the run id beside the
// execution state rather than inside it, and upstream's own read fills the field
// and leaves the state's copy empty — so a conflict taking the run from the
// state names nobody, and the start path skips its whole conflict-resolution
// branch on an empty run id.
func TestTheDelegatedCurrentRowConflictNamesTheRunItCollidedWith(t *testing.T) {
	del, err := fold.New(shard).Check(mkCreate(runY))
	require.NoError(t, err)
	require.NotNil(t, del.Current)

	err = del.Current.Verify(&p.InternalGetCurrentExecutionResponse{
		RunID: runX,
		ExecutionState: &persistencespb.WorkflowExecutionState{
			State:      enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			RequestIds: map[string]*persistencespb.RequestIDInfo{"request-" + runX: {}},
		},
	}, 11)

	failed, ok := err.(*p.CurrentWorkflowConditionFailedError) //nolint:errorlint // the concrete type is the assertion
	require.True(t, ok, "a brand-new create over a row the store holds is a current-row conflict, got %T: %v", err, err)
	require.Equal(t, runX, failed.RunID)
	require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, failed.State)
	require.EqualValues(t, 11, failed.LastWriteVersion)
	require.Contains(t, failed.RequestIDs, "request-"+runX,
		"the start path deduplicates a retried start on them")
}

// rowContent is one current-execution row, stated as content rather than as a
// value: the two sites below realise the same row differently, one by writing
// it into the window and one by reading it back off the store.
type rowContent struct {
	run     string
	state   enumsspb.WorkflowExecutionState
	version int64
}

// currentCase is one assertion against one row, and the store's answer to the
// pair: its message, or empty when the assertion holds.
type currentCase struct {
	name string
	// assert is a mutation carrying the assertion under test.
	assert mutation.Mutation
	// row is the current-execution row, nil when there is none.
	row     *rowContent
	wantMsg string
}

// bypass is an update that writes a run without claiming the current row, which
// is what asserts current != run.
func bypass(run string, version int64) mutation.Mutation {
	m := mkUpdate(run, version)
	m.Update.Mode = p.UpdateWorkflowModeBypassCurrent
	return m
}

// reuseRefusal is the plugin's message for the three-condition assertion, which
// names every condition whether or not it is the one that failed.
func reuseRefusal(row *rowContent, wantRun string, wantVersion int64) string {
	return fmt.Sprintf(
		"state %d must be equal to %d, current run id %s must be equal to %s, last write version %d must be equal to %d",
		row.state, enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, row.run, wantRun, row.version, wantVersion)
}

// currentCases is the whole of what a current-row assertion can be asked, in
// the four kinds the store asserts and in both directions. Stated once because
// the answer may not depend on which site asks it.
func currentCases() []currentCase {
	running := &rowContent{run: runX, state: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, version: 11}
	completed := &rowContent{run: runX, state: enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, version: 11}
	completedOther := &rowContent{run: runY, state: enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, version: 11}

	// "must exist" is the answer to any assertion but must-not-exist on a row
	// that is not there, and it is bare: there is no row to build the store's
	// payload out of.
	return []currentCase{
		{"must not exist, and there is no row", mkCreate(runY), nil, ""},
		{"must not exist, and a row is in the way", mkCreate(runY), running, "must not exist"},

		{"equals, and the row names that run", mkUpdate(runX, 2), running, ""},
		{"equals, and the row names another run", mkUpdate(runY, 2), running,
			"current run id " + runX + " must be equal to " + runY},
		{"equals, and there is no row", mkUpdate(runY, 2), nil, "must exist"},

		{"not-equals, and the row names another run", bypass(runY, 2), running, ""},
		{"not-equals, and the row names that very run", bypass(runX, 2), running,
			"current run id " + runX + " must not be equal to " + runX},
		{"not-equals, and there is no row", bypass(runY, 2), nil, "must exist"},

		// A create over a previous run requires the row to be COMPLETED as well
		// as to name that run at that version — Cassandra asserts all three in
		// templateUpdateCurrentWorkflowExecutionForNewQuery and the SQL store in
		// createWorkflowExecutionTx: left out, a start over a running run is
		// admitted here and rejected by the store.
		{"reuse, and the row is that run, completed, at that version", mkCreateOver(runY, runX, 11), completed, ""},
		{"reuse, and the run has not finished", mkCreateOver(runY, runX, 11), running,
			reuseRefusal(running, runX, 11)},
		{"reuse, and the row is at another version", mkCreateOver(runY, runX, 12), completed,
			reuseRefusal(completed, runX, 12)},
		{"reuse, and the row names another run", mkCreateOver(runY, runX, 11), completedOther,
			reuseRefusal(completedOther, runX, 11)},
		{"reuse, and there is no row", mkCreateOver(runY, runX, 11), nil, "must exist"},
	}
}

// TestTheCurrentRowIsAnsweredTheSameWhereverTheRowComesFrom walks every case
// through both evaluators: the window's own write, and the pre-window row the
// caller reads for a delegated assertion. The two see the same row and must
// answer with the same value of the store's own error, message included — an
// assertion answered one way before the append and another way after a drain is
// the window and the cold store disagreeing about one condition.
func TestTheCurrentRowIsAnsweredTheSameWhereverTheRowComesFrom(t *testing.T) {
	for _, c := range currentCases() {
		t.Run(c.name, func(t *testing.T) {
			answers := map[string]error{
				"the window's write":   windowAnswers(t, c),
				"the cold store's row": delegatedAnswers(t, c),
			}
			for site, err := range answers {
				if c.wantMsg == "" {
					require.NoError(t, err, "%s must take this assertion", site)
					continue
				}
				failed, ok := err.(*p.CurrentWorkflowConditionFailedError) //nolint:errorlint // the concrete type is the assertion
				require.True(t, ok, "%s: expected the store's own type, got %T: %v", site, err, err)
				require.Equal(t, c.wantMsg, failed.Msg, "%s: the plugin's own message", site)
			}
		})
	}
}

// windowAnswers is the case evaluated against a window holding the row: a create
// that writes it, and a delete-current behind it where the row is absent, since
// a window that never wrote the row determines nothing about it.
func windowAnswers(t *testing.T, c currentCase) error {
	t.Helper()
	a := fold.New(shard)
	row := c.row
	if row == nil {
		row = &rowContent{run: runX, state: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, version: 11}
	}
	window := []mutation.Mutation{mkCreate(row.run, withCurrentRow(t, row.run, row.state, row.version))}
	if c.row == nil {
		window = append(window, mkDeleteCurrent(row.run))
	}
	add(t, a, window...)
	return check(t, a, c.assert)
}

// delegatedAnswers is the case evaluated against a cold-store row: an empty
// window discards nothing, so the assertion is handed back and verified against
// the row the caller read.
func delegatedAnswers(t *testing.T, c currentCase) error {
	t.Helper()
	del, err := fold.New(shard).Check(c.assert)
	require.NoError(t, err)
	require.NotNil(t, del.Current, "the case's mutation must carry a current-row assertion")
	if c.row == nil {
		return del.Current.Verify(nil, 0)
	}
	// The state carries no run id, which is the shape upstream's own read hands
	// back: the run is the field beside it.
	return del.Current.Verify(&p.InternalGetCurrentExecutionResponse{
		RunID:          c.row.run,
		ExecutionState: &persistencespb.WorkflowExecutionState{State: c.row.state},
	}, c.row.version)
}

// TestAnUndeterminedCurrentRowIsRefusedRatherThanAdmitted covers the third case
// beside evaluate and delegate: an assertion the window discards but whose
// state it does not determine is refused, so drain-and-retry makes the mutation
// head a fresh window and the transaction asserts it against the cold store.
func TestAnUndeterminedCurrentRowIsRefusedRatherThanAdmitted(t *testing.T) {
	t.Run("behind a bypass-current write", func(t *testing.T) {
		// A bypass-current update records the head current-row assertion without
		// writing the row, so nothing can say what the row holds.
		a := fold.New(shard)
		add(t, a, bypass(runY, 2))

		require.ErrorIs(t, check(t, a, mkUpdate(runX, 2)), fold.ErrRefused)
		require.NoError(t, check(t, a, mkSet(runX, 4)),
			"a request that asserts nothing about the row is unaffected")
	})

	t.Run("behind a delete-current, until the drain makes it a head", func(t *testing.T) {
		// The delete is a guarded no-op with no assertion above it, and a
		// delegated read would read a row this window is about to remove.
		a := fold.New(shard)
		add(t, a, mkDeleteCurrent(runX))
		require.ErrorIs(t, check(t, a, mkCreate(runY)), fold.ErrRefused)

		a.Drain()
		require.NoError(t, check(t, a, mkCreate(runY)),
			"an empty window discards nothing, which is why drain-and-retry terminates")
	})
}

// ---------------------------------------------------------------------------
// What the window hands on.
// ---------------------------------------------------------------------------

// TestWhatTheWindowDoesNotHoldIsDelegated: an empty window discards nothing, so
// at a window of one every assertion is delegated and the authority is inert.
func TestWhatTheWindowDoesNotHoldIsDelegated(t *testing.T) {
	a := fold.New(shard)

	del, err := a.Check(mkUpdate(runX, 4))
	require.NoError(t, err)
	require.True(t, del.Any())
	require.NotNil(t, del.Current, "an update-current asserts on the row, and the empty window says nothing about it")
	require.Equal(t, nsID, del.Current.NamespaceID)
	require.Equal(t, wfID, del.Current.WorkflowID)
	require.Len(t, del.Runs, 1)
	require.Equal(t, runX, del.Runs[0].RunID)
	require.Equal(t, wfID, del.Runs[0].WorkflowID)

	// A continue-as-new names two runs, so an empty window delegates two: the
	// mutated run's version and the new run's absence, which the store asserts
	// too.
	cont := mkUpdate(runX, 4)
	contNew := snapshot(runY, 1)
	cont.Update.NewWorkflowSnapshot = &contNew
	del, err = a.Check(cont)
	require.NoError(t, err)
	require.Len(t, del.Runs, 2)
	require.Equal(t, []string{runX, runY}, []string{del.Runs[0].RunID, del.Runs[1].RunID})

	// What the window does hold is evaluated rather than delegated.
	add(t, a, mkCreate(runX))
	del, err = a.Check(mkUpdate(runX, 2))
	require.NoError(t, err)
	require.Empty(t, del.Runs, "the run is in the window, so its assertion was evaluated rather than handed on")
	require.Nil(t, del.Current, "and so was the current row the create wrote")
}

// TestTheDelegationIsSettledInTheStoresOwnOrder: the caller reads the rows, and
// which one it is asked for first is what decides the error type a doubly-stale
// caller gets — the store reports the first failing assertion in its
// registration order, and the compatibility suites read the type. So the order
// travels with the obligation rather than being restated wherever it is
// discharged.
func TestTheDelegationIsSettledInTheStoresOwnOrder(t *testing.T) {
	// A continue-as-new: a current row and two runs, all delegated against an
	// empty window.
	m := mkUpdate(runX, 4)
	contNew := snapshot(runY, 1)
	m.Update.NewWorkflowSnapshot = &contNew

	del, err := fold.New(shard).Check(m)
	require.NoError(t, err)

	var asked []string
	current := func(cur fold.DelegatedCurrent) error {
		asked = append(asked, "current "+cur.WorkflowID)
		return nil
	}
	run := func(r fold.DelegatedRun) error {
		asked = append(asked, "run "+r.RunID)
		return nil
	}

	require.NoError(t, del.Settle(current, run))
	require.Equal(t, []string{"current " + wfID, "run " + runX, "run " + runY}, asked)

	t.Run("and the first failure is the answer", func(t *testing.T) {
		refusal := fmt.Errorf("the row the caller read refuses this")
		asked = nil
		require.Equal(t, refusal, del.Settle(
			func(fold.DelegatedCurrent) error { return refusal },
			run,
		), "the answer is the failing assertion's own value")
		require.Empty(t, asked, "and no row past it is even asked for")

		require.Equal(t, refusal, del.Settle(current, func(r fold.DelegatedRun) error {
			asked = append(asked, "run "+r.RunID)
			return refusal
		}))
		require.Equal(t, []string{"current " + wfID, "run " + runX}, asked)
	})

	t.Run("and a delegation of nothing asks for no row", func(t *testing.T) {
		a := fold.New(shard)
		add(t, a, mkCreate(runX))
		del, err := a.Check(mkUpdate(runX, 2))
		require.NoError(t, err)
		require.False(t, del.Any())
		require.NoError(t, del.Settle(
			func(fold.DelegatedCurrent) error { return fmt.Errorf("no row is named") },
			func(fold.DelegatedRun) error { return fmt.Errorf("no row is named") },
		))
	})
}

// TestADelegatedRunAssertionIsAnsweredExactlyFromTheColdStoresRow: a delegated
// assertion stands on the pre-window row, and both conditions the store asserts
// about a run row — the version and the row's existence — are answerable from
// what the persistence interface returns.
func TestADelegatedRunAssertionIsAnsweredExactlyFromTheColdStoresRow(t *testing.T) {
	a := fold.New(shard)

	delegated := func(m mutation.Mutation) fold.DelegatedRun {
		t.Helper()
		del, err := a.Check(m)
		require.NoError(t, err)
		require.Len(t, del.Runs, 1)
		return del.Runs[0]
	}

	t.Run("a stale version", func(t *testing.T) {
		run := delegated(mkUpdate(runX, 4))
		require.NoError(t, run.Verify(baseRow(3)), "the row is at the version the caller read")

		err := run.Verify(baseRow(9))
		failed, ok := err.(*p.WorkflowConditionFailedError) //nolint:errorlint // the concrete type is the assertion
		require.True(t, ok, "expected the store's own type, got %T: %v", err, err)
		require.EqualValues(t, 9, failed.DBRecordVersion, "the row's version, as the store reports it")
	})

	t.Run("a run that is not there", func(t *testing.T) {
		err := delegated(mkUpdate(runX, 4)).Verify(nil)
		require.IsType(t, &p.ConditionFailedError{}, err, "got %T: %v", err, err)
	})

	t.Run("a create of a run that is", func(t *testing.T) {
		run := delegated(mkCreate(runX))
		require.NoError(t, run.Verify(nil), "the row the create asserts absent is absent")
		require.IsType(t, &p.WorkflowConditionFailedError{}, run.Verify(baseRow(1)),
			"a create over a row the cold store holds — a retry, answered rather than halted on")
	})
}

// TestADelegatedCurrentRowConflictCarriesTheRowsPayload: the answer is the
// store's own error value and the caller reads its fields, so a conflict built
// from a row read back must carry that row's, not the assertion's.
func TestADelegatedCurrentRowConflictCarriesTheRowsPayload(t *testing.T) {
	delegated := func(m mutation.Mutation) fold.DelegatedCurrent {
		t.Helper()
		del, err := fold.New(shard).Check(m)
		require.NoError(t, err)
		require.NotNil(t, del.Current)
		return *del.Current
	}

	t.Run("the run that is in the way", func(t *testing.T) {
		err := delegated(mkCreate(runX)).Verify(currentRow(runY), 0)
		failed, ok := err.(*p.CurrentWorkflowConditionFailedError) //nolint:errorlint // the concrete type is the assertion
		require.True(t, ok, "got %T: %v", err, err)
		require.Equal(t, runY, failed.RunID)
	})

	t.Run("the version the row holds", func(t *testing.T) {
		err := delegated(mkCreateOver(runY, runX, 11)).Verify(completedCurrentRow(runX), 12)
		failed, ok := err.(*p.CurrentWorkflowConditionFailedError) //nolint:errorlint // the concrete type is the assertion
		require.True(t, ok, "got %T: %v", err, err)
		require.EqualValues(t, 12, failed.LastWriteVersion,
			"the row's own version, which is what api/startworkflow re-issues the create against")
	})
}

// TestCheckLeavesTheAccumulatorExactlyAsItWas is what makes a refusal safe to
// retry after a drain: a check must not change what the window emits.
func TestCheckLeavesTheAccumulatorExactlyAsItWas(t *testing.T) {
	build := func() *fold.Accumulator {
		a := fold.New(shard)
		add(t, a,
			mkCreate(runX, withCurrentRow(t, runX, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, 11),
				snapActivity(1, "created"), snapTask("task-create")),
			mkUpdate(runX, 2, upsertActivity(7, "updated"), withTask("task-update"), withBuffered("batch")),
		)
		return a
	}

	t.Run("answering, delegating and passing", func(t *testing.T) {
		before := build().Drain()

		a := build()
		require.Error(t, check(t, a, mkUpdate(runX, 2)), "answered from the window")
		require.Error(t, check(t, a, mkCreate(runY)), "and so is the current row")
		del, err := a.Check(mkSet(runY, 3))
		require.NoError(t, err, "a run the window does not hold passes")
		require.True(t, del.Any(), "and is handed on")
		after := a.Drain()

		require.Equal(t, reqs(before), reqs(after), "a window that was checked is the window that was not")
		require.Equal(t, before.Stats(), after.Stats())
	})

	t.Run("and refusing", func(t *testing.T) {
		// A window headed by a bypass-current update determines nothing about
		// the current row, so the next assertion on it is refused; the recovery
		// is only safe if the refusal touched nothing.
		bypassWindow := func() *fold.Accumulator {
			m := mkUpdate(runY, 2, upsertActivity(3, "bypassed"))
			m.Update.Mode = p.UpdateWorkflowModeBypassCurrent
			a := fold.New(shard)
			add(t, a, m)
			return a
		}
		quiet := reqs(bypassWindow().Drain())

		a := bypassWindow()
		require.ErrorIs(t, check(t, a, mkCreate(runX)), fold.ErrRefused)
		noisy := reqs(a.Drain())

		require.Equal(t, quiet, noisy, "a refusal must leave the window it refused untouched")
	})
}

// ---------------------------------------------------------------------------
// The switch itself.
// ---------------------------------------------------------------------------

// TestEveryKindIsDecidedAndAnUnknownOneIsRefused: the authority's switch is
// exhaustive, and what it does not recognise it refuses. A kind decide forgets
// yields the zero verdict, which reads as "no assertion refuses this" — the one
// answer the condition authority forbids, an assertion the window does not
// determine being admitted rather than refused. The two tables are the
// partition, so a ninth kind is in neither and fails here by name.
func TestEveryKindIsDecidedAndAnUnknownOneIsRefused(t *testing.T) {
	asserting := map[mutation.Kind]mutation.Mutation{
		mutation.KindCreate:          mkCreate(runX),
		mutation.KindUpdate:          mkUpdate(runX, 2),
		mutation.KindConflictResolve: mkConflictResolve(runX, 2),
		mutation.KindSet:             mkSet(runX, 2),
	}
	silent := map[mutation.Kind]mutation.Mutation{
		mutation.KindDelete:             mkDelete(runX),
		mutation.KindDeleteCurrent:      mkDeleteCurrent(runX),
		mutation.KindAddTasks:           mkAddTasks(task("t")),
		mutation.KindRangeCompleteTasks: mkRangeComplete(1, 2),
	}

	for k := mutation.KindInvalid + 1; int(k) < mutation.KindCount; k++ {
		m, asserts := asserting[k], true
		if m.Kind() == mutation.KindInvalid {
			m, asserts = silent[k], false
		}
		require.Equalf(t, k, m.Kind(), "kind %s is in neither table: whether it carries an "+
			"assertion is decide's business, and a kind nobody filed is a kind whose arm "+
			"nobody wrote", k)

		t.Run(k.String(), func(t *testing.T) {
			del, err := fold.New(shard).Check(m)
			require.NoError(t, err, "an empty window determines nothing, so it refuses nothing")
			// Against an empty window every assertion is a head, so delegation
			// is what "this kind carries assertions" looks like from outside.
			require.Equal(t, asserts, del.Any())
		})
	}

	_, err := fold.New(shard).Check(mutation.Mutation{})
	require.ErrorIs(t, err, mutation.ErrNotExactlyOneRequest,
		"a mutation the switch does not recognise must be refused, not admitted by silence")
}
