package cycle

// The condition authority at this boundary: the check's placement, and the
// delegated read, which is the part fold's predicate cannot answer because it
// needs a cold store. The placement is what is pinned — before the append,
// after I10's bound, inside the loop, and not at all in sync mode.

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	p "go.temporal.io/server/common/persistence"

	"github.com/aromanovich/waltz/internal/verify/basetest"
	"github.com/aromanovich/waltz/mutation"
)

// asyncEnv is a cycle that accumulates: no sync drain and no watermark within
// reach, so the window is what the authority is asked about.
func asyncEnv(t *testing.T, store *basetest.Store) *env {
	t.Helper()
	e := newEnv(t, func(c *Config) {
		c.Sync = false
		c.Mutations, c.Bytes, c.Age = 1<<30, 1<<30, time.Hour
	})
	e.useStore(store)
	return e
}

// TestAStaleWriteOnARunTheWindowDoesNotHoldIsAnswered: an assertion the window
// hands on stands on the pre-window row, so that row is read before anything is
// appended. Without it the write is acked on the transaction's word, and a
// wrong word halts the shard on a caller already told it succeeded.
func TestAStaleWriteOnARunTheWindowDoesNotHoldIsAnswered(t *testing.T) {
	store := basetest.New()
	ns, wf, run := ids()
	store.Holds(wf, run, 5)
	e := asyncEnv(t, store)

	// The caller is writing what follows the version the cold store holds.
	require.NoError(t, e.add(t, mkUpdate(ns, wf, run, 6)))
	require.Equal(t, 1, store.Reads().Run, "the run's row was read, since the window held nothing for it")
	require.Equal(t, 1, store.Reads().Current)

	// A second workflow, and a caller two versions behind on it.
	ns2, wf2, run2 := ids()
	store.Holds(wf2, run2, 5)

	err := e.add(t, mkUpdate(ns2, wf2, run2, 3))
	failed, ok := err.(*p.WorkflowConditionFailedError) //nolint:errorlint // the concrete type is the assertion
	require.True(t, ok, "the boundary answered %T, which handleWriteErrorLocked would not recognise: %v", err, err)
	require.EqualValues(t, 5, failed.DBRecordVersion, "the row's own version, as the store would have reported it")

	s := e.c.Stats()
	require.EqualValues(t, 1, s.CommitSeqno, "the refused write acked nothing: the check runs before the append")
	require.Equal(t, 1, s.Mutations, "and folded nothing")
	require.Empty(t, e.apply.drains, "and reached no transaction")
	require.Equal(t, StateRunning, e.c.State(),
		"an ordinary condition failure must not halt the shard: that is the divergence this answers")
}

// TestWhatTheWindowDeterminesCostsNoRead: an assertion the window can evaluate
// is evaluated there and only what it hands on is fetched, which is what keeps
// the delegated read from being a round trip per write.
func TestWhatTheWindowDeterminesCostsNoRead(t *testing.T) {
	store := basetest.New()
	ns, wf, run := ids()
	e := asyncEnv(t, store)

	require.NoError(t, e.add(t, mkCreate(ns, wf, run)))
	require.Equal(t, 1, store.Reads().Run, "the create's own assertions stand on the pre-window row")
	require.Equal(t, 1, store.Reads().Current)

	require.NoError(t, e.add(t, mkUpdate(ns, wf, run, 2)))
	require.NoError(t, e.add(t, mkUpdate(ns, wf, run, 3)))
	require.Equal(t, 1, store.Reads().Run, "the window holds the run: nothing more is owed to the cold store")
	require.Equal(t, 1, store.Reads().Current)

	err := e.add(t, mkUpdate(ns, wf, run, 3))
	require.IsType(t, &p.WorkflowConditionFailedError{}, err, "got %T: %v", err, err)
	require.Equal(t, 1, store.Reads().Run, "and an answer from the window costs no read either")

	// A Set writes state and asserts no current row, so the window holds the run
	// and the current-row assertion is delegated on its own.
	ns2, wf2, run2 := ids()
	store.Holds(wf2, run2, 3)
	set := mutation.Mutation{Set: &p.InternalSetWorkflowExecutionRequest{
		ShardID: int32(testShard),
		SetWorkflowSnapshot: p.InternalWorkflowSnapshot{
			NamespaceID: ns2, WorkflowID: wf2, RunID: run2, DBRecordVersion: 4,
		},
	}}
	require.NoError(t, e.add(t, set))
	before := store.Reads().Current
	require.NoError(t, e.add(t, mkUpdate(ns2, wf2, run2, 5)))
	require.Equal(t, before+1, store.Reads().Current,
		"the current row is delegated alone, and confirming it is what keeps this write in the window")
	require.Empty(t, e.apply.drains, "and a confirmed assertion costs no drain")
}

// TestSyncModeAsksTheColdStoreNothing: in sync mode the drain that asserts
// these runs inside the same call and its outcome is the caller's answer, so
// nothing is acked unverified and the delegated read would buy nothing.
func TestSyncModeAsksTheColdStoreNothing(t *testing.T) {
	store := basetest.New()
	ns, wf, run := ids()
	e := newEnv(t, func(c *Config) { c.Sync = true })
	e.useStore(store)

	require.NoError(t, e.add(t, mkCreate(ns, wf, run)))
	require.NoError(t, e.add(t, mkUpdate(ns, wf, run, 2)))
	require.Len(t, e.apply.drains, 2, "sync mode still drains every write")
	require.Zero(t, store.Reads().Run, "a window of one delegates everything, and owes the cold store nothing for it")
	require.Zero(t, store.Reads().Current)
}

// TestAFailingCurrentRowAssertionIsAnsweredBeforeTheAppend: decided here in
// both directions, so a failure is the store's own error, nothing is appended
// and no drain runs. Refusing needs the row's last_write_version, which the
// plugin returns beside the row ([fold.DelegatedCurrent.Verify]).
func TestAFailingCurrentRowAssertionIsAnsweredBeforeTheAppend(t *testing.T) {
	store := basetest.New()
	ns, wf, run := ids()
	store.SetRun(run, 5)
	store.SetRunning(wf, "some-other-run") // the update's current assertion does not hold
	e := asyncEnv(t, store)

	// Something else is in the window first, so a refusal that drained it would
	// show as a drain.
	require.NoError(t, e.add(t, mkCreate(ids())))
	currentReads, execReads := store.Reads().Current, store.Reads().Run

	err := e.add(t, mkUpdate(ns, wf, run, 3))
	failed, ok := err.(*p.CurrentWorkflowConditionFailedError) //nolint:errorlint // the concrete type is the assertion
	require.True(t, ok, "the boundary answered %T, which handleWriteErrorLocked would not recognise: %v", err, err)
	require.Equal(t, "some-other-run", failed.RunID, "the row's own run, as the store would have reported it")

	require.Equal(t, currentReads+1, store.Reads().Current)
	require.Equal(t, execReads, store.Reads().Run,
		"and the run's row is not read: the store reports the first failing assertion, and this was it")

	require.Empty(t, e.apply.drains, "no transaction was asked, and none was needed")
	s := e.c.Stats()
	require.EqualValues(t, 1, s.CommitSeqno, "the refused write acked nothing: the check runs before the append")
	require.Equal(t, 1, s.Mutations, "and folded nothing")
	require.Equal(t, StateRunning, e.c.State(),
		"a condition its caller was told about is not a divergence this process owns")

	// And the shard keeps writing.
	require.NoError(t, e.add(t, mkCreate(ids())))
}

// mkReuseCreate is a start over a workflow whose previous run has finished:
// CreateWorkflowModeUpdateCurrent, asserting CurrentEqualsWithVersion.
func mkReuseCreate(ns, wf, run, previous string, previousVersion int64) mutation.Mutation {
	return build.CreateOver(ns, wf, run, previous, previousVersion)
}

// TestAReuseCreateIsDecidedByTheVersionColumn: the same row, run id and
// completed state answer differently by the version column alone, which this
// layer reads only through the version-carrying store extension.
func TestAReuseCreateIsDecidedByTheVersionColumn(t *testing.T) {
	store := basetest.New()
	ns, wf, run := ids()
	store.SetCompleted(wf, "previous-run", 3)
	e := asyncEnv(t, store)

	// The caller is behind: it read the previous run at 2 and the row is at 3.
	// Everything else about the assertion holds.
	err := e.add(t, mkReuseCreate(ns, wf, run, "previous-run", 2))
	failed, ok := err.(*p.CurrentWorkflowConditionFailedError) //nolint:errorlint // the concrete type is the assertion
	require.True(t, ok, "got %T: %v", err, err)
	require.EqualValues(t, 3, failed.LastWriteVersion,
		"the row's own version, which is what api/startworkflow re-issues the create against")
	require.Empty(t, e.apply.drains, "and nothing was asked of a transaction")
	require.Zero(t, e.c.Stats().CommitSeqno)

	// The same row at the version the caller carries: an ordinary write that
	// stays in the window.
	_, wf2, run2 := ids()
	store.SetCompleted(wf2, "previous-run", 3)
	require.NoError(t, e.add(t, mkReuseCreate(ns, wf2, run2, "previous-run", 3)))
	require.Empty(t, e.apply.drains, "a confirmed assertion costs no drain: this is the collapse the settle used to spend")
	require.EqualValues(t, 1, e.c.Stats().CommitSeqno)
	require.Equal(t, 1, e.c.Stats().Mutations, "and the mutation is in the window, not behind it")
}

// TestAReuseCreateOverAWorkflowWithNoCurrentRowIsRefused is the third answer to
// the same assertion, with no row to build a payload from: a bare "must exist",
// as the store gives.
func TestAReuseCreateOverAWorkflowWithNoCurrentRowIsRefused(t *testing.T) {
	store := basetest.New()
	ns, wf, run := ids()
	e := asyncEnv(t, store)

	err := e.add(t, mkReuseCreate(ns, wf, run, "previous-run", 3))
	failed, ok := err.(*p.CurrentWorkflowConditionFailedError) //nolint:errorlint // the concrete type is the assertion
	require.True(t, ok, "got %T: %v", err, err)
	require.Equal(t, "must exist", failed.Msg)
	require.Zero(t, e.c.Stats().CommitSeqno)
}

// TestSyncModeDecidesNothingHereEither: the same rule on the reuse-create
// shape, where a change that verified everywhere regardless of mode would land
// first. The transaction answers the caller, so nothing is read here.
func TestSyncModeDecidesNothingHereEither(t *testing.T) {
	store := basetest.New()
	ns, wf, run := ids()
	store.SetCompleted(wf, "previous-run", 3)
	e := newEnv(t, func(c *Config) { c.Sync = true })
	e.useStore(store)

	require.NoError(t, e.add(t, mkReuseCreate(ns, wf, run, "previous-run", 2)))
	require.Equal(t, []string{"sync"}, e.tagged("wal_drains", "trigger"))
	require.Zero(t, store.Reads().Current, "and no read was taken to decide it")
}

// TestAColdStoreThatCannotBeReadFailsTheWrite: the authority cannot verify what
// it cannot read, so the write fails with the store's own error, unwrapped. The
// sequential path does the same: an assertion it cannot read does not commit.
func TestAColdStoreThatCannotBeReadFailsTheWrite(t *testing.T) {
	store := basetest.New()
	unavailable := error(serviceerror.NewUnavailable("the cold store is unavailable"))
	store.FailAll(unavailable)
	ns, wf, run := ids()
	e := asyncEnv(t, store)

	err := e.add(t, mkCreate(ns, wf, run))
	require.True(t, err == unavailable, //nolint:errorlint // identity is the assertion
		"the store's own failure must reach the boundary untouched, got %T: %v", err, err)
	require.Zero(t, e.c.Stats().CommitSeqno, "and nothing was acked on a condition nobody could check")
}

// TestAWindowThatCannotAnswerRefusesBeforeTheAppend: an assertion neither
// source can decide takes the accumulator's drain-and-retry, and the mutation
// heads a fresh window whose transaction asserts it.
func TestAWindowThatCannotAnswerRefusesBeforeTheAppend(t *testing.T) {
	store := basetest.New()
	ns, wf, run := ids()
	e := asyncEnv(t, store)

	// The store's delete-current is a guarded no-op rather than an assertion, so
	// the row it leaves is neither the window's write nor the pre-window row.
	require.NoError(t, e.add(t, mutation.Mutation{DeleteCurrent: &p.DeleteCurrentWorkflowExecutionRequest{
		ShardID:     int32(testShard),
		NamespaceID: ns,
		WorkflowID:  wf,
		RunID:       run,
	}}))
	require.Empty(t, e.apply.drains)

	// So a create of that workflow is answerable by neither: reading the base row
	// would read a row this window is about to remove.
	_, _, run2 := ids()
	require.NoError(t, e.add(t, mkCreate(ns, wf, run2)))
	require.Len(t, e.apply.drains, 1, "the refusal drained the window, and the create heads the next one")
	require.Equal(t, []string{"refusal"}, e.tagged("wal_drains", "trigger"))
	require.Equal(t, 1, e.c.Stats().Refusals)
}

// TestTheBoundIsAnsweredBeforeTheCondition: I10 first. The bound is also
// checked outside the loop, so a shard whose applier is stuck refuses its
// writers rather than parking them behind it; a full tail answers
// ResourceExhausted whatever the condition would have said.
func TestTheBoundIsAnsweredBeforeTheCondition(t *testing.T) {
	store := basetest.New()
	ns, wf, run := ids()
	e := newEnv(t, func(c *Config) {
		c.Sync = false
		c.Mutations, c.Bytes, c.Age = 1<<30, 1<<30, time.Hour
		c.HardMaxEntries = 1
	})
	e.useStore(store)

	require.NoError(t, e.add(t, mkCreate(ns, wf, run)))
	reads := store.Reads().Run + store.Reads().Current

	// Stale by any reading (the window holds the run at 1), but the tail is full.
	err := e.add(t, mkUpdate(ns, wf, run, 1))
	var exhausted *serviceerror.ResourceExhausted
	require.True(t, errors.As(err, &exhausted), "expected I10's refusal, got %T: %v", err, err)
	require.Equal(t, reads, store.Reads().Run+store.Reads().Current,
		"and no cold store was troubled for a write that was never going to be taken")
}
