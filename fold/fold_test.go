package fold_test

// Unit tests for the fold rules, one rule at a time; nothing here compares a
// folded store against one the same stream was applied to mutation by
// mutation. Requests are built by hand: every blob but the current row's
// execution state is opaque to fold, so a two-byte blob exercises the same rule
// a real one would. That one is read back, which is what overlay_test.go's
// withRealState builds.

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"

	"github.com/aromanovich/waltz/fold"
	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/wal"
)

const (
	shard wal.ShardID = 7
	nsID              = "ns-1"
	wfID              = "wf-1"
	runX              = "run-x"
	runY              = "run-y"
)

func blob(s string) *commonpb.DataBlob {
	return &commonpb.DataBlob{Data: []byte(s)}
}

func task(name string) p.InternalHistoryTask {
	return p.InternalHistoryTask{Blob: blob(name)}
}

func taskNames(m map[tasks.Category][]p.InternalHistoryTask) []string {
	var names []string
	for _, t := range m[tasks.CategoryTransfer] {
		names = append(names, string(t.Blob.Data))
	}
	return names
}

// mkUpdate builds an update of one run at one version, with the scalar fields
// a fold moves stamped so data-from-tail is observable. ExecutionState must be
// set: the fold dereferences it, so a fixture without one panics.
func mkUpdate(run string, version int64, opts ...func(*p.InternalWorkflowMutation)) mutation.Mutation {
	req := &p.InternalUpdateWorkflowExecutionRequest{
		ShardID: int32(shard),
		Mode:    p.UpdateWorkflowModeUpdateCurrent,
		UpdateWorkflowMutation: p.InternalWorkflowMutation{
			NamespaceID:        nsID,
			WorkflowID:         wfID,
			RunID:              run,
			DBRecordVersion:    version,
			NextEventID:        version * 10,
			ExecutionInfoBlob:  blob(fmt.Sprintf("info-v%d", version)),
			ExecutionStateBlob: blob(fmt.Sprintf("state-v%d", version)),
			ExecutionState:     &persistencespb.WorkflowExecutionState{RunId: run},
		},
	}
	for _, opt := range opts {
		opt(&req.UpdateWorkflowMutation)
	}
	return mutation.Mutation{Update: req}
}

func upsertActivity(id int64, value string) func(*p.InternalWorkflowMutation) {
	return func(m *p.InternalWorkflowMutation) {
		if m.UpsertActivityInfos == nil {
			m.UpsertActivityInfos = map[int64]*commonpb.DataBlob{}
		}
		m.UpsertActivityInfos[id] = blob(value)
	}
}

func deleteActivity(id int64) func(*p.InternalWorkflowMutation) {
	return func(m *p.InternalWorkflowMutation) {
		if m.DeleteActivityInfos == nil {
			m.DeleteActivityInfos = map[int64]struct{}{}
		}
		m.DeleteActivityInfos[id] = struct{}{}
	}
}

func withTask(name string) func(*p.InternalWorkflowMutation) {
	return func(m *p.InternalWorkflowMutation) {
		m.Tasks = map[tasks.Category][]p.InternalHistoryTask{
			tasks.CategoryTransfer: {task(name)},
		}
	}
}

func withBuffered(name string) func(*p.InternalWorkflowMutation) {
	return func(m *p.InternalWorkflowMutation) { m.NewBufferedEvents = blob(name) }
}

func withClearBuffered() func(*p.InternalWorkflowMutation) {
	return func(m *p.InternalWorkflowMutation) { m.ClearBufferedEvents = true }
}

// snapshot is the external tests' fixture. authority_derivation_test.go, in the
// internal package beside this one, has a snapshot of its own under the same
// name and with different fields — it carries a LastWriteVersion and no
// NextEventID, because what it feeds is the assertion derivation rather than the
// fold. A case moved between the two files changes meaning without changing.
func snapshot(run string, version int64) p.InternalWorkflowSnapshot {
	return p.InternalWorkflowSnapshot{
		NamespaceID:        nsID,
		WorkflowID:         wfID,
		RunID:              run,
		DBRecordVersion:    version,
		NextEventID:        version * 10,
		ExecutionInfoBlob:  blob(fmt.Sprintf("info-v%d", version)),
		ExecutionStateBlob: blob(fmt.Sprintf("state-v%d", version)),
		ExecutionState:     &persistencespb.WorkflowExecutionState{RunId: run},
	}
}

func mkCreate(run string, opts ...func(*p.InternalWorkflowSnapshot)) mutation.Mutation {
	req := &p.InternalCreateWorkflowExecutionRequest{
		ShardID:             int32(shard),
		Mode:                p.CreateWorkflowModeBrandNew,
		NewWorkflowSnapshot: snapshot(run, 1),
	}
	for _, opt := range opts {
		opt(&req.NewWorkflowSnapshot)
	}
	return mutation.Mutation{Create: req}
}

func snapActivity(id int64, value string) func(*p.InternalWorkflowSnapshot) {
	return func(s *p.InternalWorkflowSnapshot) {
		if s.ActivityInfos == nil {
			s.ActivityInfos = map[int64]*commonpb.DataBlob{}
		}
		s.ActivityInfos[id] = blob(value)
	}
}

func snapTask(name string) func(*p.InternalWorkflowSnapshot) {
	return func(s *p.InternalWorkflowSnapshot) {
		s.Tasks = map[tasks.Category][]p.InternalHistoryTask{
			tasks.CategoryTransfer: {task(name)},
		}
	}
}

func mkSet(run string, version int64, opts ...func(*p.InternalWorkflowSnapshot)) mutation.Mutation {
	req := &p.InternalSetWorkflowExecutionRequest{
		ShardID:             int32(shard),
		SetWorkflowSnapshot: snapshot(run, version),
	}
	for _, opt := range opts {
		opt(&req.SetWorkflowSnapshot)
	}
	return mutation.Mutation{Set: req}
}

func mkConflictResolve(run string, version int64, opts ...func(*p.InternalWorkflowSnapshot)) mutation.Mutation {
	req := &p.InternalConflictResolveWorkflowExecutionRequest{
		ShardID:               int32(shard),
		Mode:                  p.ConflictResolveWorkflowModeUpdateCurrent,
		ResetWorkflowSnapshot: snapshot(run, version),
	}
	for _, opt := range opts {
		opt(&req.ResetWorkflowSnapshot)
	}
	return mutation.Mutation{ConflictResolve: req}
}

func mkDelete(run string) mutation.Mutation {
	return mutation.Mutation{Delete: &p.DeleteWorkflowExecutionRequest{
		ShardID: int32(shard), NamespaceID: nsID, WorkflowID: wfID, RunID: run,
	}}
}

func mkDeleteCurrent(run string) mutation.Mutation {
	return mutation.Mutation{DeleteCurrent: &p.DeleteCurrentWorkflowExecutionRequest{
		ShardID: int32(shard), NamespaceID: nsID, WorkflowID: wfID, RunID: run,
	}}
}

// add feeds mutations in order, numbering seqnos from 1.
func add(t *testing.T, a *fold.Accumulator, ms ...mutation.Mutation) {
	t.Helper()
	for i, m := range ms {
		require.NoError(t, a.Add(wal.Seqno(i+1), m), "mutation %d (%s)", i, m.Kind())
	}
}

// TestHeadTailRule: a window of updates v2..v4 onto a base row at v1 must
// assert 1 and write 4.
func TestHeadTailRule(t *testing.T) {
	a := fold.New(shard)
	add(t, a,
		mkUpdate(runX, 2),
		mkUpdate(runX, 3),
		mkUpdate(runX, 4),
	)

	batch := a.Drain()
	out, stats := reqs(batch), batch.Stats()
	require.Len(t, out, 1)
	require.Equal(t, mutation.KindUpdate, out[0].Request.Kind())

	merged := out[0].Request.Update.UpdateWorkflowMutation
	require.Equal(t, int64(4), merged.DBRecordVersion, "data comes from the tail")
	require.Equal(t, int64(40), merged.NextEventID)
	require.Equal(t, "info-v4", string(merged.ExecutionInfoBlob.Data))

	require.Equal(t, fold.RunAssertion{BaseVersion: 1}, out[0].RunAssertions()[runX],
		"the assertion comes from the head: the base row is at v1")
	require.Equal(t, 3, stats.MutationsIn)
	require.Equal(t, 1, stats.DirtyWorkflows)
}

// TestUpsertVsDeleteResolvedPerKey: a key must never be in both sets, because
// the store issues every upsert before every delete, so a key re-upserted after
// being deleted would be written and then deleted again.
func TestUpsertVsDeleteResolvedPerKey(t *testing.T) {
	t.Run("delete after upsert wins", func(t *testing.T) {
		a := fold.New(shard)
		add(t, a,
			mkUpdate(runX, 2, upsertActivity(1, "a"), upsertActivity(2, "b")),
			mkUpdate(runX, 3, deleteActivity(1)),
		)
		out := reqs(a.Drain())
		merged := out[0].Request.Update.UpdateWorkflowMutation
		require.NotContains(t, merged.UpsertActivityInfos, int64(1),
			"a key upserted and then deleted must leave the upsert set: a request carrying both "+
				"leaves the outcome to the store's statement order rather than to the window")
		require.Contains(t, merged.DeleteActivityInfos, int64(1))
		require.Contains(t, merged.UpsertActivityInfos, int64(2))
	})

	t.Run("upsert after delete wins", func(t *testing.T) {
		a := fold.New(shard)
		add(t, a,
			mkUpdate(runX, 2, deleteActivity(5)),
			mkUpdate(runX, 3, upsertActivity(5, "reborn")),
		)
		out := reqs(a.Drain())
		merged := out[0].Request.Update.UpdateWorkflowMutation
		require.Contains(t, merged.UpsertActivityInfos, int64(5))
		require.NotContains(t, merged.DeleteActivityInfos, int64(5))
	})
}

// TestSnapshotBarrierCreate is I8: a Create followed by updates emits a Create
// carrying the tail state, asserting absence.
func TestSnapshotBarrierCreate(t *testing.T) {
	a := fold.New(shard)
	add(t, a,
		mkCreate(runX, snapActivity(1, "a"), snapTask("t-create")),
		mkUpdate(runX, 2, upsertActivity(2, "b"), withTask("t-upd")),
		mkUpdate(runX, 3, deleteActivity(1)),
	)

	batch := a.Drain()
	out := reqs(batch)
	require.Len(t, out, 1, "one merged request per dirty workflow")
	require.Equal(t, mutation.KindCreate, out[0].Request.Kind())

	snap := out[0].Request.Create.NewWorkflowSnapshot
	require.Equal(t, int64(3), snap.DBRecordVersion, "data from the tail")
	require.Contains(t, snap.ActivityInfos, int64(2))
	require.NotContains(t, snap.ActivityInfos, int64(1), "deleted by the tail of the window")
	require.Equal(t, []string{"t-create", "t-upd"}, taskNames(snap.Tasks),
		"tasks are queue entries: they concatenate rather than collapse")

	require.Equal(t, fold.RunAssertion{MustNotExist: true}, out[0].RunAssertions()[runX],
		"a Create asserts absence, so the fold needs no rewriting")
	require.Equal(t, &fold.CurrentAssertion{Kind: fold.CurrentMustNotExist}, out[0].Workflow().Current)
}

// TestSnapshotBarrierConflictResolve: a snapshot-bearing request resets the
// accumulator, its tasks survive, and the head assertion stays the window's.
func TestSnapshotBarrierConflictResolve(t *testing.T) {
	a := fold.New(shard)
	add(t, a,
		mkUpdate(runX, 2, upsertActivity(42, "doomed"), withTask("t-upd")),
		mkConflictResolve(runX, 5, snapActivity(7, "reset"), snapTask("t-reset")),
		mkUpdate(runX, 6, upsertActivity(8, "after")),
	)

	out := reqs(a.Drain())
	require.Len(t, out, 1)
	require.Equal(t, mutation.KindConflictResolve, out[0].Request.Kind())

	reset := out[0].Request.ConflictResolve.ResetWorkflowSnapshot
	require.NotContains(t, reset.ActivityInfos, int64(42), "the barrier discards the superseded delta")
	require.Contains(t, reset.ActivityInfos, int64(7))
	require.Contains(t, reset.ActivityInfos, int64(8), "an update after the barrier folds into the snapshot")
	require.Equal(t, int64(6), reset.DBRecordVersion)
	require.Equal(t, []string{"t-upd", "t-reset"}, taskNames(reset.Tasks))

	require.Equal(t, fold.RunAssertion{BaseVersion: 1}, out[0].RunAssertions()[runX],
		"the head of the window is still the first update")
}

// TestAResolveMergingACurrentMutationAssertsTheRunItNames: a conflict-resolve
// whose current mutation lands on a run the window already holds merges that
// run's pending update under its own envelope and points
// CurrentWorkflowMutation at the merged one. The merge carries the arriving
// mutation's execution state across, so the head-of-window current assertion is
// the same on both sides of it — which is what lets the resolve's current-row
// facts be derived once, before anything merges, like every other kind's.
//
// The prior update ignores the current row, so that the resolve's assertion is
// the head rather than a claim the window already holds.
func TestAResolveMergingACurrentMutationAssertsTheRunItNames(t *testing.T) {
	resolve := func() mutation.Mutation {
		m := mkConflictResolve(runX, 5)
		cur := mkUpdate(runY, 3).Update.UpdateWorkflowMutation
		m.ConflictResolve.CurrentWorkflowMutation = &cur
		return m
	}
	ignoreCurrent := func(run string, version int64) mutation.Mutation {
		m := mkUpdate(run, version)
		m.Update.Mode = p.UpdateWorkflowModeIgnoreCurrent
		return m
	}

	alone := fold.New(shard)
	add(t, alone, resolve())

	merged := fold.New(shard)
	add(t, merged, ignoreCurrent(runY, 2), resolve())

	want := &fold.CurrentAssertion{Kind: fold.CurrentEquals, RunID: runY}
	require.Equal(t, want, wr(alone.Drain(), 0).Current,
		"the store asserts against the mutated current run, not the reset one")
	require.Equal(t, want, wr(merged.Drain(), 0).Current,
		"the merge replaced the request's current mutation and the assertion did not move with it")
}

// TestSnapshotBarrierSet: Set resets the accumulator the same way.
func TestSnapshotBarrierSet(t *testing.T) {
	a := fold.New(shard)
	add(t, a,
		mkUpdate(runX, 2, upsertActivity(42, "doomed"), withTask("t-upd")),
		mkSet(runX, 5, snapActivity(7, "set"), snapTask("t-set")),
	)

	out := reqs(a.Drain())
	require.Len(t, out, 1)
	require.Equal(t, mutation.KindSet, out[0].Request.Kind())

	snap := out[0].Request.Set.SetWorkflowSnapshot
	require.NotContains(t, snap.ActivityInfos, int64(42))
	require.Contains(t, snap.ActivityInfos, int64(7))
	require.Equal(t, []string{"t-upd", "t-set"}, taskNames(snap.Tasks))
	require.Equal(t, fold.RunAssertion{BaseVersion: 1}, out[0].RunAssertions()[runX])

	// A Set heading its window asserts one below the version it writes.
	b := fold.New(shard)
	add(t, b, mkSet(runX, 5))
	out = reqs(b.Drain())
	require.Equal(t, fold.RunAssertion{BaseVersion: 4}, out[0].RunAssertions()[runX])
}

// TestSetOntoCreateStaysCreate: a snapshot superseding a snapshot replaces
// content under the head's envelope, so Create's current-row upsert is not lost
// to a Set that carries none.
func TestSetOntoCreateStaysCreate(t *testing.T) {
	a := fold.New(shard)
	add(t, a,
		mkCreate(runX, snapTask("t-create")),
		mkSet(runX, 4, snapActivity(9, "set"), snapTask("t-set")),
	)

	out := reqs(a.Drain())
	require.Len(t, out, 1)
	require.Equal(t, mutation.KindCreate, out[0].Request.Kind(),
		"the Set's content rides under the Create's envelope")
	snap := out[0].Request.Create.NewWorkflowSnapshot
	require.Contains(t, snap.ActivityInfos, int64(9))
	require.Equal(t, int64(4), snap.DBRecordVersion)
	require.Equal(t, []string{"t-create", "t-set"}, taskNames(snap.Tasks))
	require.Equal(t, fold.RunAssertion{MustNotExist: true}, out[0].RunAssertions()[runX])
}

// TestBufferedEventsDoNotMerge: one NewBufferedEvents slot per mutation, so two
// mutations come out as two batches and the merged request's own slot is nil.
func TestBufferedEventsDoNotMerge(t *testing.T) {
	a := fold.New(shard)
	add(t, a,
		mkUpdate(runX, 2, withBuffered("batch-1")),
		mkUpdate(runX, 3, withBuffered("batch-2")),
	)

	out := reqs(a.Drain())
	require.Len(t, out, 1)
	require.Len(t, out[0].BufferedBatches, 2, "two mutations, two batches")
	require.Equal(t, "batch-1", string(out[0].BufferedBatches[0].Blob.Data))
	require.Equal(t, "batch-2", string(out[0].BufferedBatches[1].Blob.Data))
	require.Equal(t, []string{runX, runX}, []string{out[0].BufferedBatches[0].RunID, out[0].BufferedBatches[1].RunID},
		"each batch names the run that accumulated it: apply writes the row from it")
	require.Nil(t, out[0].Request.Update.UpdateWorkflowMutation.NewBufferedEvents,
		"the merged mutation's slot stays empty; apply hands the batches over one at a time")
}

// TestClearBufferedEventsDropsEarlierBatches: the clear drops the batches
// before it in the window and keeps the flag, so the store's pre-window rows
// go too.
func TestClearBufferedEventsDropsEarlierBatches(t *testing.T) {
	a := fold.New(shard)
	add(t, a,
		mkUpdate(runX, 2, withBuffered("stale")),
		mkUpdate(runX, 3, withClearBuffered(), withBuffered("fresh")),
	)

	out := reqs(a.Drain())
	require.Len(t, out[0].BufferedBatches, 1)
	require.Equal(t, "fresh", string(out[0].BufferedBatches[0].Blob.Data))
	require.True(t, out[0].Request.Update.UpdateWorkflowMutation.ClearBufferedEvents)
}

// TestTombstone: a deletion collapses the run's pending state into the
// Delete; the collapsed mutations' tasks survive as orphans (I7).
func TestTombstone(t *testing.T) {
	a := fold.New(shard)
	add(t, a,
		mkUpdate(runX, 2, upsertActivity(1, "a"), withTask("t-upd")),
		mkDelete(runX),
	)

	err := a.Add(3, mkUpdate(runX, 3))
	require.ErrorIs(t, err, fold.ErrAfterTombstone)

	// Deleting an absent row succeeds sequentially, so a second delete is a
	// no-op rather than an error.
	require.NoError(t, a.Add(4, mkDelete(runX)))

	batch := a.Drain()
	out, stats := reqs(batch), batch.Stats()
	require.Len(t, out, 1, "the update collapsed into the tombstone")
	require.Equal(t, mutation.KindDelete, out[0].Request.Kind())
	require.Equal(t, []string{"t-upd"}, taskNames(out[0].OrphanedTasks()),
		"a task is in the WAL tail or in the store (I7): the fold may not lose it")
	require.Equal(t, fold.RunAssertion{BaseVersion: 1}, out[0].RunAssertions()[runX])
	require.Equal(t, 3, stats.MutationsIn, "the failed add does not count; the idempotent delete does")
}

// TestCreateBehindTombstone: the only mutation that gives a tombstoned run row
// state again. A second delete of that run is an idempotent no-op, and neither
// the current row nor the task kinds are checked against a run's tombstone at
// all. Both requests emit in window order, and the Create keeps the
// head-of-window run assertion, because at apply time the pre-window row is
// still there.
func TestCreateBehindTombstone(t *testing.T) {
	a := fold.New(shard)
	add(t, a,
		mkUpdate(runX, 2),
		mkDelete(runX),
		mkCreate(runX),
	)

	out := reqs(a.Drain())
	require.Len(t, out, 2)
	require.Equal(t, mutation.KindDelete, out[0].Request.Kind())
	require.Equal(t, mutation.KindCreate, out[1].Request.Kind())
	require.Less(t, out[0].TailSeqno, out[1].TailSeqno, "apply must keep this order")
	require.Equal(t, fold.RunAssertion{BaseVersion: 1}, out[1].RunAssertions()[runX],
		"the head of the window asserted v1, and that is what holds at apply time")
}

// TestContinueAsNewFoldsIntoOneRequest: one request owns both runs, and a later
// update of the new run folds into its snapshot part.
func TestContinueAsNewFoldsIntoOneRequest(t *testing.T) {
	can := mkUpdate(runX, 3)
	newSnap := snapshot(runY, 1)
	can.Update.NewWorkflowSnapshot = &newSnap

	a := fold.New(shard)
	add(t, a,
		mkUpdate(runX, 2),
		can,
		mkUpdate(runY, 2, upsertActivity(4, "in-new-run")),
	)

	batch := a.Drain()
	out, stats := reqs(batch), batch.Stats()
	require.Len(t, out, 1, "one request owns both runs")
	req := out[0].Request.Update
	require.NotNil(t, req.NewWorkflowSnapshot)
	require.Contains(t, req.NewWorkflowSnapshot.ActivityInfos, int64(4),
		"the new run's update folded into the snapshot part")
	require.Equal(t, fold.RunAssertion{BaseVersion: 1}, out[0].RunAssertions()[runX])
	require.Equal(t, fold.RunAssertion{MustNotExist: true}, out[0].RunAssertions()[runY])
	require.Equal(t, 3.0, stats.CollapseRatio())
}

// TestRefusalLeavesAccumulatorUnchanged: a delete that would strip one run out
// of a two-run request is refused without side effects, so the caller can drain
// what folded and retry on a fresh window.
func TestRefusalLeavesAccumulatorUnchanged(t *testing.T) {
	can := mkUpdate(runX, 2)
	newSnap := snapshot(runY, 1)
	can.Update.NewWorkflowSnapshot = &newSnap

	a := fold.New(shard)
	add(t, a, can)

	err := a.Add(2, mkDelete(runX))
	require.ErrorIs(t, err, fold.ErrRefused)

	batch := a.Drain()
	out, stats := reqs(batch), batch.Stats()
	require.Len(t, out, 1)
	require.Equal(t, mutation.KindUpdate, out[0].Request.Kind())
	require.NotNil(t, out[0].Request.Update.NewWorkflowSnapshot, "the window is exactly as before the refusal")
	require.Equal(t, 1, stats.MutationsIn)

	// The recovery: on the next window the same delete heads the accumulator.
	require.NoError(t, a.Add(2, mkDelete(runX)))
	out = reqs(a.Drain())
	require.Len(t, out, 1)
	require.Equal(t, mutation.KindDelete, out[0].Request.Kind())
}

// TestCurrentAssertionAfterDeleteCurrentRefuses: delete-current removes the row
// only if the row names its run, so a window holding one determines nothing
// about the row and a later mutation asserting on it is refused. Synthesising
// current==run for the delete instead would turn a legal sequential no-op into
// a false invariant violation.
func TestCurrentAssertionAfterDeleteCurrentRefuses(t *testing.T) {
	a := fold.New(shard)
	require.NoError(t, a.Add(1, mkDeleteCurrent(runX)))

	err := a.Add(2, mkCreate(runY))
	require.ErrorIs(t, err, fold.ErrRefused,
		"a create's current-row assertion cannot stand on a window a delete-current already modified")

	// The recovery: drain, and the refused mutation heads the next window.
	batch := a.Drain()
	out := reqs(batch)
	require.Len(t, out, 1)
	require.Equal(t, mutation.KindDeleteCurrent, out[0].Request.Kind())
	require.Nil(t, out[0].Workflow().Current, "a guarded delete-current asserts nothing")
	require.Nil(t, out[0].Workflow().CurrentWrite, "and writes nothing — the removal rides the request itself")

	require.NoError(t, a.Add(2, mkCreate(runY)))
	batch = a.Drain()
	out = reqs(batch)
	require.Len(t, out, 1)
	require.Equal(t, &fold.CurrentAssertion{Kind: fold.CurrentMustNotExist}, out[0].Workflow().Current,
		"at the fresh window's head the delete has landed, and brand-new asserts absence honestly")
}

// TestCollapseRatio: mutations in over dirty workflows out, before and after a
// drain, which resets the window.
func TestCollapseRatio(t *testing.T) {
	a := fold.New(shard)
	add(t, a,
		mkUpdate(runX, 2),
		mkUpdate(runX, 3),
		mkUpdate(runX, 4),
		mkUpdate(runX, 5),
		mkUpdate(runY, 2, inWorkflow("wf-2")),
		mkUpdate(runY, 3, inWorkflow("wf-2")),
	)

	require.Equal(t, fold.Stats{MutationsIn: 6, DirtyWorkflows: 2}, a.Stats())
	require.Equal(t, 3.0, a.Stats().CollapseRatio())

	batch := a.Drain()
	out, stats := reqs(batch), batch.Stats()
	require.Len(t, out, 2)
	require.Equal(t, 3.0, stats.CollapseRatio(), "Drain reports the window it emitted")
	require.Equal(t, fold.Stats{}, a.Stats(), "the drain resets the window")
}

// TestAddValidation: one shard per accumulator, strictly increasing seqnos
// across drains, exactly one request per mutation.
func TestAddValidation(t *testing.T) {
	a := fold.New(shard)
	require.NoError(t, a.Add(5, mkUpdate(runX, 2)))

	require.Error(t, a.Add(5, mkUpdate(runX, 3)), "a seqno cannot repeat")
	require.Error(t, a.Add(4, mkUpdate(runX, 3)), "a seqno cannot go back")
	require.Error(t, a.Add(6, mutation.Mutation{}), "an empty mutation holds no request")

	wrongShard := mkUpdate(runX, 3)
	wrongShard.Update.ShardID = int32(shard) + 1
	require.Error(t, a.Add(6, wrongShard))

	a.Drain()
	require.Error(t, a.Add(5, mkUpdate(runX, 3)), "the seqno floor survives the drain: one accumulator, one log")
	require.NoError(t, a.Add(6, mkUpdate(runX, 3)))
}
