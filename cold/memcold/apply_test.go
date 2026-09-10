package memcold_test

// What one drain does to a real database. Every case here is about the
// all-or-nothing part rather than about row layout, which the four conformance
// suites already judge: a drain that commits leaves its rows and its watermark,
// and a drain that fails leaves neither — including the rows the statements
// before the failing one had already written.

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/tasks"

	"github.com/aromanovich/waltz/apply"
	"github.com/aromanovich/waltz/cold/memcold"
	"github.com/aromanovich/waltz/fold"
	"github.com/aromanovich/waltz/internal/verify/mutbuild"
	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/wal"
)

func TestADrainCommitsItsRowsAndItsWatermark(t *testing.T) {
	h := newDrains(t)
	wf, run := uuid.NewString(), uuid.NewString()

	batch := h.fold(h.create(wf, run))
	require.NoError(t, h.store.Apply(h.ctx, h.shard, h.epoch, batch))

	require.EqualValues(t, 1, h.runVersion(wf, run), "the run row is at the version the create wrote")
	current, _ := h.current(wf)
	require.NotNil(t, current)
	require.Equal(t, run, current.RunID)
	require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, current.ExecutionState.State)

	seqno, ok := h.watermark()
	require.True(t, ok)
	require.Equal(t, batch.Watermark(), seqno, "the drain's own position, committed with its rows")
}

func TestSeveralWorkflowsLandTogether(t *testing.T) {
	h := newDrains(t)
	wfs := [][2]string{
		{uuid.NewString(), uuid.NewString()},
		{uuid.NewString(), uuid.NewString()},
		{uuid.NewString(), uuid.NewString()},
	}

	batch := h.fold(
		h.create(wfs[0][0], wfs[0][1]),
		h.create(wfs[1][0], wfs[1][1]),
		h.create(wfs[2][0], wfs[2][1]),
	)
	require.Equal(t, 3, batch.Len())
	require.NoError(t, h.store.Apply(h.ctx, h.shard, h.epoch, batch))

	for _, w := range wfs {
		require.EqualValues(t, 1, h.runVersion(w[0], w[1]), "workflow %s", w[0])
		current, _ := h.current(w[0])
		require.NotNil(t, current, "workflow %s has no current row", w[0])
		require.Equal(t, w[1], current.RunID)
	}
	seqno, ok := h.watermark()
	require.True(t, ok)
	require.Equal(t, batch.Watermark(), seqno, "one watermark for the three of them")
}

// TestAStaleEpochShadowsTheVersionFailureUnderIt is the epoch's position stated
// as behaviour. The drain is stale and also stands on a version the row does
// not hold — which is what being fenced looks like from inside, the new owner
// having moved the row — and the answer has to be the shard rather than the
// version: a halt over a broken invariant is terminal, and this shard's
// invariant is not broken.
func TestAStaleEpochShadowsTheVersionFailureUnderIt(t *testing.T) {
	h := newDrains(t)
	wf, run := uuid.NewString(), uuid.NewString()

	landed := h.fold(h.create(wf, run))
	require.NoError(t, h.store.Apply(h.ctx, h.shard, h.epoch, landed))
	held := h.epoch

	// A new owner takes the shard, which is what a stale epoch means.
	h.bumpRange()

	second := uuid.NewString()
	err := h.store.Apply(h.ctx, h.shard, held, h.fold(h.update(wf, run, 3), h.create(second, uuid.NewString())))
	require.Equal(t, apply.ClassShardLost, apply.Classify(err), "got %v", err)
	require.ErrorAs(t, err, new(*p.ShardOwnershipLostError))

	require.EqualValues(t, 1, h.runVersion(wf, run), "the update must not have landed")
	require.Nil(t, h.mustCurrent(second), "the create beside it must not have landed either")
	seqno, ok := h.watermark()
	require.True(t, ok)
	require.Equal(t, landed.Watermark(), seqno, "the fenced drain moved nothing")
}

func TestAVersionAssertionWritesNothingAndNamesTheRow(t *testing.T) {
	h := newDrains(t)
	wf, run := uuid.NewString(), uuid.NewString()

	landed := h.fold(h.create(wf, run))
	require.NoError(t, h.store.Apply(h.ctx, h.shard, h.epoch, landed))

	// The row is at 1; this update stands on 2.
	err := h.store.Apply(h.ctx, h.shard, h.epoch, h.fold(h.update(wf, run, 3)))
	require.Equal(t, apply.ClassInvariantViolated, apply.Classify(err), "got %v", err)
	require.ErrorAs(t, err, new(*p.WorkflowConditionFailedError),
		"the store's own condition failure must stay reachable under the attribution")

	var violation *apply.InvariantViolationError
	require.ErrorAs(t, err, &violation)
	require.Len(t, violation.Diverged, 1)
	d := violation.Diverged[0]
	require.Equal(t, wf, d.WorkflowID)
	require.Equal(t, run, d.RunID)
	require.EqualValues(t, 2, d.AssertedBase)
	require.EqualValues(t, 1, d.ActualBase)

	require.EqualValues(t, 1, h.runVersion(wf, run), "the refused update must not have moved the row")
	seqno, ok := h.watermark()
	require.True(t, ok)
	require.Equal(t, landed.Watermark(), seqno)
}

// TestTheLastWriteFailingUndoesTheEarlierOnes is what makes "one drain, one
// transaction" a fact rather than a claim: the two creates ahead of the failing
// request are statements that ran and inserted rows, and what must be true
// afterwards is that none of them is there.
func TestTheLastWriteFailingUndoesTheEarlierOnes(t *testing.T) {
	h := newDrains(t)
	wfA, runA := uuid.NewString(), uuid.NewString()
	wfB, runB := uuid.NewString(), uuid.NewString()
	wfC, runC := uuid.NewString(), uuid.NewString()

	// C's update stands on a run row nothing ever created, and its seqno is the
	// highest, so it is the last request the drain drives.
	batch := h.fold(
		h.create(wfA, runA),
		h.create(wfB, runB),
		h.update(wfC, runC, 5),
	)
	require.Equal(t, 3, batch.Len())

	err := h.store.Apply(h.ctx, h.shard, h.epoch, batch)
	require.Equal(t, apply.ClassInvariantViolated, apply.Classify(err), "got %v", err)

	require.Nil(t, h.mustCurrent(wfA), "A's current row survived a rolled-back drain")
	require.Nil(t, h.mustCurrent(wfB), "B's current row survived a rolled-back drain")
	require.False(t, h.runExists(wfA, runA), "A's run row survived a rolled-back drain")
	require.False(t, h.runExists(wfB, runB), "B's run row survived a rolled-back drain")

	_, ok := h.watermark()
	require.False(t, ok, "a drain that wrote nothing may not leave a position behind")
}

// TestARangeDeleteActsBeforeTheDrainsOwnInserts pins the one ordering rule that
// is not visible in the rows: fold keeps a task that arrived after a range
// delete, because the sequential path keeps it, and it only survives if the
// delete runs before the insert. Getting it backwards deletes a timer nobody
// asked to be gone.
func TestARangeDeleteActsBeforeTheDrainsOwnInserts(t *testing.T) {
	h := newDrains(t)
	category := tasks.CategoryTransfer

	stale := h.build.AddTasks(category, immediateTask(10, "stale"))
	require.NoError(t, h.store.Apply(h.ctx, h.shard, h.epoch, h.fold(stale)))
	require.Equal(t, []string{"stale"}, h.transferTasks())

	fresh := h.fold(
		h.build.RangeComplete(category, tasks.NewImmediateKey(0), tasks.NewImmediateKey(100)),
		h.build.AddTasks(category, immediateTask(20, "fresh")),
	)
	require.NoError(t, h.store.Apply(h.ctx, h.shard, h.epoch, fresh))

	require.Equal(t, []string{"fresh"}, h.transferTasks(),
		"the range removed what the store held and left the task written after it")
}

// TestARunTombstonedAndRecreatedInOneWindow is the sequential-statement
// property the applier's doc claims, driven: the window's two requests touch
// one workflow, the current row's assertion is the pre-window one and its write
// is the tail's, and each statement sees what the one before it left.
func TestARunTombstonedAndRecreatedInOneWindow(t *testing.T) {
	h := newDrains(t)
	wf := uuid.NewString()
	first, second := uuid.NewString(), uuid.NewString()

	done := h.build.Create(h.namespaceID, wf, first,
		mutbuild.WithInfoBlob(blob("info")),
		mutbuild.WithState(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED))
	require.NoError(t, h.store.Apply(h.ctx, h.shard, h.epoch, h.fold(done)))

	over := h.build.CreateOver(h.namespaceID, wf, second, first, 0, mutbuild.WithInfoBlob(blob("info")))
	batch := h.fold(h.build.Delete(h.namespaceID, wf, first), over)
	require.NoError(t, h.store.Apply(h.ctx, h.shard, h.epoch, batch))

	require.False(t, h.runExists(wf, first), "the tombstoned run's row is gone")
	require.EqualValues(t, 1, h.runVersion(wf, second))
	current, _ := h.current(wf)
	require.NotNil(t, current)
	require.Equal(t, second, current.RunID, "the current row names the run the window's tail created")
}

// The seven collections a run's rows are spread over reach the database only
// because applyMutation and applySnapshotCollections each name all seven in a
// literal of their own. A collection missing from either is rows this drain
// acknowledged and never wrote — and the watermark commits with the batch, so
// the log is trimmed past them. It is the one failure on this path with nothing
// behind it: a drain that refuses, fails or dies leaves its entries in the log
// for the next owner, and this one does not.
//
// Nothing above catches it. The four conformance suites drive upstream's own 28
// methods, none of which goes through Apply, and no suite in this repository
// compares what a drain wrote against the request it carried. Removing the two
// CHASM lines from applyMutation leaves the whole of `go test ./...` green.
//
// upsertOf is a hand list because what a store will take is not derivable from a
// type — a CHASM node is two blobs, a signal id is a row with no payload of its
// own — and it is held to the type below, in both directions.
var upsertOf = map[string]struct {
	delta    func(*p.InternalWorkflowMutation)
	snapshot func(*p.InternalWorkflowSnapshot)
}{
	"UpsertActivityInfos": {
		func(m *p.InternalWorkflowMutation) {
			m.UpsertActivityInfos = map[int64]*commonpb.DataBlob{1: blob("activity")}
		},
		func(s *p.InternalWorkflowSnapshot) {
			s.ActivityInfos = map[int64]*commonpb.DataBlob{1: blob("activity")}
		},
	},
	"UpsertTimerInfos": {
		func(m *p.InternalWorkflowMutation) {
			m.UpsertTimerInfos = map[string]*commonpb.DataBlob{"timer": blob("timer")}
		},
		func(s *p.InternalWorkflowSnapshot) {
			s.TimerInfos = map[string]*commonpb.DataBlob{"timer": blob("timer")}
		},
	},
	"UpsertChildExecutionInfos": {
		func(m *p.InternalWorkflowMutation) {
			m.UpsertChildExecutionInfos = map[int64]*commonpb.DataBlob{2: blob("child")}
		},
		func(s *p.InternalWorkflowSnapshot) {
			s.ChildExecutionInfos = map[int64]*commonpb.DataBlob{2: blob("child")}
		},
	},
	"UpsertRequestCancelInfos": {
		func(m *p.InternalWorkflowMutation) {
			m.UpsertRequestCancelInfos = map[int64]*commonpb.DataBlob{3: blob("cancel")}
		},
		func(s *p.InternalWorkflowSnapshot) {
			s.RequestCancelInfos = map[int64]*commonpb.DataBlob{3: blob("cancel")}
		},
	},
	"UpsertSignalInfos": {
		func(m *p.InternalWorkflowMutation) {
			m.UpsertSignalInfos = map[int64]*commonpb.DataBlob{4: blob("signal")}
		},
		func(s *p.InternalWorkflowSnapshot) {
			s.SignalInfos = map[int64]*commonpb.DataBlob{4: blob("signal")}
		},
	},
	"UpsertSignalRequestedIDs": {
		func(m *p.InternalWorkflowMutation) {
			m.UpsertSignalRequestedIDs = map[string]struct{}{"signal-id": {}}
		},
		func(s *p.InternalWorkflowSnapshot) {
			s.SignalRequestedIDs = map[string]struct{}{"signal-id": {}}
		},
	},
	"UpsertChasmNodes": {
		func(m *p.InternalWorkflowMutation) {
			m.UpsertChasmNodes = map[string]p.InternalChasmNode{
				"node": {Metadata: blob("metadata"), Data: blob("data")}}
		},
		func(s *p.InternalWorkflowSnapshot) {
			s.ChasmNodes = map[string]p.InternalChasmNode{
				"node": {Metadata: blob("metadata"), Data: blob("data")}}
		},
	},
}

func TestEveryCollectionOfARunReachesTheDatabase(t *testing.T) {
	upserts := 0
	for f := range reflect.TypeFor[p.InternalWorkflowMutation]().Fields() {
		if !strings.HasPrefix(f.Name, "Upsert") {
			continue
		}
		upserts++
		require.Containsf(t, upsertOf, f.Name, "a delta carries %s and nothing here drives it "+
			"through a drain: a collection the applier's literal does not name is rows it "+
			"acknowledged and never wrote, with the log trimmed past them", f.Name)
	}
	require.NotZero(t, upserts, "no field of a delta is named Upsert*: this has judged nothing")
	require.Len(t, upsertOf, upserts, "an entry here for a collection a delta no longer has: "+
		"the read-back below would assert on a field the mutable state does not have")

	t.Run("a delta writes them", func(t *testing.T) {
		h := newDrains(t)
		wf, run := uuid.NewString(), uuid.NewString()
		require.NoError(t, h.store.Apply(h.ctx, h.shard, h.epoch, h.fold(h.create(wf, run))))

		m := h.update(wf, run, 2)
		for _, fill := range upsertOf {
			fill.delta(&m.Update.UpdateWorkflowMutation)
		}
		require.NoError(t, h.store.Apply(h.ctx, h.shard, h.epoch, h.fold(m)))

		requireEveryCollectionHeld(t, h.runState(wf, run))
	})

	t.Run("a snapshot writes them", func(t *testing.T) {
		h := newDrains(t)
		wf, run := uuid.NewString(), uuid.NewString()

		m := h.create(wf, run)
		for _, fill := range upsertOf {
			fill.snapshot(&m.Create.NewWorkflowSnapshot)
		}
		require.NoError(t, h.store.Apply(h.ctx, h.shard, h.epoch, h.fold(m)))

		requireEveryCollectionHeld(t, h.runState(wf, run))
	})
}

// requireEveryCollectionHeld reads the run back through the store's own read and
// requires every collection a drain carried to be there. The field it looks for
// is the upsert's name without its prefix, which is what upstream calls the same
// collection on whole state.
func requireEveryCollectionHeld(t *testing.T, state *p.InternalWorkflowMutableState) {
	t.Helper()
	held := reflect.ValueOf(state).Elem()
	for name := range upsertOf {
		field, _ := strings.CutPrefix(name, "Upsert")
		v := held.FieldByName(field)
		require.Truef(t, v.IsValid(),
			"a delta upserts %s and the mutable state has no %s to read it back from", name, field)
		require.NotZerof(t, v.Len(), "the run holds no %s after a drain that carried one: the "+
			"collection is missing from the applier's literal, so those rows were acknowledged, "+
			"never written, and the log trimmed past them", field)
	}
}

// drains is one store, one shard held at one epoch, and a seqno that only ever
// rises: a second drain reusing the first's seqnos would move the watermark
// backwards, which is a shard that re-folds rather than a test.
type drains struct {
	t     *testing.T
	ctx   context.Context
	store *memcold.Store
	shard wal.ShardID
	epoch wal.Epoch
	build mutbuild.Builder

	shards      p.ShardManager
	info        *persistencespb.ShardInfo
	namespaceID string
	seqno       wal.Seqno
}

func newDrains(t *testing.T) *drains {
	t.Helper()
	ctx := context.Background()
	store := newStore(t)

	const shardID = 1
	shards := p.NewShardManager(store.ShardStore(), serialization.NewSerializer())
	shard, err := shards.GetOrCreateShard(ctx, &p.GetOrCreateShardRequest{
		ShardID:          shardID,
		InitialShardInfo: &persistencespb.ShardInfo{ShardId: shardID, RangeId: 1},
	})
	require.NoError(t, err)

	h := &drains{
		t:           t,
		ctx:         ctx,
		store:       store,
		shard:       shardID,
		build:       mutbuild.For(shardID),
		shards:      shards,
		info:        shard.ShardInfo,
		namespaceID: uuid.NewString(),
	}
	// A drain writes under the range its owner acquired, which is one above
	// what the previous owner left.
	h.bumpRange()
	return h
}

// bumpRange takes the shard at the next range id, which is what a new owner
// does and what makes every epoch below it stale.
func (h *drains) bumpRange() {
	h.t.Helper()
	previous := h.info.RangeId
	h.info.RangeId++
	require.NoError(h.t, h.shards.UpdateShard(h.ctx, &p.UpdateShardRequest{
		ShardInfo:       h.info,
		PreviousRangeID: previous,
	}))
	h.epoch = wal.Epoch(h.info.RangeId)
}

// fold folds the mutations into one batch, at seqnos above every batch this
// fixture has produced before.
func (h *drains) fold(ms ...mutation.Mutation) fold.Batch {
	h.t.Helper()
	acc := fold.New(h.shard)
	for _, m := range ms {
		h.seqno++
		require.NoError(h.t, acc.Add(h.seqno, m))
	}
	return acc.Drain()
}

// create and update carry an execution-info blob, which mutbuild leaves nil
// because every other caller writes to a double. A store dereferences it: a
// request without one is not a shape this layer receives.
func (h *drains) create(workflowID, runID string) mutation.Mutation {
	return h.build.Create(h.namespaceID, workflowID, runID, mutbuild.WithInfoBlob(blob("info")))
}

func (h *drains) update(workflowID, runID string, version int64) mutation.Mutation {
	return h.build.Update(h.namespaceID, workflowID, runID, version,
		func(m *p.InternalWorkflowMutation) { m.ExecutionInfoBlob = blob("info") })
}

func (h *drains) watermark() (wal.Seqno, bool) {
	h.t.Helper()
	seqno, ok, err := h.store.Watermark(h.ctx, h.shard)
	require.NoError(h.t, err)
	return seqno, ok
}

func (h *drains) runVersion(workflowID, runID string) int64 {
	h.t.Helper()
	row, err := h.store.GetWorkflowExecution(h.ctx, &p.GetWorkflowExecutionRequest{
		ShardID: int32(h.shard), NamespaceID: h.namespaceID, WorkflowID: workflowID, RunID: runID,
	})
	require.NoError(h.t, err)
	return row.DBRecordVersion
}

// runState is the whole of what the store holds for one run, read back through
// the store's own read rather than out of the tables.
func (h *drains) runState(workflowID, runID string) *p.InternalWorkflowMutableState {
	h.t.Helper()
	row, err := h.store.GetWorkflowExecution(h.ctx, &p.GetWorkflowExecutionRequest{
		ShardID: int32(h.shard), NamespaceID: h.namespaceID, WorkflowID: workflowID, RunID: runID,
	})
	require.NoError(h.t, err)
	return row.State
}

func (h *drains) runExists(workflowID, runID string) bool {
	h.t.Helper()
	_, err := h.store.GetWorkflowExecution(h.ctx, &p.GetWorkflowExecutionRequest{
		ShardID: int32(h.shard), NamespaceID: h.namespaceID, WorkflowID: workflowID, RunID: runID,
	})
	return err == nil
}

// current is the workflow's current row and its last write version; mustCurrent
// is the same read for a caller that expects there to be none.
func (h *drains) current(workflowID string) (*p.InternalGetCurrentExecutionResponse, int64) {
	h.t.Helper()
	row, version, err := h.store.GetCurrentExecutionWithLastWriteVersion(h.ctx, &p.GetCurrentExecutionRequest{
		ShardID: int32(h.shard), NamespaceID: h.namespaceID, WorkflowID: workflowID,
	})
	require.NoError(h.t, err)
	return row, version
}

func (h *drains) mustCurrent(workflowID string) *p.InternalGetCurrentExecutionResponse {
	h.t.Helper()
	row, _, err := h.store.GetCurrentExecutionWithLastWriteVersion(h.ctx, &p.GetCurrentExecutionRequest{
		ShardID: int32(h.shard), NamespaceID: h.namespaceID, WorkflowID: workflowID,
	})
	if err != nil {
		return nil
	}
	return row
}

// transferTasks names the rows the transfer queue would read, by their blobs.
func (h *drains) transferTasks() []string {
	h.t.Helper()
	page, err := h.store.GetHistoryTasks(h.ctx, &p.GetHistoryTasksRequest{
		ShardID:             int32(h.shard),
		TaskCategory:        tasks.CategoryTransfer,
		InclusiveMinTaskKey: tasks.NewImmediateKey(0),
		ExclusiveMaxTaskKey: tasks.NewImmediateKey(1000),
		BatchSize:           100,
	})
	require.NoError(h.t, err)
	names := make([]string, 0, len(page.Tasks))
	for _, task := range page.Tasks {
		names = append(names, string(task.Blob.Data))
	}
	return names
}

func immediateTask(id int64, name string) p.InternalHistoryTask {
	return p.InternalHistoryTask{Key: tasks.NewImmediateKey(id), Blob: blob(name)}
}

func blob(name string) *commonpb.DataBlob {
	return &commonpb.DataBlob{Data: []byte(name), EncodingType: enumspb.ENCODING_TYPE_PROTO3}
}

// TestACreateOfARunThatExistsFailsAtItsAssertion pins which statement answers a
// duplicate, and it is the reason [Store.Apply] does not translate a failed
// insert into a condition failure of its own.
//
// The drain asserts every run row it touches under the transaction's lock, one
// statement before it writes any of them, so a create whose run is already there
// is answered there — with the row named. An insert reached past that assertion
// and failed anyway is not a duplicate at all: fencing makes this layer the
// shard's only writer, so nothing legitimate put the row in between. Reading it
// as one would hand a definite answer to what the driver reports for a disk that
// is full or an I/O error that interrupted the write, whose transaction may yet
// commit.
func TestACreateOfARunThatExistsFailsAtItsAssertion(t *testing.T) {
	h := newDrains(t)
	wf, run := uuid.NewString(), uuid.NewString()

	require.NoError(t, h.store.Apply(h.ctx, h.shard, h.epoch, h.fold(h.create(wf, run))))

	err := h.store.Apply(h.ctx, h.shard, h.epoch, h.fold(h.create(wf, run)))
	require.Error(t, err)
	require.Equal(t, apply.ClassInvariantViolated, apply.Classify(err),
		"a run that is already there is a divergence and not an ambiguity")

	violation, ok := err.(*apply.InvariantViolationError) //nolint:errorlint // the attribution is the assertion
	require.True(t, ok, "got %T", err)
	require.NotEmpty(t, violation.Diverged, "the assertion names the row it found")
	require.Equal(t, run, violation.Diverged[0].RunID)
}
