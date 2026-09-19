package memcold_test

// What one drain does to a real database. Every case here is about the
// all-or-nothing part rather than about row layout, which the four conformance
// suites already judge: a drain that commits leaves its rows and its watermark,
// and a drain that fails leaves neither — including the rows the statements
// before the failing one had already written.

import (
	"context"
	"maps"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

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

// upsertOf is the entry each of the seven collections carries here, hand-written
// because what a store will take is not derivable from a type: a CHASM node is
// two blobs, a signal id is a row with no payload of its own. There is no second
// list for the snapshot arm — a snapshot's field is the upsert's name without
// its prefix, at the same type — and none for the read-back, which is held
// against these same values.
//
// Why a collection missing from the applier's literal is the one failure on this
// path with nothing behind it is in .claude/rules/cold.md.
var upsertOf = map[string]any{
	"UpsertActivityInfos":       map[int64]*commonpb.DataBlob{1: blob("activity")},
	"UpsertTimerInfos":          map[string]*commonpb.DataBlob{"timer": blob("timer")},
	"UpsertChildExecutionInfos": map[int64]*commonpb.DataBlob{2: blob("child")},
	"UpsertRequestCancelInfos":  map[int64]*commonpb.DataBlob{3: blob("cancel")},
	"UpsertSignalInfos":         map[int64]*commonpb.DataBlob{4: blob("signal")},
	"UpsertSignalRequestedIDs":  map[string]struct{}{"signal-id": {}},
	"UpsertChasmNodes": map[string]p.InternalChasmNode{
		"node": {Metadata: blob("metadata"), Data: blob("data")},
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

		update := h.update(wf, run, 2, func(m *p.InternalWorkflowMutation) {
			fillEveryCollection(t, m, func(name string) string { return name })
		})
		require.NoError(t, h.store.Apply(h.ctx, h.shard, h.epoch, h.fold(update)))

		requireEveryCollectionHeld(t, h.runState(wf, run))
	})

	t.Run("a snapshot writes them", func(t *testing.T) {
		h := newDrains(t)
		wf, run := uuid.NewString(), uuid.NewString()

		create := h.create(wf, run, func(s *p.InternalWorkflowSnapshot) {
			fillEveryCollection(t, s, wholeStateField)
		})
		require.NoError(t, h.store.Apply(h.ctx, h.shard, h.epoch, h.fold(create)))

		requireEveryCollectionHeld(t, h.runState(wf, run))
	})
}

// wholeStateField is what a delta's Upsert collection is called everywhere that
// holds whole state — a snapshot, and the mutable state a read answers with.
func wholeStateField(upsert string) string { return strings.TrimPrefix(upsert, "Upsert") }

// fillEveryCollection puts each entry of upsertOf into the field `field` names
// it by, on whatever request is being built. The map is copied in, so the two
// arms cannot reach one value between them.
func fillEveryCollection(t *testing.T, target any, field func(string) string) {
	t.Helper()
	for name, value := range upsertOf {
		want := reflect.ValueOf(value)
		into := reflect.ValueOf(target).Elem().FieldByName(field(name))
		require.Truef(t, into.IsValid(), "%T has no %s: the fixture names a collection this "+
			"request shape does not carry", target, field(name))
		require.Equalf(t, into.Type(), want.Type(), "%s is %s and the fixture carries %s",
			field(name), into.Type(), want.Type())

		fresh := reflect.MakeMapWithSize(want.Type(), want.Len())
		for _, k := range want.MapKeys() {
			fresh.SetMapIndex(k, want.MapIndex(k))
		}
		into.Set(fresh)
	}
}

// requireEveryCollectionHeld reads the run back and requires every collection the
// drain carried to be there, holding the same entry: a count alone would pass a
// drain that wrote two collections into each other's tables.
func requireEveryCollectionHeld(t *testing.T, state *p.InternalWorkflowMutableState) {
	t.Helper()
	for name, want := range upsertOf {
		field := wholeStateField(name)
		held := reflect.ValueOf(state).Elem().FieldByName(field)
		require.Truef(t, held.IsValid(),
			"a delta upserts %s and the mutable state has no %s to read it back from", name, field)

		got := held.Interface()
		if set, isSet := want.(map[string]struct{}); held.Kind() == reflect.Slice && isSet {
			// The one collection a delta carries as a set and whole state as a list.
			want = slices.Sorted(maps.Keys(set))
		}
		require.Equalf(t, want, got, "the run's %s is not what the drain carried: the collection "+
			"is missing from the applier's literal or went to another table, so those rows were "+
			"acknowledged, never written, and the log trimmed past them", field)
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
// Options are forwarded rather than applied to what comes back, so a caller's
// own fields are in the request mutbuild validates rather than written past it.
func (h *drains) create(workflowID, runID string, opts ...mutbuild.SnapshotOpt) mutation.Mutation {
	return h.build.Create(h.namespaceID, workflowID, runID,
		append([]mutbuild.SnapshotOpt{mutbuild.WithInfoBlob(blob("info"))}, opts...)...)
}

func (h *drains) update(
	workflowID, runID string, version int64, opts ...mutbuild.MutationOpt,
) mutation.Mutation {
	return h.build.Update(h.namespaceID, workflowID, runID, version,
		append([]mutbuild.MutationOpt{
			func(m *p.InternalWorkflowMutation) { m.ExecutionInfoBlob = blob("info") },
		}, opts...)...)
}

func (h *drains) watermark() (wal.Seqno, bool) {
	h.t.Helper()
	seqno, ok, err := h.store.Watermark(h.ctx, h.shard)
	require.NoError(h.t, err)
	return seqno, ok
}

// run is the store's own read of one run; the three below differ only in what
// they take off it, and runExists is why it hands the error back rather than
// requiring on it.
func (h *drains) run(workflowID, runID string) (*p.InternalGetWorkflowExecutionResponse, error) {
	return h.store.GetWorkflowExecution(h.ctx, &p.GetWorkflowExecutionRequest{
		ShardID: int32(h.shard), NamespaceID: h.namespaceID, WorkflowID: workflowID, RunID: runID,
	})
}

func (h *drains) runVersion(workflowID, runID string) int64 {
	h.t.Helper()
	row, err := h.run(workflowID, runID)
	require.NoError(h.t, err)
	return row.DBRecordVersion
}

// runState is the whole of what the store holds for one run, read out of the
// store rather than out of the tables.
func (h *drains) runState(workflowID, runID string) *p.InternalWorkflowMutableState {
	h.t.Helper()
	row, err := h.run(workflowID, runID)
	require.NoError(h.t, err)
	return row.State
}

func (h *drains) runExists(workflowID, runID string) bool {
	h.t.Helper()
	_, err := h.run(workflowID, runID)
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

// TestEveryBufferedBatchReachesTheDatabase is the guard above for the one
// collection it cannot see. A buffered batch travels on neither a delta nor a
// snapshot: batches never merge, so the fold strips each into
// fold.Emitted.BufferedBatches with the run it belongs to and the applier writes
// one row per batch. Nothing enumerated off a request shape therefore reaches
// them, and deleting the applier's loop over them leaves the whole of
// `go test ./...` green — rows acked, folded and dropped, with the watermark
// committed beside them and the log trimmed past them.
//
// What is lost is worse than a stale answer: a buffered batch is the event a
// signal became after the caller was told it had landed, and the run's history
// flushes without it.
//
// Payloads are compared as a set: which order the table hands them back in is
// the store's business, and what this is about is whether any of them is missing.
//
// The second workflow holds what this does *not* reach. A row's run comes off the
// batch and its workflow off the emitted request, so this catches a batch that
// leaked across workflows and not one filed under the wrong run of the same
// workflow — two workflows being two emitted requests, each carrying one run's
// batches. The unguarded half needs a request that carries two runs at once, a
// continue-as-new or a conflict-resolve, which mutbuild does not build.
func TestEveryBufferedBatchReachesTheDatabase(t *testing.T) {
	h := newDrains(t)
	first, second := uuid.NewString(), uuid.NewString()
	firstRun, secondRun := uuid.NewString(), uuid.NewString()

	require.NoError(t, h.store.Apply(h.ctx, h.shard, h.epoch,
		h.fold(h.create(first, firstRun), h.create(second, secondRun))))

	// Two batches for one run, in one window: they must arrive as two rows, which
	// is the half a merged slot would silently lose.
	buffered := func(name string) mutbuild.MutationOpt {
		return func(m *p.InternalWorkflowMutation) { m.NewBufferedEvents = blob(name) }
	}
	require.NoError(t, h.store.Apply(h.ctx, h.shard, h.epoch, h.fold(
		h.update(first, firstRun, 2, buffered("signal-one")),
		h.update(first, firstRun, 3, buffered("signal-two")),
		h.update(second, secondRun, 2, buffered("other-workflows-signal")),
	)))

	payloads := func(workflowID, runID string) []string {
		t.Helper()
		var out []string
		for _, b := range h.runState(workflowID, runID).BufferedEvents {
			out = append(out, string(b.Data))
		}
		slices.Sort(out)
		return out
	}
	require.Equal(t, []string{"signal-one", "signal-two"}, payloads(first, firstRun),
		"a batch the window acked is not in the run's buffered events: batches never merge, so "+
			"each is a row of its own and a missing one is an acked signal the history flushes without")
	require.Equal(t, []string{"other-workflows-signal"}, payloads(second, secondRun),
		"another workflow's batches reached this run, so the row's workflow and run are not the "+
			"batch's own")
}

// deleteOf is upsertOf's mirror: the key each collection upserts, as the set a
// delta removes it with. One entry per Delete* field of a delta, held to the type
// by the test below exactly as the upserts are.
var deleteOf = map[string]any{
	"DeleteActivityInfos":       map[int64]struct{}{1: {}},
	"DeleteTimerInfos":          map[string]struct{}{"timer": {}},
	"DeleteChildExecutionInfos": map[int64]struct{}{2: {}},
	"DeleteRequestCancelInfos":  map[int64]struct{}{3: {}},
	"DeleteSignalInfos":         map[int64]struct{}{4: {}},
	"DeleteSignalRequestedIDs":  map[string]struct{}{"signal-id": {}},
	"DeleteChasmNodes":          map[string]struct{}{"node": {}},
}

// TestEveryCollectionsDeletesReachTheDatabase is the guard above in the
// direction it did not cover. Its enumeration is over Upsert* fields, so the
// delete half of the same seven collections was driven by nothing here — and by
// nothing anywhere: the corpus deletes sub-entity keys from activities and timers
// only (mutgen's `DeleteAfterUpsert`), so the differential oracle exercises two of
// the seven and the other five had no guard at all. Dropping any of those five
// lines from the applier's deletions literal left the whole of `go test ./...`
// green.
//
// A dropped delete is not the mirror image of a dropped upsert in what it costs,
// which is why it is worth its own guard rather than a footnote. The row stays,
// and a row that stays is state the sequential path does not have: a signal id
// still in the requested set is a signal the next one deduplicates against and
// drops, and a child or a cancel still present is a run tracking something it
// already finished with. The write that removed it was acknowledged.
func TestEveryCollectionsDeletesReachTheDatabase(t *testing.T) {
	deletes := 0
	for f := range reflect.TypeFor[p.InternalWorkflowMutation]().Fields() {
		if !strings.HasPrefix(f.Name, "Delete") {
			continue
		}
		deletes++
		require.Containsf(t, deleteOf, f.Name, "a delta carries %s and nothing here drives it "+
			"through a drain: a collection missing from the applier's deletions literal is a "+
			"delete acknowledged and never applied, leaving a row the sequential path removed", f.Name)
	}
	require.NotZero(t, deletes, "no field of a delta is named Delete*: this has judged nothing")
	require.Len(t, deleteOf, deletes,
		"an entry here for a collection a delta no longer has")

	h := newDrains(t)
	wf, run := uuid.NewString(), uuid.NewString()
	require.NoError(t, h.store.Apply(h.ctx, h.shard, h.epoch, h.fold(h.create(wf, run))))

	// Upserted and deleted in two windows, never one: a key an accumulator sees
	// both ways in one window is resolved by the fold, so a single window would be
	// judging that rule instead of this one.
	filled := h.update(wf, run, 2, func(m *p.InternalWorkflowMutation) {
		fillEveryCollection(t, m, func(name string) string { return name })
	})
	require.NoError(t, h.store.Apply(h.ctx, h.shard, h.epoch, h.fold(filled)))
	requireEveryCollectionHeld(t, h.runState(wf, run))

	emptied := h.update(wf, run, 3, func(m *p.InternalWorkflowMutation) {
		for name, keys := range deleteOf {
			into := reflect.ValueOf(m).Elem().FieldByName(name)
			require.Truef(t, into.IsValid(), "a delta has no %s", name)
			want := reflect.ValueOf(keys)
			require.Equalf(t, into.Type(), want.Type(), "%s is %s and the fixture carries %s",
				name, into.Type(), want.Type())
			into.Set(want)
		}
	})
	require.NoError(t, h.store.Apply(h.ctx, h.shard, h.epoch, h.fold(emptied)))

	requireEveryCollectionEmptied(t, h.runState(wf, run),
		"the collection is missing from the applier's deletions literal, so that delete was "+
			"acknowledged and never applied and the row the sequential path removed is still there")
}

// requireEveryCollectionEmptied is requireEveryCollectionHeld's inverse, over the
// same enumeration: every collection upsertOf names must have come back empty.
func requireEveryCollectionEmptied(t *testing.T, state *p.InternalWorkflowMutableState, why string) {
	t.Helper()
	for upsert := range upsertOf {
		field := wholeStateField(upsert)
		held := reflect.ValueOf(state).Elem().FieldByName(field)
		require.Truef(t, held.IsValid(), "the mutable state has no %s", field)
		require.Zerof(t, held.Len(), "the run's %s still holds %v: %s", field, held.Interface(), why)
	}
}

// TestASnapshotClearsWhatTheRunHeldBefore is the third literal of seven, after
// the applier's upserts and its deletes: the clears a snapshot-bearing write runs
// before it writes whole state. A snapshot replaces a run's tables rather than
// amending them, so a collection missing from that literal leaves rows from before
// the snapshot in place — state the sequential path does not have, answered to the
// next reader and written back by the next snapshot-bearing write.
//
// Five of the seven had no guard: dropping the child-execution, request-cancel,
// signal, signals-requested or CHASM clear left the whole of `go test ./...` green,
// while activities and timers were caught by the differential oracle, whose two
// arms present the applier different requests once the fold has resolved an upsert
// against a delete. The corpus removes sub-entity keys from those two collections
// only, which is what bounds the oracle's reach here.
func TestASnapshotClearsWhatTheRunHeldBefore(t *testing.T) {
	h := newDrains(t)
	wf, run := uuid.NewString(), uuid.NewString()
	require.NoError(t, h.store.Apply(h.ctx, h.shard, h.epoch, h.fold(h.create(wf, run))))

	filled := h.update(wf, run, 2, func(m *p.InternalWorkflowMutation) {
		fillEveryCollection(t, m, func(name string) string { return name })
	})
	require.NoError(t, h.store.Apply(h.ctx, h.shard, h.epoch, h.fold(filled)))
	requireEveryCollectionHeld(t, h.runState(wf, run))

	// A Set carries whole state and names no collection, so every row above is one
	// the store owes the clear. In its own window: folded with the update it would
	// be the update that disappeared, which is fold's rule rather than this one.
	set := h.build.Set(h.namespaceID, wf, run, 3, mutbuild.WithInfoBlob(blob("info")))
	require.NoError(t, h.store.Apply(h.ctx, h.shard, h.epoch, h.fold(set)))

	requireEveryCollectionEmptied(t, h.runState(wf, run),
		"a snapshot-bearing write replaced the run's whole state and this collection is missing "+
			"from the applier's clears, so rows from before it survived a write that does not "+
			"carry them")
}

// TestEveryPartOfAResetReachesTheDatabase drives the shape whose second and third
// arms were written by code no fixture reached. A conflict-resolve carries up to
// three runs — the run being reset, the run that was current until now, and the
// new run the reset starts — and the applier writes each in its own arm. Deleting
// either of the last two left the whole of `go test ./...` green, the differential
// oracle included: `mutgen.emitConflictResolve` emits the reset snapshot and nil
// for the other two, so nothing in the tree ever built a reset that carried them,
// and both arms of the oracle run through this same applier anyway.
//
// What a missing arm costs is a whole run's state, acked: the new run a reset
// starts is the run the workflow continues as, so losing it leaves the current row
// naming a run with no execution row at all.
func TestEveryPartOfAResetReachesTheDatabase(t *testing.T) {
	h := newDrains(t)
	wf := uuid.NewString()
	reset, current, added := uuid.NewString(), uuid.NewString(), uuid.NewString()

	// The rows a reset stands on: the run it rewrites and the run that is current,
	// both at version 1, the second having taken the current row from the first.
	done := h.build.Create(h.namespaceID, wf, reset,
		mutbuild.WithInfoBlob(blob("info")),
		mutbuild.WithState(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED))
	over := h.build.CreateOver(h.namespaceID, wf, current, reset, 0, mutbuild.WithInfoBlob(blob("info")))
	require.NoError(t, h.store.Apply(h.ctx, h.shard, h.epoch, h.fold(done, over)))

	// One request, three runs, at version 2 over the rows above.
	require.NoError(t, h.store.Apply(h.ctx, h.shard, h.epoch,
		h.fold(h.build.ConflictResolve(h.namespaceID, wf, reset, current, added, 2))))

	require.EqualValues(t, 2, h.runVersion(wf, reset),
		"the reset run's own row did not move: the first arm is the one every fixture already drove")
	require.EqualValues(t, 2, h.runVersion(wf, current),
		"the run that was current was not written: a reset's second arm carries its close, so "+
			"the run stays open in the store with its caller told otherwise")
	require.True(t, h.runExists(wf, added),
		"the new run the reset starts has no execution row: the third arm is what creates it, and "+
			"the current row below names it, so the workflow continues as a run the store does not have")
	require.EqualValues(t, 1, h.runVersion(wf, added))

	row, _ := h.current(wf)
	require.NotNil(t, row)
	require.Equal(t, added, row.RunID, "the current row names the run the reset started")
}

// TestTheDrainRefusesWhatItCannotWrite pins the applier's pre-flight refusals,
// which nothing drove: deleting all five left the whole of `go test ./...` green.
// Each stands between a batch this store cannot write and a transaction that
// commits part of it, and they are worth a test each rather than a comment because
// a refusal is the only outcome on this path with no recovery behind it —
// [apply.ClassRefused] says there is an input to fix and no outcome to undo, so a
// missing refusal is the batch going through instead.
//
// The shard one has no upper bound on what it costs: a batch folded for one shard
// and written under another's id lands one shard's rows in another's tables, and
// both are wrong afterwards with nothing in either that says so.
//
// Three of the five, not five: the remaining two — a request kind that reaches no
// write path, and a current-row assertion kind nothing evaluates — are default arms
// over values no fold can produce. Reaching them needs a batch built by hand, and
// `fold.Batch` is deliberately `Drain`'s alone to build, so they are unreachable
// from outside this package rather than untested. A sweep that finds them green has
// found that, and there is nothing to write.
func TestTheDrainRefusesWhatItCannotWrite(t *testing.T) {
	t.Run("an epoch of zero", func(t *testing.T) {
		h := newDrains(t)
		wf, run := uuid.NewString(), uuid.NewString()
		err := h.store.Apply(h.ctx, h.shard, 0, h.fold(h.create(wf, run)))
		require.Equal(t, apply.ClassRefused, apply.Classify(err), "got %v", err)
		require.False(t, h.runExists(wf, run),
			"a refused drain may not have written a row: zero is the epoch an absent fence "+
				"reports, so the CAS it would be written under is nobody's")
		_, ok := h.watermark()
		require.False(t, ok, "and it may not have left a position behind")
	})

	t.Run("a batch carrying nothing", func(t *testing.T) {
		h := newDrains(t)
		err := h.store.Apply(h.ctx, h.shard, h.epoch, fold.New(h.shard).Drain())
		require.Equal(t, apply.ClassRefused, apply.Classify(err), "got %v", err)
		_, ok := h.watermark()
		require.False(t, ok, "an empty drain carries watermark zero, and committing that would "+
			"tell the store every entry below it is applied")
	})

	t.Run("a batch folded for another shard", func(t *testing.T) {
		h := newDrains(t)
		wf, run := uuid.NewString(), uuid.NewString()
		batch := h.fold(h.create(wf, run))

		// The call names a shard the batch was not folded for, which is what a
		// registry handing a cycle's batch to the wrong applier looks like from
		// here.
		err := h.store.Apply(h.ctx, h.shard+1, h.epoch, batch)
		require.Equal(t, apply.ClassRefused, apply.Classify(err), "got %v", err)
		require.ErrorContains(t, err, "folded shard",
			"the refusal must name both shards: it is the only place the mix-up is visible")
		require.False(t, h.runExists(wf, run),
			"the row must not have been written under either shard's id")
	})
}

// TestADeletedRunLeavesNoneOfItsRowsBehind pins the two writes a tombstone makes
// beyond the execution row, both of which nothing drove: `deleteRun` clears the
// run's seven collections and its buffered events before removing the row itself,
// and dropping either left the whole of `go test ./...` green.
//
// What it costs is not a lost write — a deleted run's rows are unreachable through
// the store once its execution row is gone, run ids being uuids that are never
// handed out twice. It is unbounded growth: every workflow a namespace ever
// completes leaves its activities, timers, signals and buffered batches in those
// tables for good, and nothing in the cluster reads them again to notice.
//
// Reusing the run id is how that becomes observable and is not a shape Temporal
// produces. What is being pinned is the store's obligation — a delete removes the
// run's rows — and a probe that asks for the same run back is the only way to ask
// the store what it kept, the tables not being reachable from outside the package.
func TestADeletedRunLeavesNoneOfItsRowsBehind(t *testing.T) {
	h := newDrains(t)
	wf, run := uuid.NewString(), uuid.NewString()
	require.NoError(t, h.store.Apply(h.ctx, h.shard, h.epoch, h.fold(h.create(wf, run))))

	filled := h.update(wf, run, 2, func(m *p.InternalWorkflowMutation) {
		fillEveryCollection(t, m, func(name string) string { return name })
		m.NewBufferedEvents = blob("a signal that arrived mid-task")
		// Closed, because the probe below stands over a finished run: the current
		// row takes its state from this mutation.
		m.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
		m.ExecutionState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
		stateBlob, err := serialization.WorkflowExecutionStateToBlob(m.ExecutionState)
		require.NoError(t, err)
		m.ExecutionStateBlob = stateBlob
	})
	require.NoError(t, h.store.Apply(h.ctx, h.shard, h.epoch, h.fold(filled)))
	requireEveryCollectionHeld(t, h.runState(wf, run))

	require.NoError(t, h.store.Apply(h.ctx, h.shard, h.epoch,
		h.fold(h.build.Delete(h.namespaceID, wf, run))))
	require.False(t, h.runExists(wf, run), "the tombstone took the execution row")

	// The probe: the same run id written again. A delete leaves the current row
	// alone, so the row still names this run at version 0, which is the assertion
	// CreateOver stands on.
	require.NoError(t, h.store.Apply(h.ctx, h.shard, h.epoch,
		h.fold(h.build.CreateOver(h.namespaceID, wf, run, run, 0, mutbuild.WithInfoBlob(blob("info"))))))

	state := h.runState(wf, run)
	requireEveryCollectionEmptied(t, state,
		"a run created where a deleted one stood holds the deleted one's rows: the tombstone did "+
			"not clear the collection, so every completed workflow leaves its share of that table "+
			"behind for good")
	require.Empty(t, state.BufferedEvents,
		"the deleted run's buffered events are still there: they outlive every workflow that ever "+
			"buffered one, and the run they belonged to is gone")
}

// TestADrainAssertsTheCurrentRowInsideItsTransaction is the current row's half of
// the condition authority, at the end where it is the last thing standing. What
// the layer confirmed before the ack and what the drain asserts are the same
// question asked twice, and the second asking is the one that runs inside the
// transaction that writes: between the two the row can only have moved if this
// shard changed hands, which is what makes a failure here a divergence rather
// than contention.
//
// It was driven by nothing. Deleting the block that evaluates it left the whole of
// `go test ./...` green — the run-row assertions have a test of their own, and
// fold's predicate has its own table, but no run put a *current-row* assertion
// through a real drain against a row that does not satisfy it. Unasserted, the
// window's write lands anyway: the current row stops naming the run it named, and
// nothing above ever learns the workflow's pointer moved.
func TestADrainAssertsTheCurrentRowInsideItsTransaction(t *testing.T) {
	h := newDrains(t)
	wf := uuid.NewString()
	held, intruder := uuid.NewString(), uuid.NewString()

	require.NoError(t, h.store.Apply(h.ctx, h.shard, h.epoch, h.fold(h.create(wf, held))))
	current, _ := h.current(wf)
	require.NotNil(t, current)
	require.Equal(t, held, current.RunID)

	// A brand-new create asserts the workflow has no current row at all, and this
	// one does. The window cannot know: it was folded against a row that has since
	// moved, which is the only way this assertion reaches the drain still false.
	err := h.store.Apply(h.ctx, h.shard, h.epoch, h.fold(h.create(wf, intruder)))
	require.Equal(t, apply.ClassInvariantViolated, apply.Classify(err), "got %v", err)
	require.ErrorAs(t, err, new(*p.CurrentWorkflowConditionFailedError),
		"the store's own current-row conflict must stay reachable under the attribution")

	after, _ := h.current(wf)
	require.NotNil(t, after)
	require.Equal(t, held, after.RunID,
		"the refused drain moved the workflow's current row: the run it named is no longer "+
			"reachable through it, and the write that took it over was acknowledged")
	require.False(t, h.runExists(wf, intruder), "and it wrote the intruder's own row too")
}

// TestATimerTaskLandsInTheTimerTable is the one place a task's *category*
// decides which table it goes to, and nothing drove it. A scheduled category is
// written by fire time, and the timer category has a table of its own
// (`timer_tasks`) that the timer queue is the only reader of. Delete the branch
// that picks it and the rows go to the generic scheduled table instead: the drain
// commits, the watermark moves, the log is trimmed, and the timer queue reads its
// own table and finds nothing. An acked timer that never fires is not a stale
// answer — nothing retries it, and the workflow waits for ever.
//
// The differential oracle cannot see this: both of its arms run through this same
// applier, so a row sent to the wrong table is sent there twice and the comparison
// is empty. What catches it is reading the task back through the store's own read
// for the category it was written under.
func TestATimerTaskLandsInTheTimerTable(t *testing.T) {
	h := newDrains(t)
	fires := time.Date(2026, 9, 19, 3, 0, 0, 0, time.UTC)

	timer := p.InternalHistoryTask{
		Key:  tasks.NewKey(fires, 77),
		Blob: blob("the timer that must fire"),
	}
	require.NoError(t, h.store.Apply(h.ctx, h.shard, h.epoch,
		h.fold(h.build.AddTasks(tasks.CategoryTimer, timer))))

	page, err := h.store.GetHistoryTasks(h.ctx, &p.GetHistoryTasksRequest{
		ShardID:             int32(h.shard),
		TaskCategory:        tasks.CategoryTimer,
		InclusiveMinTaskKey: tasks.NewKey(fires.Add(-time.Hour), 0),
		ExclusiveMaxTaskKey: tasks.NewKey(fires.Add(time.Hour), 0),
		BatchSize:           100,
	})
	require.NoError(t, err)
	require.Len(t, page.Tasks, 1,
		"the timer queue reads timer_tasks and found nothing there: the row went to another "+
			"table, so the drain committed, the watermark moved, the log was trimmed, and the "+
			"timer will never fire")
	require.Equal(t, "the timer that must fire", string(page.Tasks[0].Blob.Data))
	require.Equal(t, int64(77), page.Tasks[0].Key.TaskID)
}
