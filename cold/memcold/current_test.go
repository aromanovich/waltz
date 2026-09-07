package memcold_test

// The versioned current-row read is the one method this store adds to the
// embedded one, so no upstream suite covers it. What the layer stands on is
// here: the version arrives, absence arrives as absence, and the version tracks
// the write.

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/tests"

	"github.com/aromanovich/waltz/baserow"
	"github.com/aromanovich/waltz/cold/memcold"
)

func TestBaserowAcceptsTheStore(t *testing.T) {
	rows, err := baserow.Of(newStore(t))
	require.NoError(t, err)
	require.NotNil(t, rows)
}

func TestCurrentRowArrivesWithItsVersion(t *testing.T) {
	f := newWorkflow(t)
	f.create(t, 41)

	row, version, err := f.store.GetCurrentExecutionWithLastWriteVersion(f.ctx, f.request())
	require.NoError(t, err)
	require.Equal(t, f.runID, row.RunID)
	require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_CREATED, row.ExecutionState.State)
	require.EqualValues(t, 41, version)
}

func TestAbsentCurrentRowIsNotFound(t *testing.T) {
	f := newWorkflow(t)

	// Nothing was created, and the shard the read names exists: this is an
	// unstarted workflow, not a broken store.
	_, _, err := f.store.GetCurrentExecutionWithLastWriteVersion(f.ctx, f.request())
	require.ErrorAs(t, err, new(*serviceerror.NotFound))

	// Which is what the layer reads it as.
	rows, err := baserow.Of(f.store)
	require.NoError(t, err)
	row, version, err := rows.Current(f.ctx, f.shardID, f.namespaceID, f.workflowID)
	require.NoError(t, err)
	require.Nil(t, row)
	require.Zero(t, version)
}

func TestVersionMovesWithTheWrite(t *testing.T) {
	f := newWorkflow(t)
	snapshot := f.create(t, 41)

	_, before, err := f.store.GetCurrentExecutionWithLastWriteVersion(f.ctx, f.request())
	require.NoError(t, err)
	require.EqualValues(t, 41, before)

	f.update(t, snapshot, 42)

	_, after, err := f.store.GetCurrentExecutionWithLastWriteVersion(f.ctx, f.request())
	require.NoError(t, err)
	require.EqualValues(t, 42, after)
}

// workflow is one store, one shard and one workflow's identity, which is all
// the current-row read is about.
type workflow struct {
	ctx     context.Context
	store   *memcold.Store
	manager p.ExecutionManager

	shardID int32
	rangeID int64

	namespaceID string
	workflowID  string
	runID       string
	branchToken []byte
}

func newWorkflow(t *testing.T) *workflow {
	t.Helper()
	ctx := context.Background()
	store := newStore(t)
	serializer := serialization.NewSerializer()

	const shardID = 1
	shards := p.NewShardManager(store.ShardStore(), serializer)
	shard, err := shards.GetOrCreateShard(ctx, &p.GetOrCreateShardRequest{
		ShardID:          shardID,
		InitialShardInfo: &persistencespb.ShardInfo{ShardId: shardID, RangeId: 1},
	})
	require.NoError(t, err)
	// A write asserts the range it was issued under, and the range a shard is
	// created at is the one the previous owner would have held.
	previous := shard.ShardInfo.RangeId
	shard.ShardInfo.RangeId++
	require.NoError(t, shards.UpdateShard(ctx, &p.UpdateShardRequest{
		ShardInfo:       shard.ShardInfo,
		PreviousRangeID: previous,
	}))

	f := &workflow{
		ctx:   ctx,
		store: store,
		manager: p.NewExecutionManager(
			store,
			serializer,
			nil,
			log.NewNoopLogger(),
			dynamicconfig.GetIntPropertyFn(4*1024*1024),
		),
		shardID:     shardID,
		rangeID:     shard.ShardInfo.RangeId,
		namespaceID: uuid.NewString(),
		workflowID:  uuid.NewString(),
		runID:       uuid.NewString(),
	}
	f.branchToken = tests.RandomBranchToken(f.namespaceID, f.workflowID, f.runID, &p.HistoryBranchUtilImpl{})
	return f
}

func (f *workflow) request() *p.GetCurrentExecutionRequest {
	return &p.GetCurrentExecutionRequest{
		ShardID:     f.shardID,
		NamespaceID: f.namespaceID,
		WorkflowID:  f.workflowID,
	}
}

func (f *workflow) create(t *testing.T, lastWriteVersion int64) *p.WorkflowSnapshot {
	t.Helper()
	snapshot, events := tests.RandomSnapshot(
		t,
		f.namespaceID, f.workflowID, f.runID,
		common.FirstEventID,
		lastWriteVersion,
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		1,
		f.branchToken,
	)
	_, err := f.manager.CreateWorkflowExecution(f.ctx, &p.CreateWorkflowExecutionRequest{
		ShardID:             f.shardID,
		RangeID:             f.rangeID,
		Mode:                p.CreateWorkflowModeBrandNew,
		NewWorkflowSnapshot: *snapshot,
		NewWorkflowEvents:   events,
	})
	require.NoError(t, err)
	return snapshot
}

func (f *workflow) update(t *testing.T, snapshot *p.WorkflowSnapshot, lastWriteVersion int64) {
	t.Helper()
	mutation, events := tests.RandomMutation(
		t,
		f.namespaceID, f.workflowID, f.runID,
		snapshot.NextEventID,
		lastWriteVersion,
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		snapshot.DBRecordVersion+1,
		f.branchToken,
	)
	_, err := f.manager.UpdateWorkflowExecution(f.ctx, &p.UpdateWorkflowExecutionRequest{
		ShardID:                f.shardID,
		RangeID:                f.rangeID,
		Mode:                   p.UpdateWorkflowModeUpdateCurrent,
		UpdateWorkflowMutation: *mutation,
		UpdateWorkflowEvents:   events,
	})
	require.NoError(t, err)
}
