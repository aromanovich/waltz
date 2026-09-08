package waltz

// Configuration and composition without a cluster: the mode, policy and graph.

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/service/history/tasks"

	"github.com/aromanovich/waltz/baserow"
	"github.com/aromanovich/waltz/cycle"
	"github.com/aromanovich/waltz/internal/verify/coldtest"
	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/wal"
	"github.com/aromanovich/waltz/wal/memwal"
)

// Every decision the layer takes samples the policy and the budget assertion
// samples it first, so a caller that states none has to be refused where the
// other required inputs are — otherwise it is a nil dereference inside a
// composition that had already been accepted.
func TestTheCompositionRefusesAPolicyNobodyStated(t *testing.T) {
	_, err := Compose(Backends{}, nil, DefaultTaskCategories(), nil, nil)
	require.ErrorContains(t, err, "no policy")
}

// The layer decodes an inherited tail with the registry this cluster is
// configured for, not the default set: an unknown category id is fatal to a
// replay, so a default registry on a cluster with archival enabled would refuse
// exactly the entries carrying archival tasks.
func TestTheRegistryIsTheServersOwn(t *testing.T) {
	dc := dynamicconfig.NewCollection(dynamicconfig.NewNoopClient(), log.NewNoopLogger())

	off := TaskCategories(dc, &config.Config{})
	_, found := off.Categories().GetCategoryByID(tasks.CategoryArchival.ID())
	require.False(t, found, "archival is off, so the category is not in the registry")

	on := TaskCategories(dc, &config.Config{
		Archival: config.Archival{History: config.HistoryArchival{State: "enabled"}},
	})
	_, found = on.Categories().GetCategoryByID(tasks.CategoryArchival.ID())
	require.True(t, found,
		"archival is on, so the entries this cluster writes can name a category a default registry has never heard of")
}

// composed is the composition over the backends this library ships: a log in
// memory and a writer that commits nothing, so cfg is the whole of what varies
// between the callers.
func composed(t *testing.T, cfg cycle.Config) (*Layer, *coldtest.Cold) {
	t.Helper()
	cold := coldtest.New()
	layer, err := Compose(
		Backends{Log: memwal.New(), Cold: cold},
		cycle.Fixed(cfg),
		DefaultTaskCategories(),
		log.NewNoopLogger(), nil)
	require.NoError(t, err)
	return layer, cold
}

// The composition over a log in memory, which is what makes [Backends] a seam
// rather than a parameter list: the same Compose a binary calls, over a writer
// that commits nothing, with no cluster anywhere. A caller that could not reach
// this graph would build a second cycle.NewManager instead, which is the one
// thing this package asks callers not to do.
func TestTheCompositionIsReachableWithoutAColdStore(t *testing.T) {
	ctx := context.Background()
	const shard, epoch = wal.ShardID(3), wal.Epoch(7)

	layer, _ := composed(t, cycle.Defaults())
	t.Cleanup(func() { layer.Shutdown(ctx, time.Minute) })

	// Through Options, because that is the face the binary hands the factory:
	// the registry is the acquire's observer and the write path at once.
	require.NoError(t, layer.Options().Layer.ShardAcquired(ctx, shard, epoch))

	_, ok := layer.ShardStats(shard)
	require.True(t, ok, "the acquire installed a cycle for the shard")
	require.Equal(t, 1, layer.Totals().Shards, "and the layer counts it")
}

// closingLog counts the closes of the log it wraps, which is the only thing
// about [wal.Log.Close] a composition can be asked.
type closingLog struct {
	wal.Log
	closes int
}

func (l *closingLog) Close() { l.closes++; l.Log.Close() }

// A shutdown releases the log, not only the cycles that were writing to it. A
// backend holding a connection or a pinged ownership transaction has nothing
// above it but the composition that knows when the last append has happened —
// so a drain that stops the cycles and leaves the backend open keeps shards
// this process has stopped writing to.
func TestShutdownReleasesTheLog(t *testing.T) {
	logs := &closingLog{Log: memwal.New()}
	cold := coldtest.New()
	layer, err := Compose(
		Backends{Log: logs, Cold: cold},
		cycle.Fixed(cycle.Defaults()),
		DefaultTaskCategories(),
		log.NewNoopLogger(), nil)
	require.NoError(t, err)

	require.Zero(t, logs.closes, "a composed layer has not released anything yet")
	layer.Shutdown(context.Background(), time.Minute)
	require.Equal(t, 1, logs.closes, "the shutdown released the log")
}

// TestTheShutdownDrainOutlivesTheContextThatAsksForIt: a shutdown drain runs
// where a context has just been cancelled — that is what shutdown is — so the
// budget goes on a context of the layer's own. A drain that inherited the
// caller's cancellation would return at once and leave a tail behind, which is
// not a failure anybody reads: the next owner replays it and the run looks
// clean.
func TestTheShutdownDrainOutlivesTheContextThatAsksForIt(t *testing.T) {
	const shard, epoch = wal.ShardID(3), wal.Epoch(7)

	layer, cold := composed(t, cycle.Defaults())

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, layer.Options().Layer.ShardAcquired(ctx, shard, epoch))
	require.NoError(t, layer.Options().Layer.Write(ctx, mutation.Mutation{Create: aCreate(shard)}, epoch, baserow.New(emptyStore{})))
	require.Zero(t, cold.Drains(), "a windowed write of one mutation reaches no watermark")

	cancel()
	layer.Shutdown(ctx, time.Minute)

	require.Equal(t, 1, cold.Drains(),
		"the window the shutdown found went to the cold store, cancelled context and all")
}

// TestOneCompositionEmitsToOneHandler drives the composition the way a binary
// running several services in one process does: one options value, one
// NewFactory per service, each with that service's metrics handler. Both halves
// of the layer — the wrapper's counters and the cycles' — must land in the same
// handler, because they are read against each other: what the stores sent at
// the layer over what the drains carried is not a ratio when the two numbers
// are on different scrapes.
//
// Which handler wins is walmetrics.Emitter.Use's decision; what this asserts is
// that one decision covers both halves.
func TestOneCompositionEmitsToOneHandler(t *testing.T) {
	ctx := context.Background()
	const shard, epoch = wal.ShardID(3), wal.Epoch(7)

	// Sync, so the write this test makes drains inside itself and the cycles'
	// half of the numbers exists by the time the assertions run.
	cfg := cycle.Defaults()
	cfg.Sync = true

	layer, _ := composed(t, cfg)
	t.Cleanup(func() { layer.Shutdown(ctx, time.Minute) })

	factory := layer.AbstractFactory(baseFactory{base: coldStores{}})

	history, matching := metricstest.NewCaptureHandler(), metricstest.NewCaptureHandler()
	factory.NewFactory(config.CustomDatastoreConfig{}, resolver.NewNoopResolver(), "test", log.NewNoopLogger(), history)
	// The second service's persistence graph, and the stores this test writes
	// through: whichever service builds last must not take the numbers with it.
	stores := factory.NewFactory(config.CustomDatastoreConfig{}, resolver.NewNoopResolver(), "test", log.NewNoopLogger(), matching)

	shards, err := stores.NewShardStore()
	require.NoError(t, err)
	require.NoError(t, shards.UpdateShard(ctx, &p.InternalUpdateShardRequest{
		ShardID: int32(shard), RangeID: int64(epoch), PreviousRangeID: int64(epoch) - 1,
	}))

	store, err := stores.NewExecutionStore()
	require.NoError(t, err)

	first, second := history.StartCapture(), matching.StartCapture()
	create := aCreate(shard)
	create.RangeID = int64(epoch)
	_, err = store.CreateWorkflowExecution(ctx, create)
	require.NoError(t, err)

	emitted := first.Snapshot()
	require.Len(t, emitted["wal_intercepted_writes"], 1,
		"the wrapper's own series went somewhere else: the store built for the second service is emitting through a handler of its own")
	require.Len(t, emitted["wal_drains"], 1,
		"the cycles' series went somewhere else")
	require.Empty(t, second.Snapshot(),
		"the second service's handler took part of the layer's numbers with it")
}

// aCreate is the smallest brand-new workflow the write path folds and encodes.
func aCreate(shard wal.ShardID) *p.InternalCreateWorkflowExecutionRequest {
	run := uuid.NewString()
	return &p.InternalCreateWorkflowExecutionRequest{
		ShardID: int32(shard),
		Mode:    p.CreateWorkflowModeBrandNew,
		NewWorkflowSnapshot: p.InternalWorkflowSnapshot{
			NamespaceID: uuid.NewString(),
			WorkflowID:  "one-emitter",
			RunID:       run,
			ExecutionState: &persistencespb.WorkflowExecutionState{
				CreateRequestId: uuid.NewString(),
				RunId:           run,
				State:           enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
				Status:          enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			},
			DBRecordVersion: 1,
		},
	}
}

// baseFactory stands in for the abstract factory of the plugin holding the cold
// store: it hands back the same stores whatever service asks, which is what
// makes the handler the only thing that differs between the two NewFactory
// calls above.
type baseFactory struct{ base p.DataStoreFactory }

func (f baseFactory) NewFactory(
	config.CustomDatastoreConfig, resolver.ServiceResolver, string, log.Logger, metrics.Handler,
) p.DataStoreFactory {
	return f.base
}

// coldStores is the cold half in the shape intercept mode needs of it: a shard
// store whose rangeID lands, and an execution store answering the two
// pre-window reads with the truth about a run that does not exist. Every other
// method is promoted from a nil interface, so an intercepted call that fell
// through to the cold store panics here rather than being answered.
type coldStores struct{ p.DataStoreFactory }

func (coldStores) NewShardStore() (p.ShardStore, error)         { return landingShardStore{}, nil }
func (coldStores) NewExecutionStore() (p.ExecutionStore, error) { return emptyStore{}, nil }

type landingShardStore struct{ p.ShardStore }

func (landingShardStore) UpdateShard(context.Context, *p.InternalUpdateShardRequest) error {
	return nil
}

type emptyStore struct{ p.ExecutionStore }

func (emptyStore) GetWorkflowExecution(context.Context, *p.GetWorkflowExecutionRequest) (*p.InternalGetWorkflowExecutionResponse, error) {
	return nil, &serviceerror.NotFound{Message: "no such run"}
}

func (emptyStore) GetCurrentExecution(context.Context, *p.GetCurrentExecutionRequest) (*p.InternalGetCurrentExecutionResponse, error) {
	return nil, &serviceerror.NotFound{Message: "no current row"}
}

// GetCurrentExecutionWithLastWriteVersion is how the layer takes the
// current-row read ([baserow.Store]).
func (emptyStore) GetCurrentExecutionWithLastWriteVersion(
	context.Context, *p.GetCurrentExecutionRequest,
) (*p.InternalGetCurrentExecutionResponse, int64, error) {
	return nil, 0, &serviceerror.NotFound{Message: "no current row"}
}
