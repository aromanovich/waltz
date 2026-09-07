package wrapper

// The metrics seam. The server's metrics handler exists later than the layer
// does, so it reaches the layer as a hand-off at NewFactory rather than as a
// constructor argument; when that hand-off silently does not happen everything
// still works and every series is empty.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/persistence/mock"
	"go.temporal.io/server/common/resolver"
	"go.uber.org/mock/gomock"

	"github.com/aromanovich/waltz/walmetrics"
)

// recordingSink is the layer below in its metrics-receiving shape: it keeps
// every handler it was offered, in order, so that both "was it handed over" and
// "how many times" can be asked.
type recordingSink struct {
	recordingLayer
	got []metrics.Handler
}

func (s *recordingSink) Use(h metrics.Handler) { s.got = append(s.got, h) }

// emittingLayer is the layer below in the shape a composition has it: it holds
// the emitter its own numbers go through, and the hand-off is what points that
// emitter somewhere. The stores above are built over the same value.
type emittingLayer struct {
	recordingLayer
	emit *walmetrics.Emitter
}

func (l *emittingLayer) Use(h metrics.Handler) { l.emit.Use(h) }

var (
	_ MetricsSink = (*recordingSink)(nil)
	_ ShardLayer  = (*recordingSink)(nil)
	_ ShardLayer  = (*emittingLayer)(nil)
)

// fakeAbstractFactory stands in for the plugin's own abstract factory, so the
// decorator is driven the way the server drives it.
type fakeAbstractFactory struct{ base p.DataStoreFactory }

func (f fakeAbstractFactory) NewFactory(
	config.CustomDatastoreConfig, resolver.ServiceResolver, string, log.Logger, metrics.Handler,
) p.DataStoreFactory {
	return f.base
}

var _ client.AbstractDataStoreFactory = fakeAbstractFactory{}

// TestTheMetricsHandlerReachesTheLayerThroughTheFactory: NewFactory is the only
// place in the process where the layer can see the server's handler, so that is
// where it is offered one.
func TestTheMetricsHandlerReachesTheLayerThroughTheFactory(t *testing.T) {
	ctrl := gomock.NewController(t)
	base := mock.NewMockDataStoreFactory(ctrl)
	sink := &recordingSink{}
	// One layer value, so one offer per NewFactory; the counting below is on
	// that.
	factory := NewAbstractDataStoreFactory(fakeAbstractFactory{base: base}, Options{Layer: sink})

	first, second := metricstest.NewCaptureHandler(), metricstest.NewCaptureHandler()
	factory.NewFactory(config.CustomDatastoreConfig{}, resolver.NewNoopResolver(), "test", log.NewNoopLogger(), first)

	require.Len(t, sink.got, 1, "the layer was never handed the server's metrics handler")
	require.Same(t, first, sink.got[0])

	// NewFactory runs once per service that builds persistence, several times
	// in a development binary. The wrapper offers every handler; deduplicating
	// to the first is the sink's job, so the tags do not depend on which
	// service was constructed last.
	factory.NewFactory(config.CustomDatastoreConfig{}, resolver.NewNoopResolver(), "test", log.NewNoopLogger(), second)
	require.Len(t, sink.got, 2, "the wrapper stopped offering the handler; the *sink* is what deduplicates")

	emit := walmetrics.New(nil)
	emit.Use(first)
	emit.Use(second)
	capture := second.StartCapture()
	emit.AnsweredConditionFailure()
	require.Empty(t, capture.Snapshot(), "the second handler took over: Use is not first-wins")
}

// TestTheStoresGetTheHandlerToo: the wrapper's own counters are emitted from
// the stores the factory builds, through the emitter the options carry — so the
// one hand-off that points that emitter at the server's handler has to make
// this series appear as well, with nothing filled in per factory.
func TestTheStoresGetTheHandlerToo(t *testing.T) {
	ctrl := gomock.NewController(t)
	base := mock.NewMockDataStoreFactory(ctrl)
	base.EXPECT().NewExecutionStore().Return(versioned(ctrl), nil)

	handler := metricstest.NewCaptureHandler()
	emitter := walmetrics.New(nil)
	factory := NewAbstractDataStoreFactory(fakeAbstractFactory{base: base},
		Options{Layer: &emittingLayer{emit: emitter}, Metrics: emitter})
	built := factory.NewFactory(config.CustomDatastoreConfig{}, resolver.NewNoopResolver(), "test", log.NewNoopLogger(), handler)

	store, err := built.NewExecutionStore()
	require.NoError(t, err)

	capture := handler.StartCapture()
	require.NoError(t, store.AddHistoryTasks(context.Background(), &p.InternalAddHistoryTasksRequest{ShardID: 1}))
	require.Len(t, capture.Snapshot()["wal_intercepted_writes"], 1,
		"the store built by the factory emits nothing: the hand-off did not reach the emitter its options carry")
}

// TestOptionsWithNoEmitterStillServe pins [Options.Metrics]'s nil: the store
// works and counts, and its series go nowhere — there being no way back to the
// layer's emitter from here. Every Options value built by hand in this package
// is that case.
func TestOptionsWithNoEmitterStillServe(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := newStore(t, versioned(ctrl), Options{Layer: &recordingLayer{}})

	require.NoError(t, store.AddHistoryTasks(context.Background(), &p.InternalAddHistoryTasksRequest{ShardID: 1}))
	require.EqualValues(t, 1, store.Counts().TasksWritten)
}

// TestEveryInterceptedWriteNamesItsMethod pins the operation tag, which is what
// makes the eight-and-only-eight write partition visible in a deployment: a
// ninth intercepted method shows up there as a ninth tag value.
func TestEveryInterceptedWriteNamesItsMethod(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	base := versioned(ctrl)

	handler := metricstest.NewCaptureHandler()
	store := newStore(t, base, Options{Layer: &recordingLayer{}, Metrics: walmetrics.New(handler)})
	capture := handler.StartCapture()

	_, err := store.CreateWorkflowExecution(ctx, &p.InternalCreateWorkflowExecutionRequest{ShardID: 3, RangeID: 1})
	require.NoError(t, err)
	require.NoError(t, store.UpdateWorkflowExecution(ctx, &p.InternalUpdateWorkflowExecutionRequest{ShardID: 3, RangeID: 1}))
	require.NoError(t, store.ConflictResolveWorkflowExecution(ctx, &p.InternalConflictResolveWorkflowExecutionRequest{ShardID: 3, RangeID: 1}))
	require.NoError(t, store.SetWorkflowExecution(ctx, &p.InternalSetWorkflowExecutionRequest{ShardID: 3, RangeID: 1}))
	require.NoError(t, store.DeleteWorkflowExecution(ctx, &p.DeleteWorkflowExecutionRequest{ShardID: 3}))
	require.NoError(t, store.DeleteCurrentWorkflowExecution(ctx, &p.DeleteCurrentWorkflowExecutionRequest{ShardID: 3}))
	require.NoError(t, store.AddHistoryTasks(ctx, &p.InternalAddHistoryTasksRequest{ShardID: 3}))
	require.NoError(t, store.RangeCompleteHistoryTasks(ctx, &p.RangeCompleteHistoryTasksRequest{ShardID: 3}))

	snapshot := capture.Snapshot()
	var ops []string
	for _, r := range snapshot["wal_intercepted_writes"] {
		ops = append(ops, r.Tags["operation"])
	}
	require.ElementsMatch(t, []string{
		"CreateWorkflowExecution", "UpdateWorkflowExecution", "ConflictResolveWorkflowExecution",
		"SetWorkflowExecution", "DeleteWorkflowExecution", "DeleteCurrentWorkflowExecution",
		"AddHistoryTasks", "RangeCompleteHistoryTasks",
	}, ops)

	require.Empty(t, snapshot["wal_transiting_writes"],
		"nothing intercepted goes around the log, so the series could only ever read zero, "+
			"and one that can only be zero reads as a system doing no work")
}

// TestARefusedWriteIsStillAWriteThisStoreSent: the counter is taken on the way
// in, before the layer answers, so a write the tail refused is in it. Only then
// does the pair read as a ratio — what this store sent at the layer against
// what the layer did with it — during a backpressure incident.
func TestARefusedWriteIsStillAWriteThisStoreSent(t *testing.T) {
	ctrl := gomock.NewController(t)
	base := versioned(ctrl)

	handler := metricstest.NewCaptureHandler()
	refusing := &recordingLayer{err: p.ErrPersistenceSystemLimitExceeded}
	store := newStore(t, base, Options{Layer: refusing, Metrics: walmetrics.New(handler)})
	capture := handler.StartCapture()

	err := store.UpdateWorkflowExecution(context.Background(), &p.InternalUpdateWorkflowExecutionRequest{ShardID: 3})
	require.ErrorIs(t, err, p.ErrPersistenceSystemLimitExceeded)
	require.Len(t, capture.Snapshot()["wal_intercepted_writes"], 1)
	require.EqualValues(t, 1, store.Counts().Intercepted)
}

// TestTheThreeReadsAreCountedApart: the two mutable-state reads and the task
// read go out as separate series, since one moving while the other does not is
// something a single counter would hide. Both are taken on the way in, like the
// write counters; a counter that fired only on a window hit would read zero
// both on an idle cluster and on a layer wired up wrong.
func TestTheThreeReadsAreCountedApart(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	base := versioned(ctrl)

	handler := metricstest.NewCaptureHandler()
	store := newStore(t, base, Options{Layer: &recordingLayer{}, Metrics: walmetrics.New(handler)})
	capture := handler.StartCapture()

	_, _ = store.GetWorkflowExecution(ctx, &p.GetWorkflowExecutionRequest{ShardID: 3})
	_, _ = store.GetCurrentExecution(ctx, &p.GetCurrentExecutionRequest{ShardID: 3})
	_, _ = store.GetHistoryTasks(ctx, &p.GetHistoryTasksRequest{ShardID: 3})

	snapshot := capture.Snapshot()
	var reads []string
	for _, r := range snapshot["wal_overlaid_reads"] {
		reads = append(reads, r.Tags["operation"])
	}
	require.ElementsMatch(t, []string{"GetWorkflowExecution", "GetCurrentExecution"}, reads)
	require.Len(t, snapshot["wal_merged_task_pages"], 1,
		"the task read has a series of its own, and one page went through the merge")

	require.EqualValues(t, 2, store.Counts().Overlaid)
	require.EqualValues(t, 1, store.Counts().TaskReads)
}
