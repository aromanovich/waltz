// Package wrapper is the WAL layer's seam into a running temporal-server: a
// decorator over the base plugin's DataStoreFactory whose ExecutionStore and
// ShardStore the history service talks to in place of the plugin's own.
//
// [Options] is the only mode switch. Without a layer it is passthrough: every
// call transits. With one it is intercept: twelve methods go through the WAL
// and a thirteenth is refused ([ErrCompleteHistoryTaskUnsupported]).
//
// [ADR 0003] explains why this runs in the server's process.
//
// [ADR 0003]: ../docs/adr/0003-wal-layer-runs-in-process.md
package wrapper

import (
	"context"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/resolver"

	"github.com/aromanovich/waltz/baserow"
	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/wal"
	"github.com/aromanovich/waltz/walmetrics"
)

// ShardObserver is told when a shard's rangeID moves; epoch is the new value
// (invariant I11). This may be the current owner again: the server bumps the
// rangeID when it exhausts its ID range. Closing a shard is not reported, as it
// makes no persistence call.
type ShardObserver interface {
	// ShardAcquired runs before the base store commits the bump. An error fails
	// the acquire without calling the base store, so a failed fence never leaves
	// a moved rangeID. The error reaches the shard context unwrapped, which
	// retries the acquire with backoff.
	ShardAcquired(ctx context.Context, shard wal.ShardID, epoch wal.Epoch) error
}

// ShardLayer is the WAL layer as one shard's stores see it. It is one interface
// so the four parts cannot be configured apart: a writer without its reader
// reads stale, a writer never told of the acquire refuses every write, and a
// layer never handed the metrics handler silently emits nothing.
type ShardLayer interface {
	ShardObserver
	ShardWriter
	ShardReader
	MetricsSink
}

// ShardWriter is the WAL layer's write path: a write is acked into the shard's
// log and, in sync mode, applied before the call returns.
type ShardWriter interface {
	// WritesHistory reports whether an intercepted write's event batches ride
	// the appended record. If false, the caller must write them to the base
	// store before calling Write, then strip them from the mutation.
	//
	// It is part of the layer, not a field of [Options], so it cannot disagree
	// with the store: claiming the batches ride the record when the store does
	// not write them would ack mutable state over history nobody wrote.
	WritesHistory() bool

	// Write acks m into its shard's log (m names the shard) and returns the
	// apply outcome.
	//
	// The layer takes ownership of m's request: windowed modes keep it and merge
	// it in place. The caller must not read or reuse it after Write returns.
	//
	// epoch is the rangeID the caller wrote under; a fenced-out shard context is
	// refused, not re-stamped. Zero means "no epoch", not epoch 0: the two
	// deletes and the range delete carry none and are fenced by the drain's CAS.
	//
	// The error is the store's own, unwrapped (condition failure, fenced shard,
	// tail at its bound). A condition failure always belongs to this caller:
	// windowed writes settle conditions before the append, and a sync drain
	// carries only this mutation. base is called on the goroutine that owns the
	// window, at most once per asserted row.
	Write(
		ctx context.Context,
		m mutation.Mutation,
		epoch wal.Epoch,
		base *baserow.Rows,
	) error
}

// ShardReader is the read path: the four reads a write can change. base reaches
// the cold store (the layer may not name one) and is called on the goroutine
// that owns the window, so a read never sees a drain in flight. Errors come
// back unwrapped.
type ShardReader interface {
	// GetWorkflowExecution returns the base row merged with the window's state
	// for the run, the window's state alone if it holds whole state, or NotFound
	// if it holds a tombstone.
	GetWorkflowExecution(
		ctx context.Context,
		req *p.GetWorkflowExecutionRequest,
		base func(context.Context) (*p.InternalGetWorkflowExecutionResponse, error),
	) (*p.InternalGetWorkflowExecutionResponse, error)

	// GetCurrentExecution answers from the window's last writer, not from the
	// merged request.
	GetCurrentExecution(
		ctx context.Context,
		req *p.GetCurrentExecutionRequest,
		base func(context.Context) (*p.InternalGetCurrentExecutionResponse, error),
	) (*p.InternalGetCurrentExecutionResponse, error)

	// GetHistoryTasks returns one page of a task range, merging cold rows with
	// the window. base takes its own request: BatchSize minus the window's
	// share, resumed from the base's token. The returned token is the layer's
	// and wraps the base's. Unlike the reads above, it is refused for a shard
	// the layer does not hold; otherwise its caller could ack past a page
	// missing the tail.
	GetHistoryTasks(
		ctx context.Context,
		req *p.GetHistoryTasksRequest,
		base func(context.Context, *p.GetHistoryTasksRequest) (*p.InternalGetHistoryTasksResponse, error),
	) (*p.InternalGetHistoryTasksResponse, error)

	// ReadHistoryBranch returns one page of a branch, merging cold rows with
	// the window's event batches. The wrapper parses treeID from the branch
	// token, since only it can reach the store's codec.
	//
	// It is called even if [ShardWriter.WritesHistory] is false: a replayed
	// tail may carry batches written under another node's composition.
	ReadHistoryBranch(
		ctx context.Context,
		req *p.InternalReadHistoryBranchRequest,
		treeID string,
		base func(context.Context, *p.InternalReadHistoryBranchRequest) (*p.InternalReadHistoryBranchResponse, error),
	) (*p.InternalReadHistoryBranchResponse, error)
}

// MetricsSink receives the server's metrics handler, which reaches
// [AbstractDataStoreFactory.NewFactory] only after the layer is composed. It is
// part of [ShardLayer] so a layer that cannot take it does not compile.
type MetricsSink interface {
	// Use is called with the handler given to NewFactory, before its stores
	// serve anything, once per persistence graph. Implementations keep the
	// first handler and ignore the rest.
	Use(h metrics.Handler)
}

// Options is what the layer contributes to the stores it decorates. Zero value:
// pure passthrough.
type Options struct {
	// Layer, when set, selects intercept mode: it hears acquires, takes the
	// eight writes into the WAL and serves the four reads. Nil is passthrough.
	Layer ShardLayer
	// Metrics receives the wrapper's counters. It is the layer's own emitter,
	// so one [MetricsSink.Use] points both halves at the same handler.
	//
	// Nil records nothing. waltz.Layer.Options fills it in; hand-built Options
	// without it give a working store that emits nothing.
	Metrics *walmetrics.Emitter
}

// AbstractDataStoreFactory decorates the plugin's abstract factory: the value a
// custom main hands to temporal.WithCustomDataStoreFactory.
type AbstractDataStoreFactory struct {
	base client.AbstractDataStoreFactory
	opts Options
}

var _ client.AbstractDataStoreFactory = (*AbstractDataStoreFactory)(nil)

// NewAbstractDataStoreFactory decorates base, in production the abstract
// factory of the plugin that owns the cold data.
func NewAbstractDataStoreFactory(base client.AbstractDataStoreFactory, opts Options) *AbstractDataStoreFactory {
	return &AbstractDataStoreFactory{base: base, opts: opts}
}

// NewFactory returns a decorated data store factory; every argument transits
// untouched. The metrics handler enters the layer here, via [MetricsSink], and
// only here: the first service to build persistence sets the handler the whole
// layer reports to.
func (f *AbstractDataStoreFactory) NewFactory(
	cfg config.CustomDatastoreConfig,
	r resolver.ServiceResolver,
	clusterName string,
	logger log.Logger,
	metricsHandler metrics.Handler,
) p.DataStoreFactory {
	if f.opts.Layer != nil {
		// Passthrough needs no handler: every counter here is on the
		// intercepted path.
		f.opts.Layer.Use(metricsHandler)
	}
	return NewDataStoreFactory(f.base.NewFactory(cfg, r, clusterName, logger, metricsHandler), f.opts)
}

// DataStoreFactory wraps the ExecutionStore and ShardStore; every other store
// is outside the layer and returned as the plugin built it.
type DataStoreFactory struct {
	base p.DataStoreFactory
	opts Options
}

var _ p.DataStoreFactory = (*DataStoreFactory)(nil)

func NewDataStoreFactory(base p.DataStoreFactory, opts Options) *DataStoreFactory {
	return &DataStoreFactory{base: base, opts: opts}
}

func (f *DataStoreFactory) Close() { f.base.Close() }

func (f *DataStoreFactory) NewExecutionStore() (p.ExecutionStore, error) {
	store, err := f.base.NewExecutionStore()
	if err != nil {
		return nil, err
	}
	// Returning the pair directly would box a nil *ExecutionStore into a
	// non-nil interface that panics on first use.
	decorated, err := NewExecutionStore(store, f.opts)
	if err != nil {
		return nil, err
	}
	return decorated, nil
}

func (f *DataStoreFactory) NewShardStore() (p.ShardStore, error) {
	store, err := f.base.NewShardStore()
	if err != nil {
		return nil, err
	}
	return NewShardStore(store, f.opts), nil
}

func (f *DataStoreFactory) NewTaskStore() (p.TaskStore, error) { return f.base.NewTaskStore() }

func (f *DataStoreFactory) NewFairTaskStore() (p.TaskStore, error) { return f.base.NewFairTaskStore() }

func (f *DataStoreFactory) NewMetadataStore() (p.MetadataStore, error) {
	return f.base.NewMetadataStore()
}

func (f *DataStoreFactory) NewQueue(queueType p.QueueType) (p.Queue, error) {
	return f.base.NewQueue(queueType)
}

func (f *DataStoreFactory) NewQueueV2() (p.QueueV2, error) { return f.base.NewQueueV2() }

func (f *DataStoreFactory) NewClusterMetadataStore() (p.ClusterMetadataStore, error) {
	return f.base.NewClusterMetadataStore()
}

func (f *DataStoreFactory) NewNexusEndpointStore() (p.NexusEndpointStore, error) {
	return f.base.NewNexusEndpointStore()
}
