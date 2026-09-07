// Package wrapper is the WAL layer's seam into a running temporal-server: a
// decorator over the base plugin's DataStoreFactory whose ExecutionStore and
// ShardStore the history service talks to in place of the plugin's own.
//
// [Options] is the whole of the mode switch: no layer is passthrough, where
// every call transits; a layer is intercept, where eleven methods are answered
// from the WAL and a twelfth is refused ([ErrCompleteHistoryTaskUnsupported]).
//
// [ADR 0003] is why this runs in the server's process. A § number here cites
// the design brief the research prototype was written against, which is not in
// this tree; what it says that still binds is in the handbook.
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

// ShardObserver is told that a shard changed hands; the epoch is the new
// rangeID (invariant I11). Nothing reports the other direction: closing a shard
// makes no persistence call.
type ShardObserver interface {
	// ShardAcquired runs before the base store commits the bump, and an error
	// from it fails the acquire without the base store being called, so a failed
	// fence never leaves a moved rangeID behind (design §9 step 1). The error
	// reaches the shard controller unwrapped.
	ShardAcquired(ctx context.Context, shard wal.ShardID, epoch wal.Epoch) error
}

// ShardLayer is the WAL layer as one shard's stores see it. One interface
// rather than four fields, because a writer with no reader reads stale, a write
// path never told about the acquire refuses every write for that shard, and a
// layer nobody handed the metrics handler to emits nothing while every suite
// stays green. The last of those is the quietest, which is why it is a face
// here rather than a type assertion at the hand-off.
type ShardLayer interface {
	ShardObserver
	ShardWriter
	ShardReader
	MetricsSink
}

// ShardWriter is the WAL layer's write path: one write, acked into the shard's
// log and applied, with the outcome reported before the call returns.
type ShardWriter interface {
	// Write acks m into its shard's log and reports what the apply transaction
	// did with it; the mutation names its own shard.
	//
	// The layer takes ownership of m's request: in a windowed mode it is
	// retained past this call, and the drain stamps its rangeID. A caller may
	// not read or reuse it once Write has returned.
	//
	// epoch is the rangeID the caller wrote under, so a write from a fenced-out
	// shard context is refused rather than re-stamped with this node's. Zero
	// means "the caller named no epoch", not "epoch 0"; the two deletes and the
	// range delete carry none, and the drain's own CAS fences them instead.
	//
	// The error is the store's own (condition failure, fenced shard, tail at its
	// bound), unwrapped, and is attributable to this caller only at a window of
	// one. base is called inside the goroutine that owns the window, at most
	// once per asserted row.
	Write(
		ctx context.Context,
		m mutation.Mutation,
		epoch wal.Epoch,
		base *baserow.Rows,
	) error
}

// ShardReader is the read path: the three reads whose answer one of the writes
// can change. base is how the cold store is reached, since the layer below may
// not name a store; the layer calls it inside the goroutine that owns the
// window, so a read cannot observe a drain in flight. Errors come back
// unwrapped.
type ShardReader interface {
	// GetWorkflowExecution answers a mutable-state read: the base row merged
	// with whatever the shard's window holds for the run, or the window's answer
	// alone where it holds whole state, or NotFound where it holds a tombstone.
	GetWorkflowExecution(
		ctx context.Context,
		req *p.GetWorkflowExecutionRequest,
		base func(context.Context) (*p.InternalGetWorkflowExecutionResponse, error),
	) (*p.InternalGetWorkflowExecutionResponse, error)

	// GetCurrentExecution answers a current-execution read, a separate question
	// from the one above: that row is written by the window's last writer, not
	// by the merged request.
	GetCurrentExecution(
		ctx context.Context,
		req *p.GetCurrentExecutionRequest,
		base func(context.Context) (*p.InternalGetCurrentExecutionResponse, error),
	) (*p.InternalGetCurrentExecutionResponse, error)

	// GetHistoryTasks answers one page of a task range from the cold store's
	// rows and the shard's window at once. Its base closure takes a request
	// because the merge asks a different question than the caller did: BatchSize
	// minus what the window contributes, resumed from the base's own token. The
	// returned token is this layer's, carrying the base's inside it. Unlike the
	// two above, refused for a shard the layer does not hold, whose one caller
	// would otherwise ack past a page short a tail.
	GetHistoryTasks(
		ctx context.Context,
		req *p.GetHistoryTasksRequest,
		base func(context.Context, *p.GetHistoryTasksRequest) (*p.InternalGetHistoryTasksResponse, error),
	) (*p.InternalGetHistoryTasksResponse, error)
}

// MetricsSink is the layer's half of the metrics hand-off: the server's own
// handler reaches [NewFactory] only after the layer is composed, so it arrives
// afterwards rather than at construction. A face of [ShardLayer], so a layer
// that cannot take it does not compile — the failure it replaces was silent, a
// renamed method leaving the layer unmetered with every suite green.
type MetricsSink interface {
	// Use is called with the handler the server gave NewFactory, before the
	// stores it built have served anything, and once per persistence graph: an
	// implementation takes the first handler and ignores the rest.
	Use(h metrics.Handler)
}

// Options is what the layer contributes to the stores it decorates. Zero value:
// pure passthrough.
type Options struct {
	// Layer, when set, is intercept mode: acquires are reported to it, the eight
	// writes go into the WAL through it, the two mutable-state reads through its
	// overlay and the task read through its merge. Nil is passthrough.
	Layer ShardLayer
	// Metrics is where the wrapper's own counters go, and it is the emitter the
	// layer records through rather than a handler of this seam's own: both
	// halves of the numbers are then pointed at the server's stack by the one
	// [MetricsSink.Use] below, so no service's handler can take half of them.
	//
	// Nil records nowhere and stays that way — nothing here can reach the
	// layer's emitter, [MetricsSink] being a write-only door. A caller that
	// wants the counters takes its options from a composition
	// (waltz.Layer.Options), which fills this in; one built by hand gets a store
	// that works and emits nothing.
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

// NewFactory hands the server a decorated data store factory. Every argument
// transits untouched (the plugin discards the resolver and hard-wires its own),
// and this is where the server's metrics handler enters the layer, through
// [MetricsSink] and only there: the stores this builds record into
// [Options.Metrics], which the layer already holds, so the handler taken by the
// first service to build persistence is the one the whole layer reports to.
func (f *AbstractDataStoreFactory) NewFactory(
	cfg config.CustomDatastoreConfig,
	r resolver.ServiceResolver,
	clusterName string,
	logger log.Logger,
	metricsHandler metrics.Handler,
) p.DataStoreFactory {
	if f.opts.Layer != nil {
		// Passthrough composes no layer, so there is nothing to tell — and
		// nothing to count either: every counter this package raises is on the
		// intercepted path.
		f.opts.Layer.Use(metricsHandler)
	}
	return NewDataStoreFactory(f.base.NewFactory(cfg, r, clusterName, logger, metricsHandler), f.opts)
}

// DataStoreFactory is the decorator proper: the ExecutionStore and the
// ShardStore come back wrapped, everything else as the plugin built it, since
// matching, visibility, cluster metadata and the queues are outside the layer
// (§0).
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
	// Returning the constructor's pair straight through would box a nil
	// *ExecutionStore into a non-nil p.ExecutionStore, so a caller branching on
	// the store rather than the error gets one whose first method dereferences
	// nil — the stack trace the refusal exists to avoid.
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
