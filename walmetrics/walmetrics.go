// Package walmetrics emits the WAL layer's numbers through the server's own
// [metrics.Handler], which arrives after the layer is built; [Emitter.Use] is
// that handover, late because the handler is taken by the first service to
// build persistence and the layer is composed before the server is.
//
// Per-shard quantities are histograms rather than gauges: a node-wide gauge
// with no shard tag reports whichever shard recorded last, and a shard tag is a
// cardinality class upstream has nowhere. Ratios go out as two counters, so the
// division is the reader's and "everything was dropped" is distinguishable from
// "there was nothing to drop". Nothing here is a latency; [WindowAge] is the
// age of the oldest un-drained mutation, which is what the age watermark fires
// on.
package walmetrics

import (
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/metrics"
)

// The New*Def calls register these with the server's global metric registry at
// package initialisation, so importing this package anywhere in the binary is
// what puts their description and unit in a scrape.
var (
	// InterceptedWrites counts writes the store sent at the layer, taken on the
	// way in: a write the tail refused, or one a halted cycle never appended, is
	// in it, and a retried write is in it once per attempt.
	InterceptedWrites = metrics.NewCounterDef("wal_intercepted_writes",
		metrics.WithDescription("Writes this store sent at the layer, by store method."))

	// OverlaidReads counts reads routed through the overlay, not reads the
	// window could answer: a counter that only fired on a hit would read zero
	// on a healthy idle cluster and zero on a layer wired up wrong.
	OverlaidReads = metrics.NewCounterDef("wal_overlaid_reads",
		metrics.WithDescription("Reads routed through the overlay, by store method."))

	// MergedTaskPages counts GetHistoryTasks pages routed at the layer, for
	// [OverlaidReads]' reason: routed, not merged. The name says merged and the
	// counter does not; renaming it would break every alert expression over it,
	// so the description carries the distinction instead.
	MergedTaskPages = metrics.NewCounterDef("wal_merged_task_pages",
		metrics.WithDescription("GetHistoryTasks pages routed at the layer's merge."))
	// MergedTaskCollisions counts keys both sources carried. The sources are
	// disjoint by construction — the window drops a task when the drain carrying
	// it takes the window, and the store gains that row only when the same drain
	// commits — so a non-zero value means something is wrong.
	MergedTaskCollisions = metrics.NewCounterDef("wal_merged_task_collisions",
		metrics.WithDescription("Task keys a merged page found in both the window and the cold store."))

	// Drains is tagged by what tripped it: size is the design working, age is a
	// shard nobody is pushing on, refusal is the accumulator's drain-and-retry,
	// sync is sync mode's one drain per write.
	Drains = metrics.NewCounterDef("wal_drains",
		metrics.WithDescription("Committed drains, by what triggered them."))
	DrainedMutations = metrics.NewCounterDef("wal_drained_mutations",
		metrics.WithDescription("Mutations carried into a committed drain — the collapse ratio's numerator."))
	DrainedWorkflows = metrics.NewCounterDef("wal_drained_workflows",
		metrics.WithDescription("Workflows written by a committed drain — the collapse ratio's denominator."))

	// AnsweredConditionFailures counts sync mode's carve-out: a drain of one mutation
	// whose condition did not hold, answered to its caller instead of halting
	// the shard. Expected traffic; the rate is what tells an operator whether
	// the shard is losing races or diverging.
	AnsweredConditionFailures = metrics.NewCounterDef("wal_answered_condition_failures",
		metrics.WithDescription("Synchronous drains whose condition did not hold and were answered to the caller."))

	// BackpressureRefusals is a write refused before its append, tagged by why:
	// entries means a stalled applier, bytes a workflow near the server's own
	// blob limits, unresolved an applier that cannot say what its last drain
	// did.
	BackpressureRefusals = metrics.NewCounterDef("wal_backpressure_refusals",
		metrics.WithDescription("Writes the shard refused before appending them, by what refused."))

	// Halts is tagged by class because the two are opposites: halted-lost is
	// fencing working and the next owner takes over, halted-invariant is a
	// divergence nobody else can pick up. Summing them would page for the first.
	Halts = metrics.NewCounterDef("wal_halts",
		metrics.WithDescription("Apply cycles that stopped, by class."))

	// Trims is the log's compaction, by outcome. A failed trim is retried at the
	// next cadence and halts nothing, so the pair is all a scrape sees of it; the
	// cause is in the trim's own warning.
	Trims = metrics.NewCounterDef("wal_trims",
		metrics.WithDescription("Log trims, by outcome."))

	// The three tail units, one observation per shard per event.
	TailEntries = metrics.NewDimensionlessHistogramDef("wal_tail_entries",
		metrics.WithDescription("Entries acked and not yet settled on one shard, at each append and each drain."))
	TailBytes = metrics.NewBytesHistogramDef("wal_tail_bytes",
		metrics.WithDescription("Encoded bytes acked and not yet settled on one shard, at each append and each drain."))
	// A timer's unit is the handler's, not the def's. Under the otel handler it
	// is milliseconds, truncated — every sub-millisecond age records 0 — unless
	// the operator sets recordTimerInSeconds; the tally fallback and the
	// capture handler the tests use record the duration whole.
	WindowAge = metrics.NewTimerDef("wal_window_age",
		metrics.WithDescription("Age of the oldest mutation in a window when it drained."))

	// UnappliedEntries is commitSeqno − appliedSeqno: how far the cold store is
	// behind the log. It is not the tail — a settle that keeps the watermark
	// parts the two — and both are emitted because no one number does both
	// jobs: a tail counting settled entries refuses writes over memory nobody
	// holds, and a watermark moved over an unsettled one strands a recovering
	// owner.
	UnappliedEntries = metrics.NewDimensionlessHistogramDef("wal_unapplied_entries",
		metrics.WithDescription("Seqnos acked into the log and not yet committed to the cold store, per shard."))

	// ReplayedEntries and ReplayDroppedEntries are the replay rate out of real
	// failovers: what a new owner found above the watermark and applied, against
	// the provisional entries among them it dropped. A node where the second
	// moves at all changed hands with a write in flight.
	ReplayedEntries = metrics.NewCounterDef("wal_replayed_entries",
		metrics.WithDescription("Entries a new owner read out of the tail a previous owner left and folded; "+
			"the ones it did not apply are wal_replay_dropped_entries, a subset of this count."))
	ReplayDroppedEntries = metrics.NewCounterDef("wal_replay_dropped_entries",
		metrics.WithDescription("Replayed entries dropped because their ack was provisional and their condition did not hold."))

	// DroppedTasks and WrittenTasks are invariant I7's drop: task rows a
	// committed drain did not write because their queue had already deleted the
	// range they fall in, against the rows it did write. Tagged by task
	// category, because immediate and scheduled tasks drop at unrelated rates.
	DroppedTasks = metrics.NewCounterDef("wal_dropped_tasks",
		metrics.WithDescription("Tasks a drain did not write because the queue had already completed past them (I7), by category."))
	WrittenTasks = metrics.NewCounterDef("wal_written_tasks",
		metrics.WithDescription("Tasks a drain wrote to the cold store, by category."))
)

// Tag keys this layer adds. Each has a small closed set of values.
const (
	TagTrigger = "trigger"
	TagLimit   = "limit"
	TagState   = "state"
	TagOutcome = "outcome"
)

// Trigger values for [Drains].
const (
	TriggerMutations = "mutations" // the size trigger, in mutations
	TriggerBytes     = "bytes"     // the size trigger, in bytes
	TriggerAge       = "age"       // the age trigger
	TriggerRefusal   = "refusal"   // fold.ErrRefused: a window the accumulator cannot express
	TriggerSync      = "sync"      // sync mode: one write, one drain
	TriggerReplay    = "replay"    // a tail a previous owner left, being applied
	TriggerExplicit  = "explicit"  // a caller asked — shutdown, or a test
	TriggerRead      = "read"      // [cycle.Config.DrainOnRead]: a read emptying the window it would have merged
)

// Limit values for [BackpressureRefusals], and outcome values for [Trims].
const (
	LimitEntries = "entries"
	LimitBytes   = "bytes"
	// LimitUnresolved is the bound that is not a size: the applier cannot read
	// what its last drain did, so nothing may be applied over it.
	LimitUnresolved = "unresolved"

	TrimStarted = "started"
	TrimFailed  = "failed"
)

// Emitter is the layer's handle on the stack. One per node, shared by every
// shard's cycle and by the ExecutionStore wrapper.
//
// [Emitter.Use] swaps in the server's handler and is the only mutation; every
// other method is an atomic load and a Record, safe from any goroutine.
//
// The zero value is not usable; [New] with a nil handler is the noop.
type Emitter struct {
	in   atomic.Pointer[instruments]
	used atomic.Bool
}

// instruments are the resolved metric objects, resolved once here rather than
// once per record.
type instruments struct {
	intercepted metrics.CounterIface
	overlaid    metrics.CounterIface
	taskPages   metrics.CounterIface
	collisions  metrics.CounterIface
	drains      metrics.CounterIface
	mutationsIn metrics.CounterIface
	workflows   metrics.CounterIface
	conditions  metrics.CounterIface
	refusals    metrics.CounterIface
	halts       metrics.CounterIface
	trims       metrics.CounterIface
	dropped     metrics.CounterIface
	written     metrics.CounterIface
	replayed    metrics.CounterIface
	replayDrops metrics.CounterIface

	tailEntries metrics.HistogramIface
	tailBytes   metrics.HistogramIface
	unapplied   metrics.HistogramIface
	windowAge   metrics.TimerIface
}

func resolve(h metrics.Handler) *instruments {
	return &instruments{
		intercepted: InterceptedWrites.With(h),
		overlaid:    OverlaidReads.With(h),
		taskPages:   MergedTaskPages.With(h),
		collisions:  MergedTaskCollisions.With(h),
		drains:      Drains.With(h),
		mutationsIn: DrainedMutations.With(h),
		workflows:   DrainedWorkflows.With(h),
		conditions:  AnsweredConditionFailures.With(h),
		refusals:    BackpressureRefusals.With(h),
		halts:       Halts.With(h),
		trims:       Trims.With(h),
		dropped:     DroppedTasks.With(h),
		written:     WrittenTasks.With(h),
		replayed:    ReplayedEntries.With(h),
		replayDrops: ReplayDroppedEntries.With(h),

		tailEntries: TailEntries.With(h),
		tailBytes:   TailBytes.With(h),
		unapplied:   UnappliedEntries.With(h),
		windowAge:   WindowAge.With(h),
	}
}

// New builds an emitter over h. A nil handler gives the noop one, which a
// component built before the server handed one over holds until [Emitter.Use]
// replaces it, and keeps for good if none ever arrives.
func New(h metrics.Handler) *Emitter {
	if h == nil {
		h = metrics.NoopMetricsHandler
	}
	e := &Emitter{}
	e.in.Store(resolve(h))
	return e
}

// Use points the emitter at the server's handler, once; later calls are
// ignored, so the layer's numbers stay with the first service whose stores it
// decorated. Safe to call while the layer is running.
func (e *Emitter) Use(h metrics.Handler) {
	if h == nil || e.used.Swap(true) {
		return
	}
	e.in.Store(resolve(h))
}

func (e *Emitter) load() *instruments { return e.in.Load() }

// InterceptedWrite counts one write into the WAL, tagged by store method.
func (e *Emitter) InterceptedWrite(op string) {
	e.load().intercepted.Record(1, metrics.OperationTag(op))
}

// OverlaidRead counts one read that went to the layer instead of straight to
// the store below, tagged by store method.
func (e *Emitter) OverlaidRead(op string) {
	e.load().overlaid.Record(1, metrics.OperationTag(op))
}

// MergedTaskPage counts one page of GetHistoryTasks routed at the layer.
func (e *Emitter) MergedTaskPage() { e.load().taskPages.Record(1) }

// TaskCollisions records what a merged page found in both sources. Emitted
// from the shard's own goroutine, the only place that knows: the wrapper counts
// pages routed, and a refused or passed-through page has no collisions.
func (e *Emitter) TaskCollisions(n int) {
	if n > 0 {
		e.load().collisions.Record(int64(n))
	}
}

// Drained records one committed drain: what tripped it, what the accumulator
// collapsed, and how old the window had got. One call rather than three, so a
// change cannot record the collapse of a window whose drain it did not count.
// The collapse pair arrives as two ints rather than a fold.Stats so that this
// package does not import fold.
func (e *Emitter) Drained(trigger string, mutations, workflows int, age time.Duration) {
	i := e.load()
	i.drains.Record(1, metrics.StringTag(TagTrigger, trigger))
	i.mutationsIn.Record(int64(mutations))
	i.workflows.Record(int64(workflows))
	i.windowAge.Record(age)
}

// Tail records the shard's tail as it stands, in the two units I10 bounds, plus
// how far the cold store is behind. Called wherever the tail moves.
func (e *Emitter) Tail(entries, bytes, unapplied int) {
	i := e.load()
	i.tailEntries.Record(int64(entries))
	i.tailBytes.Record(int64(bytes))
	i.unapplied.Record(int64(unapplied))
}

// Tasks records one committed drain's task accounting for one category: rows I7
// dropped and rows written. One call carries both, so a drop cannot be recorded
// without the denominator it is a share of. A category the drain did not carry
// emits nothing rather than a pair of zeroes.
func (e *Emitter) Tasks(category string, dropped, written int) {
	i := e.load()
	tag := metrics.TaskCategoryTag(category)
	if dropped > 0 {
		i.dropped.Record(int64(dropped), tag)
	}
	if written > 0 {
		i.written.Record(int64(written), tag)
	}
}

// Replayed records one completed replay: the entries a new owner read out of
// the tail it inherited, and the provisional ones among them it dropped.
// Emitted once the replay has finished rather than per entry, because a replay
// that failed part-way is retried whole from the watermark and would otherwise
// count its entries twice.
func (e *Emitter) Replayed(entries, dropped int) {
	i := e.load()
	if entries > 0 {
		i.replayed.Record(int64(entries))
	}
	if dropped > 0 {
		i.replayDrops.Record(int64(dropped))
	}
}

// AnsweredConditionFailure records an answered drain: sync mode's window of one.
func (e *Emitter) AnsweredConditionFailure() { e.load().conditions.Record(1) }

// BackpressureRefusal records I10 refusing a write, tagged with what refused
// it: one of the two size bounds, or the unresolved drain that is neither.
func (e *Emitter) BackpressureRefusal(limit string) {
	e.load().refusals.Record(1, metrics.StringTag(TagLimit, limit))
}

// Halt records a cycle stopping, tagged with the class. The caller passes the
// state's own name, so a state added to the cycle cannot be silently folded
// into a bucket here.
func (e *Emitter) Halt(state string) {
	e.load().halts.Record(1, metrics.StringTag(TagState, state))
}

// Trim records one trim, by outcome.
func (e *Emitter) Trim(outcome string) {
	e.load().trims.Record(1, metrics.StringTag(TagOutcome, outcome))
}
