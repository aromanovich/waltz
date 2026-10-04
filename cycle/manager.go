package cycle

import (
	"context"
	"fmt"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"

	"github.com/aromanovich/waltz/cold"
	"github.com/aromanovich/waltz/wal"
	"github.com/aromanovich/waltz/walmetrics"
)

// Manager holds the node's cycles, one per shard, each pinned to the epoch it
// was created at. The wrapper's ShardObserver hook talks to it, and it is the
// only place a cycle is created. A cycle leaves the map only when a higher
// epoch supersedes it or the node shuts down; an idle one is never reaped (a
// bounded leak: one goroutine, an empty accumulator). A cycle stopped by name
// (Layer.RetireShard) stays in the map, reporting halted-lost, because its
// tail is acked entries the reports still read.
type Manager struct {
	deps   Deps
	policy Policy

	// held owns the shard map, the retired counters and their mutex. Manager
	// has no lock of its own, so nothing here can hold one across a call into
	// a cycle ([held]).
	held *held
}

// Totals is every cycle this node has held, added up. One shard's state comes
// from that shard's cycle.
type Totals struct {
	// Shards is the cycles held now; Epochs counts every cycle ever created,
	// so Epochs > Shards means the node has re-acquired.
	Shards int
	Epochs int

	// Counters summed over every cycle this node has held, retired included.
	Counters

	// Acked and Applied are log positions and TailEntries the tail's current
	// size, so all three come from the cycles held now, outside [Counters]: a
	// retired cycle's tail is its successor's to replay and count again.
	Acked, Applied wal.Seqno
	TailEntries    int

	// Halted names every shard whose current cycle is not running, including
	// one stopped by name. Superseded cycles are left out: that is the fence
	// working.
	Halted []string
}

// Totals reports the node's counters, retired cycles included.
func (m *Manager) Totals() Totals {
	retired, current := m.held.totals()

	// Ask each cycle outside the lock: the read path resolves through
	// [Manager.Shard], which wants the same mutex.
	total := retired
	total.Shards = len(current)
	total.Epochs = retired.Epochs + len(current)
	for _, c := range current {
		s := c.Stats()
		total.Counters.add(s.Counters)
		total.Acked += s.CommitSeqno
		total.Applied += s.AppliedSeqno
		total.TailEntries += s.TailEntries
		if s.State != StateRunning {
			total.Halted = append(total.Halted,
				fmt.Sprintf("shard %d at epoch %d: %v", c.Shard(), c.Epoch(), s.State))
		}
	}
	return total
}

// NewManager builds the registry. Every cycle reads the same [Policy] source,
// not a copy, so a moved setting reaches cycles already held. An error means
// the layer refuses to start: HardMaxBytes × MaxShards does not fit the tail
// budget ([Config.CheckBudget]), or there is no task-category registry.
// Reading the budget once is sound because [Moving] carries none of its fields.
func NewManager(deps Deps, policy Policy) (*Manager, error) {
	if err := policy().CheckBudget(); err != nil {
		return nil, err
	}
	// Without a registry a node silently recovers nothing until the first
	// failover. See [Deps.Registry].
	if deps.Registry == nil {
		return nil, ErrNoRegistry
	}
	if deps.Logger == nil {
		deps.Logger = log.NewNoopLogger()
	}
	if deps.Metrics == nil {
		// One emitter per node, so [Manager.Use] reaches even cycles acquired
		// before the server handed a handler over.
		deps.Metrics = walmetrics.New(nil)
	}
	return &Manager{deps: deps, policy: policy, held: newHeld()}, nil
}

// WritesHistory reports whether an intercepted write's event batches ride the
// appended record. A cold store that declares [cold.HistoryApplier] writes them
// in the drain; any other gets them through the base store before the append.
// There is no setting: deriving it from the composed store keeps who writes the
// batches and who is told to from being configured apart.
func (m *Manager) WritesHistory() bool {
	_, ok := m.deps.Writer.(cold.HistoryApplier)
	return ok
}

// Use points this node's cycles at the server's metrics handler (it satisfies
// wrapper.MetricsSink). First call wins; a nil handler is ignored.
func (m *Manager) Use(h metrics.Handler) { m.deps.Metrics.Use(h) }

// ShardAcquired fences the log at the new epoch and installs a fresh cycle.
// The fence comes first and the rangeID lands only if it held: the log's epoch
// may never lag the database's. An acquire at the held epoch is a no-op; one
// at a lower epoch returns wrapped wal.ErrFenced naming both. The log's own
// Fence error is returned unwrapped.
func (m *Manager) ShardAcquired(ctx context.Context, shard wal.ShardID, epoch wal.Epoch) error {
	current := m.held.get(shard)

	if current != nil && current.Epoch() == epoch {
		return nil
	}
	if current != nil && current.Epoch() > epoch {
		return fmt.Errorf("cycle: shard %d is held at epoch %d, refusing to acquire at %d: %w",
			shard, current.Epoch(), epoch, wal.ErrFenced)
	}
	if err := m.deps.Log.Fence(ctx, shard, epoch); err != nil {
		// Unwrapped: the shard's write path switches on concrete types, and a
		// wrapped one falls to the default arm.
		return err
	}

	fresh := New(shard, epoch, m.deps, m.policy)

	previous, took := m.held.install(shard, fresh)
	if !took {
		// The fence stays (it only changes ownership; the next owner fences
		// above it). The cycle may not: it would ack into a log no longer
		// drained.
		fresh.Retire()
		return fmt.Errorf("%w: shard %d at epoch %d", ErrClosed, shard, epoch)
	}

	if previous != nil {
		// Stopped without a drain: its epoch is fenced out, so its entries stay
		// in the log for this epoch. Retire itself returns the counters, so
		// nothing it does on the way out falls between stop and count.
		// No lock may be held here: the read path resolves under the same
		// mutex, so a cycle inside a base read would deadlock with us.
		m.held.retire(previous.Retire())
		m.deps.Logger.Info("apply cycle superseded",
			tag.ShardID(int32(shard)), tag.NewInt64("epoch", int64(epoch)))
	}
	return nil
}

// Shard returns the shard's current cycle, or nil when this node has not
// acquired it. The ExecutionStore wrapper asks per write.
func (m *Manager) Shard(shard wal.ShardID) *Cycle { return m.held.get(shard) }

// Residue is one shard a shutdown could not empty: its tail still held acked
// entries no drain applied. They are not lost (a successor replays them from
// the log), so a residue is not a failed shutdown. It is reported because only
// the operator knows whether a successor follows; a node restarted in
// passthrough reads no log, so this is the last chance to name them.
type Residue struct {
	Shard wal.ShardID
	Epoch wal.Epoch
	// Entries is the tail at its last publish: acked, unsettled, for the next
	// owner to apply. For a cycle halted inside its replay it can read zero
	// over entries the log still holds (an open entry in DURABILITY.md).
	Entries int
	// Cause is what the shutdown drain returned (the halt or stall that kept
	// the tail), or nil if the cycle was already stopped by name.
	Cause error
}

// Close drains and stops every cycle and returns every shard whose tail it
// could not empty.
func (m *Manager) Close(ctx context.Context) []Residue {
	var left []Residue
	for _, c := range m.held.takeAll() {
		err := c.Close(ctx)
		if err != nil {
			m.deps.Logger.Warn("apply cycle: the shutdown drain did not commit",
				tag.ShardID(int32(c.Shard())), tag.Error(err))
		}
		residue, held := c.residue(err)
		if !held {
			continue
		}
		m.deps.Logger.Warn("apply cycle: stopped holding acked entries no drain applied",
			tag.ShardID(int32(c.Shard())), tag.NewInt64("epoch", int64(c.Epoch())),
			tag.NewInt64("entries", int64(residue.Entries)))
		left = append(left, residue)
	}
	return left
}

// residue is what a cycle held when its loop stopped, read off the mirror
// since no loop is left to ask. cause is its shutdown drain's error.
// A non-nil cause makes even an empty tail a residue: a failed close never
// established what the shard holds. A failed watermark or log read leaves the
// tail at its floor, which reads as zero exactly like a clean shard, and the
// caller is deciding whether removing the layer strands anything.
func (c *Cycle) residue(cause error) (Residue, bool) {
	entries, _ := c.mirror.Size()
	if entries == 0 && cause == nil {
		return Residue{}, false
	}
	return Residue{Shard: c.shard, Epoch: c.epoch, Entries: int(entries), Cause: cause}, true
}
