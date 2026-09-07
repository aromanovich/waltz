package cycle

import (
	"context"
	"fmt"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"

	"github.com/aromanovich/waltz/wal"
	"github.com/aromanovich/waltz/walmetrics"
)

// Manager is the node's cycles, one per shard, each pinned to the epoch it was
// created at. The wrapper's ShardObserver hook talks to it, and it is the only
// place a cycle is created or retired. An acquire is observable and a close is
// not, so a cycle is retired only by a higher epoch superseding it and nothing
// reaps an idle one — one goroutine and an empty accumulator, a bounded leak.
type Manager struct {
	deps   Deps
	policy Policy

	// held owns the shard map, the retired counters and the mutex over both.
	// Manager has no lock of its own, which is what stops any code here holding
	// one across a call into a cycle ([held]).
	held *held
}

// Totals is every cycle this node has held, added up. A witness's reading: a
// caller that needs one shard's state asks that shard's cycle.
type Totals struct {
	// Shards is the cycles held right now; Epochs counts every cycle this node
	// has ever created, so Epochs > Shards is a node that has re-acquired.
	Shards int
	Epochs int

	// Counters summed over every cycle this node has held, retired ones
	// included, in the same type a cycle counts into so this seam loses nothing.
	Counters

	// Acked, Applied and TailEntries come from the cycles held now: positions in
	// a log that outlives the cycle, so they stay outside [Counters]'s merge.
	Acked, Applied wal.Seqno
	TailEntries    int

	// Halted names every shard whose current cycle is not running. A retired
	// cycle's state stays out: being superseded is the fence working.
	Halted []string
}

// Totals reports the node's counters, retired cycles included.
func (m *Manager) Totals() Totals {
	retired, current := m.held.totals()

	// Asked outside the lock, which the shape above guarantees: each is answered
	// by that cycle's own goroutine, and every read path resolves through
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

// NewManager builds the registry. Every cycle it creates reads the same
// [Policy] — the source and not a copy, so a watermark that moves reaches the
// cycles this node already holds. It refuses a node whose hard_max × shards
// does not fit its tail budget ([Config.CheckBudget]), and a binary with no
// registry has no cycle at all, so that error is the layer refusing to start.
// Reading the budget once is sound because those four fields are the ones
// [Moving] does not carry.
func NewManager(deps Deps, policy Policy) (*Manager, error) {
	if err := policy().CheckBudget(); err != nil {
		return nil, err
	}
	// The other startup assertion: without a registry a node recovers nothing,
	// silently, until the first failover. See [Deps.Registry].
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

// Use points this node's cycles at the server's metrics handler; it satisfies
// wrapper.MetricsSink, which is how a handler built long after this registry
// reaches it. First call wins, and a nil handler is ignored.
func (m *Manager) Use(h metrics.Handler) { m.deps.Metrics.Use(h) }

// ShardAcquired fences the log at the new epoch and installs a fresh cycle for
// it. Fence first, and let the rangeID land only if the fence held: the log's
// epoch may never lag the database's. An acquire at an epoch a cycle already
// holds is idempotent, since fencing is; one at a lower epoch is refused with
// wal.ErrFenced, unwrapped so the shard controller sees the store's own error.
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

	previous := m.held.install(shard, fresh)

	if previous != nil {
		// Stopped without a drain: its epoch is fenced out, so what it held is
		// not its to apply and the entries stay in the log for this epoch. The
		// stop is what answers with its count, so nothing it does on the way
		// out — an in-flight page, a trim committing — falls between the two.
		//
		// No lock is held here and none can be: the read path resolves under
		// the same mutex, so a cycle inside a base read would wait on an
		// acquire waiting on it.
		m.held.retire(previous.Retire())
		m.deps.Logger.Info("apply cycle superseded",
			tag.ShardID(int32(shard)), tag.NewInt64("epoch", int64(epoch)))
	}
	return nil
}

// Shard returns the shard's current cycle, or nil when this node has not
// acquired it. The ExecutionStore wrapper asks per write.
func (m *Manager) Shard(shard wal.ShardID) *Cycle { return m.held.get(shard) }

// Close drains and stops every cycle. Shutdown is the one moment a tail is
// drained without a watermark asking for it.
func (m *Manager) Close(ctx context.Context) {
	for _, c := range m.held.takeAll() {
		if err := c.Close(ctx); err != nil {
			m.deps.Logger.Warn("apply cycle: the shutdown drain did not commit",
				tag.ShardID(int32(c.Shard())), tag.Error(err))
		}
	}
}
