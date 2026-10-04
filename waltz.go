// Package waltz puts a write-ahead log in front of a Temporal history shard's
// cold store: mutations are acked into the log and many are folded into one
// cold-store transaction. The log ([wal.Log]) and the cold store ([cold.Store])
// are the caller's. The shipped ones, wal/memwal and cold/memcold, die with the
// process.
//
// waltz targets go.temporal.io/server v1.29.6 and Go 1.26+. The server version
// is a floor, not a pin, so a newer server builds with no warning. The WAL
// record format mirrors v1.29.6's request structs field for field, and that is
// checked only here and not for every struct (open in DURABILITY.md): a field
// a newer server adds is dropped from a write already acked.
//
// [Compose] is the only composition; a new caller's need becomes a parameter
// there. [Layer.AbstractFactory] returns what a custom main passes to
// temporal.WithCustomDataStoreFactory.
//
// Configuration is a `wal` section in the custom datastore's options. Absent
// means passthrough; malformed refuses to start ([ADR 0006]).
//
// Compose the layer before building the server, so a failed budget check stops
// the binary. Call [Layer.Shutdown] after the server stops, so the drain runs
// with no writers left.
//
// [ADR 0006]: docs/adr/0006-the-wal-configuration-is-a-section-of-the-datastore-options.md
package waltz

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/temporal"

	"github.com/aromanovich/waltz/cold"
	"github.com/aromanovich/waltz/cycle"
	"github.com/aromanovich/waltz/wal"
	"github.com/aromanovich/waltz/walmetrics"
	"github.com/aromanovich/waltz/wrapper"
)

// Layer is the process's WAL layer: the shard registry the wrapper talks to,
// over the backends it was composed with.
//
// Build exactly one per process, not one per data store factory: two
// registries would hold two windows for one shard, each unaware of the other.
type Layer struct {
	// policy is kept so callers read the same policy the cycles do; sampling the
	// dynamic config again could give a different start-up value.
	policy  cycle.Policy
	manager *cycle.Manager
	// log is kept only to close it; nothing else releases its connections.
	log wal.Log
	// metrics is the node's one emitter, shared by the cycles and the stores
	// built over [Layer.Options], so one handler hand-off reaches both.
	metrics *walmetrics.Emitter
}

// TaskCategories builds the task category registry the layer decodes an
// inherited tail with. The caller builds it because the layer is composed
// before fx could provide it. The archival category exists only when cfg
// enables archival, so a default registry cannot replay archival tasks.
func TaskCategories(dc *dynamicconfig.Collection, cfg *config.Config) Registry {
	return Registry{r: temporal.TaskCategoryRegistryProvider(resource.ArchivalMetadataProvider(dc, cfg))}
}

// Registry is the task category registry a node decodes a tail with. Only
// [TaskCategories] and [DefaultTaskCategories] build one, so
// tasks.NewDefaultTaskCategoryRegistry cannot reach a composition: it matches
// today and diverges once archival is configured.
//
// [cycle.NewManager] refuses the zero value: a node with no registry would
// recover nothing.
type Registry struct{ r tasks.TaskCategoryRegistry }

// Categories unwraps the upstream interface that [cycle.Deps] takes.
func (reg Registry) Categories() tasks.TaskCategoryRegistry { return reg.r }

// DefaultTaskCategories is [TaskCategories] for a cluster with no archival. It
// goes through the same upstream path a node uses rather than calling
// tasks.NewDefaultTaskCategoryRegistry, so there is one rule for the set.
func DefaultTaskCategories() Registry {
	return TaskCategories(dynamicconfig.NewNoopCollection(), &config.Config{})
}

// checkPolicy refuses a nil policy up front; otherwise it would panic at the
// first read, the budget check.
func checkPolicy(policy cycle.Policy) error {
	if policy == nil {
		return errors.New("waltz: no policy: it is the whole of what this node runs at, so a process " +
			"reading a dynamic config passes NewPolicy(dc, cfg.WAL) and one holding numbers passes cycle.Fixed")
	}
	return nil
}

// Backends is where a layer's bytes go: the log that orders appends, and the
// cold store each drain commits to and an unknown outcome is read back from.
//
// The caller builds both; [Compose] builds neither. To run intercept mode over
// in-process backends, pass them here; never build a second registry.
type Backends struct {
	Log  wal.Log
	Cold cold.Store
}

// Compose builds the layer; every process running intercept mode calls it.
//
//   - policy is required. It is read at each decision: pass [NewPolicy] for a
//     dynamic config or [cycle.Fixed] for fixed numbers.
//   - categories is required: replay decodes a tail with it.
//   - logger may be nil (noop); the layer's warnings, such as a shutdown drain
//     that did not commit or a failed trim, are then dropped.
//   - handler is nil in production: the server hands one over later through
//     [wrapper.MetricsSink].
//
// Compose opens nothing and takes no context; connecting happens when the
// caller builds the backends. The backends stay the caller's and must outlive
// the layer, because [Layer.Shutdown] drains through them. The budget check
// (cycle.Config.CheckBudget) runs here, so call Compose before building the
// server.
func Compose(
	backends Backends,
	policy cycle.Policy,
	categories Registry,
	logger log.Logger,
	handler metrics.Handler,
) (*Layer, error) {
	if err := checkPolicy(policy); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = log.NewNoopLogger()
	}

	// Built here so the cycles and the stores ([Layer.Options]) share it. A nil
	// handler records nowhere until [walmetrics.Emitter.Use] supplies one.
	emitter := walmetrics.New(handler)

	manager, err := cycle.NewManager(cycle.Deps{
		Log: backends.Log,
		// Deps keeps writer and recoverer apart so a suite can fail one alone.
		Writer:    backends.Cold,
		Recoverer: backends.Cold,
		Registry:  categories.Categories(),
		Logger:    logger,
		Metrics:   emitter,
	}, policy)
	if err != nil {
		return nil, fmt.Errorf("waltz: building the cycle manager: %w", err)
	}
	return &Layer{
		manager: manager,
		log:     backends.Log,
		policy:  policy,
		metrics: emitter,
	}, nil
}

// Options is everything the wrapper needs: the registry (shard observer, write
// path and read path in one) and the layer's emitter, so the stores and the
// cycles report to the one handler [wrapper.MetricsSink] hands over.
func (l *Layer) Options() wrapper.Options {
	return wrapper.Options{Layer: l.manager, Metrics: l.metrics}
}

// AbstractFactory decorates base, the persistence plugin that owns the cold
// store, with opts, for temporal.WithCustomDataStoreFactory. Zero opts is
// passthrough, so a binary with no `wal` section uses this too.
//
// The server's metrics handler does not exist yet; it arrives at
// [wrapper.AbstractDataStoreFactory.NewFactory], which hands it to the layer
// through [wrapper.MetricsSink].
func AbstractFactory(base client.AbstractDataStoreFactory, opts wrapper.Options) client.AbstractDataStoreFactory {
	return wrapper.NewAbstractDataStoreFactory(base, opts)
}

// AbstractFactory is [AbstractFactory] with this layer's options. A layer
// never handed to its factory silently runs passthrough, so build the server
// with
//
//	temporal.WithCustomDataStoreFactory(layer.AbstractFactory(base))
//
// where base is the plugin whose stores hold the cold data.
func (l *Layer) AbstractFactory(base client.AbstractDataStoreFactory) client.AbstractDataStoreFactory {
	return AbstractFactory(base, l.Options())
}

// Policy is the source this layer's cycles read, section and dynamic config
// combined. Live settings may answer differently on each call.
func (l *Layer) Policy() cycle.Policy { return l.policy }

// Totals sums the counters of every shard this layer has held.
func (l *Layer) Totals() cycle.Totals { return l.manager.Totals() }

// ShardStats returns a snapshot of one shard's cycle, or false if this node
// holds none. It returns a value, not the cycle or the registry, so callers
// can read counters and epoch without gaining a write path or Retire/Close.
func (l *Layer) ShardStats(shard wal.ShardID) (cycle.Stats, bool) {
	c := l.manager.Shard(shard)
	if c == nil {
		return cycle.Stats{}, false
	}
	return c.Stats(), true
}

// RetireShard stops one shard's cycle without draining it, as a killed process
// would, and reports whether epoch matched. Unlike [Layer.Shutdown] it writes
// nothing.
//
// epoch names the acquisition to retire; a mismatch retires nothing. Without
// it, a late unload after the shard was reacquired would stop the new owner.
// [cycle.Manager.Write] checks epochs for the same reason.
//
// The stopped cycle stays registered on purpose: its tail is acked entries
// still in the log, and [Layer.ShardStats] and [Layer.Totals] must keep
// reporting them. Removing it would report zero, which reads as "nothing
// stranded".
func (l *Layer) RetireShard(shard wal.ShardID, epoch wal.Epoch) bool {
	c := l.manager.Shard(shard)
	// Retire the cycle whose epoch was checked, never a re-lookup, so a
	// concurrent acquire cannot redirect the retire onto its new cycle.
	if c == nil || c.Epoch() != epoch {
		return false
	}
	c.Retire()
	return true
}

// Shutdown drains every shard that still holds a window into the cold store,
// reports halted shards instead of draining them, and closes the log.
//
// Call it after the server has stopped, so no writer is still adding to the
// windows being drained, and while the backends are still open.
//
// budget bounds the apply transactions, run shard by shard (a shard whose tail
// is not yet replayed applies the replay first). A trim already in flight is
// waited out regardless: each attempt has its own detached one-minute context,
// plus one follow-up a forced trim may have queued. A drain cut short leaves a tail in the log, acked and
// not lost (I2), for the next owner's replay.
//
// The budget runs on a context detached from ctx's cancellation, because
// shutdown usually starts just after a cancel; inheriting it would return at
// once and leave a tail silently. With a valid budget the only error is an
// [*UndrainedError]. budget <= 0 is refused: [context.WithTimeout] treats zero as already
// expired, so it would drain nothing.
func (l *Layer) Shutdown(ctx context.Context, budget time.Duration) error {
	if budget <= 0 {
		return fmt.Errorf("waltz: a shutdown budget of %s is not a budget: pass the time the drains "+
			"may take, since zero here is a deadline already past rather than no limit", budget)
	}
	drainCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), budget)
	defer cancel()
	return l.close(drainCtx)
}

// UndrainedError lists the shards still holding acked entries after
// [Layer.Shutdown], with each one's entry count. A listed shard is never
// clean, even at count zero: a cycle that halted in replay and then failed to
// re-read the log reports zero (open in DURABILITY.md).
//
// It is not a lost write: the entries are in the log for the next owner's
// replay, so a node restarting with the same config can ignore it. It matters
// when removing the `wal` section: passthrough cannot see the log, so these
// entries would be stranded silently. Remove the section only after a
// Shutdown that returned nil.
type UndrainedError struct {
	Shards []cycle.Residue
}

func (e *UndrainedError) Error() string {
	entries := 0
	for _, r := range e.Shards {
		entries += r.Entries
	}
	var b strings.Builder
	fmt.Fprintf(&b, "waltz: the shutdown left %d acked entries unapplied across %d shard(s):",
		entries, len(e.Shards))
	for _, r := range e.Shards {
		fmt.Fprintf(&b, " shard %d at epoch %d holds %d", r.Shard, r.Epoch, r.Entries)
		if r.Cause != nil {
			fmt.Fprintf(&b, " (%v)", r.Cause)
		}
		b.WriteByte(';')
	}
	b.WriteString(" they are in the log for the next owner to replay, and a node that comes back " +
		"without the wal section will not replay them")
	return b.String()
}

// close is [Layer.Shutdown] on the detached context. The log closes after the
// drain because the drain still trims through it.
func (l *Layer) close(ctx context.Context) error {
	left := l.manager.Close(ctx)
	l.log.Close()
	if len(left) == 0 {
		return nil
	}
	return &UndrainedError{Shards: left}
}
