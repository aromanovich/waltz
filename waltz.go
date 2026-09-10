// Package waltz puts a write-ahead log in front of a Temporal history shard's
// cold store, so that many mutations are acked into the log and folded into one
// cold-store transaction. It implements no persistence itself: the log is a
// [wal.Log] and the cold store is a [cold.Store], both the caller's, and the
// only log shipped here is wal/memwal, in memory.
//
// waltz is developed against go.temporal.io/server v1.29.6 and needs Go 1.26 or
// newer. The requirement on the server is a floor under minimal version
// selection and not a pin, so a consumer already on a newer one builds against
// it with no diagnostic: the WAL record format mirrors v1.29.6's request
// structs field-for-field, and the mirror's completeness is checked against
// that version here, never in a consumer's build. A field a newer server adds
// is a field this codec drops from a write it has already acked.
//
// [Compose] is the only composition; a new caller's need belongs there as a
// parameter. [Layer.AbstractFactory] is the door out: the value a custom main
// hands to temporal.WithCustomDataStoreFactory, which is the whole of how a
// server is built over this layer.
//
// The configuration is a `wal` section inside the custom datastore's own
// options. An absent section is passthrough; a malformed one is a refusal to
// start ([ADR 0006]).
//
// The layer's lifecycle brackets the server's: it is composed before the server
// is built, so a failed budget assertion stops the binary, and [Layer.Shutdown]
// runs after it has stopped, so the shutdown drain still has a store to write
// to.
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

// Layer is the WAL layer this process owns: the registry the wrapper talks to,
// over the backends it was composed with.
//
// Exactly one per process, not one per data store factory: a shard's cycle
// carries its epoch from the acquire through the writes that follow, so two
// registries would be two windows for one shard, each unaware of the other.
type Layer struct {
	// policy is what this layer's cycles read, kept so a caller can ask what the
	// node runs at rather than sample the dynamic config again: two samples of a
	// start-up setting can differ.
	policy  cycle.Policy
	manager *cycle.Manager
	// log is kept for its close alone: the cycles reach it through the manager,
	// and a backend holding a connection or a pinger has nobody else to release
	// it ([wal.Log.Close]).
	log wal.Log
	// metrics is this node's one emitter: the cycles record through it and so do
	// the stores [Layer.Options] composes, so the server's handler reaches both
	// halves of the layer's numbers by being handed over once.
	metrics *walmetrics.Emitter
}

// TaskCategories builds the task category registry the layer decodes an
// inherited tail with, here rather than from fx because the layer is composed
// first. It depends on cfg: the archival category exists exactly when archival
// is enabled, and a default registry would refuse to replay entries carrying
// archival tasks.
func TaskCategories(dc *dynamicconfig.Collection, cfg *config.Config) Registry {
	return Registry{r: temporal.TaskCategoryRegistryProvider(resource.ArchivalMetadataProvider(dc, cfg))}
}

// Registry is the answer to "which registry does this node decode a tail
// with", and it is constructible only by [TaskCategories] and
// [DefaultTaskCategories]. Upstream's
// interface is what [cycle.Deps] takes and must be — mutation.Decode needs it —
// but a composition that accepted it directly would accept
// tasks.NewDefaultTaskCategoryRegistry too, which is a second answer: the same
// set today, and a different one the day archival is configured.
//
// The zero value is refused by [cycle.NewManager] rather than treated as the
// default, since a node that decodes with no registry recovers nothing until
// the first failover.
type Registry struct{ r tasks.TaskCategoryRegistry }

// Categories is the registry as the layer's own components take it.
func (reg Registry) Categories() tasks.TaskCategoryRegistry { return reg.r }

// DefaultTaskCategories is [TaskCategories] for a cluster that configures no
// archival. It answers the same set as tasks.NewDefaultTaskCategoryRegistry and
// is not that call: a registry a node decodes with is built the way a node
// builds one, so a caller reaching for the plain default is a second answer to
// which registry a node has.
func DefaultTaskCategories() Registry {
	return TaskCategories(dynamicconfig.NewNoopCollection(), &config.Config{})
}

// checkPolicy refuses a composition with nothing to read its decisions from.
// Every one of them samples the policy, the budget assertion first, so a nil is
// a dereference at that first read rather than a composition that did not
// happen.
func checkPolicy(policy cycle.Policy) error {
	if policy == nil {
		return errors.New("waltz: no policy: it is the whole of what this node runs at, so a process " +
			"reading a dynamic config passes NewPolicy(dc, cfg.WAL) and one holding numbers passes cycle.Fixed")
	}
	return nil
}

// Backends is where a composed layer's bytes go: the log its appends are
// ordered in, and the cold store one drain becomes a transaction on and an
// unknown outcome is read back from.
//
// They are [Compose]'s parameter rather than something it builds, and that is
// the point of the type: this library implements neither. Both are seams the
// layer is meant to be answerable at without a cluster — wal/memwal is a whole
// implementation of the WAL contract (ADR 0002), and [cold.Store] exists so a
// drain's outcome can be varied without one. A caller that wants the intercept
// path in process reaches it here rather than by building a second registry,
// which is the one thing this package asks callers not to do.
type Backends struct {
	Log  wal.Log
	Cold cold.Store
}

// Compose is the one graph every process running intercept mode builds. Its
// four varying inputs:
//
//   - backends is where the bytes go, and the reason it is a parameter is on
//     [Backends];
//   - policy is required, and is a [cycle.Policy]: a source read at each
//     decision, so a caller holding one that moves ([NewPolicy]) and one holding
//     fixed numbers ([cycle.Fixed]) reach the same composition;
//   - categories is required: it is what replay decodes a tail with, and
//     [cycle.NewManager] refuses nil rather than starting a node whose recovery
//     is silently off;
//   - handler is optional, and nil is the production value: the server hands one
//     down through [wrapper.MetricsSink] after this runs.
//
// It opens nothing, reaches nothing and takes no context: everything that talks
// to a cluster happens while the backends are built. So whatever they hold
// stays the caller's, and must outlive the layer, since [Layer.Shutdown] drains
// through it.
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

	// The node's one emitter, built here rather than left to [cycle.NewManager]
	// so that the value the cycles record through is one this layer can also
	// hand the stores ([Layer.Options]). A nil handler records nowhere until
	// [walmetrics.Emitter.Use] arrives with the server's.
	emitter := walmetrics.New(handler)

	manager, err := cycle.NewManager(cycle.Deps{
		Log: backends.Log,
		// [cycle.Deps] keeps the two halves apart and this is the only place they
		// are spliced, so what a deployment cannot express a suite still can: a
		// watermark that stops answering while its applier goes on committing is
		// how a replay is driven to abandon a tail it has already applied.
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

// Options is the whole of what the wrapper is composed with: the registry is
// the shard observer, the write path and the read path at once, and the emitter
// is the one this layer's cycles record through, so a store built over these
// options and the cycle behind it report to the same handler — the one
// [wrapper.MetricsSink] hands over.
func (l *Layer) Options() wrapper.Options {
	return wrapper.Options{Layer: l.manager, Metrics: l.metrics}
}

// AbstractFactory is the value a custom main hands to
// temporal.WithCustomDataStoreFactory: base — the persistence plugin that owns
// the cold store — decorated with opts. opts is the whole of the mode, the zero
// value being passthrough, so this is the door a binary with no `wal` section
// takes as well.
//
// Metrics are not an argument: the server's handler does not exist yet and
// arrives at [wrapper.AbstractDataStoreFactory.NewFactory], which fills it into
// the stores and hands it to the layer through [wrapper.MetricsSink].
func AbstractFactory(base client.AbstractDataStoreFactory, opts wrapper.Options) client.AbstractDataStoreFactory {
	return wrapper.NewAbstractDataStoreFactory(base, opts)
}

// AbstractFactory is [AbstractFactory] carrying this layer's own options, and
// the reason it is a method: the pairing of a composition with the factory that
// carries it was written out at every call site, and a layer composed but never
// handed to one is a node running passthrough with a `wal` section that says
// otherwise — which nothing reports, since that is what an empty layer looks
// like from outside. So the whole of building a server over this library is
//
//	temporal.WithCustomDataStoreFactory(layer.AbstractFactory(base))
//
// where base is the plugin whose stores hold the cold data.
func (l *Layer) AbstractFactory(base client.AbstractDataStoreFactory) client.AbstractDataStoreFactory {
	return AbstractFactory(base, l.Options())
}

// Policy is what this layer's cycles read: the section's half and the dynamic
// config's, as one source. Calling it answers the policy now, which for the
// live settings need not be what it answered a minute ago.
func (l *Layer) Policy() cycle.Policy { return l.policy }

// Totals is what every shard this layer has held reports, summed — the number a
// witness reads to say the layer was not empty.
func (l *Layer) Totals() cycle.Totals { return l.manager.Totals() }

// ShardStats is what one shard's cycle knows about itself, and false if this
// node holds none. These and not the registry: the registry is also a write
// path, and [wrapper.Options] is the one that has an epoch check in front of it.
//
// A value and not the cycle: callers may observe its counters and epoch, but
// may not gain its Retire and Close controls.
func (l *Layer) ShardStats(shard wal.ShardID) (cycle.Stats, bool) {
	c := l.manager.Shard(shard)
	if c == nil {
		return cycle.Stats{}, false
	}
	return c.Stats(), true
}

// RetireShard stops one shard's cycle without draining it, and reports whether
// there was one. It is what a process that died leaves behind, which is why it
// is named apart from [Layer.Shutdown]: a drain writes, and a kill does not.
func (l *Layer) RetireShard(shard wal.ShardID) bool {
	c := l.manager.Shard(shard)
	if c == nil {
		return false
	}
	c.Retire()
	return true
}

// Shutdown stops the layer: every shard that still holds a window is drained
// into the cold store, and the log is released.
//
// It must run after the server has stopped: the drain writes to the cold store
// the mutations of writers the server is shutting down. budget bounds the apply
// transactions — one per shard, in sequence — and not a trim already in flight,
// which is waited out on the minute of its own detached context; a drain the
// budget cuts short leaves a tail, not lost data (invariant I2: it is in the
// log, acked), which the next owner's replay picks up.
//
// The budget is put on a context detached from the caller's cancellation,
// and that detach is why this is a door rather than an idiom every caller
// writes. A shutdown drain runs where a context has just been cancelled — that
// is what "shutdown" means — and one inheriting that cancellation returns at
// once, leaving a tail behind and nothing in the log that says so.
// Given a budget, the error is an [*UndrainedError] and nothing else: every
// tail emptied, or these did not. A budget that is not one is refused instead of
// obeyed: [context.WithTimeout] reads zero as a deadline already past, where
// much of Go reads it as no limit, so obeying it would drain nothing and report
// every shard as holding a tail — the answer a caller passing zero meant least.
func (l *Layer) Shutdown(ctx context.Context, budget time.Duration) error {
	if budget <= 0 {
		return fmt.Errorf("waltz: a shutdown budget of %s is not a budget: pass the time the drains "+
			"may take, since zero here is a deadline already past rather than no limit", budget)
	}
	drainCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), budget)
	defer cancel()
	return l.close(drainCtx)
}

// UndrainedError is what [Layer.Shutdown] answers when a tail outlived it: the
// shards still holding acked entries no drain applied, and how many each holds.
//
// It reports neither a lost write nor a failed shutdown. Those entries are in
// the log and a successor's replay is what they are there for, so a node
// restarting into the same configuration needs nothing from this value. One
// caller does: whoever is taking the layer *out*. Removing the `wal` section
// strands exactly these entries and says nothing, because passthrough composes
// no log and so cannot see that they exist — which makes a shutdown that
// answered nil the only evidence that removing it is safe.
type UndrainedError struct {
	// Shards is every shard whose tail outlived the shutdown.
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

// close is [Layer.Shutdown] once the context is the layer's own.
//
// The log is closed after the drain and not before: a drain still trims through
// it, and a backend whose close ends the ownership that trim rests on would fail
// it and leave the log unshortened.
func (l *Layer) close(ctx context.Context) error {
	left := l.manager.Close(ctx)
	l.log.Close()
	if len(left) == 0 {
		return nil
	}
	return &UndrainedError{Shards: left}
}
