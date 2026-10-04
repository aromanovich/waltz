// Package cycle is the layer's state machine: one goroutine per (shard, epoch)
// owns the accumulator, the drain, the apply transaction, the trim cadence and
// the four reads.
//
// It names no cold store; it reaches one only through [cold.Applier],
// [cold.Watermarker] and closures the caller passes in. The halted states exist
// because ownership loss is discovered, not announced: a shard close makes no
// persistence call. See [Chapter 06].
//
// [Chapter 06]: ../docs/handbook/06-shard-lifecycle.md
package cycle

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/channel"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/service/history/tasks"

	"github.com/aromanovich/waltz/apply"
	"github.com/aromanovich/waltz/baserow"
	"github.com/aromanovich/waltz/cold"
	"github.com/aromanovich/waltz/cycle/tailstate"
	"github.com/aromanovich/waltz/cycle/trim"
	"github.com/aromanovich/waltz/cycle/window"
	"github.com/aromanovich/waltz/fold"
	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/wal"
	"github.com/aromanovich/waltz/walmetrics"
)

// State is where a cycle stands. Only [StateRunning] accepts work.
type State int

const (
	// StateRunning: the shard is this cycle's to write.
	StateRunning State = iota
	// StateHaltedLost: the shard was fenced away (I4). The window is dropped,
	// nothing is trimmed, and the next owner replays the tail.
	StateHaltedLost
	// StateHaltedInvariant: this process diverged from what it acked. Causes: a
	// condition failure not attributable to one caller, an acked entry that
	// would not fold, an ambiguous drain that had not committed, a second writer
	// at this epoch ([ErrTailNotEmpty]), an unreadable append outcome, or a
	// replay entry it could not take. No retry, no failover.
	StateHaltedInvariant
)

func (s State) String() string {
	switch s {
	case StateRunning:
		return "running"
	case StateHaltedLost:
		return "halted-lost"
	case StateHaltedInvariant:
		return "halted-invariant"
	}
	return fmt.Sprintf("State(%d)", int(s))
}

// ErrHalted matches (errors.Is) every refusal from a halted cycle, and [ask]'s
// answer once the loop is gone. [storeError] turns a halted-lost write into
// ShardOwnershipLost; reads get it only where [loopRoute] or [stoppedRoute]
// refuses as halted. The cause is wrapped, so [apply.InvariantViolationError]
// stays reachable.
var ErrHalted = errors.New("cycle: the shard is halted")

// ErrTailNotEmpty reports an append refused because the log already holds that
// seqno. The cycle replays past the whole tail before appending and reads back
// its own ambiguous appends ([Cycle.settleAppend]), so this means a second
// writer at this epoch. It halts.
var ErrTailNotEmpty = errors.New("cycle: the log holds an entry at a seqno this cycle replayed past")

// ErrBudget refuses a policy whose HardMaxBytes × MaxShards does not fit the
// node's tail budget. See [Config.CheckBudget].
var ErrBudget = errors.New("cycle: the per-shard tail bound does not fit the node's budget")

// ErrNoRegistry refuses a nil [Deps.Registry]: replay decodes an inherited tail
// through it, so nil would be recovery silently switched off.
var ErrNoRegistry = errors.New("cycle: no task category registry, so no tail could ever be replayed")

// ErrClosed refuses an acquire after [Manager.Close]. Such a cycle would ack
// into a log being released, be drained by nothing and appear in no [Residue],
// while an empty residue is the caller's only evidence that removing the layer
// strands no acked entry.
var ErrClosed = errors.New("cycle: the layer has shut down and acquires no more shards")

// ErrNoBaseRow refuses a write whose assertion must be checked against the cold
// store when the caller brought no [baserow.Rows]. It refuses rather than skips:
// a refused write acked nothing, while a skip would ack an unchecked assertion
// and halt the next owner.
var ErrNoBaseRow = errors.New("cycle: an assertion the window does not determine, and no cold-store read to settle it")

// Config is the cycle's policy. The zero value is not usable; [Defaults] is
// the measured one.
type Config struct {
	// Mutations and Bytes are the size trigger, whichever trips first. Both
	// sit at the measured collapse knee; changing either means re-measuring.
	Mutations int
	Bytes     int
	// Age drains an idle window, and is how often a stalled shard (last drain's
	// outcome unreadable) re-asks the cold store. A recovery-budget choice, not
	// a measured one. Non-positive means the default, not "no age rule"
	// ([Config.fill]).
	Age time.Duration
	// TrimEvery and TrimAfter are the trim cadence, whichever trips first. A
	// Log.Trim per drain is a transaction per drain for no gain.
	TrimEvery int
	TrimAfter time.Duration
	// Sync makes every [Cycle.write] drain before it returns and report the
	// drain's outcome.
	Sync bool

	// DrainOnRead drains the window before each read, so the cold store serves
	// every read. Off in [Defaults]: a transaction per read over a window.
	DrainOnRead bool

	// HardMaxEntries and HardMaxBytes are I10's bound on one shard's tail
	// (acked, not yet settled). Both are needed: entries alone do not bound
	// bytes (mutable state can be 8 MB), bytes alone do not bound replay. See
	// [Config.CheckBudget].
	HardMaxEntries int
	HardMaxBytes   int

	// MaxShards and TailBudgetBytes are the node's side of that budget.
	// MaxShards is what this node may own at once, not the cluster's shard
	// count: 256 is 128 in steady state, doubled for a failover.
	MaxShards       int
	TailBudgetBytes int

	// timeSource is the clock for the age and trim cadences; tests drive it
	// with clock.EventTimeSource.
	timeSource clock.TimeSource
}

// Defaults is the measured policy.
func Defaults() Config {
	return Config{
		Mutations:       256,
		Bytes:           256 << 10,
		Age:             5 * time.Second,
		TrimEvery:       16,
		TrimAfter:       60 * time.Second,
		HardMaxEntries:  8192,
		HardMaxBytes:    8 << 20,
		MaxShards:       256,
		TailBudgetBytes: 2 << 30,
	}
}

// fill defaults the clock, the four bounds and the age, and nothing else. Other
// zeros are meaningful: a size trigger of zero drains every write
// ([window.Window.Trips]), a trim cadence of zero trims at every drain. A zero
// bound would refuse everything or hold an unbounded tail; a zero age makes
// the loop's timer fire continuously, a CPU per shard.
//
// [Fixed] runs it once; [Live] runs it at every answer, since the age is a
// live setting and a dynamic-config zero would otherwise cause that spin.
func (c *Config) fill() {
	if c.timeSource == nil {
		c.timeSource = clock.NewRealTimeSource()
	}
	d := Defaults()
	if c.Age <= 0 {
		c.Age = d.Age
	}
	if c.HardMaxEntries <= 0 {
		c.HardMaxEntries = d.HardMaxEntries
	}
	if c.HardMaxBytes <= 0 {
		c.HardMaxBytes = d.HardMaxBytes
	}
	if c.MaxShards <= 0 {
		c.MaxShards = d.MaxShards
	}
	if c.TailBudgetBytes <= 0 {
		c.TailBudgetBytes = d.TailBudgetBytes
	}
}

// watermarks is the size trigger in the window's terms.
func (c Config) watermarks() window.Watermarks {
	return window.Watermarks{Mutations: c.Mutations, Bytes: c.Bytes}
}

// cadence is the trim cadence in the trimmer's terms.
func (c Config) cadence() trim.Cadence {
	return trim.Cadence{Every: c.TrimEvery, After: c.TrimAfter}
}

// CheckBudget asserts [Config.HardMaxBytes] × [Config.MaxShards] fits in
// [Config.TailBudgetBytes] ([Defaults] fits exactly). It bounds encoded bytes,
// not RSS.
//
// [Config.MaxShards] is a premise, not a limit: acquires past it are not
// refused, since a node over its share is a cluster that just lost hosts. Such
// a node may exceed the budget; I10 still bounds each shard.
func (c Config) CheckBudget() error {
	cfg := c
	cfg.fill()
	held := int64(cfg.HardMaxBytes) * int64(cfg.MaxShards)
	// Signed overflow can wrap below any budget, so an oversized bound would
	// pass the comparison. Refuse it here. Both factors are positive after
	// fill, so the division is safe.
	if held/int64(cfg.MaxShards) != int64(cfg.HardMaxBytes) {
		return fmt.Errorf("%w: %d shards × %d bytes overflows a signed 64-bit count, so no node holds "+
			"it and the budget of %d is not the reason",
			ErrBudget, cfg.MaxShards, cfg.HardMaxBytes, cfg.TailBudgetBytes)
	}
	if held > int64(cfg.TailBudgetBytes) {
		return fmt.Errorf("%w: %d shards × %d bytes is %d bytes of tail, over the node's budget of %d",
			ErrBudget, cfg.MaxShards, cfg.HardMaxBytes, held, cfg.TailBudgetBytes)
	}
	return nil
}

// Deps are the collaborators one cycle needs. All are shared across shards; the
// cycle owns none of them.
type Deps struct {
	Log       wal.Log
	Writer    cold.Applier
	Recoverer cold.Watermarker
	Logger    log.Logger
	// Registry is required ([ErrNoRegistry]): replay decodes task groups
	// through it, and an unknown category fails the replay. It must be the
	// server's own registry, since the archival category exists only where
	// archival is configured.
	Registry tasks.TaskCategoryRegistry
	// Metrics is where the numbers go; nil is the noop emitter.
	Metrics *walmetrics.Emitter
}

// Stats is what the cycle knows about itself. Read it through [Cycle.Stats],
// which asks the cycle's own goroutine rather than reading its fields.
type Stats struct {
	State State
	// Epoch is this cycle's ownership token. It never changes, so a stopped
	// cycle still reports it, telling its stats apart from a successor's.
	Epoch wal.Epoch
	// Mutations and Bytes are the window's size since the last drain.
	Mutations int
	Bytes     int
	// CommitSeqno is the last seqno acked into the log; AppliedSeqno is the
	// last a drain committed: the cold store's watermark and the trim limit.
	CommitSeqno  wal.Seqno
	AppliedSeqno wal.Seqno
	// TailEntries and TailBytes are what I10 bounds: acked, unsettled entries.
	// TailEntries is not CommitSeqno − AppliedSeqno; the gap is entries a
	// [tailstate.KeepWatermark] settle released without a transaction (e.g.
	// sync mode's answered condition failures). See [tailstate.Tail].
	TailEntries int
	TailBytes   int
	// Counters is everything this cycle counted, embedded so [Totals] sums the
	// same fields. The positions above are not summable and stay out.
	Counters
	// LastStats is fold's counters from the last drain.
	LastStats fold.Stats
}

// Cycle is one shard's apply cycle at one epoch: a goroutine, an accumulator
// and the policy above. Every method is safe for concurrent use; those touching
// the window are answered by that goroutine, so the accumulator needs no lock.
// The four reads are unexported and go through [Manager], because a caller
// holding a *Cycle cannot know whether it is still the shard's cycle.
type Cycle struct {
	shard wal.ShardID
	epoch wal.Epoch
	// policy is read at each decision so triggers can change under a running
	// shard ([Policy]). Do not cache a [Config] beside it: it would hold stale
	// numbers.
	policy Policy
	// clock is kept from the policy because it never changes.
	clock clock.TimeSource
	deps  Deps

	jobs chan job
	// stop is closed once, by whoever retires the cycle; done is closed by the
	// loop alone, on its way out.
	stop *channel.ShutdownOnceImpl
	done chan struct{}

	// mirroredState mirrors the loop's state so [Cycle.State] can answer after
	// the goroutine is gone: a stopped cycle reports the state it stopped in.
	mirroredState atomic.Int32

	// finished is what the loop counted, written as it exits and read by
	// [Cycle.Retire] after done closes (that close is the happens-before; no
	// lock). A stopped cycle's [Cycle.Stats] reports no counters, so Retire is
	// the only way to this value.
	finished Counters

	// mirror is the tail's off-loop copy, so [Cycle.write] can check I10 before
	// queueing and [Cycle.stoppedRead] can answer after the loop is gone.
	mirror *tailstate.Mirror

	// trimmer trims the log beside the loop ([trim]). It lives on the cycle,
	// not in state, because [Cycle.Retire] waits for it after the loop exits.
	trimmer *trim.Trimmer

	// pressure is the log's optional pressure face, asserted once at [New];
	// nil means the backend reports none.
	pressure wal.PressureSource
}

// job is one operation run on the cycle's goroutine, closing over its own
// arguments and result. The state it is handed must not outlive the call.
type job func(*state)

// answer is what a job hands back to whoever asked for it.
type answer[T any] struct {
	v   T
	err error
}

// New starts a cycle for one shard at one epoch. The caller must already have
// fenced the log at that epoch ([Manager.ShardAcquired] does both, in that
// order): the log's epoch may never lag the database's.
//
// The watermark is read lazily on the first request, so an idle shard costs
// one goroutine and no queries. policy is kept, not copied; a caller holding a
// [Config] passes [Fixed].
func New(shard wal.ShardID, epoch wal.Epoch, deps Deps, policy Policy) *Cycle {
	if deps.Logger == nil {
		deps.Logger = log.NewNoopLogger()
	}
	if deps.Metrics == nil {
		deps.Metrics = walmetrics.New(nil)
	}
	c := &Cycle{
		shard:  shard,
		epoch:  epoch,
		policy: policy,
		clock:  policy().timeSource,
		deps:   deps,
		jobs:   make(chan job),
		stop:   channel.NewShutdownOnce(),
		done:   make(chan struct{}),
		// The mirror holds the emitter: the tail is measured where it moves.
		mirror: tailstate.NewMirror(deps.Metrics),
	}
	c.trimmer = trim.New(shard, deps.Log, c.clock, deps.Metrics, deps.Logger, c.clock.Now())
	c.pressure, _ = deps.Log.(wal.PressureSource)
	go c.run()
	return c
}

// pressureLevel polls the backend's pressure. Each consumer polls rather than
// sharing a snapshot: the level can rise inside an append, and a snapshot from
// before it would miss that.
func (c *Cycle) pressureLevel() wal.PressureLevel {
	if c.pressure == nil {
		return wal.PressureNone
	}
	return c.pressure.Pressure(c.shard)
}

// Shard and Epoch name what this cycle owns.
func (c *Cycle) Shard() wal.ShardID { return c.shard }
func (c *Cycle) Epoch() wal.Epoch   { return c.epoch }

// write acks one mutation into the log and folds it into the window. It returns
// once the mutation is durable; in sync mode, once its drain committed, so a
// condition failure is this call's error. Folding takes ownership of the
// request; rows serves assertions the window does not determine.
//
// Unexported because I11's epoch check is [Manager.Write]'s: a *Cycle holder
// has already resolved the shard, so a fenced-out write would be accepted.
//
// [Cycle.writeRefused] is checked twice: here off the mirror, so writers are
// not parked behind a loop stuck on the cold store, and in the loop at the
// append, so concurrent callers cannot all pass a tail one short of the bound.
// A halted cycle skips the first and answers with the halt: a shard that lost
// its epoch must not be told to retry later.
func (c *Cycle) write(ctx context.Context, m mutation.Mutation, rows *baserow.Rows) error {
	if c.State() == StateRunning {
		entries, bytes := c.mirror.Size()
		if err := c.writeRefused(entries, bytes, c.mirror.StalledAt(), c.policy()); err != nil {
			return err
		}
	}
	return tell(ctx, c, func(s *state) error { return c.add(ctx, s, m, rows) })
}

// drainNow drains the window regardless of triggers (no-op when empty). Tests
// only.
func (c *Cycle) drainNow(ctx context.Context) error {
	return tell(ctx, c, func(s *state) error { return c.drain(ctx, s, drainExplicit) })
}

// Stats reports the cycle's counters, asked of its own goroutine.
func (c *Cycle) Stats() Stats {
	st, stopped, _ := ask(context.Background(), c, func(s *state) (Stats, error) { return c.stats(s), nil })
	if stopped {
		// The counters are gone with the loop, but the tail is not: its entries
		// are still in the log, and a zero would be read as "nothing held".
		// Report it off the mirror, as [Cycle.residue] does.
		entries, bytes := c.mirror.Size()
		return Stats{
			State: c.State(), Epoch: c.epoch,
			TailEntries: int(entries), TailBytes: int(bytes),
		}
	}
	return st
}

// State is the cycle's state; a halted one never leaves it, and a stopped one
// keeps reporting the state its goroutine stopped in.
func (c *Cycle) State() State { return State(c.mirroredState.Load()) }

// Close starts the cycle if needed, drains the window, waits for any trim in
// flight and stops the goroutine. The start matters: a cycle nothing asked
// about has never read the log and may hold a dead owner's acked entries, and
// a nil here is what an operator removes the layer on.
//
// A halted cycle drains nothing (its tail is not its to apply) and returns the
// halt. Exception: one halted inside its replay never started, so start runs
// again and floors the tail the halt was holding; if that replay cannot read
// the log, the tail stays floored and the read's error is returned. This is an
// open entry in DURABILITY.md.
//
// A retired cycle (loop gone) returns nil, not the stopped refusal:
// [Cycle.residue] reads what it held off the mirror, and the refusal would
// report a drained shard as residue.
func (c *Cycle) Close(ctx context.Context) error {
	_, stopped, err := ask(ctx, c, func(s *state) (struct{}, error) {
		if err := c.start(ctx, s); err != nil {
			return struct{}{}, err
		}
		return struct{}{}, c.drain(ctx, s, drainExplicit)
	})
	c.Retire()
	if stopped {
		return nil
	}
	return err
}

// Retire stops the cycle without draining: its epoch was fenced out, so its
// entries stay in the log for the next owner. A cycle retired while running
// reports [StateHaltedLost].
//
// It returns the final counters, which nothing else can read: before the stop
// the loop still counts, after it [Cycle.Stats] has no loop to ask. Trim
// counters are read after [trim.Trimmer.Wait] to include a last trim.
func (c *Cycle) Retire() Counters {
	c.mirroredState.CompareAndSwap(int32(StateRunning), int32(StateHaltedLost))
	c.stop.Shutdown()
	<-c.done
	c.trimmer.Wait()

	counted := c.finished
	counted.Trims, counted.TrimsCommitted = c.trimmer.Counters()
	return counted
}

// ask runs body on the cycle's goroutine and waits for it. stopped reports the
// goroutine was already gone: err is then the standing refusal and the zero T
// is not an answer, so callers may substitute their own ([Cycle.stoppedRead],
// [Cycle.Stats]).
//
// The result channel is buffered so a caller whose context expires mid-job
// does not wedge the loop on a send.
func ask[T any](ctx context.Context, c *Cycle, body func(*state) (T, error)) (T, bool, error) {
	var zero T
	res := make(chan answer[T], 1)
	select {
	case c.jobs <- func(s *state) { v, err := body(s); res <- answer[T]{v, err} }:
	case <-c.done:
		return zero, true, fmt.Errorf("%w: shard %d's cycle at epoch %d has stopped", ErrHalted, c.shard, c.epoch)
	case <-ctx.Done():
		return zero, false, ctx.Err()
	}
	select {
	case a := <-res:
		return a.v, false, a.err
	case <-ctx.Done():
		return zero, false, ctx.Err()
	}
}

// tell is ask for an operation whose whole answer is whether it failed.
func tell(ctx context.Context, c *Cycle, body func(*state) error) error {
	_, _, err := ask(ctx, c, func(s *state) (struct{}, error) { return struct{}{}, body(s) })
	return err
}

// state is everything the goroutine owns. Nothing outside run() touches it.
type state struct {
	st    State
	cause error

	acc     *fold.Accumulator
	started bool // the floor has been read and the tail replayed

	next wal.Seqno // the seqno the next append takes

	// tail holds what is acked and not yet settled ([tailstate]); every move
	// publishes to the mirror.
	tail tailstate.Tail

	// window is the size and age of what was folded since the last drain
	// ([window]). Separate from the tail: the window empties when a drain
	// starts, the tail only when its transaction commits.
	window window.Window

	// Counters is what this cycle counted, as [Stats] and [Totals] carry it.
	// AckedRanges moves at the fold (a folded range is in the window whether
	// or not its drain commits); DroppedTasks and WrittenTasks only on a
	// committed drain; Kinds at the accept. The trim counters are read from
	// the trimmer at [Cycle.stats].
	Counters
	// attempt collects counts during a replay so an abandoned one can be
	// dropped whole. See [state.counted].
	attempt   *Counters
	lastDrain fold.Stats
}

// counted is where an accepted entry's counts go: the replay attempt in flight
// (adopted or dropped whole by [Cycle.start]), else the cycle. Only accept-time
// counters go here, since a retry re-accepts every entry above the watermark
// but never repeats a committed drain. An abandoned attempt thus under-reports
// rather than double-counting. [Cycle.stats] never sees an attempt open.
func (s *state) counted() *Counters {
	if s.attempt != nil {
		return s.attempt
	}
	return &s.Counters
}

// run is the loop. It deliberately does not recover panics: the process goes
// down and acked entries stay in the log. Recovering into a halt is unsafe: a
// halted cycle with an empty tail passes reads to the cold store, and a panic
// may leave the tail half-moved. The deferred close means later calls get
// [ErrHalted] rather than hang.
func (c *Cycle) run() {
	defer close(c.done)

	s := &state{
		acc:  fold.New(c.shard),
		tail: tailstate.New(c.mirror),
	}
	// Deferred after close(c.done), so it runs before it: the count is final
	// when done closes.
	defer func() { c.finished = s.Counters }()
	ageC, age := c.clock.NewTimer(c.policy().Age)
	defer age.Stop()
	// The age timer always runs; a tick on an empty window does nothing unless
	// a stall or storage pressure stands. Arming it only on a non-empty window
	// would need re-arming from the drain, where a missed reset never ages out.
	for {
		select {
		case <-c.stop.Channel():
			return
		case <-ageC:
			// One policy read, so the tick judges and re-arms by the same age.
			maxAge := c.policy().Age
			age.Reset(maxAge)
			// [tickActionOf] decides what the tick does.
			_, stalled := s.tail.Stalled()
			switch tickActionOf(s.st, s.started, stalled,
				s.window.Aged(c.clock.Now(), maxAge), c.pressureLevel(), s.window.Empty()) {
			case tickDrainAge:
				// Error discarded here and below: no caller to answer, and
				// drain records any halt on the state itself.
				_ = c.drain(context.Background(), s, drainWatermarkAge)
			case tickDrainPressure:
				_ = c.drain(context.Background(), s, drainStoragePressure)
			case tickForceTrim:
				c.trimmer.Force(s.tail.Applied())
			}
		case j := <-c.jobs:
			j(s)
		}
	}
}

func (c *Cycle) stats(s *state) Stats {
	// The trim counters are the trimmer's, and already this cycle's whole
	// count, so they are assigned rather than added.
	counters := s.Counters
	counters.Trims, counters.TrimsCommitted = c.trimmer.Counters()

	mutations, bytes := s.window.Size()
	return Stats{
		State:        s.st,
		Epoch:        c.epoch,
		Mutations:    mutations,
		Bytes:        bytes,
		CommitSeqno:  s.tail.Commit(),
		AppliedSeqno: s.tail.Applied(),
		TailEntries:  s.tail.Entries(),
		TailBytes:    s.tail.Bytes(),

		Counters:  counters,
		LastStats: s.lastDrain,
	}
}

// writeRefused applies the pre-append refusals (decide.go) and emits the
// refusal metric, so the pure rule itself counts nothing.
//
// cfg is the caller's snapshot, so [Cycle.add] refuses and appends under one
// policy.
func (c *Cycle) writeRefused(entries, bytes int64, stalled wal.Seqno, cfg Config) error {
	refusal, limit := writeRefused(entries, bytes, stalled, c.pressureLevel(), c.shard, cfg)
	if refusal == nil {
		return nil
	}
	c.deps.Metrics.BackpressureRefusal(limit)
	return refusal
}

// halted returns a halted cycle's refusal, wrapping the cause; nil if running.
func (c *Cycle) halted(s *state) error {
	if s.st == StateRunning {
		return nil
	}
	return fmt.Errorf("%w (%s), shard %d: %w", ErrHalted, s.st, c.shard, s.cause)
}

// add is the write path: ack first, fold second, because a mutation folded
// before it is durable is one the tail would lose.
func (c *Cycle) add(ctx context.Context, s *state, m mutation.Mutation, rows *baserow.Rows) error {
	if err := c.halted(s); err != nil {
		return err
	}
	if got, want := m.ShardID(), int32(c.shard); got != want {
		return fmt.Errorf("cycle: mutation belongs to shard %d, this cycle owns shard %d", got, want)
	}
	if err := c.start(ctx, s); err != nil {
		return err
	}
	// One policy read for the whole write: a second could refuse and append
	// under different bounds, or encode as provisional and then not drain.
	cfg := c.policy()

	// Before the append, so a refusal wrote nothing and consumes no seqno.
	entries, bytes := s.tail.Size()
	unresolved, _ := s.tail.Stalled()
	if err := c.writeRefused(entries, bytes, unresolved.Seqno, cfg); err != nil {
		return err
	}
	// The condition authority, also before the append, since the ack is the
	// answer. After the refusal by choice (either order acks nothing): a shard
	// past the bound refuses without a delegated cold-store read.
	if err := c.check(ctx, s, m, rows, cfg.Sync); err != nil {
		return err
	}

	// In sync mode the ack is provisional: the condition is verified only by the
	// drain below, after the entry is durable. Replay reads the bit back.
	//
	// The mutation holds only the event batches nobody has written yet, and the
	// encoder carries whatever is there (ADR 0014).
	encode := mutation.Encode
	if cfg.Sync {
		encode = mutation.EncodeProvisional
	}
	payload, err := encode(m)
	if err != nil {
		return fmt.Errorf("cycle: encoding a mutation of shard %d: %w", c.shard, err)
	}
	if err := c.deps.Log.Append(ctx, c.shard, c.epoch, s.next, payload); err != nil {
		// nil means the failed append is durable anyway, so fall through to
		// the accept: the caller is owed success.
		if err := c.appendFailed(ctx, s, err, payload); err != nil {
			return err
		}
	}
	if err := c.accept(ctx, s, m, len(payload)); err != nil {
		return err
	}

	if cfg.Sync {
		// Pressure is irrelevant here: the drain runs anyway, and its commit
		// consults the level for the trim.
		return c.drain(ctx, s, drainSync)
	}
	// Polled after the append: its response is where a backend most often
	// learns of pressure.
	if c.pressureLevel() >= wal.PressureDrain {
		return c.drain(ctx, s, drainStoragePressure)
	}
	switch s.window.Trips(cfg.watermarks()) {
	case window.TripMutations:
		return c.drain(ctx, s, drainWatermarkMutations)
	case window.TripBytes:
		return c.drain(ctx, s, drainWatermarkBytes)
	}
	return nil
}

// accept takes an entry already durable at s.next: the seqno advances, the
// tail grows by size (encoded payload length, as the bytes trigger and I10
// count), and the accumulator folds it.
//
// Shared by [Cycle.add] and [Cycle.replayEntry]; it must not depend on which,
// since a replayed entry has no caller to answer.
func (c *Cycle) accept(ctx context.Context, s *state, m mutation.Mutation, size int) error {
	s.tail.Ack(s.next, size)
	s.next++
	// No bounds check: KindCount is derived from the last kind, so every value
	// Kind() can return indexes Kinds.
	kind := m.Kind()
	s.counted().Kinds[kind]++

	// On a refusal fold drains the window and lets the mutation head a fresh
	// one; this side owns the counter and the halt.
	refusal, err := s.acc.AddOrDrain(s.tail.Commit(), m, func() error {
		s.counted().Refusals++
		return c.drain(ctx, s, drainRefusal)
	})
	if err != nil {
		// A failed drain already classified itself: pass its error through and
		// refold the entry it left. Anything else is an acked entry that will
		// not fold (even after the drain), which no retry can help.
		if !refusal.DrainFailed {
			c.halt(s, StateHaltedInvariant, err)
			return err
		}
		c.refold(s, m, kind, size)
		return err
	}
	c.folded(s, kind, size)
	return nil
}

// appendFailed handles a non-nil append error, returning nil when the append
// turns out durable ([Cycle.settleAppend]). The contract's named refusals have
// definite outcomes; any other error leaves the seqno's fate to be read back.
func (c *Cycle) appendFailed(ctx context.Context, s *state, cause error, payload []byte) error {
	switch appendOutcomeOf(cause) {
	case appendFenced:
		// The shard has a new owner: I4 working, not an incident.
		c.halt(s, StateHaltedLost, cause)
	case appendTaken:
		// A second writer at this epoch. See [ErrTailNotEmpty].
		c.halt(s, StateHaltedInvariant, fmt.Errorf("%w (seqno %d): %w", ErrTailNotEmpty, s.next, cause))
	case appendUnknown:
		return c.settleAppend(ctx, s, cause, payload)
	}
	return cause
}

// settleAppend resolves an append of unknown outcome by reading the seqno back
// from the log, the only witness (as the watermark is for a drain). The read is
// detached from the caller's cancellation: a client deadline expiring inside the
// append is the commonest cause, and a read on that context could not answer.
//
// Seqno absent: nothing was written; return the append's error. Own payload
// present: the append is durable; return nil, since reporting failure would be
// a lie the caller acts on. Otherwise halt: a foreign entry is
// [ErrTailNotEmpty]'s second writer, and a failed read leaves the seqno's fate
// open, so no other mutation may take it.
func (c *Cycle) settleAppend(ctx context.Context, s *state, cause error, payload []byte) error {
	entry, held, err := wal.EntryAt(context.WithoutCancel(ctx), c.deps.Log, c.shard, s.next)
	switch {
	case err != nil:
		c.deps.Logger.Warn("apply cycle: an append's outcome could not be read",
			tag.ShardID(int32(c.shard)), tag.NewInt64("seqno", int64(s.next)), tag.Error(err))
		c.halt(s, StateHaltedInvariant, fmt.Errorf(
			"the append at seqno %d has an outcome nobody could read (%w): %w", s.next, cause, err))
		return c.halted(s)
	case !held:
		return cause
	case entry.Epoch != c.epoch || !bytes.Equal(entry.Payload, payload):
		c.halt(s, StateHaltedInvariant, fmt.Errorf(
			"%w (seqno %d): the log holds an entry at epoch %d this cycle did not write: %w",
			ErrTailNotEmpty, s.next, entry.Epoch, cause))
		return c.halted(s)
	}
	c.deps.Logger.Info("apply cycle: an ambiguous append had landed",
		tag.ShardID(int32(c.shard)), tag.NewInt64("seqno", int64(s.next)), tag.Error(cause))
	return nil
}

// refold folds an entry whose fold was refused and whose recovery drain then
// failed. Otherwise it would be acked, durable and in no window, and the next
// watermark move would step over it and a trim delete it.
//
// A drain that reached its transaction emptied the accumulator, so the fold
// succeeds ([fold.Accumulator.AddOrDrain]'s premise). One that failed earlier
// did not, and the fold refuses: that halts, keeping the entry in the log for
// the successor, rather than forgetting it.
func (c *Cycle) refold(s *state, m mutation.Mutation, kind mutation.Kind, size int) {
	if s.st != StateRunning {
		// The drain halted; the entry is in the log for the next owner. Folding
		// it would move the window counters that [Cycle.residue] reports, for
		// bytes no drain could ever take.
		return
	}
	if err := s.acc.Add(s.tail.Commit(), m); err != nil {
		c.halt(s, StateHaltedInvariant, err)
		return
	}
	c.folded(s, kind, size)
}

// folded records an entry taken into the window, by either fold.
func (c *Cycle) folded(s *state, kind mutation.Kind, size int) {
	if kind == mutation.KindRangeCompleteTasks {
		s.counted().AckedRanges++
	}
	s.window.Add(size, c.clock.Now())
}

// check is the condition authority: assertions the window determines are
// answered with the store's own error, delegated ones are read from the cold
// store ([Cycle.checkDelegated]), and the rest are refused with
// [fold.ErrRefused], recovered by one drain ([fold.Accumulator.CheckOrDrain]).
func (c *Cycle) check(ctx context.Context, s *state, m mutation.Mutation, rows *baserow.Rows, sync bool) error {
	del, _, err := s.acc.CheckOrDrain(m, func() error {
		s.counted().Refusals++
		return c.drain(ctx, s, drainRefusal)
	})
	if err != nil {
		return err
	}
	if sync {
		// The sync drain inside this call asserts these and answers the
		// caller, so the reads would buy nothing.
		return nil
	}
	return c.checkDelegated(ctx, del, rows)
}

// checkDelegated reads the pre-window row each delegated assertion names and
// verifies it. Order and first-failure are [fold.Delegated.Settle]'s; this adds
// the reads, and [ErrNoBaseRow] when the caller brought no rows.
//
// The reads run on the loop, so no drain intervenes before the append.
func (c *Cycle) checkDelegated(ctx context.Context, del fold.Delegated, rows *baserow.Rows) error {
	return del.Settle(
		func(cur fold.DelegatedCurrent) error {
			if rows == nil {
				return fmt.Errorf("%w: the current-execution row of workflow %s", ErrNoBaseRow, cur.WorkflowID)
			}
			row, version, err := rows.Current(ctx, int32(c.shard), cur.NamespaceID, cur.WorkflowID)
			if err != nil {
				return err
			}
			return cur.Verify(row, version)
		},
		func(run fold.DelegatedRun) error {
			if rows == nil {
				return fmt.Errorf("%w: the row of run %s", ErrNoBaseRow, run.RunID)
			}
			row, err := rows.Run(ctx, int32(c.shard), run.NamespaceID, run.WorkflowID, run.RunID)
			if err != nil {
				return err
			}
			return run.Verify(row)
		},
	)
}

// start reads the watermark and replays what the log holds above it
// (replay.go). It runs lazily on the loop, so it is the readiness gate:
// requests arriving mid-replay wait in [ask]. A failed attempt leaves the
// cycle unstarted with an empty window and is dropped whole (counters,
// accumulator, acked bytes); the next request retries from the watermark. An
// attempt that halted keeps its tail and counters, since no retry follows.
//
// It does not check for a halt; its callers do, except [Cycle.Close]. So Close
// restarts a cycle halted inside its replay: the floor empties the tail the
// halt was holding, the replay counts its entries again, and a failed log read
// leaves the tail empty. This is an open entry in DURABILITY.md.
func (c *Cycle) start(ctx context.Context, s *state) error {
	if s.started {
		return nil
	}
	mark, found, err := c.deps.Recoverer.Watermark(ctx, c.shard)
	if err != nil {
		return err
	}
	if !found {
		// No drain ever committed: start just below the first seqno.
		mark = wal.FirstSeqno - 1
	}
	s.tail.Floor(mark)
	s.next = mark + 1

	var attempt Counters
	s.attempt = &attempt
	err = c.replay(ctx, s)
	s.attempt = nil
	if err != nil {
		s.acc = fold.New(c.shard)
		s.window.Take(c.clock.Now())
		if s.st == StateRunning {
			// The retry re-acks everything above the watermark, so reset the
			// tail to the floor and drop the counters. Keeping the tail would
			// count it once per attempt, and I10's pre-queue check would
			// refuse writers on a working shard.
			s.tail.Floor(s.tail.Applied())
			return err
		}
		// Halted inside the replay; only Close retries. The tail stays as
		// evidence of acked, unapplied entries ([Cycle.routeRead] refuses reads
		// on it), and so do the attempt's counters.
		s.Counters.add(attempt)
		return err
	}
	s.Counters.add(attempt)
	s.started = true
	// Under pressure, trim the previous owner's applied entries before
	// appending; replay's drains force a trim only if there was a tail.
	if c.pressureLevel() >= wal.PressureDrain {
		c.trimmer.Force(s.tail.Applied())
	}
	return nil
}

// callerRule is the attribution half of a [drainCause]: is the writer of this
// window's one mutation still waiting? dropsProvisional marks a drain of one
// replayed provisional entry, whose condition failure its caller already has.
type callerRule int

const (
	noCaller callerRule = iota
	answersCaller
	dropsProvisional
)

// drainCause is why a drain runs, whether its outcome answers a caller, and
// whose clock may cut it short: facts of the call site the drain cannot see.
//
// The legal values are the fixed list below, with no constructor: too
// permissive an attribution reports a failure to a caller who did not write
// the mutation, on entries left in the log marked settled.
type drainCause struct {
	trigger string
	caller  callerRule
	// detached means the enclosing call is not waiting for this drain, so its
	// cancellation must not bound it. The transaction carries earlier writers'
	// acked mutations; letting one caller's deadline fail it would halt the
	// shard and fail over the whole window.
	detached bool
}

var (
	// drainSync is sync mode's drain, the only cause that answers a caller.
	// Legal only in [Cycle.add] after the append, where the window holds one
	// mutation whose writer is waiting for this transaction, so its clock is
	// the right bound.
	drainSync = drainCause{walmetrics.TriggerSync, answersCaller, false}

	// The size and age triggers. Their writers were already told they
	// succeeded, so neither a condition failure nor a deadline is theirs.
	drainWatermarkMutations = drainCause{walmetrics.TriggerMutations, noCaller, true}
	drainWatermarkBytes     = drainCause{walmetrics.TriggerBytes, noCaller, true}
	// The age drain runs on the timer's own context, so detached is inert.
	drainWatermarkAge = drainCause{walmetrics.TriggerAge, noCaller, true}

	// drainRefusal empties the window so a refused mutation can head a fresh
	// one; that mutation is not in this drain.
	drainRefusal = drainCause{walmetrics.TriggerRefusal, noCaller, true}

	// drainExplicit is [Cycle.Close]'s shutdown drain (or [Cycle.drainNow]'s).
	// It keeps the caller's clock: the shutdown budget bounds each transaction.
	drainExplicit = drainCause{walmetrics.TriggerExplicit, noCaller, false}

	// drainRead is [Config.DrainOnRead]'s drain before a read is answered from
	// the cold store; the reader waits for it.
	drainRead = drainCause{walmetrics.TriggerRead, noCaller, false}

	// drainReplay applies a previous owner's tail. The request that triggered
	// [Cycle.start] waits through it, and replay has no bound of its own, so
	// the caller's clock is the only thing that can interrupt it.
	drainReplay = drainCause{walmetrics.TriggerReplay, noCaller, false}

	// drainReplayProvisional replays one provisional entry alone: its condition
	// was unverified at ack, so a failure drops the entry instead of halting.
	// Legal only for a window of exactly one replayed entry.
	drainReplayProvisional = drainCause{walmetrics.TriggerReplay, dropsProvisional, false}

	// drainStoragePressure drains at any size when the backend wants storage
	// back ([wal.PressureSource]), so the forced trim reaches everything acked.
	// Its writers were answered at their acks.
	drainStoragePressure = drainCause{walmetrics.TriggerStoragePressure, noCaller, true}
)

// drain applies the window as one transaction and moves the watermark with it.
func (c *Cycle) drain(ctx context.Context, s *state, cause drainCause) error {
	if err := c.halted(s); err != nil {
		return err
	}
	// Detach before anything touches the store, the stall re-ask included.
	// [Cycle.resolve] also detaches, for its own reason: even causes that keep
	// the caller's clock may not settle an ambiguity on it.
	if cause.detached {
		ctx = context.WithoutCancel(ctx)
	}
	// Before the window is taken, so a failed re-ask leaves this drain's work
	// in place.
	if err := c.resolveStalled(ctx, s); err != nil {
		return err
	}
	// The window's bytes leave the tail only on commit, so they are held: an
	// unknown outcome leaves them acked, unapplied and counted.
	held := s.window.Take(c.clock.Now())
	batch := s.acc.Drain()
	if batch.Empty() {
		// No transaction ran, so the watermark stays put: moving it would let a
		// trim strand a recovering owner. Still settle what the window acked,
		// or its bytes leak; a window that acked nothing settles nothing.
		if seqno, ok := batch.Settles(); ok {
			s.tail.Settle(seqno, &held, tailstate.KeepWatermark)
		}
		return nil
	}
	stats, work := batch.Stats(), batch.Tasks()
	s.lastDrain = stats
	// The batch's watermark, the highest seqno it applies.
	seqno := batch.Watermark()

	err := c.deps.Writer.Apply(ctx, c.shard, c.epoch, batch)
	switch c.settlement(err, cause, stats.MutationsIn) {
	case settlesForward:
	case asksTheWatermark:
		// The watermark is the only witness; never re-derive the answer from
		// base versions ([cold.Watermarker]). nil means it had committed, so
		// settle forward below.
		if rerr := c.resolve(ctx, s, seqno, err); rerr != nil {
			// Unreadable: stall the tail at this seqno with this drain's bytes
			// and cause; nothing settles or commits over it until answered.
			s.tail.Stall(seqno, &held, err)
			return rerr
		}
	case answersItsWriter:
		return c.answerWriter(s, seqno, &held, err)
	case dropsTheEntry:
		return c.dropProvisional(s, seqno, &held, err)
	case haltsLost:
		c.halt(s, StateHaltedLost, err)
		return err
	case haltsInvariant:
		c.halt(s, StateHaltedInvariant, err)
		return err
	}

	// The only settle that moves the watermark: the rows are in the cold
	// store, so trim and replay may go past them.
	s.tail.Settle(seqno, &held, tailstate.MoveWatermark)
	s.Drains++
	c.deps.Metrics.Drained(cause.trigger, stats.MutationsIn, stats.DirtyWorkflows, held.Age())
	c.countTasks(s, work)
	// A halted cycle does not trim: its log belongs to the next owner or to
	// whoever investigates.
	if s.st == StateRunning {
		// Every committed drain checks pressure, whatever its cause.
		if c.pressureLevel() >= wal.PressureDrain {
			c.trimmer.Force(s.tail.Applied())
		} else {
			c.trimmer.Drained(s.tail.Applied(), c.policy().cadence())
		}
	}
	return nil
}

// settlement classifies Apply's error and asks [settlementOf] what it means.
func (c *Cycle) settlement(err error, cause drainCause, mutationsIn int) settlement {
	return settlementOf(apply.Classify(err), cause, mutationsIn)
}

// countTasks emits and counts what a drain wrote and dropped, per category.
// Only for a committed batch: one that did not commit wrote and dropped nothing.
func (c *Cycle) countTasks(s *state, work fold.TaskWork) {
	for name, n := range work.Counts {
		c.deps.Metrics.Tasks(name, n.Dropped, n.Written)
		s.DroppedTasks += n.Dropped
		s.WrittenTasks += n.Written
	}
}

// answerWriter returns a condition failure to the caller instead of halting.
// Legal only when the window held one mutation whose caller is still waiting;
// a multi-writer window's failure cannot be pinned on one writer.
//
// The entry stays in the log (appends are not undoable and seqnos are
// gap-free) but is settled ([tailstate.Tail]). The returned error is the
// store's own, unwrapped, because the caller type-switches on it.
func (c *Cycle) answerWriter(s *state, seqno wal.Seqno, held *window.Taken, err error) error {
	// Nothing was written, so the watermark stays.
	s.tail.Settle(seqno, held, tailstate.KeepWatermark)
	// Its rate tells an operator ordinary races from divergence.
	c.deps.Metrics.AnsweredConditionFailure()

	var violation *apply.InvariantViolationError
	if !errors.As(err, &violation) {
		// Apply raises the class only through that type.
		return err
	}
	c.deps.Logger.Debug("apply cycle: a synchronous drain's condition did not hold",
		tag.ShardID(int32(c.shard)), tag.NewInt64("seqno", int64(seqno)), tag.Error(violation))
	return violation.Cause
}

// resolveStalled re-asks the watermark for a stalled drain. Committed: the
// stall ends. Provably not: the shard halts. Still unreadable: the caller's
// drain does nothing. Every drain runs it, since nothing may be applied over a
// stall ([tailstate.Tail]).
func (c *Cycle) resolveStalled(ctx context.Context, s *state) error {
	unresolved, stalled := s.tail.Stalled()
	if !stalled {
		return nil
	}
	if err := c.resolve(ctx, s, unresolved.Seqno, unresolved.Cause); err != nil {
		return err
	}
	s.tail.Resolve()
	return nil
}

// resolve turns an unknown drain outcome into a known one by reading the
// watermark. Only exact equality with the drain's seqno means it committed.
//
// The read is detached from the caller's cancellation: the commonest cause of
// an unknown outcome is that context expiring inside [cold.Applier.Apply], so a
// read on it could not answer. A committed drain would then stall until the
// age tick re-asked on its own [context.Background]; this is the same, one
// drain earlier, so a store that never answers hangs the loop no worse.
//
// A watermark above the seqno does not mean "mine committed". This cycle cannot
// commit over an unresolved drain ([Cycle.resolveStalled] runs before every
// drain), so another owner moved it, and may have replayed and dropped this
// very entry. Halting as lost is the true and recoverable answer.
func (c *Cycle) resolve(ctx context.Context, s *state, seqno wal.Seqno, cause error) error {
	mark, found, err := c.deps.Recoverer.Watermark(context.WithoutCancel(ctx), c.shard)
	if err != nil {
		// Still unknown. Not a halt, which would turn a blip into a lost shard:
		// the caller stalls the tail and every later drain re-asks here.
		c.deps.Logger.Warn("apply cycle: a drain's outcome could not be read",
			tag.ShardID(int32(c.shard)), tag.NewInt64("seqno", int64(seqno)), tag.Error(err))
		return fmt.Errorf("cycle: shard %d: the drain at seqno %d has an unknown outcome: %w", c.shard, seqno, err)
	}
	if !found || mark < seqno {
		// Not committed. Not re-applied: its requests were already driven, so
		// re-driving them would build a transaction from mutated state. Replay
		// is the recovery; the original error is kept in the halt cause.
		c.halt(s, StateHaltedInvariant,
			fmt.Errorf("the drain at seqno %d did not commit (watermark %d): %w", seqno, mark, cause))
		return c.halted(s)
	}
	if mark > seqno {
		c.halt(s, StateHaltedLost,
			fmt.Errorf("the watermark is at %d, past this drain's own seqno %d, so another owner committed over it: %s: %w",
				mark, seqno, FencedAway, cause))
		return c.halted(s)
	}
	c.deps.Logger.Info("apply cycle: an ambiguous drain had committed",
		tag.ShardID(int32(c.shard)), tag.NewInt64("seqno", int64(seqno)))
	return nil
}

// halt stops the cycle for good. Both halts keep the log for the next owner.
//
// The first halt stands: a second must not overwrite the cause or the class
// (halted-invariant must never become an ordinary failover). Only [Cycle.Close]
// can reach a second halt, by restarting a cycle halted in replay (untested);
// that restart floors the tail, an open entry in DURABILITY.md.
func (c *Cycle) halt(s *state, st State, cause error) {
	if s.st != StateRunning {
		return
	}
	s.st, s.cause = st, cause
	c.mirroredState.Store(int32(st))
	// The window is dropped, not settled: its entries stay in the log and
	// their bytes are not this cycle's to release.
	s.window.Take(c.clock.Now())
	// The tail is left as is, so a halted shard reports what it held.
	s.acc = fold.New(c.shard)
	// Tagged by state: the two are opposites, and summing them would page for
	// fencing working.
	c.deps.Metrics.Halt(st.String())
	c.deps.Logger.Warn("apply cycle halted",
		tag.ShardID(int32(c.shard)), tag.NewStringTag("state", st.String()), tag.Error(cause))
}
