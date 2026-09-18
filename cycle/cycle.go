// Package cycle is the layer's state machine: one goroutine per (shard, epoch)
// owns the accumulator, the drain, the apply transaction, the trim cadence and
// the three reads, so that "who is touching this shard" has one answer.
//
// It names no cold store (the seam is [cold]'s); the store is reached only
// through [cold.Applier], [cold.Watermarker] and the closures a caller passes
// in. The states exist because ownership loss is discovered rather than
// announced: a
// shard close makes no persistence call. [Chapter 06] is what this implements.
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
	// StateHaltedInvariant: an assertion failed in a window whose failure could
	// not be pinned on one caller, so there is no retry and no failover.
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

// ErrHalted matches (via errors.Is) every refusal a halted cycle's loop answers
// with, and the one [ask] gives once that loop is gone. It is not always what
// the caller sees: [storeError] turns a halted-lost write into
// ShardOwnershipLost, and a read is [loopRoute]'s, which hands this back only on
// the route that refuses as halted. The class is in [Cycle.State]; the cause
// travels wrapped, so a caller can still reach the
// [apply.InvariantViolationError].
var ErrHalted = errors.New("cycle: the shard is halted")

// ErrTailNotEmpty reports an append refused because the log already holds the
// seqno the cycle meant to write. A cycle replays past the whole tail before it
// appends, so either a second writer holds this cycle's epoch, or one of this
// cycle's own appends failed ambiguously and was durable after all. It halts.
var ErrTailNotEmpty = errors.New("cycle: the log holds an entry at a seqno this cycle replayed past")

// ErrBudget refuses a policy whose hard_max × shards per node does not fit the
// node's tail budget. See [Config.CheckBudget].
var ErrBudget = errors.New("cycle: the per-shard tail bound does not fit the node's budget")

// ErrNoRegistry refuses a nil [Deps.Registry]: replay decodes an inherited tail
// through it, so nil would be recovery silently switched off.
var ErrNoRegistry = errors.New("cycle: no task category registry, so no tail could ever be replayed")

// ErrClosed refuses an acquire on a registry [Manager.Close] has already
// emptied. The shard would be taken by a cycle acking into a log the layer is
// releasing, drained by nothing, and named in no [Residue] — and a shutdown
// that answered nothing is the only evidence a caller removing the layer has
// that no acked entry is being stranded.
var ErrClosed = errors.New("cycle: the layer has shut down and acquires no more shards")

// ErrNoBaseRow is what a write gets when the condition authority delegates an
// assertion to the cold store and the caller brought no [baserow.Rows]. A refusal
// and not a skip: a refused write provably acked nothing, where a skip would
// ack an assertion nobody evaluated and halt the next owner.
var ErrNoBaseRow = errors.New("cycle: an assertion the window does not determine, and no cold-store read to settle it")

// Config is the cycle's policy. The zero value is not usable; [Defaults] is
// the measured one.
type Config struct {
	// Mutations and Bytes are the size watermark, whichever trips first. Both
	// sit at the measured collapse knee; changing either means re-measuring.
	Mutations int
	Bytes     int
	// Age drains a tail nothing is pushing on, and is also how often a shard
	// whose last drain had no readable outcome re-asks the cold store — the one
	// clock a cycle that refuses its writers has left. A recovery-budget choice,
	// not a measured one. Non-positive is not "no age rule": it is filled with
	// the default, because the loop arms a timer from it (see [Config.fill]).
	Age time.Duration
	// TrimEvery and TrimAfter are the trim cadence, whichever trips first. A
	// DeleteRange per drain is a transaction per drain for no gain.
	TrimEvery int
	TrimAfter time.Duration
	// Sync makes every [Cycle.write] drain before it returns and report the
	// drain's outcome.
	Sync bool

	// DrainOnRead drains the window before answering a read, so every read is
	// served by the cold store. Off in [Defaults]: it costs a transaction per
	// read that crosses a window.
	DrainOnRead bool

	// HardMaxEntries and HardMaxBytes are I10's bound on one shard's tail: what
	// has been acked and not yet applied. Neither unit works alone: one workflow
	// near the server's 8 MB mutable-state limit turns an entries-only bound
	// into a byte budget with no ceiling, and bytes alone bound no replay. See
	// [Config.CheckBudget].
	HardMaxEntries int
	HardMaxBytes   int

	// MaxShards and TailBudgetBytes are the node's half of that arithmetic, so
	// HardMaxBytes cannot be raised without the product being re-checked.
	// MaxShards is what this node may own at once, not the cluster's shard
	// count: 256 is 128 in steady state, doubled for a failover.
	MaxShards       int
	TailBudgetBytes int

	// timeSource is where the age and trim cadences read the clock, drivable in
	// a test with clock.EventTimeSource. Unexported: the policy is
	// configurable, the clock is not.
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

// fill defaults the clock, the four bounds and the age, and nothing else: a
// struct literal may leave any other field at zero on purpose, and the four
// that stay zero have readings — a size watermark of zero drains every write
// ([window.Window.Trips]), a trim cadence of zero trims at every drain. What is
// filled here is what has none. The bounds' zero is "refuse everything", which
// stops the shard, or "hold everything", which is the unbounded tail I10
// prevents. The age's is worse than either, because it is not a policy at all:
// the loop re-arms its timer from this field, so a zero fires the tick
// immediately and then again, forever, at a whole CPU per shard held.
//
// [Fixed] runs it once, at construction. [Live] runs it again at every answer,
// because one of its five getters reaches a field filled here — the age — and a
// dynamic-config key set to zero is otherwise that spin, on a running node, with
// no restart.
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

// watermarks is the size half of the policy in the shape the window states its
// rule over, so the two numbers are paired once rather than at each decision.
func (c Config) watermarks() window.Watermarks {
	return window.Watermarks{Mutations: c.Mutations, Bytes: c.Bytes}
}

// cadence is the trim half of the policy in the shape the trimmer states its
// rule over, paired once rather than at the decision.
func (c Config) cadence() trim.Cadence {
	return trim.Cadence{Every: c.TrimEvery, After: c.TrimAfter}
}

// CheckBudget asserts the node's RAM arithmetic: [Config.HardMaxBytes] per
// shard over [Config.MaxShards] shards must fit in [Config.TailBudgetBytes],
// which [Defaults] does exactly. It bounds encoded bytes, not RSS — what is
// resident is decoded protos plus the accumulator's indices.
func (c Config) CheckBudget() error {
	cfg := c
	cfg.fill()
	held := int64(cfg.HardMaxBytes) * int64(cfg.MaxShards)
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
	// Registry is required ([ErrNoRegistry]): replay decodes a payload's task
	// groups through it and an unknown category id fails the replay. It must be
	// the server's own registry, since the archival category exists only where
	// archival is configured.
	Registry tasks.TaskCategoryRegistry
	// Metrics is where the numbers go; nil is the noop emitter.
	Metrics *walmetrics.Emitter
}

// Stats is what the cycle knows about itself. Read it through [Cycle.Stats],
// which asks the cycle's own goroutine rather than reading its fields.
type Stats struct {
	State State
	// Epoch is the shard-ownership token this cycle holds. It never moves, so a
	// stopped cycle still reports it — which is what tells one owner's stats
	// from its successor's.
	Epoch wal.Epoch
	// Mutations and Bytes are the window's size since the last drain, in the two
	// units the size watermark counts.
	Mutations int
	Bytes     int
	// CommitSeqno is the last seqno acked into the log; AppliedSeqno is the last
	// one a drain committed, which is the cold store's watermark and as far as a
	// trim may go.
	CommitSeqno  wal.Seqno
	AppliedSeqno wal.Seqno
	// TailEntries and TailBytes are what I10 bounds: acked entries whose fate is
	// not yet settled. TailEntries is not CommitSeqno − AppliedSeqno; the gap is
	// what a [tailstate.KeepWatermark] settle released — entries no transaction
	// wrote, sync mode's answered condition failures among them, which hold no
	// memory. See the resolved field of [tailstate.Tail].
	TailEntries int
	TailBytes   int
	// Counters is everything this cycle counted, embedded so a counter added to
	// it cannot be lost at the [Totals] seam. The positions above stay out
	// because they are not summable.
	Counters
	// LastStats is fold's counters from the last drain, so the collapse ratio is
	// reported with the window it came from.
	LastStats fold.Stats
}

// Cycle is one shard's apply cycle at one epoch: a goroutine, an accumulator
// and the policy above. Every method is safe for concurrent use, and the ones
// touching the window are answered by that goroutine, which is what makes the
// accumulator single-threaded without a lock. The three reads are unexported
// and [Manager] is the door to them, because a caller holding a *Cycle cannot
// know whether it is still the shard's cycle.
type Cycle struct {
	shard wal.ShardID
	epoch wal.Epoch
	// policy is read at every decision that consults one, which is what lets a
	// watermark move under a running shard ([Policy]). Do not cache a [Config]
	// beside it: the snapshot would hold the numbers this cycle was created
	// with rather than the ones in force.
	policy Policy
	// clock is the one field of the policy kept, because it never moves.
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

	// finished is what the loop counted, written by it on its way out and read
	// by [Cycle.Retire] after done is closed — which is the happens-before, and
	// the reason no lock is needed. It is not [Cycle.Stats]'s answer: a stopped
	// cycle still reports no counters, so the only way to this value is the
	// call that stopped it.
	finished Counters

	// mirror is the off-loop half of the tail, published wherever the tail
	// moves, so [Cycle.write] can answer I10's bound before queueing and
	// [Cycle.stoppedRead] can answer after the goroutine is gone.
	mirror *tailstate.Mirror

	// trimmer keeps the log short beside the loop ([trim]). It is on the cycle
	// rather than in its state because [Cycle.Retire] waits for it once the
	// loop is gone.
	trimmer *trim.Trimmer
}

// job is one operation, served on the cycle's own goroutine. It closes over its
// own arguments and its own result, so the loop knows nothing but how to run
// one. The state it is handed is the loop's and does not outlive the call.
type job func(*state)

// answer is what a job hands back to whoever asked for it.
type answer[T any] struct {
	v   T
	err error
}

// New starts a cycle for one shard at one epoch. The caller must already have
// fenced the log at that epoch ([Manager.ShardAcquired] does both, in that
// order), because the log's epoch may never lag the database's.
//
// The first seqno is the watermark's successor, read lazily on the first
// request, so a shard acquired and never written costs one idle goroutine and
// no queries. policy is kept as the source rather than copied; [Fixed] is what
// a caller holding a [Config] passes.
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
	go c.run()
	return c
}

// Shard and Epoch name what this cycle owns.
func (c *Cycle) Shard() wal.ShardID { return c.shard }
func (c *Cycle) Epoch() wal.Epoch   { return c.epoch }

// write acks one mutation into the log and folds it into the window. It returns
// once the mutation is durable, and in sync mode once the drain carrying it has
// committed, so a condition failure is this call's error. Folding takes
// ownership of the request; rows reaches the pre-window rows for assertions the
// window does not determine ([baserow.Rows]).
//
// Unexported because I11's epoch check is [Manager.Write]'s (write.go): a caller
// holding a *Cycle out of [Manager.Shard] has already resolved the shard, so an
// append here is a fenced-out shard context's write re-stamped with whatever
// epoch this node currently holds, and accepted.
//
// A shard the rule in decide.go refuses answers ResourceExhausted instead. It is
// asked here off the mirrored state, so a writer need not queue behind an applier
// stuck on the cold store — which is both refusals' reason to exist, the stalled
// one most of all, since the loop it would queue behind is inside the very
// watermark read that is failing — and again inside the loop at the append, where
// concurrent callers cannot all pass a tail one short of the bound. A halted or
// retired cycle skips it and answers with the halt: a shard that lost its epoch
// must not be told to retry later.
func (c *Cycle) write(ctx context.Context, m mutation.Mutation, rows *baserow.Rows) error {
	if c.State() == StateRunning {
		entries, bytes := c.mirror.Size()
		if err := c.writeRefused(entries, bytes, c.mirror.StalledAt(), c.policy()); err != nil {
			return err
		}
	}
	return tell(ctx, c, func(s *state) error { return c.add(ctx, s, m, rows) })
}

// drainNow applies the window whatever the watermarks say; a no-op on an empty
// one. The only drain asked for from outside the loop, and it exists for
// shutdown.
func (c *Cycle) drainNow(ctx context.Context) error {
	return tell(ctx, c, func(s *state) error { return c.drain(ctx, s, drainExplicit) })
}

// Stats reports the cycle's counters, asked of its own goroutine.
func (c *Cycle) Stats() Stats {
	st, stopped, _ := ask(context.Background(), c, func(s *state) (Stats, error) { return c.stats(s), nil })
	if stopped {
		// No loop left to count, so the counters are gone with it — but the tail
		// is not, and answering zero for it is the one number here that would be
		// read as a fact. A cycle stopped by [Manager.RetireShard] stays the
		// shard's, so this is what a caller staging what a dead owner left asks,
		// and the entries are in the log whether or not a goroutine is left to
		// say so. Read off the mirror, as [Cycle.residue] and [Cycle.stoppedRead]
		// already do.
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

// Close starts the cycle if nothing has yet, drains what the window holds, waits
// for any trim in flight and stops the goroutine. A halted cycle drains nothing
// (its tail is not its to apply) and returns the halt.
//
// The start is what makes the answer about the *shard* rather than about this
// cycle's own window. A cycle replays lazily, on the first request to reach it,
// so one installed by an acquire that nothing then asked about has never read
// its watermark and never looked at the log — and what it inherited is a dead
// owner's acked entries, which is what the recovery path exists for. Draining
// its empty window and reporting nothing held is how a shutdown says "clean"
// about a shard it never looked at, and a nil here is what an operator removes
// the layer on.
// A cycle whose loop is already gone answers nil rather than the stopped
// refusal: it was retired, so there is no window left to drain and nothing left
// open — the mirror holds what it stopped holding, which is the answer
// [Cycle.residue] reads. Passing the refusal on would make every shard a harness
// retired into a residue, which tells an operator that removing the layer
// strands something over a shard that drained.
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

// Retire stops the cycle without draining: its epoch has been fenced out, so
// what it carries is not its to apply and the entries stay in the log for the
// next owner. A cycle retired while running reports [StateHaltedLost].
//
// It answers with what this cycle counted, because stopping it and taking its
// count are one thing rather than two a caller has to order. Taken separately
// they cannot both be right: before the stop the loop can still count, and
// after it [Cycle.Stats] has no loop to ask. The trim's two are read after
// [trim.Trimmer.Wait], so a trim that commits on the way out is in the number
// rather than a drain behind it.
func (c *Cycle) Retire() Counters {
	c.mirroredState.CompareAndSwap(int32(StateRunning), int32(StateHaltedLost))
	c.stop.Shutdown()
	<-c.done
	c.trimmer.Wait()

	counted := c.finished
	counted.Trims, counted.TrimsCommitted = c.trimmer.Counters()
	return counted
}

// ask runs body on the cycle's goroutine and waits for it. stopped reports that
// the goroutine was already gone, so err is the standing refusal rather than an
// answer, and the zero T is not one: a caller that has something better to do
// than pass it on decides what to answer instead ([Cycle.stoppedRead],
// [Cycle.Stats]).
//
// The result channel is buffered, so a caller whose context expires while the
// job is in flight does not wedge the loop behind a send nobody will receive.
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

	// tail is every number about what has been acked and not yet settled, and
	// the only thing that moves them ([tailstate]). It carries the mirror other
	// goroutines read, so nothing here has to remember to publish.
	tail tailstate.Tail

	// window is the size and age of what has been folded since the last drain
	// ([window]). Two counters and not the tail's two: this empties when a drain
	// starts, the tail only when its transaction commits.
	window window.Window

	// Counters is what this cycle counted, the same value [Stats] and [Totals]
	// carry. When each moves is a rule of this file: AckedRanges at the fold,
	// because a folded range raised the bound whether or not its drain
	// committed; DroppedTasks and WrittenTasks only for a committed drain;
	// Kinds at the accept, so it counts what this cycle acked. The two trim
	// counters are the trimmer's and are read at [Cycle.stats].
	Counters
	// attempt is where an accepted entry counts while a replay is in flight,
	// so that abandoning one is dropping a value rather than restoring fields.
	// See [state.counted].
	attempt   *Counters
	lastDrain fold.Stats
}

// counted is where what an accepted entry moves goes. A replay in flight counts
// into its own attempt, which [Cycle.start] adopts whole or drops whole; every
// other caller counts straight onto the cycle.
//
// It is the accept's counters that go here and not the drain's, and the line is
// what a retry re-does: a retry re-reads from the watermark it finds, so every
// entry an abandoned attempt accepted is one its successor accepts again, while
// a drain that committed is rows in the cold store that nothing repeats.
//
// An abandoned attempt therefore under-reports rather than double-reports: the
// entries a committed drain carried before the attempt failed are dropped with
// it, having been counted nowhere else. That is the reading Replayed and
// Dropped already had — an abandoned attempt takes what it counted with it —
// and this is the rest of the counters joining it, in the direction that
// undercounts a rare incident instead of inventing acks on every retry.
//
// Nothing outside the loop can see an attempt open: replay runs on the loop, so
// [Cycle.stats] cannot be answered while one is.
func (s *state) counted() *Counters {
	if s.attempt != nil {
		return s.attempt
	}
	return &s.Counters
}

func (c *Cycle) run() {
	defer close(c.done)

	s := &state{
		acc:  fold.New(c.shard),
		tail: tailstate.New(c.mirror),
	}
	// The count is published before done closes — deferred calls run in
	// reverse, so this one is ordered ahead of the close above — which is what
	// makes "the loop has stopped" and "its count is final" the same moment.
	defer func() { c.finished = s.Counters }()
	ageC, age := c.clock.NewTimer(c.policy().Age)
	defer age.Stop()
	// The age timer runs always, and ticks on an empty window are ignored.
	// Arming it only when the window fills would mean re-arming from inside the
	// drain, where a missed reset is a tail that never ages out.
	for {
		select {
		case <-c.stop.Channel():
			return
		case <-ageC:
			// One read of the policy for both the answer and the next arming,
			// so a tick cannot judge the window by one age and re-arm at
			// another.
			maxAge := c.policy().Age
			age.Reset(maxAge)
			// The tick is also the only thing that re-asks the watermark for a
			// stalled tail: a cycle in that state refuses its writers, so no
			// write arrives to bring a drain with it, and an empty window would
			// leave the stall standing for good. [Config.Age] is that retry
			// cadence as well as this one.
			_, stalled := s.tail.Stalled()
			if s.st == StateRunning && (stalled || s.window.Aged(c.clock.Now(), maxAge)) {
				// Discarded rather than unchecked: this drain has no caller to
				// answer, and the outcome it carries is on the state already —
				// drain halts the cycle itself.
				_ = c.drain(context.Background(), s, drainWatermarkAge)
			}
		case j := <-c.jobs:
			j(s)
		}
	}
}

func (c *Cycle) stats(s *state) Stats {
	// The two counters the loop does not own: the cadence and its outcome are
	// the trimmer's, so they are read off it rather than carried in s. Assigned
	// and not added, because they are already this cycle's whole count.
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

// writeRefused applies the pre-append refusals (the rule is in decide.go) and
// emits the refusal's metric. It is the counting half, so that asking the rule
// itself what it would answer counts nothing.
//
// cfg is the caller's own snapshot rather than a read of its own: the two call
// sites are one write, and [Cycle.add] refuses and appends under one policy.
func (c *Cycle) writeRefused(entries, bytes int64, stalled wal.Seqno, cfg Config) error {
	refusal, limit := writeRefused(entries, bytes, stalled, c.shard, cfg)
	if refusal == nil {
		return nil
	}
	c.deps.Metrics.BackpressureRefusal(limit)
	return refusal
}

// halted reports the refusal a halted cycle answers with, carrying the cause
// so the caller can still reach the attribution.
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
	// One read of the policy for this whole write, taken above every decision
	// that wants one: a second read could refuse under one pair of bounds and
	// append under another, verify the delegated assertions under one mode and
	// encode under the other, or encode a mutation as provisional and then not
	// drain it.
	cfg := c.policy()

	// Before the append, so a refused operation provably wrote nothing. It
	// consumes no seqno — the next accepted mutation lands where this one would.
	entries, bytes := s.tail.Size()
	unresolved, _ := s.tail.Stalled()
	if err := c.writeRefused(entries, bytes, unresolved.Seqno, cfg); err != nil {
		return err
	}
	// The condition authority, also before the append: the ack is the answer,
	// so an assertion this layer means to answer must be evaluated while the
	// caller is still on the line.
	if err := c.check(ctx, s, m, rows, cfg.Sync); err != nil {
		return err
	}

	// The ack is provisional exactly where the drain below answers the caller,
	// which is sync mode's every write: there the condition is not yet verified
	// when the entry becomes durable. Replay reads the bit back.
	encode := mutation.Encode
	if cfg.Sync {
		encode = mutation.EncodeProvisional
	}
	payload, err := encode(m)
	if err != nil {
		return fmt.Errorf("cycle: encoding a mutation of shard %d: %w", c.shard, err)
	}
	if err := c.deps.Log.Append(ctx, c.shard, c.epoch, s.next, payload); err != nil {
		// nil here is the one append that failed and is durable anyway, which
		// falls through to the accept below: the entry is in the log at this
		// seqno, so the caller is owed what a nil append would have given it.
		if err := c.appendFailed(ctx, s, err, payload); err != nil {
			return err
		}
	}
	if err := c.accept(ctx, s, m, len(payload)); err != nil {
		return err
	}

	if cfg.Sync {
		return c.drain(ctx, s, drainSync)
	}
	switch s.window.Trips(cfg.watermarks()) {
	case window.TripMutations:
		return c.drain(ctx, s, drainWatermarkMutations)
	case window.TripBytes:
		return c.drain(ctx, s, drainWatermarkBytes)
	}
	return nil
}

// accept takes an entry already durable at s.next into this cycle: the seqno
// counters move, the tail grows by size (the payload's encoded length, which is
// what both the watermark and I10 count), and the accumulator folds it.
//
// Shared by [Cycle.add] and [Cycle.replayEntry], and nothing here may depend on
// which: a replayed entry has no caller to answer and no ack to give.
func (c *Cycle) accept(ctx context.Context, s *state, m mutation.Mutation, size int) error {
	s.tail.Ack(s.next, size)
	s.next++
	// No bounds check: KindCount is derived from the last kind, so every value
	// Kind() can return indexes Kinds.
	kind := m.Kind()
	s.counted().Kinds[kind]++

	// The recovery from a refusal is fold's own: drain the window, let the
	// refused mutation head a fresh one. This side owns the counter and the
	// halt.
	refusal, err := s.acc.AddOrDrain(s.tail.Commit(), m, func() error {
		s.counted().Refusals++
		return c.drain(ctx, s, drainRefusal)
	})
	if err != nil {
		// A failed drain already classified itself, so it passes through as it
		// came, and the entry it left unfolded is folded below. Anything else is
		// an already-acked entry that will not fold, including a refusal that
		// survived the drain, and no retry can help.
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

// appendFailed says what becomes of a write whose append did not return nil,
// and returns nil for the one that is durable regardless ([Cycle.settleAppend]).
//
// The three refusals the contract names each say the write is whole one way or
// the other, so nothing has to be established about them. What is left is
// everything a transport can fail with, and the seqno's fate then has to be
// read rather than assumed — which is what this exists for.
func (c *Cycle) appendFailed(ctx context.Context, s *state, cause error, payload []byte) error {
	switch appendOutcomeOf(cause) {
	case appendFenced:
		// The shard has a new owner: I4 working, not an incident.
		c.halt(s, StateHaltedLost, cause)
	case appendTaken:
		// Someone acked a seqno this cycle already replayed past, at this
		// cycle's own epoch. See [ErrTailNotEmpty].
		c.halt(s, StateHaltedInvariant, fmt.Errorf("%w (seqno %d): %w", ErrTailNotEmpty, s.next, cause))
	case appendUnknown:
		return c.settleAppend(ctx, s, cause, payload)
	}
	return cause
}

// settleAppend turns an append whose outcome the contract cannot name into one
// it can, by reading the seqno back. The log is the only witness, exactly as the
// cold store's watermark is for a drain, and the read is detached from the
// caller's cancellation for that same reason: a client deadline expiring inside
// the append is the commonest way the outcome became unreadable, so a read on
// that context could not answer in the one case it exists to answer.
//
// The three answers, and nil for the one that is a successful write:
//
//   - the log does not hold the seqno, so the append wrote nothing and the
//     seqno stays this cycle's next. The caller gets the append's own error,
//     which is now established rather than assumed;
//   - the log holds this cycle's own payload at it, so the append was durable
//     and reporting it failed would be a lie the caller acts on — a write it
//     would replay against a state this entry is about to move;
//   - anything else halts. An entry this cycle did not write, at its own epoch
//     and above everything it replayed, is [ErrTailNotEmpty]'s second writer; a
//     read that failed leaves the fate of the seqno open, and the one thing that
//     may not follow is another mutation taking it.
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

// refold folds an entry whose own fold was refused and whose recovery drain then
// failed. It is acked and durable and in no window at all, and an acked entry no
// batch carries is one the watermark moves past: the mutation is then in the log,
// in no store, and below a trim.
//
// A drain that got as far as its transaction emptied the accumulator, so this is
// the fold an empty window determines ([fold.Accumulator.AddOrDrain]'s own
// premise). One that returned above it did not, and the refusal below is how
// that arrives: an acked entry this cycle cannot apply is a halt, keeping the
// log as the successor's evidence, rather than a mutation it forgets.
func (c *Cycle) refold(s *state, m mutation.Mutation, kind mutation.Kind, size int) {
	if s.st != StateRunning {
		// The drain halted: its window is dropped and everything acked behind it
		// is the next owner's to replay.
		return
	}
	if err := s.acc.Add(s.tail.Commit(), m); err != nil {
		c.halt(s, StateHaltedInvariant, err)
		return
	}
	c.folded(s, kind, size)
}

// folded is what an entry taken into the window moves, whichever of the two
// folds took it.
func (c *Cycle) folded(s *state, kind mutation.Kind, size int) {
	if kind == mutation.KindRangeCompleteTasks {
		// Counted at the fold and not at the drain: the range has already
		// raised the window's bound.
		s.counted().AckedRanges++
	}
	s.window.Add(size, c.clock.Now())
}

// check is the condition authority at this boundary: what the window determines
// it answers with the store's own error, what the window hands on it takes to
// the cold store ([Cycle.checkDelegated]), and what neither can answer it
// refuses with [fold.ErrRefused]. The recovery from a refusal is one drain, and
// why one suffices is at [fold.Accumulator.CheckOrDrain].
func (c *Cycle) check(ctx context.Context, s *state, m mutation.Mutation, rows *baserow.Rows, sync bool) error {
	del, _, err := s.acc.CheckOrDrain(m, func() error {
		s.counted().Refusals++
		return c.drain(ctx, s, drainRefusal)
	})
	if err != nil {
		return err
	}
	if sync {
		// The drain asserting all of these runs inside this call and its
		// outcome is what the caller is told, so the reads would buy nothing.
		return nil
	}
	return c.checkDelegated(ctx, del, rows)
}

// checkDelegated settles the assertions the window handed on: each names a
// pre-window row, which this side reads and hands back to the predicate that
// came with it. The order and the first-failure rule are the walk's
// ([fold.Delegated.Settle]); what is added here is the read, and the answer to a
// caller that brought none — which the walk names, being the row it reached
// first.
//
// The reads run in this goroutine, so no drain can intervene between one and the
// append.
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

// start reads the seqno floor (the watermark's successor) and then applies
// whatever the log holds above it (replay.go). It runs lazily, on the first
// request, and in this goroutine, which is what makes the readiness gate free
// rather than a flag: a request arriving mid-replay is already parked in
// [ask] on its own context. A failure leaves the cycle unstarted and the
// window empty, so the next request starts again from the watermark.
//
// The attempt is abandoned whole, and everything it moved is either dropped
// with it or re-planted at the top of the next one: its counters are a value
// this never adopts ([state.counted]), its accumulator and window are replaced,
// and its acked bytes go with the floor, which is read again from the cold
// store's own watermark. There is no field here to remember to restore.
func (c *Cycle) start(ctx context.Context, s *state) error {
	if s.started {
		return nil
	}
	mark, found, err := c.deps.Recoverer.Watermark(ctx, c.shard)
	if err != nil {
		return err
	}
	if !found {
		// No drain has ever committed here, so the floor is the seqno below the
		// first one: the tail starts empty at the bottom of the log.
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
			// A retry is coming and reads the watermark again, so everything
			// this attempt acked is read again with it: the acks go back to the
			// floor and the counters are dropped. Holding either is counting
			// one incident once per attempt — and the tail is what I10 reads
			// off the mirror before a write is queued, so a shard that is
			// working would refuse its writers before ever reaching the retry.
			s.tail.Floor(s.tail.Applied())
			return err
		}
		// Halted inside the replay, where no attempt follows. The tail stays:
		// it is the evidence those entries were acked and never applied, which
		// is what [Cycle.routeRead] refuses reads on. What the attempt counted
		// stays with it for the same reason — nothing will count it again.
		s.Counters.add(attempt)
		return err
	}
	s.Counters.add(attempt)
	s.started = true
	return nil
}

// callerRule is the attribution half of a [drainCause]: is the caller of the
// [Cycle.write] that appended this window's one mutation still on the line?
// dropsProvisional is the third answer: a drain of exactly one replayed entry
// whose ack was provisional, where a condition failure is the answer its caller
// already has.
type callerRule int

const (
	noCaller callerRule = iota
	answersCaller
	dropsProvisional
)

// drainCause is why a drain is happening, whether its outcome is anybody's
// answer, and whose clock may cut it short. All three are properties of the
// call site that the drain cannot see.
//
// The legal triples are the fixed list below and there is no constructor:
// attribution that is too permissive reports a failure to a caller who did not
// write the mutation, on entries that stay in the log marked settled.
type drainCause struct {
	trigger string
	caller  callerRule
	// detached says the call this drain runs inside is not waiting for its
	// outcome, so that caller's cancellation is not a bound on it. What such a
	// transaction carries is earlier writers' acked mutations, and they have
	// been told it succeeded; the writer still on the line is waiting for its
	// own append. Bounding the transaction by its clock makes one caller's
	// deadline a failed drain for everybody in the window, which is a halt and
	// a failover ([Cycle.resolve] is where that road ends).
	detached bool
}

var (
	// drainSync is sync mode's one write, one drain, and the only cause that
	// answers a caller. Legal in [Cycle.add] alone, after the append, where the
	// window holds one mutation whose writer is still inside this call — and is
	// waiting for this transaction, which is what makes its clock the right one.
	drainSync = drainCause{walmetrics.TriggerSync, answersCaller, false}

	// The three watermarks, in [Cycle.add] and on the age timer. Their windows
	// hold work whose callers were already told it succeeded, so a condition
	// failure is nobody's answer and neither is a deadline.
	drainWatermarkMutations = drainCause{walmetrics.TriggerMutations, noCaller, true}
	drainWatermarkBytes     = drainCause{walmetrics.TriggerBytes, noCaller, true}
	drainWatermarkAge       = drainCause{walmetrics.TriggerAge, noCaller, true}

	// drainRefusal empties the window so a refused mutation can head a fresh
	// one. That mutation is not in what this drains, and neither is its writer
	// waiting for it.
	drainRefusal = drainCause{walmetrics.TriggerRefusal, noCaller, true}

	// drainExplicit is [Cycle.drainNow]: shutdown, or a test. Its caller asked
	// for this drain and nothing else, and the shutdown budget is what bounds
	// the apply transactions one at a time, so this one keeps that clock.
	drainExplicit = drainCause{walmetrics.TriggerExplicit, noCaller, false}

	// drainRead is [Config.DrainOnRead]'s arm: a read emptying the window it
	// would have merged over, and then answered out of the cold store. The
	// reader is waiting for exactly this.
	drainRead = drainCause{walmetrics.TriggerRead, noCaller, false}

	// drainReplay is a previous owner's tail being applied. The request that
	// triggered [Cycle.start] waits through the whole of it, and an inherited
	// tail has no bound of its own, so this is the one drain a caller's clock
	// is the only thing that can interrupt.
	drainReplay = drainCause{walmetrics.TriggerReplay, noCaller, false}

	// drainReplayProvisional is a provisional entry replayed alone: its
	// condition was never verified before its ack, so a failure is the answer
	// its caller already has and the entry is dropped rather than the shard
	// halted. Legal only for a window of exactly one replayed entry.
	drainReplayProvisional = drainCause{walmetrics.TriggerReplay, dropsProvisional, false}
)

// drain applies the window as one transaction and moves the watermark with it.
func (c *Cycle) drain(ctx context.Context, s *state, cause drainCause) error {
	if err := c.halted(s); err != nil {
		return err
	}
	// Above everything the drain reaches the store with, the standing stall's
	// re-ask included ([drainCause.detached]). [Cycle.resolve] detaches again on
	// its own account, which is not this rule twice: the causes that keep the
	// caller's clock still may not settle an ambiguity on it.
	if cause.detached {
		ctx = context.WithoutCancel(ctx)
	}
	// Before the window is taken, so a re-ask that fails leaves this drain's own
	// work exactly where it was.
	if err := c.resolveStalled(ctx, s); err != nil {
		return err
	}
	// The window's bytes leave the tail only when the transaction commits, so
	// they are held here rather than dropped with the window: an outcome that
	// stays unknown leaves them acked, unapplied and counted.
	held := s.window.Take(c.clock.Now())
	batch := s.acc.Drain()
	if batch.Empty() {
		// The watermark stays put whatever the batch acked: no transaction ran,
		// so a trim moved with it would strand a recovering owner at rows the
		// cold store does not hold. The bytes leak until the entries settle, so
		// a window that acked nothing is the only one to leave alone.
		if seqno, ok := batch.Settles(); ok {
			s.tail.Settle(seqno, &held, tailstate.KeepWatermark)
		}
		return nil
	}
	stats, work := batch.Stats(), batch.Tasks()
	s.lastDrain = stats
	// The batch's own watermark, above everything the transaction applies. Set
	// by the drain that assigned those seqnos; apply asserts it rather than
	// recomputing it.
	seqno := batch.Watermark()

	err := c.deps.Writer.Apply(ctx, c.shard, c.epoch, batch)
	switch c.settlement(err, cause, stats.MutationsIn) {
	case settlesForward:
	case asksTheWatermark:
		// The watermark is the only witness; never re-derive the answer from
		// base versions ([cold.Watermarker]'s whole point). A nil here is a drain
		// that had committed after all, so it settles forward below.
		if rerr := c.resolve(ctx, s, seqno, err); rerr != nil {
			// Unreadable, which is not an answer: the seqno becomes the tail's
			// floor and this drain's bytes and cause go with it, so nothing
			// settles or commits over it until a later drain gets an answer.
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
		// Nothing to re-fold and nobody to tell: the window is gone and the
		// entries behind it are acked.
		c.halt(s, StateHaltedInvariant, err)
		return err
	}

	// The one settle that moves the watermark: this window's rows are in the
	// cold store, so a trim and a replay may read past them.
	s.tail.Settle(seqno, &held, tailstate.MoveWatermark)
	s.Drains++
	c.deps.Metrics.Drained(cause.trigger, stats.MutationsIn, stats.DirtyWorkflows, held.Age())
	c.countTasks(s, work)
	// A halted cycle's log is not its to shorten: halted-lost belongs to the
	// next owner and halted-invariant to whoever reads it.
	if s.st == StateRunning {
		c.trimmer.Drained(s.tail.Applied(), c.policy().cadence())
	}
	return nil
}

// settlement classifies what Apply returned and asks [settlementOf] what it
// means. Beside the call site, like the other four rules, so that what a caller
// can still get wrong is which values it hands over.
func (c *Cycle) settlement(err error, cause drainCause, mutationsIn int) settlement {
	return settlementOf(apply.Classify(err), cause, mutationsIn)
}

// countTasks emits and counts what a drain wrote and dropped, per category.
// Called once the transaction has an outcome: a batch that did not commit wrote
// no rows, so a drop counted for it would be a saving nobody made.
func (c *Cycle) countTasks(s *state, work fold.TaskWork) {
	for name, n := range work.Counts {
		c.deps.Metrics.Tasks(name, n.Dropped, n.Written)
		s.DroppedTasks += n.Dropped
		s.WrittenTasks += n.Written
	}
}

// answerWriter returns a condition failure to the caller instead of halting,
// which is legal only where the window held one mutation and that caller is
// still waiting for this call: a drained window mixes many writers, so a
// condition that did not hold cannot be pinned on one of them.
//
// The entry stays in the log, because an append is not undoable and gap-freedom
// is what the seqno means, but it is settled: see the resolved field of
// [tailstate.Tail]. The returned error is the store's own, with the readback's
// attribution dropped, because the caller type-switches on what it gets.
func (c *Cycle) answerWriter(s *state, seqno wal.Seqno, held *window.Taken, err error) error {
	// Nothing was written, so the watermark stays where the cold store put it.
	s.tail.Settle(seqno, held, tailstate.KeepWatermark)
	// Counted, and not as a drain: its rate is what tells an operator whether a
	// shard is losing ordinary races or diverging.
	c.deps.Metrics.AnsweredConditionFailure()

	var violation *apply.InvariantViolationError
	if !errors.As(err, &violation) {
		// Apply raises the class only through that type; anything else is
		// already the store's own error.
		return err
	}
	c.deps.Logger.Debug("apply cycle: a synchronous drain's condition did not hold",
		tag.ShardID(int32(c.shard)), tag.NewInt64("seqno", int64(seqno)), tag.Error(violation))
	return violation.Cause
}

// resolveStalled re-asks the watermark for a drain whose outcome could not be
// read, and is where a stall ends: committed after all, and the tail settles at
// it; provably not, and the shard halts; still unreadable, and the caller's own
// drain does nothing at all.
//
// Every drain runs it, because nothing may be applied over a stall — the reason
// is at the stalled field of [tailstate.Tail].
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

// resolve turns an unknown outcome into a known one. The watermark is the only
// witness, and it is read against the drain's own seqno exactly: a watermark is
// a position and not a name, so equality is the whole of what identifies the
// drain that moved it.
//
// The read is detached from the caller's cancellation, and that is the whole of
// why this one may not simply take ctx: the commonest way an outcome becomes
// unreadable is the caller's own clock running out inside [cold.Applier.Apply],
// and a read on that context cannot answer in the one case it exists to answer.
// A drain that had committed would be stalled, its writer told it failed, and
// the shard would refuse every write and both reads until the age tick asked
// again — which is the tick's own [context.Background]. This is that context
// one drain earlier, so a store that never answers hangs the loop exactly where
// it already would.
//
// Above that seqno is the case worth stating, because "at or above" is the
// tempting rule and it is wrong. Nothing of this cycle's can commit over an
// unresolved drain — [Cycle.resolveStalled] runs before every drain and returns
// its error — so a watermark that has moved *past* this drain was moved by
// another owner's. Reading it as "mine committed" tells a caller its write
// succeeded on the strength of somebody else's transaction, and under sync mode
// that caller is still on the line: its entry may have been the one the new
// owner replayed, met a failing condition on, and dropped. Losing the shard is
// the true answer and a recoverable one — the caller re-acquires and reads.
func (c *Cycle) resolve(ctx context.Context, s *state, seqno wal.Seqno, cause error) error {
	mark, found, err := c.deps.Recoverer.Watermark(context.WithoutCancel(ctx), c.shard)
	if err != nil {
		// Still unknown, and not a halt: halting on a read failure would turn a
		// blip into a lost shard. The caller stalls the tail instead, and every
		// later drain comes back through here. The window is gone, so the
		// entries stay in the log to be replayed.
		c.deps.Logger.Warn("apply cycle: a drain's outcome could not be read",
			tag.ShardID(int32(c.shard)), tag.NewInt64("seqno", int64(seqno)), tag.Error(err))
		return fmt.Errorf("cycle: shard %d: the drain at seqno %d has an unknown outcome: %w", c.shard, seqno, err)
	}
	if !found || mark < seqno {
		// It did not commit, and it is not re-applied here: the window is
		// drained and its requests were driven, so re-driving them would build
		// a transaction out of mutated state. Replay is the recovery, and the
		// ambiguous error rides into the halt as the only record of why the
		// outcome was unreadable.
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

// halt is where a cycle stops for good. Both halts keep the log: the entries
// are the evidence, and the next owner fences and continues it.
func (c *Cycle) halt(s *state, st State, cause error) {
	if s.st != StateRunning {
		return
	}
	s.st, s.cause = st, cause
	c.mirroredState.Store(int32(st))
	// Dropped rather than settled, so what Take reports is discarded: the
	// entries stay in the log for the next owner, and their bytes are not this
	// cycle's to release.
	s.window.Take(c.clock.Now())
	// The tail counters are left alone: they are what the next owner replays,
	// and an operator asking a halted shard what it held should be told. Nothing
	// reads them for a bound, since a halted cycle refuses every write anyway.
	s.acc = fold.New(c.shard)
	// The state's own name is the tag: the two states are opposites, and a
	// dashboard that added them would page for fencing working.
	c.deps.Metrics.Halt(st.String())
	c.deps.Logger.Warn("apply cycle halted",
		tag.ShardID(int32(c.shard)), tag.NewStringTag("state", st.String()), tag.Error(cause))
}
