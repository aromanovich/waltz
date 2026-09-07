package cycle

// I10 without a cluster: the bound that keeps a cold-store incident from
// becoming an OOM, and with it the claim that a tripped bound is degradation
// and not loss.
//
// The log here is the memwal backend rather than the fake beside it, because
// three of the four claims are claims about the log — its length after a
// refusal, the seqno the next accepted append takes, and what a replay from the
// watermark reconstructs. Against a fake they would be assertions about this
// file's own bookkeeping.

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/service/history/tasks"

	"github.com/aromanovich/waltz/fold"
	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/verify/basetest"
	"github.com/aromanovich/waltz/verify/mutbuild"
	"github.com/aromanovich/waltz/wal"
	"github.com/aromanovich/waltz/wal/memwal"
)

const testEpoch wal.Epoch = 7

// ---------------------------------------------------------------------------
// A cycle over backend #2, and an applier the test decides when to run.
// ---------------------------------------------------------------------------

// heldApplier commits whatever it is given — what these tests are about is the
// tail, not the transaction — and can be stopped inside Apply, so that a cycle
// can be asked something while its goroutine is busy applying.
type heldApplier struct {
	// entered is signalled on the way in, without blocking, so a test can wait
	// until the loop is inside Apply. hold, while open, is what keeps it there.
	entered chan struct{}
	hold    chan struct{}
	// err, when set, is the outcome every drain gets instead of a commit.
	err error

	// store, when set, is the cold store this applier commits into — see
	// [commitToColdStore].
	store *basetest.Store

	mu     sync.Mutex
	drains int
	seqnos []wal.Seqno
}

func (a *heldApplier) Apply(_ context.Context, _ wal.ShardID, _ wal.Epoch, batch fold.Batch) error {
	if a.entered != nil {
		select {
		case a.entered <- struct{}{}:
		default:
		}
	}
	if a.hold != nil {
		<-a.hold
	}
	if a.err != nil {
		return a.err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.drains++
	a.seqnos = append(a.seqnos, batch.Watermark())
	commitToColdStore(a.store, batch)
	return nil
}

func (a *heldApplier) committed() []wal.Seqno {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.seqnos)
}

// bpEnv is one cycle, the log it acks into and the applier holding it back.
type bpEnv struct {
	t     *testing.T
	log   *memwal.Backend
	apply *heldApplier
	mark  *fakeWatermark
	c     *Cycle
	cfg   Config
	// store is the cold store the condition authority settles the residual
	// against, empty until a test puts rows in it: an update asserts that the
	// current row names the run it updates, so a window starting with one needs
	// the workflow to exist here.
	store *basetest.Store
}

func newTailEnv(t *testing.T, shape func(*Config)) *bpEnv {
	t.Helper()
	logs := memwal.New()
	cfg := Defaults()
	cfg.Sync = false
	// Nothing drains on its own: no size watermark within reach, and an age
	// timer that will not fire inside a test.
	cfg.Mutations, cfg.Bytes, cfg.Age = 1<<30, 1<<30, time.Hour
	if shape != nil {
		shape(&cfg)
	}

	e := &bpEnv{t: t, log: logs, apply: &heldApplier{}, mark: &fakeWatermark{}, cfg: cfg, store: basetest.New()}
	e.apply.store = e.store
	e.c = standUp(t, testEpoch, Deps{Log: logs, Writer: e.apply, Recoverer: e.mark}, cfg)
	return e
}

// coldWorkflow is [env.coldWorkflow] for this harness: the workflow the
// window's first update is an update of.
func (e *bpEnv) coldWorkflow(wf, run string, baseVersion int64) {
	e.store.Holds(wf, run, baseVersion)
}

func (e *bpEnv) add(m mutation.Mutation) error {
	e.t.Helper()
	return e.c.write(context.Background(), m, e.store.Rows())
}

func (e *bpEnv) entries() []wal.Entry {
	e.t.Helper()
	return logEntries(e.t, e.log)
}

// mkFat is a create carrying a blob at the server's own per-event limit
// (limit.blobSize.error, 2 MB): the mutation an entries-only bound cannot see.
func mkFat(ns, wf, run string, size int) mutation.Mutation {
	return build.Create(ns, wf, run, mutbuild.WithInfoBlob(&commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		Data:         make([]byte, size),
	}))
}

// requireRefusal is I10's error contract, asserted the way the shard reads it:
// a type assertion rather than errors.As, because
// ContextImpl.handleWriteErrorLocked switches on the concrete type and a
// wrapped refusal would satisfy errors.As while falling to its default arm.
func requireRefusal(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	refusal, ok := err.(*serviceerror.ResourceExhausted) //nolint:errorlint // the concrete type is the assertion
	require.True(t, ok, "backpressure must reach the shard as *serviceerror.ResourceExhausted, got %T: %v", err, err)
	require.Equal(t, enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_LIMIT, refusal.Cause,
		"the cause is the persistence rate limiter's: a persistence limit, not a tenant's doing")
	require.Equal(t, enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM, refusal.Scope,
		"SCOPE_SYSTEM keeps the retry inside the history client and bills the incident to the system")
}

// ---------------------------------------------------------------------------
// The bound, in both units.
// ---------------------------------------------------------------------------

// TestRefusalsBeginAtTheEntryBound: the unit I10 is written in — commitSeqno −
// appliedSeqno — with the boundary asserted on both sides of itself.
func TestRefusalsBeginAtTheEntryBound(t *testing.T) {
	e := newTailEnv(t, func(c *Config) { c.HardMaxEntries = 8 })
	ns, wf, run := ids()
	e.coldWorkflow(wf, run, 0)

	for i := 1; i <= 8; i++ {
		require.NoError(t, e.add(mkUpdate(ns, wf, run, int64(i))),
			"the tail is not full until it is full: entry %d", i)
	}
	s := e.c.Stats()
	require.EqualValues(t, 8, s.CommitSeqno-s.AppliedSeqno, "eight acked, none applied")

	requireRefusal(t, e.add(mkUpdate(ns, wf, run, 9)))
}

// TestOneOversizedMutationTripsTheByteBound: why the bound counts two units.
// The entry count says one, three orders of magnitude below its own limit; the
// byte count says the shard is over budget, and it is right.
func TestOneOversizedMutationTripsTheByteBound(t *testing.T) {
	const bound = 1 << 20
	e := newTailEnv(t, func(c *Config) { c.HardMaxBytes = bound })
	ns, wf, run := ids()

	require.NoError(t, e.add(mkFat(ns, wf, run, 2<<20)),
		"a mutation the server itself accepted is never refused for its own size: "+
			"a refusal it could only repeat is a stuck workflow, not a degradation")

	s := e.c.Stats()
	require.EqualValues(t, 1, s.CommitSeqno-s.AppliedSeqno)
	require.Less(t, int(s.CommitSeqno-s.AppliedSeqno), e.cfg.HardMaxEntries/1000,
		"the entry bound is nowhere near tripping, which is the whole argument for counting bytes")
	require.Greater(t, s.TailBytes, bound, "one blob at the server's own limit is over the byte budget")

	requireRefusal(t, e.add(mkUpdate(ns, wf, run, 2)))
}

// TestARefusedWriteIsNotAWrite: the refusal is checked before the append, so
// there is nothing to undo — the log did not move, the accumulator did not see
// it, and the seqno it would have taken is still there for the next one.
func TestARefusedWriteIsNotAWrite(t *testing.T) {
	e := newTailEnv(t, func(c *Config) { c.HardMaxEntries = 4 })
	ns, wf, run := ids()
	e.coldWorkflow(wf, run, 0)
	for i := 1; i <= 4; i++ {
		require.NoError(t, e.add(mkUpdate(ns, wf, run, int64(i))))
	}

	before := e.entries()
	requireRefusal(t, e.add(mkUpdate(ns, wf, run, 5)))
	require.Equal(t, before, e.entries(), "a refused write left the log exactly where it was")
	require.EqualValues(t, 4, e.c.Stats().CommitSeqno, "and consumed no seqno")

	// Draining is the only way to ask the accumulator what it holds: four in
	// means the refused mutation never reached fold, so the collapse ratio the
	// oracle judges is untouched by backpressure.
	require.NoError(t, e.c.drainNow(context.Background()))
	require.Equal(t, 4, e.c.Stats().LastStats.MutationsIn)

	require.NoError(t, e.add(mkUpdate(ns, wf, run, 5)), "with the tail drained the same write goes through")
	all := e.entries()
	require.Len(t, all, 5)
	require.Equal(t, wal.Seqno(5), all[4].Seqno,
		"the accepted append lands at the seqno the refused one would have taken: no hole, no skip")
}

// TestTheBoundIsAnsweredWhileTheApplierIsBusy is why [Cycle.write] checks the
// bound before it queues anything: a shard whose apply is stuck on a cold store
// must refuse its writers, not park them behind it, since a queue of blocked
// callers is the same unbounded memory by another road.
func TestTheBoundIsAnsweredWhileTheApplierIsBusy(t *testing.T) {
	e := newTailEnv(t, func(c *Config) { c.HardMaxEntries = 4; c.Mutations = 4 })
	e.apply.entered = make(chan struct{}, 1)
	e.apply.hold = make(chan struct{})
	// Registered after the cycle's own cleanup and so run before it: a failure
	// below must release the apply, or the cycle's goroutine is still in it when
	// Retire waits for it and the failure becomes a hung binary.
	var once sync.Once
	release := func() { once.Do(func() { close(e.apply.hold) }) }
	t.Cleanup(release)
	ns, wf, run := ids()
	e.coldWorkflow(wf, run, 0)

	for i := 1; i <= 3; i++ {
		require.NoError(t, e.add(mkUpdate(ns, wf, run, int64(i))))
	}
	// The fourth reaches the window watermark, so the cycle's goroutine goes
	// into the apply and stays there.
	fourth := make(chan error, 1)
	go func() { fourth <- e.add(mkUpdate(ns, wf, run, 4)) }()
	<-e.apply.entered

	refused := make(chan error, 1)
	go func() { refused <- e.add(mkUpdate(ns, wf, run, 5)) }()
	select {
	case err := <-refused:
		requireRefusal(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("the write blocked behind the held apply: backpressure must answer without reaching the loop")
	}

	release()
	require.NoError(t, <-fourth)
	require.Equal(t, []wal.Seqno{4}, e.apply.committed())
	require.NoError(t, e.add(mkUpdate(ns, wf, run, 5)), "a drained tail takes writes again")
}

// TestAnUnresolvedDrainStaysInTheTail: the window and the tail empty at two
// different moments — the window when a drain starts, the tail only once the
// transaction has committed, and an outcome nobody could read is not a commit.
// Counting those entries as applied loses track of exactly the memory a
// cold-store incident leaves behind.
func TestAnUnresolvedDrainStaysInTheTail(t *testing.T) {
	e := newTailEnv(t, func(c *Config) { c.HardMaxEntries = 4 })
	e.apply.err = context.DeadlineExceeded
	// The floor read, and then a readback that answers nothing: the drain's
	// outcome stays unknown, which leaves the cycle running.
	e.mark.answers = []wmAnswer{{}, {err: context.DeadlineExceeded}}
	ns, wf, run := ids()
	e.coldWorkflow(wf, run, 0)

	require.NoError(t, e.add(mkUpdate(ns, wf, run, 1)))
	require.NoError(t, e.add(mkUpdate(ns, wf, run, 2)))
	require.Error(t, e.c.drainNow(context.Background()))
	require.Equal(t, StateRunning, e.c.State(), "a read that failed is not an answer, and not a halt")

	s := e.c.Stats()
	require.Zero(t, s.Mutations, "the window is gone")
	require.NotZero(t, s.TailBytes, "the entries are not: they are acked, unapplied, and this node's problem")
	require.Equal(t, 2, s.TailEntries, "and the bound counts them, in the other unit too")
	require.EqualValues(t, 2, s.CommitSeqno-s.AppliedSeqno)
	// This is the case that must not settle: the batch may have committed and
	// nothing yet knows, where sync mode's settled condition failure is an entry
	// no drain will ever carry. What the cycle may do while that stands is
	// unresolved_test.go's subject; here it is the counting.
	requireRefusal(t, e.add(mkUpdate(ns, wf, run, 3)))
}

// ---------------------------------------------------------------------------
// Degradation, not loss.
// ---------------------------------------------------------------------------

// TestATrippedTailLosesNothing: release the applier and the refusals stop, and
// what a replay reads back is exactly the set that was acked — every accepted
// mutation, in order, and none of the refused ones.
func TestATrippedTailLosesNothing(t *testing.T) {
	e := newTailEnv(t, func(c *Config) { c.HardMaxEntries = 4 })
	ns, wf, run := ids()
	e.coldWorkflow(wf, run, 0)

	var acked []int64
	for i := int64(1); i <= 4; i++ {
		require.NoError(t, e.add(mkUpdate(ns, wf, run, i)))
		acked = append(acked, i)
	}
	for i := int64(5); i <= 7; i++ {
		requireRefusal(t, e.add(mkUpdate(ns, wf, run, i)))
	}

	// Releasing the applier ends the degradation: the window commits, the
	// watermark moves, and the tail the bound was counting is gone.
	require.NoError(t, e.c.drainNow(context.Background()))
	require.Equal(t, []wal.Seqno{4}, e.apply.committed())
	s := e.c.Stats()
	require.EqualValues(t, 0, s.CommitSeqno-s.AppliedSeqno)
	require.Zero(t, s.TailBytes, "an applied window is not tail any more")

	// The refused writes are retried at the versions they were refused at: a
	// caller that was told "no" still holds the row it read.
	for i := int64(5); i <= 7; i++ {
		require.NoError(t, e.add(mkUpdate(ns, wf, run, i)), "the refusals stopped with the tail")
		acked = append(acked, i)
	}

	// The replay: read the log the way an acquire would and decode what it
	// finds.
	registry := tasks.NewDefaultTaskCategoryRegistry()
	var replayed []int64
	for i, entry := range e.entries() {
		require.Equal(t, wal.FirstSeqno+wal.Seqno(i), entry.Seqno, "gap-free, as the contract promises")
		m, err := mutation.Decode(entry.Payload, registry)
		require.NoError(t, err)
		replayed = append(replayed, m.Update.UpdateWorkflowMutation.DBRecordVersion)
	}
	require.Equal(t, acked, replayed)
}

// TestBackpressureNeverCostsTheNodeItsShard: a tripped shard is degraded, not
// lost. Nothing but a write is refused — the shard renews its rangeID through
// the same registry and is told nothing about the tail, because refusing an
// acquire would convert degradation into the failover I10 exists to avoid.
func TestBackpressureNeverCostsTheNodeItsShard(t *testing.T) {
	ctx := context.Background()
	logs := memwal.New()
	cfg := Defaults()
	cfg.Sync = false
	cfg.Mutations, cfg.Bytes, cfg.Age = 1<<30, 1<<30, time.Hour
	cfg.HardMaxEntries = 2

	m, err := NewManager(Deps{Log: logs, Writer: &heldApplier{}, Recoverer: &fakeWatermark{}, Registry: testRegistry()}, Fixed(cfg))
	require.NoError(t, err)
	require.NoError(t, m.ShardAcquired(ctx, testShard, testEpoch))

	c := m.Shard(testShard)
	ns, wf, run := ids()
	// The workflow these updates update, as the cold store holds it.
	store := basetest.New()
	store.Holds(wf, run, 0)
	rows := store.Rows()
	require.NoError(t, c.write(ctx, mkUpdate(ns, wf, run, 1), rows))
	require.NoError(t, c.write(ctx, mkUpdate(ns, wf, run, 2), rows))
	requireRefusal(t, c.write(ctx, mkUpdate(ns, wf, run, 3), rows))

	require.Equal(t, StateRunning, c.State(), "a full tail is not a halt: the shard is still this node's")
	require.EqualValues(t, 2, c.Stats().CommitSeqno, "and it still answers what it is holding")

	require.NoError(t, m.ShardAcquired(ctx, testShard, testEpoch+1),
		"the ShardStore path is never refused: a rangeID renewal that fails is a lost shard")
	// What the fresh cycle makes of the entries the previous epoch left behind
	// is replay's business, and not this test's.
}

// ---------------------------------------------------------------------------
// The node's budget.
// ---------------------------------------------------------------------------

// TestTheNodeBudgetIsAStartupAssertion: §2 puts `hard_max × shards per node` in
// the node's RAM calculation, so raising the per-shard bound has to stop the
// node rather than quietly overcommit it. The registry is where that happens,
// because a composed binary has no cycle without one.
func TestTheNodeBudgetIsAStartupAssertion(t *testing.T) {
	deps := Deps{Log: memwal.New(), Writer: &heldApplier{}, Recoverer: &fakeWatermark{}, Registry: testRegistry()}

	t.Run("the measured policy fits, exactly", func(t *testing.T) {
		d := Defaults()
		require.Equal(t, 8192, d.HardMaxEntries)
		require.Equal(t, 8<<20, d.HardMaxBytes)
		require.Equal(t, 256, d.MaxShards)
		require.Equal(t, 2<<30, d.TailBudgetBytes,
			"the budget is where the 8 MB came from: 2 GB of tail over 256 shards")
		require.NoError(t, d.CheckBudget())

		m, err := NewManager(deps, Fixed(d))
		require.NoError(t, err)
		require.NotNil(t, m)
	})

	t.Run("a raised bound is refused with the arithmetic", func(t *testing.T) {
		cfg := Defaults()
		cfg.HardMaxBytes = 16 << 20

		err := cfg.CheckBudget()
		require.ErrorIs(t, err, ErrBudget)
		require.ErrorContains(t, err, "256 shards", "the message says what to change")

		m, err := NewManager(deps, Fixed(cfg))
		require.ErrorIs(t, err, ErrBudget)
		require.Nil(t, m, "no registry means no cycle: this is the layer refusing to start")
	})

	t.Run("more shards on the node is the same arithmetic from the other side", func(t *testing.T) {
		cfg := Defaults()
		cfg.MaxShards = 512
		require.ErrorIs(t, cfg.CheckBudget(), ErrBudget)
	})

	t.Run("a bound left at zero takes the measured one", func(t *testing.T) {
		// Neither reading of a zero is safe — "refuse everything" stops the
		// shard, "hold everything" is the unbounded tail — so it is filled.
		var cfg Config
		cfg.fill()
		require.Equal(t, Defaults().HardMaxBytes, cfg.HardMaxBytes)
		require.Equal(t, Defaults().HardMaxEntries, cfg.HardMaxEntries)
		require.NoError(t, cfg.CheckBudget())
	})
}
