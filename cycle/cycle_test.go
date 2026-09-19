package cycle

// The cycle without a cluster: what a drain's outcome does to the state
// machine, when the watermarks fire, and what the trim cadence allows. The log
// is memwal behind a fault the test sets; the applier and the watermark are
// fakes.

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics/metricstest"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"

	"github.com/aromanovich/waltz/apply"
	"github.com/aromanovich/waltz/baserow"
	"github.com/aromanovich/waltz/cold"
	"github.com/aromanovich/waltz/fold"
	"github.com/aromanovich/waltz/internal/verify/basetest"
	"github.com/aromanovich/waltz/internal/verify/mutbuild"
	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/wal"
	"github.com/aromanovich/waltz/wal/memwal"
	"github.com/aromanovich/waltz/wal/waltest"
	"github.com/aromanovich/waltz/walmetrics"
)

const testShard wal.ShardID = 3

// testRegistry is the default registry: nothing here interprets a category.
func testRegistry() tasks.TaskCategoryRegistry { return tasks.NewDefaultTaskCategoryRegistry() }

// ---------------------------------------------------------------------------
// The log, and the fakes: one answer each, programmable per call.
// ---------------------------------------------------------------------------

// newLog is what every test here drives: memwal, which keeps the contract,
// behind the decorator a test sets a failure on.
func newLog() *waltest.Faulty { return waltest.NewFaulty(memwal.New()) }

// fakeApplier records the drains and answers with whatever the test queued.
type fakeApplier struct {
	drains  [][]*fold.Emitted
	seqnos  []wal.Seqno
	errs    []error // one per call, nil-padded
	applied []wal.Seqno
	// committed, when set, runs on an accepted drain before it answers.
	committed func([]*fold.Emitted, fold.TaskWork)
	// store, when set, is the cold store this applier commits into
	// ([commitToColdStore]).
	store *basetest.Store
}

func (a *fakeApplier) Apply(_ context.Context, _ wal.ShardID, _ wal.Epoch, batch fold.Batch) error {
	i := len(a.drains)
	requests := slices.Collect(batch.Each())
	a.drains = append(a.drains, requests)
	a.seqnos = append(a.seqnos, batch.Watermark())
	if i < len(a.errs) && a.errs[i] != nil {
		return a.errs[i]
	}
	commitToColdStore(a.store, batch)
	if a.committed != nil {
		a.committed(requests, batch.Tasks())
	}
	a.applied = append(a.applied, batch.Watermark())
	return nil
}

// wmAnswer is one queued watermark answer. Past the queued ones the fake
// reports a shard with no watermark.
type wmAnswer struct {
	seqno wal.Seqno
	found bool
	err   error
}

type fakeWatermark struct {
	answers []wmAnswer
	reads   int
}

// A read on a dead context fails, as it does on every store: the queued answers
// are what this shard holds, not what it can be asked for.
func (w *fakeWatermark) Watermark(ctx context.Context, _ wal.ShardID) (wal.Seqno, bool, error) {
	i := w.reads
	w.reads++
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	if i < len(w.answers) {
		a := w.answers[i]
		return a.seqno, a.found, a.err
	}
	return 0, false, nil
}

// testDeps is the trio a manager here is built over, named once: a new required
// field of [Deps] is then filled in one place rather than in twelve literals.
func testDeps(log wal.Log, apply cold.Applier) Deps {
	return Deps{Log: log, Writer: apply, Recoverer: &fakeWatermark{}, Registry: testRegistry()}
}

// standUp is how a cycle is stood up over fakes here. The cycle does not fence
// — its manager does, before the rangeID lands — so the fence belongs with the
// construction: a harness standing one up over an unfenced log is driving a
// shard nobody acquired. The registry, the policy and the retire are the same
// everywhere; the applier, the watermark and the emitter are what each harness
// is about, so they arrive in deps.
//
// Not for a cycle left unfenced at its epoch ([zombie]) or left without a
// registry: in both the absence is what is under test.
func standUp(t *testing.T, epoch wal.Epoch, deps Deps, cfg Config) *Cycle {
	t.Helper()
	require.NoError(t, deps.Log.Fence(context.Background(), testShard, epoch))
	deps.Registry = testRegistry()
	c := New(testShard, epoch, deps, Fixed(cfg))
	t.Cleanup(func() { c.Retire() })
	return c
}

// logEntries is the whole log: what a cycle acked into, and what a replay after
// an acquire would read.
func logEntries(t *testing.T, log wal.Log) []wal.Entry {
	t.Helper()
	all, err := log.ReadFrom(context.Background(), testShard, wal.FirstSeqno, 1<<20)
	require.NoError(t, err)
	return all
}

// env is one cycle, the log it acks into and the fakes behind it.
type env struct {
	log   *waltest.Faulty
	apply *fakeApplier
	mark  *fakeWatermark
	cfg   Config
	c     *Cycle
	// clock is the cycle's time, driven by the test; moving it fires the age
	// timer.
	clock *clock.EventTimeSource
	// store is the cold store behind the condition authority; rows is its two
	// reads. Empty rather than absent ([ErrNoBaseRow]), so a test whose window
	// updates a run must put that run in here first.
	store *basetest.Store
	rows  *baserow.Rows
	// handler and capture are the metrics stack, on in every env.
	handler *metricstest.CaptureHandler
	capture *metricstest.Capture
}

// neverDrains is a cycle that folds and does not drain on its own: no size
// watermark within reach and an age timer that will not fire inside a test, so
// what drains the window is the test. Callers add the bound they are about.
func neverDrains(c *Config) {
	c.Sync = false
	c.Mutations, c.Bytes, c.Age = 1<<30, 1<<30, time.Hour
}

// newEnv builds a cycle whose clock is the test's: time only moves when the
// test moves it.
func newEnv(t *testing.T, shape func(*Config)) *env {
	t.Helper()
	e := &env{
		log:   newLog(),
		apply: &fakeApplier{},
		mark:  &fakeWatermark{},
		clock: clock.NewEventTimeSource(),
	}
	e.clock.Update(time.Unix(1700000000, 0))
	e.useStore(basetest.New())
	e.cfg = Defaults()
	e.cfg.Sync = false
	if shape != nil {
		shape(&e.cfg)
	}
	e.cfg.timeSource = e.clock

	e.handler = metricstest.NewCaptureHandler()
	e.capture = e.handler.StartCapture()
	t.Cleanup(func() { e.handler.StopCapture(e.capture) })

	e.c = standUp(t, testEpoch, Deps{
		Log:       e.log,
		Writer:    e.apply,
		Recoverer: e.mark,
		Metrics:   walmetrics.New(e.handler),
	}, e.cfg)
	// One round trip, so the age timer is armed before the test moves the clock:
	// the loop arms it on its own goroutine, and a test that advanced first
	// would land its tick a beat late.
	e.c.Stats()
	return e
}

func (e *env) entries(t *testing.T) []wal.Entry {
	t.Helper()
	return logEntries(t, e.log)
}

// inherit puts a previous owner's entries in the log, through seqno upTo: the
// log a watermark of upTo comes with. A cycle continuing that log appends at
// upTo+1, which on a log that never held them is [wal.ErrGap]. Nothing reads
// them — replay starts above the watermark — so their payloads say only where
// they came from.
func (e *env) inherit(t *testing.T, upTo wal.Seqno) {
	t.Helper()
	for i := range upTo {
		require.NoError(t, e.log.Append(context.Background(), testShard, testEpoch,
			wal.FirstSeqno+i, []byte("an entry of the previous owner")))
	}
}

// recorded is every recording of one metric, in the order it was made.
func (e *env) recorded(name string) []*metricstest.CapturedRecording {
	return e.capture.Snapshot()[name]
}

// tagged is the values one tag took across a metric's recordings.
func (e *env) tagged(name, key string) []string {
	var out []string
	for _, r := range e.recorded(name) {
		out = append(out, r.Tags[key])
	}
	return out
}

// advance moves the cycle's clock by d, which fires the age timer, and waits
// for the loop to catch up.
func (e *env) advance(t *testing.T, d time.Duration) {
	t.Helper()
	e.clock.Advance(d)
	e.c.Stats() // the loop answers this only after the timer branch returns
}

// add is the write path over the env's cold store.
func (e *env) add(t *testing.T, m mutation.Mutation) error {
	t.Helper()
	return e.c.write(context.Background(), m, e.rows)
}

// Mutations the codec and the accumulator both accept, built with no cluster by
// [mutbuild] so that what makes one well-formed is Temporal's answer rather
// than this file's.

var build = mutbuild.For(int32(testShard))

func mkCreate(ns, wf, run string) mutation.Mutation { return build.Create(ns, wf, run) }

func mkUpdate(ns, wf, run string, version int64) mutation.Mutation {
	return build.Update(ns, wf, run, version)
}

// mkContinueAsNew is the shape fold refuses when the run's window state is a
// snapshot: a continue-as-new out of a created run.
//
// It is assembled here rather than asked for, and that is deliberate: a run
// left RUNNING while its update carries a successor is what
// ValidateUpdateWorkflowModeState case 2 forbids, so [mutbuild] would refuse to
// build it. What the test drives is a request the store would reject and the
// fold must still have an answer for — see mutbuild's own note on the two
// fixtures that step outside its check.
func mkContinueAsNew(ns, wf, run, newRun string, version int64) mutation.Mutation {
	m := mkUpdate(ns, wf, run, version)
	// The successor's own snapshot is valid on its own terms, so it is taken
	// off a create rather than restated.
	snapshot := build.Create(ns, wf, newRun).Create.NewWorkflowSnapshot
	m.Update.NewWorkflowSnapshot = &snapshot
	return m
}

func ids() (string, string, string) { return uuid.NewString(), uuid.NewString(), uuid.NewString() }

// ---------------------------------------------------------------------------
// The write path.
// ---------------------------------------------------------------------------

// TestAddAcksBeforeItFolds: the log first, the accumulator second. A mutation
// folded before it is durable is lost from a window that already counted it.
func TestAddAcksBeforeItFolds(t *testing.T) {
	e := newEnv(t, nil)
	ns, wf, run := ids()

	require.NoError(t, e.add(t, mkCreate(ns, wf, run)))

	entries := e.entries(t)
	require.Len(t, entries, 1)
	require.Equal(t, wal.FirstSeqno, entries[0].Seqno,
		"no watermark means no drain ever committed: the log starts at its first seqno")
	require.Equal(t, testEpoch, entries[0].Epoch)
	require.Empty(t, e.apply.drains, "one mutation is a window, not a drain")

	s := e.c.Stats()
	require.Equal(t, StateRunning, s.State)
	require.Equal(t, 1, s.Mutations)
	require.NotZero(t, s.Bytes, "the window is counted in bytes as well as mutations (the size watermark's two units)")
	require.EqualValues(t, 1, s.CommitSeqno)
	require.Zero(t, s.AppliedSeqno)
}

// TestTheCycleContinuesTheLogAboveTheWatermark: a cycle starts at the
// watermark's successor, and reads that floor once rather than per mutation.
func TestTheCycleContinuesTheLogAboveTheWatermark(t *testing.T) {
	e := newEnv(t, nil)
	e.inherit(t, 41)
	e.mark.answers = []wmAnswer{{seqno: 41, found: true}}
	ns, wf, run := ids()

	require.NoError(t, e.add(t, mkCreate(ns, wf, run)))
	require.Equal(t, wal.Seqno(42), last(e.entries(t)).Seqno)
	require.Equal(t, 1, e.mark.reads, "the floor is read once, on the first mutation")

	require.NoError(t, e.add(t, mkUpdate(ns, wf, run, 2)))
	require.Equal(t, wal.Seqno(43), last(e.entries(t)).Seqno)
	require.Equal(t, 1, e.mark.reads)
}

// TestSyncModeDrainsEveryWrite: the window is 1, and the drain's outcome is
// this call's answer.
func TestSyncModeDrainsEveryWrite(t *testing.T) {
	e := newEnv(t, func(c *Config) { c.Sync = true })
	ns, wf, run := ids()

	require.NoError(t, e.add(t, mkCreate(ns, wf, run)))
	require.NoError(t, e.add(t, mkUpdate(ns, wf, run, 2)))

	require.Len(t, e.apply.drains, 2, "one drain per intercepted write")
	require.Equal(t, []wal.Seqno{1, 2}, e.apply.seqnos)
	require.Len(t, e.apply.drains[0], 1)
	s := e.c.Stats()
	require.EqualValues(t, 2, s.AppliedSeqno, "the watermark follows every write")
	require.Zero(t, s.Mutations)
	require.Equal(t, 1.0, s.LastStats.CollapseRatio(), "a window of one collapses nothing, and says so")
}

// TestSizeWatermarksDrain: both units fire, whichever first.
func TestSizeWatermarksDrain(t *testing.T) {
	t.Run("mutations", func(t *testing.T) {
		e := newEnv(t, func(c *Config) { c.Mutations = 3 })
		ns, wf, run := ids()
		require.NoError(t, e.add(t, mkCreate(ns, wf, run)))
		require.NoError(t, e.add(t, mkUpdate(ns, wf, run, 2)))
		require.Empty(t, e.apply.drains, "below the watermark nothing drains")

		require.NoError(t, e.add(t, mkUpdate(ns, wf, run, 3)))
		require.Len(t, e.apply.drains, 1)
		require.Equal(t, wal.Seqno(3), e.apply.seqnos[0], "the drain is watermarked at its last entry")
		require.Equal(t, 3, e.c.Stats().LastStats.MutationsIn)
	})

	t.Run("bytes", func(t *testing.T) {
		e := newEnv(t, func(c *Config) { c.Mutations = 1 << 20; c.Bytes = 1 })
		ns, wf, run := ids()
		require.NoError(t, e.add(t, mkCreate(ns, wf, run)))
		require.Len(t, e.apply.drains, 1, "one encoded mutation is already past a 1-byte size watermark")
	})
}

// TestAgeWatermarkDrainsAnIdleTail: the trigger for a tail nothing pushes on.
func TestAgeWatermarkDrainsAnIdleTail(t *testing.T) {
	e := newEnv(t, func(c *Config) { c.Age = 5 * time.Second })
	ns, wf, run := ids()

	// The timer was armed at loop start, so opening the window a second in makes
	// the two independent: the loop re-checks the window's own age rather than
	// trusting the tick.
	e.advance(t, time.Second)
	require.NoError(t, e.add(t, mkCreate(ns, wf, run)))

	e.advance(t, 4*time.Second) // the tick lands; the window is 4s old
	require.Empty(t, e.apply.drains, "a tail younger than the age watermark stays")

	e.advance(t, 5*time.Second) // the next tick; the window is 9s old
	require.Len(t, e.apply.drains, 1, "an idle tail past the age watermark drains itself")

	e.advance(t, 5*time.Second)
	require.Len(t, e.apply.drains, 1, "an empty window has nothing to drain")
}

// TestAnAgeOfZeroIsTheMeasuredOneAndNotEveryTick: the loop arms its timer from
// the policy, so an age of zero is a tick that is due the moment it is set and
// a goroutine that never sleeps — a whole CPU per shard held, on a node nobody
// restarted, since wal.windowAge is one of the five settings read live. It has
// no reading as a policy either: "drain a tail the instant it exists" is what a
// window of one already says, in a mode that can attribute the outcome.
func TestAnAgeOfZeroIsTheMeasuredOneAndNotEveryTick(t *testing.T) {
	e := newEnv(t, func(c *Config) { c.Age = 0 })
	ns, wf, run := ids()

	require.NoError(t, e.add(t, mkCreate(ns, wf, run)))

	e.advance(t, time.Second)
	require.Empty(t, e.apply.drains, "a tail one second old is not past the measured age")

	e.advance(t, Defaults().Age)
	require.Len(t, e.apply.drains, 1, "and the tail drains at the age the cycle actually runs at")
}

// TestARefusedWindowForceDrains: fold.ErrRefused is not a failure but a drain
// trigger — drain, then let the refused mutation head a fresh window.
func TestARefusedWindowForceDrains(t *testing.T) {
	e := newEnv(t, nil)
	ns, wf, run := ids()
	require.NoError(t, e.add(t, mkCreate(ns, wf, run)))
	require.NoError(t, e.add(t, mkContinueAsNew(ns, wf, run, uuid.NewString(), 2)))

	require.Len(t, e.apply.drains, 1, "the window that could not hold it was drained")
	require.Equal(t, wal.Seqno(1), e.apply.seqnos[0])
	s := e.c.Stats()
	require.Equal(t, 1, s.Refusals)
	require.Equal(t, 1, s.Mutations, "the refused mutation heads the fresh window")
	require.Equal(t, StateRunning, s.State)
}

// ---------------------------------------------------------------------------
// The states.
// ---------------------------------------------------------------------------

// TestAFencedAppendHaltsLost: a fenced log is I4 working — the cycle stops, and
// it does not trim.
func TestAFencedAppendHaltsLost(t *testing.T) {
	e := newEnv(t, nil)
	e.log.OnAppend(waltest.Once(wal.ErrFenced))
	ns, wf, run := ids()

	err := e.add(t, mkCreate(ns, wf, run))
	require.ErrorIs(t, err, wal.ErrFenced)
	require.Equal(t, StateHaltedLost, e.c.State())

	again := e.add(t, mkUpdate(ns, wf, run, 2))
	require.ErrorIs(t, again, ErrHalted)
	require.Empty(t, e.entries(t), "a halted cycle acks nothing")
	require.Empty(t, e.log.Trims(), "the log is the next owner's evidence: a lost shard trims nothing")
}

// TestAVersionFailureHaltsInvariant: the class that must not be retried and
// must not become ShardOwnershipLost. The window holds two mutations, since a
// drain that mixes callers has nobody to attribute the failure to.
func TestAVersionFailureHaltsInvariant(t *testing.T) {
	e := newEnv(t, func(c *Config) { c.Mutations = 2 })
	cause := &p.WorkflowConditionFailedError{Msg: "stale"}
	e.apply.errs = []error{cause}
	ns, wf, run := ids()

	require.NoError(t, e.add(t, mkCreate(ns, wf, run)))
	err := e.add(t, mkUpdate(ns, wf, run, 2))
	require.ErrorAs(t, err, new(*p.WorkflowConditionFailedError))
	require.Equal(t, StateHaltedInvariant, e.c.State())

	next := e.add(t, mkUpdate(ns, wf, run, 3))
	require.ErrorIs(t, next, ErrHalted)
	require.ErrorAs(t, next, new(*p.WorkflowConditionFailedError),
		"the attribution travels with the refusal, or the halt says nothing about what diverged")
	require.Len(t, e.apply.drains, 1, "a version failure is not retried: the accumulator is the authority")
	require.Equal(t, 1, e.mark.reads,
		"the seqno floor and nothing else: a known outcome needs no readback")
}

// ---------------------------------------------------------------------------
// Sync mode: the window of one.
// ---------------------------------------------------------------------------

// TestSyncModeAnswersAConditionFailureToItsCaller: at a window of one the
// failed assertion is that caller's own, so it goes back as the store's own
// error value and the shard keeps running.
func TestSyncModeAnswersAConditionFailureToItsCaller(t *testing.T) {
	e := newEnv(t, func(c *Config) { c.Sync = true })
	cause := &p.WorkflowConditionFailedError{Msg: "stale", DBRecordVersion: 4}
	e.apply.errs = []error{cause}
	ns, wf, run := ids()

	err := e.add(t, mkCreate(ns, wf, run))
	require.True(t, err == error(cause), //nolint:errorlint // identity is the assertion
		"the caller must get the store's own error unwrapped, got %v", err)
	require.Equal(t, StateRunning, e.c.State(),
		"a condition the caller was told about is not a divergence this process owns")

	// And the shard keeps writing.
	require.NoError(t, e.add(t, mkCreate(ns, wf, run)))
	require.Len(t, e.apply.drains, 2)
	require.Equal(t, []wal.Seqno{2}, e.apply.applied,
		"the watermark commits past the failed entry rather than at it")
}

// TestSyncModeUnwrapsApplysAttribution: the history service type-switches on
// concrete types with no errors.As, so the caller gets apply's cause rather
// than the InvariantViolationError wrapped around it.
func TestSyncModeUnwrapsApplysAttribution(t *testing.T) {
	e := newEnv(t, func(c *Config) { c.Sync = true })
	cause := &p.CurrentWorkflowConditionFailedError{Msg: "another run is current"}
	e.apply.errs = []error{&apply.InvariantViolationError{
		Cause:    cause,
		Diverged: []apply.Diverged{{WorkflowID: "wf", Detail: "asserted absent, the row exists"}},
	}}
	ns, wf, run := ids()

	err := e.add(t, mkCreate(ns, wf, run))
	require.True(t, err == error(cause), //nolint:errorlint // identity is the assertion
		"the caller must get the store's own error, not apply's wrapper around it: %T", err)
}

// TestASyncConditionFailureSettlesItsEntry: the entry stays in the log but is
// settled, so the tail I10 bounds stops counting it. The watermark must not
// move with it, since trim goes to the watermark and trimming past what the
// cold store holds strands a recovering owner.
func TestASyncConditionFailureSettlesItsEntry(t *testing.T) {
	e := newEnv(t, func(c *Config) {
		c.Sync = true
		c.HardMaxEntries = 2
	})
	e.apply.errs = []error{&p.WorkflowConditionFailedError{Msg: "stale"}}
	ns, wf, run := ids()

	require.Error(t, e.add(t, mkCreate(ns, wf, run)))

	s := e.c.Stats()
	require.EqualValues(t, 1, s.CommitSeqno, "the entry was acked and stays acked")
	require.EqualValues(t, 0, s.AppliedSeqno, "and no watermark moved: nothing committed")
	require.Zero(t, s.TailEntries, "its fate is settled, so it is not tail")
	require.Zero(t, s.TailBytes, "in either unit")

	// Settled rather than merely reported: with two entries allowed, three
	// failures in a row still take writes.
	for i := range 3 {
		e.apply.errs = append(e.apply.errs, &p.WorkflowConditionFailedError{Msg: "stale"})
		require.Error(t, e.add(t, mkCreate(ns, wf, run)),
			"failure %d should reach apply rather than be refused for a tail nobody is holding", i)
	}
}

// TestShardLostFromApplyHalts: the same fence, discovered through the apply
// transaction's epoch CAS instead of through the log.
func TestShardLostFromApplyHalts(t *testing.T) {
	e := newEnv(t, func(c *Config) { c.Sync = true })
	e.apply.errs = []error{&p.ShardOwnershipLostError{ShardID: int32(testShard), Msg: "fenced"}}
	ns, wf, run := ids()

	require.Error(t, e.add(t, mkCreate(ns, wf, run)))
	require.Equal(t, StateHaltedLost, e.c.State())
}

// TestAnUnknownOutcomeReadsTheWatermarkFirst: the recovery rule. Deciding by
// re-reading base versions is what applies a committed batch twice.
func TestAnUnknownOutcomeReadsTheWatermarkFirst(t *testing.T) {
	t.Run("it had committed", func(t *testing.T) {
		e := newEnv(t, func(c *Config) { c.Sync = true })
		e.apply.errs = []error{context.DeadlineExceeded}
		// The floor first (no drain has ever committed), then the readback: a
		// watermark at the drain's own seqno.
		e.mark.answers = []wmAnswer{{}, {seqno: 1, found: true}}
		ns, wf, run := ids()

		require.NoError(t, e.add(t, mkCreate(ns, wf, run)),
			"a watermark at the drain's seqno says the transaction committed after all")
		require.Equal(t, StateRunning, e.c.State())
		require.EqualValues(t, 1, e.c.Stats().AppliedSeqno)
		require.Equal(t, 2, e.mark.reads, "the seqno floor, then the outcome")
	})

	t.Run("it had not", func(t *testing.T) {
		e := newEnv(t, func(c *Config) { c.Sync = true })
		e.apply.errs = []error{context.DeadlineExceeded}
		ns, wf, run := ids()

		err := e.add(t, mkCreate(ns, wf, run))
		require.ErrorIs(t, err, ErrHalted)
		require.Equal(t, StateHaltedInvariant, e.c.State(),
			"the window is gone and the entries are acked: rebuilding the batch here is replay, not recovery")
	})

	t.Run("the watermark itself is unreadable", func(t *testing.T) {
		e := newEnv(t, func(c *Config) { c.Sync = true })
		e.apply.errs = []error{context.DeadlineExceeded}
		e.mark.answers = []wmAnswer{{}, {err: errors.New("the WAL folder is unreachable")}}
		ns, wf, run := ids()

		require.Error(t, e.add(t, mkCreate(ns, wf, run)))
		require.Equal(t, StateRunning, e.c.State(),
			"a read that failed is not an answer: halting on it would turn a blip into a lost shard")
	})
}

// TestATakenSeqnoHalts: a cycle appends only above the tail it replayed, so a
// seqno the log already holds means a second writer at this epoch.
func TestATakenSeqnoHalts(t *testing.T) {
	e := newEnv(t, nil)
	e.log.OnAppend(waltest.Once(wal.ErrAlreadyWritten))
	ns, wf, run := ids()

	err := e.add(t, mkCreate(ns, wf, run))
	require.ErrorIs(t, err, wal.ErrAlreadyWritten)
	require.Equal(t, StateHaltedInvariant, e.c.State())
	require.ErrorIs(t, e.add(t, mkUpdate(ns, wf, run, 2)), ErrTailNotEmpty,
		"the halt names the gap it cannot fill")
}

// The append whose outcome the contract has no name for. Three answers, and
// the log is the witness for all three, as the watermark is for a drain: the
// one thing that may not follow such an append is another mutation at the same
// seqno, since with the first attempt possibly still in flight, which of the two
// ends up there is the backend's race to settle and a caller was told each of
// the two answers.
func TestAnAmbiguousAppend(t *testing.T) {
	unreachable := errors.New("the connection went away mid-append")

	t.Run("that landed is the caller's success", func(t *testing.T) {
		e := newEnv(t, nil)
		e.log.AfterAppend(waltest.Once(unreachable))
		ns, wf, run := ids()

		require.NoError(t, e.add(t, mkCreate(ns, wf, run)),
			"the entry is durable, so reporting the append failed would be a lie the caller acts on")
		require.Equal(t, StateRunning, e.c.State())

		entries := e.entries(t)
		require.Len(t, entries, 1, "the readback settled the append rather than repeating it")
		require.Equal(t, wal.FirstSeqno, entries[0].Seqno)

		s := e.c.Stats()
		require.Equal(t, 1, s.Mutations, "and the mutation is in the window exactly once")
		require.Equal(t, wal.FirstSeqno, s.CommitSeqno)
	})

	t.Run("that wrote nothing leaves the seqno free", func(t *testing.T) {
		e := newEnv(t, nil)
		e.log.OnAppend(waltest.Once(unreachable))
		ns, wf, run := ids()

		require.ErrorIs(t, e.add(t, mkCreate(ns, wf, run)), unreachable)
		require.Equal(t, StateRunning, e.c.State(),
			"the log proved the append wrote nothing, so a blip is not a lost shard")

		require.NoError(t, e.add(t, mkCreate(ns, wf, run)))
		entries := e.entries(t)
		require.Len(t, entries, 1)
		require.Equal(t, wal.FirstSeqno, entries[0].Seqno, "the next mutation takes the seqno it left")
	})

	t.Run("nobody could read the outcome of halts the shard", func(t *testing.T) {
		e := newEnv(t, nil)
		ns, wf, run := ids()
		// The first write replays a tail that is not there, which is two reads:
		// the page, and the confirmation that the log ends where the page did.
		// The readback is the call after those.
		e.log.OnRead(func(call int) error {
			if call <= 2 {
				return nil
			}
			return unreachable
		})
		e.log.AfterAppend(waltest.Once(unreachable))

		err := e.add(t, mkCreate(ns, wf, run))
		require.ErrorIs(t, err, ErrHalted)
		require.Equal(t, StateHaltedInvariant, e.c.State(),
			"the seqno's fate is open, and the one thing that may not follow is another mutation at it")
		require.Empty(t, e.log.Trims(), "a halted cycle's log is the evidence")
	})

	t.Run("finding an entry it did not write halts the shard", func(t *testing.T) {
		e := newEnv(t, nil)
		ns, wf, run := ids()
		require.NoError(t, e.add(t, mkCreate(ns, wf, run)))

		// A second writer at this cycle's own epoch takes the seqno it is about
		// to use, which is what an entry above everything this cycle replayed
		// and not its own means.
		require.NoError(t, e.log.Append(
			context.Background(), testShard, testEpoch, wal.FirstSeqno+1, []byte("somebody else's")))
		e.log.OnAppend(waltest.Once(unreachable))

		require.ErrorIs(t, e.add(t, mkUpdate(ns, wf, run, 2)), ErrTailNotEmpty)
		require.Equal(t, StateHaltedInvariant, e.c.State())
	})
}

// TestAMutationOfAnotherShardIsRefused: refused before it is acked.
func TestAMutationOfAnotherShardIsRefused(t *testing.T) {
	e := newEnv(t, nil)
	ns, wf, run := ids()
	m := mkCreate(ns, wf, run)
	m.Create.ShardID = int32(testShard) + 1

	require.ErrorContains(t, e.add(t, m), "shard")
	require.Empty(t, e.entries(t))
	require.Equal(t, StateRunning, e.c.State(), "a caller's mistake does not halt the shard")
}

// ---------------------------------------------------------------------------
// The trim.
// ---------------------------------------------------------------------------

// TestTrimRunsOnItsCadence: after the commit, to the committed appliedSeqno
// with no safety lag (recovery reads the watermark, not the log), and at most
// one per N drains or T seconds.
func TestTrimRunsOnItsCadence(t *testing.T) {
	e := newEnv(t, func(c *Config) { c.Sync = true; c.TrimEvery = 3; c.TrimAfter = time.Minute })
	ns, wf, run := ids()

	require.NoError(t, e.add(t, mkCreate(ns, wf, run)))
	require.NoError(t, e.add(t, mkUpdate(ns, wf, run, 2)))
	e.c.Retire() // waits for any trim in flight
	require.Empty(t, e.log.Trims(), "two drains do not reach a cadence of three")

	e = newEnv(t, func(c *Config) { c.Sync = true; c.TrimEvery = 3; c.TrimAfter = time.Minute })
	ns, wf, run = ids()
	require.NoError(t, e.add(t, mkCreate(ns, wf, run)))
	require.NoError(t, e.add(t, mkUpdate(ns, wf, run, 2)))
	require.NoError(t, e.add(t, mkUpdate(ns, wf, run, 3)))
	e.c.Retire()
	require.Equal(t, []wal.Seqno{3}, e.log.Trims(),
		"the trim goes to the committed appliedSeqno, with no lag")
}

// TestAFailedTrimDoesNotHaltOrBlock: the shard goes on running through a trim
// that failed. The wait between the two writes is load-bearing: the trimmer
// skips a cadence due while one is in flight, so a write issued during the
// first gets no trim (the cadence itself is [trim]'s own test).
func TestAFailedTrimDoesNotHaltOrBlock(t *testing.T) {
	e := newEnv(t, func(c *Config) { c.Sync = true; c.TrimEvery = 1; c.TrimAfter = time.Minute })
	e.log.OnTrim(waltest.Always(errors.New("the WAL table is busy")))
	ns, wf, run := ids()

	require.NoError(t, e.add(t, mkCreate(ns, wf, run)))
	e.c.trimmer.Wait()
	require.Equal(t, []wal.Seqno{1}, e.log.Trims(), "the first cadence trimmed, and it failed")

	require.NoError(t, e.add(t, mkUpdate(ns, wf, run, 2)))
	require.Equal(t, StateRunning, e.c.State(), "a trim that failed is not the shard's problem")

	e.c.Retire() // waits for the trims in flight, so what they did is readable
	require.Equal(t, []wal.Seqno{1, 2}, e.log.Trims(), "the next cadence tries again")
}

// TestAHaltedCycleStopsTrimming: on either halt the log is the evidence.
func TestAHaltedCycleStopsTrimming(t *testing.T) {
	e := newEnv(t, func(c *Config) { c.Sync = true; c.TrimEvery = 1 })
	e.apply.errs = []error{&p.ShardOwnershipLostError{ShardID: int32(testShard), Msg: "fenced"}}
	ns, wf, run := ids()

	require.Error(t, e.add(t, mkCreate(ns, wf, run)))
	e.c.Retire()
	require.Empty(t, e.log.Trims())
}

// TestATrimNeverBlocksADrain: the trim runs beside the cycle's loop, not in it,
// so a stuck trim must not stop the shard from acking and applying.
func TestATrimNeverBlocksADrain(t *testing.T) {
	e := newEnv(t, func(c *Config) { c.Sync = true; c.TrimEvery = 1 })
	hold := make(chan struct{})
	e.log.OnTrim(func(int) error { <-hold; return nil })
	ns, wf, run := ids()

	require.NoError(t, e.add(t, mkCreate(ns, wf, run)))    // trims, and the trim hangs
	require.NoError(t, e.add(t, mkUpdate(ns, wf, run, 2))) // must not wait for it
	require.NoError(t, e.add(t, mkUpdate(ns, wf, run, 3)))

	require.Len(t, e.apply.drains, 3, "the write path kept going while a trim hung")
	require.Empty(t, e.log.Trims(), "and the trim is still where it was")

	close(hold)
	e.c.Retire()
	require.Equal(t, []wal.Seqno{1}, e.log.Trims(),
		"one trim at a time: the cadence does not queue them up behind a stuck one")
}

// ---------------------------------------------------------------------------
// Shutdown and the registry.
// ---------------------------------------------------------------------------

// TestCloseDrainsWhatTheWindowHolds: shutdown is the one moment a tail drains
// with no watermark asking for it.
func TestCloseDrainsWhatTheWindowHolds(t *testing.T) {
	e := newEnv(t, nil)
	ns, wf, run := ids()
	require.NoError(t, e.add(t, mkCreate(ns, wf, run)))
	require.Empty(t, e.apply.drains)

	require.NoError(t, e.c.Close(context.Background()))
	require.Len(t, e.apply.drains, 1)
	require.Equal(t, wal.Seqno(1), e.apply.seqnos[0])
}

// TestManagerSupersedesByEpoch: an acquire fences the log before the rangeID
// lands, a higher epoch retires the cycle below it, the same epoch is
// idempotent, and a lower one is refused.
func TestManagerSupersedesByEpoch(t *testing.T) {
	logs := newLog()
	m, err := NewManager(testDeps(logs, &fakeApplier{}), Fixed(Defaults()))
	require.NoError(t, err)
	ctx := context.Background()

	require.NoError(t, m.ShardAcquired(ctx, testShard, 4))
	first := m.Shard(testShard)
	require.NotNil(t, first)
	require.EqualValues(t, 4, first.Epoch())
	require.Equal(t, []wal.Epoch{4}, logs.Fences())

	require.NoError(t, m.ShardAcquired(ctx, testShard, 4))
	require.Same(t, first, m.Shard(testShard), "re-acquiring at the same epoch is idempotent")
	require.Len(t, logs.Fences(), 1, "and does not re-fence")

	require.NoError(t, m.ShardAcquired(ctx, testShard, 5))
	second := m.Shard(testShard)
	require.NotSame(t, first, second)
	require.EqualValues(t, 5, second.Epoch())
	require.Equal(t, []wal.Epoch{4, 5}, logs.Fences())
	require.ErrorIs(t, first.write(ctx, mkCreate(ids()), coldRows()), ErrHalted,
		"the superseded cycle is stopped, tail and all")
	require.Equal(t, StateHaltedLost, first.State(),
		"being superseded is what halted-lost means, and it keeps saying so")

	require.ErrorIs(t, m.ShardAcquired(ctx, testShard, 3), wal.ErrFenced,
		"a lower epoch is the shard moving on, not an acquire")

	m.Close(ctx)
	require.Nil(t, m.Shard(testShard))
}

// TestAnAcquireAfterTheShutdownTakesNoShard: [Manager.Close] is the one stop
// with no successor implied, and what it answers is every shard still holding
// acked entries. A cycle installed behind it would ack into a log the layer is
// about to release, be drained by nothing and appear in no such answer — so the
// caller taking the layer out is told the entries do not exist, which is the one
// question that answer is for.
func TestAnAcquireAfterTheShutdownTakesNoShard(t *testing.T) {
	logs := newLog()
	m, err := NewManager(
		testDeps(logs, &fakeApplier{}),
		Fixed(Defaults()))
	require.NoError(t, err)
	ctx := context.Background()
	require.Empty(t, m.Close(ctx))

	require.ErrorIs(t, m.ShardAcquired(ctx, testShard, 4), ErrClosed)
	require.Nil(t, m.Shard(testShard))
	require.Empty(t, m.Close(ctx), "and the second answer names no shard either")
}

// TestAFailedFenceLeavesNoCycle: the log's epoch may never lag the database's,
// so a fence that failed must leave no cycle behind.
func TestAFailedFenceLeavesNoCycle(t *testing.T) {
	logs := newLog()
	logs.OnFence(waltest.Always(wal.ErrFenced))
	m, err := NewManager(testDeps(logs, &fakeApplier{}), Fixed(Defaults()))
	require.NoError(t, err)

	err = m.ShardAcquired(context.Background(), testShard, 9)
	require.Equal(t, wal.ErrFenced, err, "the log's error reaches the shard controller unwrapped")
	require.Nil(t, m.Shard(testShard))
}

// coldRows is an empty cold store's two reads: what a direct [Cycle.write] passes
// when the test names no pre-window rows. Not a nil [baserow.Rows], which is
// refused ([ErrNoBaseRow]).
func coldRows() *baserow.Rows { return basetest.New().Rows() }

// rowsHolding is the two reads of a cold store that already holds one workflow
// ([basetest.Store.Holds]).
func rowsHolding(wf, run string, baseVersion int64) *baserow.Rows {
	s := basetest.New()
	s.Holds(wf, run, baseVersion)
	return s.Rows()
}

// useStore points the whole cold-store side of the env at store: the rows the
// write path reads, and the rows a committed drain writes. Setting one without
// the other leaves the condition authority reading a store nothing is applied
// to.
func (e *env) useStore(store *basetest.Store) {
	e.store = store
	e.rows = store.Rows()
	e.apply.store = store
}

// coldWorkflow puts a workflow in this env's cold store.
func (e *env) coldWorkflow(wf, run string, baseVersion int64) {
	e.store.Holds(wf, run, baseVersion)
}

// commitToColdStore applies a committed drain to the rows behind it, so the
// cold store the condition authority reads holds the window's merged requests
// afterwards.
func commitToColdStore(store *basetest.Store, batch fold.Batch) {
	if store == nil {
		return
	}
	for e := range batch.Each() {
		switch e.Request.Kind() {
		case mutation.KindCreate:
			snap := e.Request.Create.NewWorkflowSnapshot
			store.SetRun(snap.RunID, snap.DBRecordVersion)
		case mutation.KindUpdate:
			mut := e.Request.Update.UpdateWorkflowMutation
			store.SetRun(mut.RunID, mut.DBRecordVersion)
			if ns := e.Request.Update.NewWorkflowSnapshot; ns != nil {
				store.SetRun(ns.RunID, ns.DBRecordVersion)
			}
		case mutation.KindSet:
			snap := e.Request.Set.SetWorkflowSnapshot
			store.SetRun(snap.RunID, snap.DBRecordVersion)
		case mutation.KindDelete:
			store.DeleteRun(e.Request.Delete.RunID)
		}
		// The current row follows what fold said the window wrote, which is the
		// value apply puts in the transaction.
		wf := e.Workflow()
		if cw := wf.CurrentWrite; cw != nil {
			store.SetCurrent(e.WorkflowID, cw.RunID, cw.State, cw.LastWriteVersion)
		}
		if wf.CurrentRemoved {
			store.DeleteCurrent(e.WorkflowID)
		}
	}
}

// TestAWriteBringingNoBaseRowsIsRefused: an assertion the window cannot determine
// is settled against the pre-window row, so the caller hands the write path the
// store's own two reads. A caller that brings none has nothing to settle it with —
// and the only two answers are to refuse the write or to ack it with the assertion
// unevaluated, which is a conditional write acknowledged by nobody having checked
// the condition.
//
// Both arms of the delegated walk ask it, because which row the store would have
// judged first is what the refusal names, and neither was driven: deleting either
// check left the whole tree green. Refusal rather than panic is deliberate — a
// refused write provably acked nothing, and "unreachable" is a claim about today's
// callers rather than about tomorrow's.
func TestAWriteBringingNoBaseRowsIsRefused(t *testing.T) {
	ctx := context.Background()
	ns, wf, run := ids()

	// A Set asserts the run's own row at the version below it and claims nothing
	// about the current-execution row, which is what puts the walk on the run arm:
	// the current row is settled first wherever a request asserts one at all.
	t.Run("a run assertion the window does not hold", func(t *testing.T) {
		e := newEnv(t, nil)
		err := e.c.write(ctx, build.Set(ns, wf, run, 2), nil)
		require.ErrorIs(t, err, ErrNoBaseRow)
		require.ErrorContains(t, err, run, "the refusal names the row the store would have judged")
		require.Empty(t, e.entries(t), "a refused write may not have appended")
	})

	t.Run("a current-row assertion the window does not hold", func(t *testing.T) {
		e := newEnv(t, nil)
		// A start over a workflow id whose previous run has finished: the current
		// row's version is a column only the store can answer for.
		err := e.c.write(ctx, mkReuseCreate(ns, wf, run, uuid.NewString(), 0), nil)
		require.ErrorIs(t, err, ErrNoBaseRow)
		require.Empty(t, e.entries(t), "a refused write may not have appended")
	})
}

// TestAnAcquireBelowTheHeldEpochIsRefused: the server hands out a strictly
// greater rangeID per acquire, so an acquire *below* the epoch a cycle already
// holds is two observations delivered out of order. Refusing it is what keeps the
// live owner's window: installing the stale cycle retires the newer one, and what
// the newer one was holding is acked entries no drain of the stale cycle can carry
// — it is fenced at a lower epoch than the log, so every write and every drain of
// it is refused, and the shard needs a third acquire before anybody can apply
// them.
//
// The behaviour was driven by nothing: every other case acquires upward. What this
// pins is the behaviour and not one mechanism, and the distinction is worth stating
// because a sweep will find it: deleting the registry's own epoch comparison leaves
// this green, since the acquire then reaches [wal.Log.Fence] and the log — already
// fenced at the higher epoch — refuses it there. Two mechanisms, one outcome. The
// comparison stays because it answers without a round trip and names both epochs,
// which is the only evidence that the acquires arrived out of order rather than
// that this node lost the shard.
func TestAnAcquireBelowTheHeldEpochIsRefused(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, nil)
	m, err := NewManager(Deps{
		Log:       e.log,
		Writer:    e.apply,
		Recoverer: e.mark,
		Registry:  testRegistry(),
		Metrics:   walmetrics.New(e.handler),
	}, Fixed(e.cfg))
	require.NoError(t, err)
	t.Cleanup(func() { m.Close(ctx) })

	const shard = wal.ShardID(11)
	require.NoError(t, m.ShardAcquired(ctx, shard, 9))
	held := m.Shard(shard)
	require.NotNil(t, held)

	err = m.ShardAcquired(ctx, shard, 8)
	require.ErrorIs(t, err, wal.ErrFenced,
		"an acquire below the held epoch is a stale observation, not a change of ownership")
	require.ErrorContains(t, err, "9", "the refusal names the epoch that holds the shard")

	require.Same(t, held, m.Shard(shard),
		"the stale acquire replaced the live cycle: its window is acked entries the stale "+
			"cycle cannot drain, being fenced below the log's own epoch")
	require.Equal(t, wal.Epoch(9), m.Shard(shard).Epoch())
}
