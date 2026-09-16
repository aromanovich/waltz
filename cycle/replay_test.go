package cycle

// Replay, and the entry class that may not be applied. Every test here is a
// shard changing hands over a real log (memwal), with the applier and the
// watermark as fakes.
//
// No upstream suite reaches this code — its test methods all run on fresh
// shards, so none re-acquires one with a window still in it — so this file and
// internal/verify/acceptance's recovery run, where one stream is cut by five
// crashes and each successor replays what its predecessor acked, are the
// coverage.

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/metrics/metricstest"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"

	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/wal"
	"github.com/aromanovich/waltz/wal/memwal"
	"github.com/aromanovich/waltz/wal/waltest"
	"github.com/aromanovich/waltz/walmetrics"
)

// appliedWatermark reports the watermark as the cold store would: whatever the
// applier last committed. It is what makes a replay's drain visible to the next
// owner.
type appliedWatermark struct{ ap *fakeApplier }

func (w appliedWatermark) Watermark(context.Context, wal.ShardID) (wal.Seqno, bool, error) {
	if len(w.ap.applied) == 0 {
		return 0, false, nil
	}
	return w.ap.applied[len(w.ap.applied)-1], true, nil
}

// owner is one process's cycle over a shared log, with a capturing metrics
// stack so the replay counters can be read off it.
type owner struct {
	c       *Cycle
	ap      *fakeApplier
	handler *metricstest.CaptureHandler
	capture *metricstest.Capture
}

// takeShard builds an owner: a cycle at a fresh epoch over the shared log, with
// the watermark the previous owner's drains left.
func takeShard(t *testing.T, log wal.Log, epoch wal.Epoch, shape func(*Config)) *owner {
	t.Helper()
	ap := &fakeApplier{}
	cfg := Defaults()
	cfg.Sync = false
	if shape != nil {
		shape(&cfg)
	}
	o := &owner{ap: ap, handler: metricstest.NewCaptureHandler()}
	o.capture = o.handler.StartCapture()
	t.Cleanup(func() { o.handler.StopCapture(o.capture) })
	o.c = standUp(t, epoch, Deps{
		Log:       log,
		Writer:    ap,
		Recoverer: appliedWatermark{ap},
		Metrics:   walmetrics.New(o.handler),
	}, cfg)
	return o
}

// recorded is the values one counter took, in the order they were recorded.
func (o *owner) recorded(name string) []int64 {
	var out []int64
	for _, r := range o.capture.Snapshot()[name] {
		out = append(out, r.Value.(int64))
	}
	return out
}

// TestTheTailIsAppliedBeforeTheFirstCallerIsServed: three mutations a previous
// owner acked and never applied are read from the log, folded into a fresh
// accumulator and drained, before the write that triggered the replay is
// appended.
func TestTheTailIsAppliedBeforeTheFirstCallerIsServed(t *testing.T) {
	ctx := context.Background()
	log := memwal.New()

	first := takeShard(t, log, 7, nil)
	ns, wf, run := ids()
	require.NoError(t, first.c.write(ctx, mkCreate(ns, wf, run), coldRows()))
	require.NoError(t, first.c.write(ctx, mkUpdate(ns, wf, run, 2), coldRows()))
	ns2, wf2, run2 := ids()
	require.NoError(t, first.c.write(ctx, mkCreate(ns2, wf2, run2), coldRows()))
	require.Empty(t, first.ap.drains, "the window is 256: nothing has been applied")
	// The node dies here: no drain, no close.
	first.c.Retire()

	second := takeShard(t, log, 8, nil)
	ns3, wf3, run3 := ids()
	require.NoError(t, second.c.write(ctx, mkCreate(ns3, wf3, run3), coldRows()))

	require.Len(t, second.ap.drains, 1, "one drain, and it is replay's own")
	require.EqualValues(t, 3, second.ap.seqnos[0], "through the tail's last seqno")
	require.Len(t, second.ap.drains[0], 2, "three mutations over two workflows, collapsed")

	s := second.c.Stats()
	require.Equal(t, 3, s.Replayed)
	require.Zero(t, s.Dropped)
	require.EqualValues(t, 4, s.CommitSeqno, "the new owner's own write continues the log")
	require.EqualValues(t, 3, s.AppliedSeqno)
	require.Equal(t, StateRunning, s.State)

	entries, err := log.ReadFrom(ctx, testShard, wal.FirstSeqno, 16)
	require.NoError(t, err)
	require.Len(t, entries, 4, "replay appends nothing and rewrites nothing")

	// Nothing was dropped, so only the replayed counter moved.
	require.Equal(t, []int64{3}, second.recorded("wal_replayed_entries"))
	require.Empty(t, second.recorded("wal_replay_dropped_entries"))
	require.Contains(t, second.capture.Snapshot()["wal_drains"][0].Tags,
		"trigger", "the drain replay asked for is tagged as its own")
	require.Equal(t, walmetrics.TriggerReplay,
		second.capture.Snapshot()["wal_drains"][0].Tags["trigger"])
}

// TestAReadWaitsForTheReplayItTriggered: the loop answers reads, so a read that
// arrives before any write replays the tail first. Without the ordering the
// read is answered from a cold store the log is ahead of, and nothing says the
// answer is stale.
func TestAReadWaitsForTheReplayItTriggered(t *testing.T) {
	ctx := context.Background()
	log := memwal.New()

	first := takeShard(t, log, 7, nil)
	ns, wf, run := ids()
	require.NoError(t, first.c.write(ctx, mkCreate(ns, wf, run), coldRows()))
	first.c.Retire()

	second := takeShard(t, log, 8, nil)
	drainsWhenBaseWasRead := -1
	base := func(context.Context) (*p.InternalGetWorkflowExecutionResponse, error) {
		drainsWhenBaseWasRead = len(second.ap.drains)
		return nil, serviceerror.NewNotFound("workflow execution not found")
	}
	_, err := second.c.getWorkflowExecution(ctx,
		getExec(ns, wf, run), base)
	require.Error(t, err, "the run is in the cold store now, and this fixture's cold store is empty")

	require.Equal(t, 1, len(second.ap.drains), "the read replayed the tail")
	require.Equal(t, 1, drainsWhenBaseWasRead,
		"and the cold store was not asked until the tail had been applied to it")
	require.Equal(t, 1, second.c.Stats().Replayed)
}

// TestATaskReadWaitsForTheReplayItTriggered is the same claim for the merged
// task read, where the failure is worse than staleness: a page answered before
// an inherited tail is applied is short a key in neither source, and its caller
// completes the range it asked for. The task is lost, not late.
func TestATaskReadWaitsForTheReplayItTriggered(t *testing.T) {
	ctx := context.Background()
	log := memwal.New()

	first := takeShard(t, log, 7, nil)
	ns, wf, run := ids()
	require.NoError(t, first.c.write(ctx, mkTasks(ns, wf, run, 2,
		map[tasks.Category][]p.InternalHistoryTask{tasks.CategoryTransfer: {immediate(41)}}),
		rowsHolding(wf, run, 1)))
	first.c.Retire()

	second := takeShard(t, log, 8, nil)
	drainsWhenBaseWasRead := -1
	base := func(_ context.Context, _ *p.GetHistoryTasksRequest) (*p.InternalGetHistoryTasksResponse, error) {
		drainsWhenBaseWasRead = len(second.ap.drains)
		return &p.InternalGetHistoryTasksResponse{}, nil
	}
	_, err := second.c.getHistoryTasks(ctx, &p.GetHistoryTasksRequest{
		ShardID:             int32(testShard),
		TaskCategory:        tasks.CategoryTransfer,
		InclusiveMinTaskKey: tasks.NewImmediateKey(0),
		ExclusiveMaxTaskKey: tasks.NewImmediateKey(1000),
		BatchSize:           10,
	}, base)
	require.NoError(t, err)

	require.Equal(t, 1, second.c.Stats().Replayed)
	require.Equal(t, 1, drainsWhenBaseWasRead,
		"the cold store was asked only after the tail's task had been written to it")
}

// TestAProvisionalEntryWhoseConditionFailsIsDropped: sync mode acks before the
// condition is verified, so the entry stays in the log after its drain answered
// the caller "no". The new owner must neither apply it nor halt on it.
func TestAProvisionalEntryWhoseConditionFailsIsDropped(t *testing.T) {
	ctx := context.Background()
	log := memwal.New()

	first := takeShard(t, log, 7, func(c *Config) { c.Sync = true })
	first.ap.errs = []error{&p.WorkflowConditionFailedError{Msg: "stale"}}
	ns, wf, run := ids()
	require.Error(t, first.c.write(ctx, mkCreate(ns, wf, run), coldRows()),
		"the caller is told its precondition did not hold")
	require.Equal(t, StateRunning, first.c.State(), "which is not an incident")
	first.c.Retire()

	// The same assertion fails again. A read triggers the replay so that what
	// it leaves behind can be inspected with nothing else in the tail.
	second := takeShard(t, log, 8, nil)
	second.ap.errs = []error{&p.WorkflowConditionFailedError{Msg: "stale"}}
	_, err := second.c.getCurrentExecution(ctx,
		getCurrent(ns, wf),
		func(context.Context) (*p.InternalGetCurrentExecutionResponse, error) {
			return nil, serviceerror.NewNotFound("no current execution")
		})
	require.Error(t, err, "the cold store of this fixture is empty, and the entry was dropped")

	s := second.c.Stats()
	require.Equal(t, StateRunning, s.State, "a dropped entry is not an incident")
	require.Equal(t, 1, s.Replayed)
	require.Equal(t, 1, s.Dropped)
	require.Zero(t, s.TailEntries, "the dropped entry is settled, not held")
	require.EqualValues(t, 0, s.AppliedSeqno, "and nothing committed, so no watermark moved")
	require.Equal(t, []int64{1}, second.recorded("wal_replay_dropped_entries"))

	ns2, wf2, run2 := ids()
	require.NoError(t, second.c.write(ctx, mkCreate(ns2, wf2, run2), coldRows()),
		"the new owner takes writes: the failure it replayed was somebody's answer")
}

// TestAVerifiedEntryWhoseConditionFailsHalts is the half that makes the drop
// above safe: an entry whose condition was verified before the ack cannot
// legitimately fail, so a failure is a divergence and the shard halts on it. Do
// not widen the drop to every condition failure at replay.
func TestAVerifiedEntryWhoseConditionFailsHalts(t *testing.T) {
	ctx := context.Background()
	log := memwal.New()

	first := takeShard(t, log, 7, nil) // windowed: the ack is the answer
	ns, wf, run := ids()
	require.NoError(t, first.c.write(ctx, mkCreate(ns, wf, run), coldRows()))
	first.c.Retire()

	second := takeShard(t, log, 8, nil)
	second.ap.errs = []error{&p.WorkflowConditionFailedError{Msg: "stale"}}
	ns2, wf2, run2 := ids()
	err := second.c.write(ctx, mkCreate(ns2, wf2, run2), coldRows())

	require.Error(t, err)
	require.Equal(t, StateHaltedInvariant, second.c.State())
	require.Equal(t, 0, second.c.Stats().Dropped)
}

// TestTheProvisionalBitIsWrittenWhereTheDrainAnswersTheCaller is the writer's
// half: the bit marks entries whose condition had not been verified when they
// became durable, which is sync mode's writes and nothing else. It is read back
// off the log, since bytes are what replay will see.
func TestTheProvisionalBitIsWrittenWhereTheDrainAnswersTheCaller(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		mode string
		sync bool
		want bool
	}{
		{"windowed: the ack is the answer, so the condition was verified first", false, false},
		{"sync: the drain is the answer, so the ack ran ahead of the condition", true, true},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			log := memwal.New()
			o := takeShard(t, log, 7, func(c *Config) { c.Sync = tc.sync })
			ns, wf, run := ids()
			require.NoError(t, o.c.write(ctx, mkCreate(ns, wf, run), coldRows()))

			entries, err := log.ReadFrom(ctx, testShard, wal.FirstSeqno, 4)
			require.NoError(t, err)
			require.Len(t, entries, 1)
			_, provisional, err := mutation.DecodeEntry(entries[0].Payload, testRegistry())
			require.NoError(t, err)
			require.Equal(t, tc.want, provisional)
		})
	}
}

// TestAProvisionalEntryIsCarriedAlone: a drain's failure is all-or-nothing, so
// a provisional entry may not share a batch — the window in front of it is
// drained first and the rest after. Otherwise a drop would discard entries
// whose callers were never told anything.
//
// The log is written by hand so that one replay sees a mixed tail; a cycle
// writing the middle entry would have replayed the two before it first.
func TestAProvisionalEntryIsCarriedAlone(t *testing.T) {
	ctx := context.Background()
	log := memwal.New()
	require.NoError(t, log.Fence(ctx, testShard, 7))

	for i := range wal.Seqno(5) {
		ns, wf, run := ids()
		encode := mutation.Encode
		if i == 2 {
			encode = mutation.EncodeProvisional
		}
		payload, err := encode(mkCreate(ns, wf, run))
		require.NoError(t, err)
		require.NoError(t, log.Append(ctx, testShard, 7, wal.FirstSeqno+i, payload))
	}

	last := takeShard(t, log, 8, nil)
	last.ap.errs = []error{nil, &p.WorkflowConditionFailedError{Msg: "stale"}}
	ns, wf, run := ids()
	require.NoError(t, last.c.write(ctx, mkCreate(ns, wf, run), coldRows()),
		"a write triggers the replay; the new owner's own entry heads a fresh window")

	s := last.c.Stats()
	require.Equal(t, 5, s.Replayed)
	require.Equal(t, 1, s.Dropped)
	require.Equal(t, StateRunning, s.State)
	require.Len(t, last.ap.drains, 3,
		"the two before it, the provisional one alone, and the two after it")
	require.Len(t, last.ap.drains[0], 2)
	require.Len(t, last.ap.drains[1], 1, "the provisional entry shares its batch with nothing")
	require.Len(t, last.ap.drains[2], 2)
	require.EqualValues(t, []wal.Seqno{2, 3, 5}, last.ap.seqnos)
	require.EqualValues(t, 5, s.AppliedSeqno,
		"the watermark is the last committed drain's, past the dropped entry")
}

// TestReplayRefusesATailWrittenAboveItsEpoch: entries above this cycle's epoch
// mean somebody fenced the log after we did, so this cycle is the zombie. It
// halts lost before a row is written, rather than leaving it to the apply
// transaction's epoch CAS: the fence and the rangeID move are not atomic, so
// the CAS could still succeed.
func TestReplayRefusesATailWrittenAboveItsEpoch(t *testing.T) {
	ctx := context.Background()
	log := memwal.New()

	current := takeShard(t, log, 9, nil)
	ns, wf, run := ids()
	require.NoError(t, current.c.write(ctx, mkCreate(ns, wf, run), coldRows()))

	// A stale acquire: this process still believes it holds the shard at 8.
	stale := &fakeApplier{}
	zombie := New(testShard, 8, Deps{
		Log:       log,
		Writer:    stale,
		Recoverer: appliedWatermark{stale},
		Registry:  testRegistry(),
	}, Fixed(Defaults()))
	defer zombie.Retire()

	ns2, wf2, run2 := ids()
	err := zombie.write(ctx, mkCreate(ns2, wf2, run2), coldRows())
	require.Error(t, err)
	require.Equal(t, StateHaltedLost, zombie.State())
	require.Empty(t, stale.drains, "nothing of somebody else's log was applied")
	// The phrase is spelled out here rather than taken from [FencedAway]: an
	// assertion against the constant would let the words change under both at
	// once.
	require.Contains(t, err.Error(), "the shard has been fenced away")
	require.Equal(t, "the shard has been fenced away", FencedAway,
		"FencedAway is an operator-facing cause the handbook's shard-lifecycle "+
			"and operations chapters name verbatim, so it is not free to change")
}

// TestAFailedTailReadIsRetriedFromTheWatermark: a failed page read leaves the
// cycle unstarted and nothing half-folded, so the next request replays the
// whole tail exactly once.
func TestAFailedTailReadIsRetriedFromTheWatermark(t *testing.T) {
	ctx := context.Background()
	log := newLog()

	first := takeShard(t, log, 7, nil)
	ns, wf, run := ids()
	require.NoError(t, first.c.write(ctx, mkCreate(ns, wf, run), coldRows()))
	require.NoError(t, first.c.write(ctx, mkUpdate(ns, wf, run, 2), coldRows()))
	first.c.Retire()

	log.OnRead(waltest.Once(errUnreachable))
	second := takeShard(t, log, 8, nil)
	ns2, wf2, run2 := ids()
	require.ErrorContains(t, second.c.write(ctx, mkCreate(ns2, wf2, run2), coldRows()), "the WAL is unreachable")
	require.Equal(t, StateRunning, second.c.State(), "a blip is not a lost shard")
	require.Empty(t, second.ap.drains)

	require.NoError(t, second.c.write(ctx, mkCreate(ns2, wf2, run2), coldRows()))
	require.Equal(t, 2, second.c.Stats().Replayed, "each entry folded once, not twice")
	require.Len(t, second.ap.drains, 1)
	require.Len(t, second.ap.drains[0], 1, "two mutations of one workflow, collapsed once")
}

// TestReplayCutsItsTransactionsWhereTheWatermarksSay: a replayed transaction is
// the size of an ordinary one. The tail may be at I10's bound, and one
// transaction of that size is one no steady-state run ever executes.
func TestReplayCutsItsTransactionsWhereTheWatermarksSay(t *testing.T) {
	ctx := context.Background()
	log := memwal.New()

	first := takeShard(t, log, 7, nil)
	for range 5 {
		ns, wf, run := ids()
		require.NoError(t, first.c.write(ctx, mkCreate(ns, wf, run), coldRows()))
	}
	first.c.Retire()

	second := takeShard(t, log, 8, func(c *Config) { c.Mutations = 2 })
	ns, wf, run := ids()
	require.NoError(t, second.c.write(ctx, mkCreate(ns, wf, run), coldRows()))

	require.Equal(t, 5, second.c.Stats().Replayed)
	require.Len(t, second.ap.drains, 3, "2 + 2 + the final drain of the remaining 1")
	require.EqualValues(t, []wal.Seqno{2, 4, 5}, second.ap.seqnos)
}

// TestACycleWithNoRegistryRefuses: a nil registry may not be read as "do not
// recover". [NewManager] refuses to build such a node, and a cycle constructed
// directly refuses every request.
func TestACycleWithNoRegistryRefuses(t *testing.T) {
	ctx := context.Background()
	ap := &fakeApplier{}
	c := New(testShard, 7, Deps{Log: memwal.New(), Writer: ap, Recoverer: appliedWatermark{ap}}, Fixed(Defaults()))
	defer c.Retire()

	ns, wf, run := ids()
	require.ErrorContains(t, c.write(ctx, mkCreate(ns, wf, run), coldRows()), "task category registry")

	_, err := NewManager(Deps{Log: memwal.New(), Writer: ap, Recoverer: appliedWatermark{ap}}, Fixed(Defaults()))
	require.ErrorIs(t, err, ErrNoRegistry)
}

// replayEnv is a successor cycle over a log a previous owner left, with the
// applier and the watermark as fakes so a drain's outcome can be made
// unreadable — which is the one way a replay is abandoned with entries already
// acked, the cycle still running, and a retry to come.
type replayEnv struct {
	c    *Cycle
	ap   *fakeApplier
	mark *fakeWatermark
}

func inheritShard(t *testing.T, log wal.Log, epoch wal.Epoch, mark *fakeWatermark, shape func(*Config)) *replayEnv {
	t.Helper()
	ap := &fakeApplier{}
	cfg := Defaults()
	cfg.Sync = false
	if shape != nil {
		shape(&cfg)
	}
	c := standUp(t, epoch, Deps{
		Log:       log,
		Writer:    ap,
		Recoverer: mark,
		Metrics:   walmetrics.New(metricstest.NewCaptureHandler()),
	}, cfg)
	return &replayEnv{c: c, ap: ap, mark: mark}
}

// TestAnAbandonedReplayHoldsNoneOfWhatItAcked: a replay whose drain leaves an
// outcome nobody can read is abandoned with its entries acked, and the retry
// re-reads every one of them from the watermark. The bytes those acks put in
// the tail are what I10 refuses writes on, so a tail still holding them after
// the abandonment counts one incident's memory once per attempt.
func TestAnAbandonedReplayHoldsNoneOfWhatItAcked(t *testing.T) {
	ctx := context.Background()
	log := memwal.New()

	first := takeShard(t, log, 7, nil)
	ns, wf, run := ids()
	require.NoError(t, first.c.write(ctx, mkCreate(ns, wf, run), coldRows()))
	require.NoError(t, first.c.write(ctx, mkUpdate(ns, wf, run, 2), coldRows()))
	require.Empty(t, first.ap.drains, "the window is 256: nothing has been applied")
	first.c.Retire()

	// The floor read answers "no watermark", so the successor replays both
	// entries; its drain then fails and the readback after it fails too, which
	// is the outcome that leaves the cycle running and the entries in the tail.
	second := inheritShard(t, log, 8, &fakeWatermark{answers: []wmAnswer{
		{}, {err: context.DeadlineExceeded},
	}}, nil)
	second.ap.errs = []error{context.DeadlineExceeded}

	ns2, wf2, run2 := ids()
	require.Error(t, second.c.write(ctx, mkCreate(ns2, wf2, run2), coldRows()))
	require.Equal(t, StateRunning, second.c.State(), "a read that failed is not an answer, and not a halt")

	st := second.c.Stats()
	require.Zero(t, st.TailBytes, "the abandoned attempt's acked bytes are still in I10's counter")
	require.Zero(t, st.TailEntries)
	require.Zero(t, st.Replayed)
	require.Zero(t, st.Kinds[mutation.KindCreate])
	require.Zero(t, st.Kinds[mutation.KindUpdate])

	// The drain had in fact not committed, so the retry finds the same two
	// entries above the same watermark and replays them — once.
	require.NoError(t, second.c.write(ctx, mkCreate(ns2, wf2, run2), coldRows()))
	st = second.c.Stats()
	require.Equal(t, 2, st.Replayed, "the inherited tail, replayed once and not once per attempt")
	require.Equal(t, 2, st.Kinds[mutation.KindCreate], "the replayed create and this caller's own")
	require.Equal(t, 1, st.Kinds[mutation.KindUpdate])
}

// TestAnAbandonedReplayDoesNotBoundTheShardOutOfWrites: the same leak read as
// the incident it causes. Bytes an abandoned attempt left behind are bytes
// nobody holds, so a bound tight enough to notice them refuses every write on a
// shard that is working — I10 turned on the node it protects.
func TestAnAbandonedReplayDoesNotBoundTheShardOutOfWrites(t *testing.T) {
	ctx := context.Background()
	log := memwal.New()

	first := takeShard(t, log, 7, nil)
	ns, wf, run := ids()
	require.NoError(t, first.c.write(ctx, mkCreate(ns, wf, run), coldRows()))
	first.c.Retire()

	// A bound of exactly one entry: empty, the tail is under it and the retry
	// proceeds; still holding the abandoned attempt's ack, it is at it, and
	// [Cycle.write] refuses off the mirror before the retry is ever reached.
	size := len(mustEncode(t, mkCreate(ids())))
	second := inheritShard(t, log, 8, &fakeWatermark{answers: []wmAnswer{
		{}, {err: context.DeadlineExceeded},
	}}, func(c *Config) { c.HardMaxBytes = size })
	second.ap.errs = []error{context.DeadlineExceeded}

	ns2, wf2, run2 := ids()
	require.Error(t, second.c.write(ctx, mkCreate(ns2, wf2, run2), coldRows()))
	require.Equal(t, StateRunning, second.c.State())

	require.NoError(t, second.c.write(ctx, mkCreate(ns2, wf2, run2), coldRows()),
		"the retry met a bound still holding the abandoned attempt's bytes")
}

// errUnreachable is the log failing under a request rather than refusing it:
// an error no caller may read as an answer.
var errUnreachable = errors.New("the WAL is unreachable")

func mustEncode(t *testing.T, m mutation.Mutation) []byte {
	t.Helper()
	payload, err := mutation.Encode(m)
	require.NoError(t, err)
	return payload
}
