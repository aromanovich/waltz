package cycle

// What the three reads answer when the replay they triggered halts the cycle.
// Replay is the only road to [StateHaltedLost] that arrives with the cycle
// unstarted, on a reader, with the halt discovered inside the call being
// answered.
//
// Two halves of the rule, both stated beside [Cycle.readHalted]: on
// halted-lost the state alone decides a task read, because another owner's acks
// are in neither this tail nor (yet) the cold store; the mutable-state reads
// keep the tail rule, because they have callers that legitimately do not own
// the shard. These also pin that the read which discovers the halt is answered
// exactly like every read after it — see [Cycle.startForRead].

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"

	"github.com/aromanovich/waltz/internal/verify/coldtasks"
	"github.com/aromanovich/waltz/internal/verify/mutbuild"
	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/wal"
	"github.com/aromanovich/waltz/wal/memwal"
	"github.com/aromanovich/waltz/wal/waltest"
)

// readAnswers is what each of a cycle's three reads answered.
type readAnswers struct{ exec, current, tasks error }

// askAllThree issues both mutable-state reads and a task read over one cycle.
func askAllThree(t *testing.T, c *Cycle, cold *coldtasks.Store) readAnswers {
	t.Helper()
	ctx := context.Background()
	ns, wf, run := ids()
	minKey, maxKey := immediateRange()

	_, exec := c.getWorkflowExecution(ctx, getExec(ns, wf, run),
		func(context.Context) (*p.InternalGetWorkflowExecutionResponse, error) {
			return &p.InternalGetWorkflowExecutionResponse{}, nil
		})
	_, current := c.getCurrentExecution(ctx, getCurrent(ns, wf),
		func(context.Context) (*p.InternalGetCurrentExecutionResponse, error) {
			return &p.InternalGetCurrentExecutionResponse{}, nil
		})
	_, taskErr := c.getHistoryTasks(ctx, taskReq(tasks.CategoryTransfer, minKey, maxKey, 100), cold.Read)
	return readAnswers{exec: exec, current: current, tasks: taskErr}
}

// zombie is a cycle at an epoch the log has already been fenced past: this
// node still believes it holds the shard, and its replay is where it finds out.
func zombie(t *testing.T, log wal.Log, epoch wal.Epoch) *Cycle {
	t.Helper()
	ap := &fakeApplier{}
	deps := testDeps(log, ap)
	deps.Recoverer = appliedWatermark{ap}
	c := New(testShard, epoch, deps, Fixed(Defaults()))
	t.Cleanup(func() { c.Retire() })
	return c
}

// TestACycleFencedAwayInsideReplayRefusesTaskReadsAndOnlyThose is the
// empty-tail case, which is also the shape sync mode is in at every call
// boundary. The zombie applied everything it acked, so "everything acked is in
// the cold store" is true of this cycle and false of the shard: a task page
// built on it is short the rows the new owner has not drained, and its reader
// completes the range and acks past them.
func TestACycleFencedAwayInsideReplayRefusesTaskReadsAndOnlyThose(t *testing.T) {
	ctx := context.Background()
	log := memwal.New()

	// The real owner, at 9, holding an unapplied entry.
	current := takeShard(t, log, 9, nil)
	ns, wf, run := ids()
	require.NoError(t, current.c.write(ctx, mkCreate(ns, wf, run), coldRows()))

	c := zombie(t, log, 8)
	cold := coldtasks.New()
	cold.Hold(tasks.CategoryTransfer, immediate(10))

	got := askAllThree(t, c, cold)
	require.Equal(t, StateHaltedLost, c.State(), "the replay found the fence")
	require.NoError(t, got.exec, "a mutable-state read keeps the tail rule and this tail is empty")
	require.NoError(t, got.current)
	require.IsType(t, &p.ShardOwnershipLostError{}, got.tasks,
		"a task page on a shard this node no longer owns must be refused, got %v", got.tasks)

	// Every read after the one that discovered the halt answers the same way: a
	// first reader given the replay's own error would hand back a value nothing
	// at the store boundary type-switches on.
	again := askAllThree(t, c, cold)
	require.NoError(t, again.exec)
	require.NoError(t, again.current)
	require.IsType(t, &p.ShardOwnershipLostError{}, again.tasks)
	require.IsType(t, got.tasks, again.tasks,
		"the read that discovered the fence and the read after it are owed the same answer")
}

// TestATaskReadThatIsItselfTheFirstRequestIsRefused makes the readiness gate's
// placement before the halt rule load-bearing: here the task read is the
// request that triggers the replay. A halt rule consulted first would see a
// running cycle, say nothing, and merge the page out of a window the replay has
// since reset — a cold-store-only page on a shard this node no longer owns,
// handed to the caller that completes the range over it.
func TestATaskReadThatIsItselfTheFirstRequestIsRefused(t *testing.T) {
	ctx := context.Background()
	log := memwal.New()

	current := takeShard(t, log, 9, nil)
	ns, wf, run := ids()
	require.NoError(t, current.c.write(ctx, mkCreate(ns, wf, run), coldRows()))

	c := zombie(t, log, 8)
	cold := coldtasks.New()
	cold.Hold(tasks.CategoryTransfer, immediate(10))

	minKey, maxKey := immediateRange()
	_, err := c.getHistoryTasks(ctx, taskReq(tasks.CategoryTransfer, minKey, maxKey, 100), cold.Read)
	require.IsType(t, &p.ShardOwnershipLostError{}, err,
		"the read that discovered the fence is owed the refusal too, got %v", err)
	require.Equal(t, StateHaltedLost, c.State())
	require.Zero(t, cold.Calls, "and a refused page must not reach the store below either")
}

// TestACycleFencedAwayHoldingATailRefusesAllThree is the other tail state: the
// zombie acked entries of its own and never applied them, so the layer knows
// the cold store is incomplete and cannot say by what. The mutable-state reads
// are refused by the tail rule rather than by the state.
func TestACycleFencedAwayHoldingATailRefusesAllThree(t *testing.T) {
	ctx := context.Background()
	log := memwal.New()

	ns, wf, run := ids()
	mine := takeShard(t, log, 8, nil)
	require.NoError(t, mine.c.write(ctx, mkCreate(ns, wf, run), coldRows()))
	mine.c.Retire() // acked at 8, never applied

	current := takeShard(t, log, 9, nil)
	ns2, wf2, run2 := ids()
	require.NoError(t, current.c.write(ctx, mkCreate(ns2, wf2, run2), coldRows()))

	c := zombie(t, log, 8)
	got := askAllThree(t, c, coldtasks.New())
	require.Equal(t, StateHaltedLost, c.State())
	require.IsType(t, &p.ShardOwnershipLostError{}, got.exec, "got %v", got.exec)
	require.IsType(t, &p.ShardOwnershipLostError{}, got.current, "got %v", got.current)
	require.IsType(t, &p.ShardOwnershipLostError{}, got.tasks, "got %v", got.tasks)
}

// TestACycleHaltedInvariantInsideReplayKeepsTheTailRuleForAllThree is why
// [Cycle.readHalted] looks at halted-lost specifically rather than at "is it
// halted": the tail rule alone applies here, and converting this to
// ShardOwnershipLost would hand the divergence on as an ordinary failover.
//
// The tail is what makes that rule safe, and an entry this build cannot decode
// is charged to it by [Cycle.strand] before the halt. Without that charge the
// tail reads empty — the entry never reached [Cycle.accept] — and an empty tail
// is exactly what [tailRoute] passes through, so all three reads would be
// answered from a cold store that does not hold this entry. For the task read
// that is loss rather than staleness: the queue completes the range it asked
// for and acks past keys the entry carries, and no owner running this build can
// ever decode it to write them.
func TestACycleHaltedInvariantInsideReplayKeepsTheTailRuleForAllThree(t *testing.T) {
	ctx := context.Background()
	log := memwal.New()
	require.NoError(t, log.Fence(ctx, testShard, 7))
	// A payload no codec can decode, at this cycle's own epoch.
	require.NoError(t, log.Append(ctx, testShard, 7, wal.FirstSeqno, []byte{0xff, 0xff}))

	c := zombie(t, log, 7)
	cold := coldtasks.New()
	cold.Hold(tasks.CategoryTransfer, immediate(10))

	got := askAllThree(t, c, cold)
	require.Equal(t, StateHaltedInvariant, c.State())
	require.ErrorIs(t, got.exec, ErrHalted)
	require.ErrorIs(t, got.current, ErrHalted)
	require.ErrorIs(t, got.tasks, ErrHalted)
	require.Zero(t, cold.Calls,
		"an entry acked and undecodable is an incomplete cold store, so nothing may be served from it")
}

// TestAReplayThatFailedWithoutHaltingIsStillAnError bounds
// [Cycle.startForRead]'s swallow: a replay that could not read its page leaves
// the cycle running and the window empty, so there is no halt rule to answer
// with and the read must fail rather than be served from a window that was
// never rebuilt.
func TestAReplayThatFailedWithoutHaltingIsStillAnError(t *testing.T) {
	ctx := context.Background()
	log := newLog()

	first := takeShard(t, log, 7, nil)
	ns, wf, run := ids()
	require.NoError(t, first.c.write(ctx, mkCreate(ns, wf, run), coldRows()))
	first.c.Retire()

	log.OnRead(waltest.Once(errUnreachable))
	c := zombie(t, log, 8)
	got := askAllThree(t, c, coldtasks.New())

	require.Error(t, got.exec, "a read whose replay failed is not an answer")
	require.Equal(t, StateRunning, c.State(), "and the cycle is not halted, so it retries")
	require.NotErrorIs(t, got.exec, ErrHalted)
	// The next request replays from the watermark and the cycle comes up.
	require.NoError(t, askAllThree(t, c, coldtasks.New()).exec)
}

// TestAnUndecodableEntryLeavesNoQueueAbleToAckPastIt is the same rule staged the
// way a deployment meets it, and with an entry that carries real work: the node
// that wrote seqno 1 had archival configured and this one does not, so the entry
// decodes to ErrUnknownCategory here. It also carries a transfer task, which
// this node's transfer queue does read.
//
// Before [Cycle.strand] existed the tail read empty and this page came back
// served, empty and short task 42 — the queue would have completed its range and
// acked past a key nothing will ever write.
func TestAnUndecodableEntryLeavesNoQueueAbleToAckPastIt(t *testing.T) {
	ctx := context.Background()
	log := memwal.New()
	require.NoError(t, log.Fence(ctx, testShard, 7))

	ns, wf, run := ids()
	payload, err := mutation.Encode(build.Update(ns, wf, run, 2,
		mutbuild.WithTaskMap(map[tasks.Category][]p.InternalHistoryTask{
			tasks.CategoryTransfer: {immediate(42)},
			tasks.CategoryArchival: {immediate(43)},
		})))
	require.NoError(t, err)
	require.NoError(t, log.Append(ctx, testShard, 7, wal.FirstSeqno, payload))

	// zombie's Deps.Registry is the default one: no archival.
	c := zombie(t, log, 7)
	cold := coldtasks.New()

	minKey, maxKey := immediateRange()
	_, readErr := c.getHistoryTasks(ctx, taskReq(tasks.CategoryTransfer, minKey, maxKey, 100), cold.Read)

	require.Equal(t, StateHaltedInvariant, c.State())
	require.ErrorIs(t, readErr, ErrHalted, "the page would have been short the acked transfer task")
	require.ErrorContains(t, readErr, "unknown task category",
		"and the cause names what this build could not read")
	require.Zero(t, cold.Calls)
}
