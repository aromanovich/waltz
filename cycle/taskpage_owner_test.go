package cycle

// Who may answer a task page: the shard's current cycle and nothing else. The
// rule is [Manager.taskPage]'s and cannot live on a Cycle, since being
// superseded is a fact about the registry's map that no cycle can read. The
// window has two halves — an acquire landing before the resolved cycle is
// called, and one landing while it answers.
//
// The harm arrives with no fence and nothing halted: a page built from a window
// that is no longer the shard's is short whatever landed on the successor, and
// its one caller completes the range it read and acks past the gap.

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"

	"github.com/aromanovich/waltz/baserow"
	"github.com/aromanovich/waltz/internal/verify/coldtasks"
	"github.com/aromanovich/waltz/mutation"
)

// windowed is a cycle that holds its tasks rather than draining each one, so
// that "which cycle's window" has two different answers.
func windowed(c *Config) {
	c.Sync = false
	c.Mutations = 1 << 20
	c.Bytes = 1 << 30
}

// installed spins until the registry answers with a cycle other than was, and
// reports whether it ever did. It runs inside the base callback, which the loop
// goroutine calls, so it may not fail the test itself: an unwind there kills the
// loop the caller is waiting on. And it is bounded because ShardAcquired has
// three early returns that install nothing — a fenced log, a closed registry, a
// refused epoch — each of which an unbounded spin turns into go test's own
// timeout at 100% CPU, with the acquire's answer unread in its channel.
func installed(m *Manager, was *Cycle) bool {
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); runtime.Gosched() {
		if m.Shard(testShard) != was {
			return true
		}
	}
	return false
}

// transferTask is one transfer task riding an update of a fresh workflow, with
// the two cold-store reads that update asserts on: a pre-window row at 1 and a
// current row naming the run.
func transferTask(id int64) (mutation.Mutation, *baserow.Rows) {
	ns, wf, run := ids()
	m := mkTasks(ns, wf, run, 2, map[tasks.Category][]p.InternalHistoryTask{
		tasks.CategoryTransfer: {immediate(id)},
	})
	return m, rowsHolding(wf, run, 1)
}

func pageIDs(resp *p.InternalGetHistoryTasksResponse) []int64 {
	out := make([]int64, 0, len(resp.Tasks))
	for _, task := range resp.Tasks {
		out = append(out, task.Key.TaskID)
	}
	return out
}

// TestAPageFromACycleSupersededBeforeTheCallIsRebuiltByItsSuccessor: resolving
// the cycle and calling it are two statements, and an acquire fits between
// them. Driven through [Manager.taskPage] with the resolve already made, which
// is the state that interleaving leaves a caller in; the exported method is
// that call with m.Shard(shard) for its third argument.
//
// The stale cycle's tail is empty on purpose: [Cycle.stoppedRead] already
// refuses a retired cycle holding one, and an empty tail is sync mode's shape
// at every call boundary. Task 90 acks into the fresh cycle.
func TestAPageFromACycleSupersededBeforeTheCallIsRebuiltByItsSuccessor(t *testing.T) {
	ctx := context.Background()
	m := newManager(t, &fakeApplier{}, windowed)
	require.NoError(t, m.ShardAcquired(ctx, testShard, 8))
	stale := m.Shard(testShard)

	// [Manager.ShardAcquired] installs the fresh cycle and only then retires this
	// one, which is what makes the window exist at all.
	require.NoError(t, m.ShardAcquired(ctx, testShard, 9))
	require.NotSame(t, stale, m.Shard(testShard), "the registry holds the fresh cycle")
	task, taskRows := transferTask(90)
	require.NoError(t, m.Shard(testShard).write(ctx, task, taskRows))

	minKey, maxKey := immediateRange()
	req := taskReq(tasks.CategoryTransfer, minKey, maxKey, 100)

	// The stale cycle asked on its own: a refusal, even though its own tail is
	// empty — that is true of this cycle and not of the shard.
	direct := coldtasks.New()
	direct.Hold(tasks.CategoryTransfer, immediate(10))
	_, err := stale.getHistoryTasks(ctx, req, direct.Read)
	require.IsType(t, &p.ShardOwnershipLostError{}, err,
		"a retired cycle cannot speak for a tail that is now another cycle's, got %v", err)
	require.Zero(t, direct.Calls, "and a refused page must not reach the store below either")

	// And the registry, handed that same stale cycle: the fresh cycle's page.
	cold := coldtasks.New()
	cold.Hold(tasks.CategoryTransfer, immediate(10))
	resp, err := m.taskPage(ctx, testShard, stale, req, cold.Read)
	require.NoError(t, err)
	require.Equal(t, []int64{10, 90}, pageIDs(resp),
		"the page must carry the successor's window, not the cold store alone")
	require.Equal(t, 1, cold.Calls, "the discarded attempt never reached the store")
}

// TestAnAcquireLandingWhileThePageIsBuiltRebuildsItToo is the second half of
// the window, and why the check is made after the answer as well as before it.
//
// The interleave is real: the acquire runs in its own goroutine and the cycle
// answering the page waits for the fresh cycle to be installed before returning
// from the store read. That wait terminates because [Manager.ShardAcquired]
// installs before it retires, and its retire is what waits on this goroutine.
// Task 90 lands in neither the stale cycle's window nor the cold store.
func TestAnAcquireLandingWhileThePageIsBuiltRebuildsItToo(t *testing.T) {
	ctx := context.Background()
	m := newManager(t, &fakeApplier{}, windowed)
	require.NoError(t, m.ShardAcquired(ctx, testShard, 8))

	answering := m.Shard(testShard)

	cold := coldtasks.New()
	cold.Hold(tasks.CategoryTransfer, immediate(10))

	var landed atomic.Bool
	acquired := make(chan error, 1)
	var wrote error
	var superseded bool
	base := func(ctx context.Context, req *p.GetHistoryTasksRequest) (*p.InternalGetHistoryTasksResponse, error) {
		if landed.CompareAndSwap(false, true) {
			go func() { acquired <- m.ShardAcquired(context.Background(), testShard, 9) }()
			superseded = installed(m, answering)
			// The write that makes the two windows differ: it goes to whichever
			// cycle the registry holds now, which is the fresh one.
			task, taskRows := transferTask(90)
			wrote = m.Shard(testShard).write(ctx, task, taskRows)
		}
		return cold.Read(ctx, req)
	}

	minKey, maxKey := immediateRange()
	resp, err := m.taskPage(ctx, testShard, answering,
		taskReq(tasks.CategoryTransfer, minKey, maxKey, 100), base)
	require.NoError(t, err)
	require.NoError(t, <-acquired)
	require.True(t, superseded, "the acquire installed no cycle, so the page was never rebuilt for one")
	require.NoError(t, wrote)
	require.Equal(t, []int64{10, 90}, pageIDs(resp),
		"the rebuilt page carries the successor's window: the ack that landed on it after the install")
	require.Equal(t, 2, cold.Calls, "the page really was built twice, and the first one discarded")
}

// TestAShardSupersededTwiceInOnePageIsDeclaredLost is the retry's bound: one
// rebuild, not a loop whose length a re-acquire storm sets. Two acquires inside
// one page read is a shard changing hands faster than a page can be built, and
// ShardOwnershipLost makes the shard's read path reload it.
func TestAShardSupersededTwiceInOnePageIsDeclaredLost(t *testing.T) {
	ctx := context.Background()
	m := newManager(t, &fakeApplier{}, windowed)
	require.NoError(t, m.ShardAcquired(ctx, testShard, 8))

	stale := m.Shard(testShard)
	task, taskRows := transferTask(70)
	require.NoError(t, stale.write(ctx, task, taskRows))
	require.NoError(t, m.ShardAcquired(ctx, testShard, 9)) // the first supersede

	cold := coldtasks.New()
	cold.Hold(tasks.CategoryTransfer, immediate(10))

	// The second lands while the retry is being answered, so the retry's answer
	// comes from a cycle that is no longer current either.
	second := m.Shard(testShard)
	var landed atomic.Bool
	acquired := make(chan error, 1)
	var superseded bool
	base := func(ctx context.Context, req *p.GetHistoryTasksRequest) (*p.InternalGetHistoryTasksResponse, error) {
		if landed.CompareAndSwap(false, true) {
			go func() { acquired <- m.ShardAcquired(context.Background(), testShard, 10) }()
			superseded = installed(m, second)
		}
		return cold.Read(ctx, req)
	}

	minKey, maxKey := immediateRange()
	_, err := m.taskPage(ctx, testShard, stale,
		taskReq(tasks.CategoryTransfer, minKey, maxKey, 100), base)
	require.NoError(t, <-acquired)
	require.True(t, superseded, "the second acquire installed no cycle, so this is one supersede and not two")
	require.IsType(t, &p.ShardOwnershipLostError{}, err,
		"unwrapped: the shard's read path matches this one concrete type and nothing else, got %v", err)
}

// TestAnAcquireDoesNotHoldTheRegistryWhileItAsksASupersededCycle: nothing may
// hold the registry's mutex across a cycle's loop, so [Manager.ShardAcquired]
// must collect the superseded cycle's counters outside it. Since [held] owns
// that mutex and hands it to nobody, this now fails only if a lock is
// reintroduced on Manager itself — it is the behavioural half of a rule the
// shape states.
//
// It is a lock inversion and not only a long hold: the read path resolves
// through [Manager.Shard], which wants that mutex, so a cycle parked in a base
// read that resolves again waits for the acquire while the acquire waits for
// it. Even with nothing parked, one cycle stuck inside an apply would stop
// every shard on the node from resolving, since they all come through this map.
// The test fails by timeout rather than by hanging the two above it.
func TestAnAcquireDoesNotHoldTheRegistryWhileItAsksASupersededCycle(t *testing.T) {
	ctx := context.Background()
	m := newManager(t, &fakeApplier{}, windowed)
	require.NoError(t, m.ShardAcquired(ctx, testShard, 8))

	busy := m.Shard(testShard)
	task, taskRows := transferTask(70)
	require.NoError(t, busy.write(ctx, task, taskRows))

	// The cycle's loop, parked inside a cold-store read it may not finish yet: a
	// slow store, which is the ordinary reason a loop is occupied.
	inRead, release := make(chan struct{}), make(chan struct{})
	cold := coldtasks.New()
	page := make(chan error, 1)
	minKey, maxKey := immediateRange()
	go func() {
		_, err := busy.getHistoryTasks(ctx, taskReq(tasks.CategoryTransfer, minKey, maxKey, 100),
			func(ctx context.Context, req *p.GetHistoryTasksRequest) (*p.InternalGetHistoryTasksResponse, error) {
				close(inRead)
				<-release
				return cold.Read(ctx, req)
			})
		page <- err
	}()
	<-inRead

	acquired := make(chan error, 1)
	go func() { acquired <- m.ShardAcquired(context.Background(), testShard, 9) }()

	// Resolve until the fresh cycle is installed, which puts the acquire at the
	// counter-collecting step, then resolve once more: that is the assertion.
	// Each resolve gets its own goroutine so a blocked one fails rather than
	// hangs.
	resolve := func(what string) *Cycle {
		t.Helper()
		got := make(chan *Cycle, 1)
		go func() { got <- m.Shard(testShard) }()
		select {
		case c := <-got:
			return c
		case <-time.After(15 * time.Second):
			t.Fatalf("%s: the acquire is holding the registry across the superseded cycle's loop", what)
			return nil
		}
	}
	for deadline := time.Now().Add(15 * time.Second); resolve("while the acquire runs") == busy; {
		require.False(t, time.Now().After(deadline),
			"the acquire never installed the fresh cycle, and the registry answered with the busy one throughout")
		runtime.Gosched()
	}
	require.NotNil(t, resolve("once the fresh cycle is installed"),
		"the registry must stay answerable while the acquire collects the superseded cycle's counters")

	close(release)
	require.NoError(t, <-acquired)
	require.NoError(t, <-page)
}

// TestAPageIsRefusedOnceTheNodeHasReleasedTheShard is the third exit: the
// registry can come back holding no cycle at all, which is a node shutting down
// ([Manager.Close] empties the map before it retires anything). The answer must
// be the one [Manager.GetHistoryTasks] gives for a shard this node never
// acquired, or one question has two answers.
func TestAPageIsRefusedOnceTheNodeHasReleasedTheShard(t *testing.T) {
	ctx := context.Background()
	m := newManager(t, &fakeApplier{}, windowed)
	require.NoError(t, m.ShardAcquired(ctx, testShard, 8))

	stale := m.Shard(testShard)
	m.Close(ctx)

	cold := coldtasks.New()
	cold.Hold(tasks.CategoryTransfer, immediate(10))
	minKey, maxKey := immediateRange()
	_, err := m.taskPage(ctx, testShard, stale,
		taskReq(tasks.CategoryTransfer, minKey, maxKey, 100), cold.Read)
	require.IsType(t, &p.ShardOwnershipLostError{}, err, "got %v", err)
	require.Zero(t, cold.Calls)
}
