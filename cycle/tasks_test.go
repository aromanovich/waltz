package cycle

// Merge-on-read from the cycle's side: the rules the merge has no freedom
// about, the cases a generated stream cannot produce, and what a shard
// this node does not own or no longer runs answers. The merge itself is
// [fold.Accumulator.TaskPage], tested in fold; what is here drives the
// whole route — loop, halt rule, counters and merge — since that is what a
// queue gets.

import (
	"context"
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"

	"github.com/aromanovich/waltz/apply"
	"github.com/aromanovich/waltz/internal/verify/coldtasks"
	"github.com/aromanovich/waltz/internal/verify/mutbuild"
	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/wal"
	"github.com/aromanovich/waltz/wal/waltest"
)

// immediateRange is the widest range a queue could ask for, in the shape
// validateTaskRange demands: an immediate category is bounded by task id.
func immediateRange() (tasks.Key, tasks.Key) {
	return tasks.NewImmediateKey(0), tasks.NewImmediateKey(math.MaxInt64)
}

func taskReq(category tasks.Category, minKey, maxKey tasks.Key, batch int) *p.GetHistoryTasksRequest {
	return &p.GetHistoryTasksRequest{
		ShardID:             int32(testShard),
		TaskCategory:        category,
		InclusiveMinTaskKey: minKey,
		ExclusiveMaxTaskKey: maxKey,
		BatchSize:           batch,
	}
}

// immediate and timer build a task with a key and a recognisable blob.
func immediate(id int64) p.InternalHistoryTask {
	return p.InternalHistoryTask{
		Key:  tasks.NewImmediateKey(id),
		Blob: &commonpb.DataBlob{Data: []byte{byte(id)}},
	}
}

func timer(fire time.Time, id int64) p.InternalHistoryTask {
	return p.InternalHistoryTask{
		Key:  tasks.NewKey(fire, id),
		Blob: &commonpb.DataBlob{Data: []byte{byte(id)}},
	}
}

// taskEnv is [newEnv] plus the workflow the fixtures here update: mkTasks rides
// an update at DBRecordVersion 2, which asserts a pre-window row at 1 and a
// current row naming the run, so the cold store holds both.
func taskEnv(t *testing.T, shape func(*Config)) *env {
	t.Helper()
	e := newEnv(t, shape)
	e.coldWorkflow("wf", "run", 1)
	return e
}

// mkTasks is a mutation carrying tasks, which is how they enter the window.
func mkTasks(ns, wf, run string, version int64, byCategory map[tasks.Category][]p.InternalHistoryTask) mutation.Mutation {
	return build.Update(ns, wf, run, version, mutbuild.WithTaskMap(byCategory))
}

// paginate drives one merged read to exhaustion the way
// collection.PagingIterator drives the store, and reports the pages so a test
// can assert on their shape as well as on their content.
func paginate(
	t *testing.T, c *Cycle, base BaseTasks, req *p.GetHistoryTasksRequest,
) [][]p.InternalHistoryTask {
	t.Helper()
	var pages [][]p.InternalHistoryTask
	ask := *req
	for range 1000 {
		resp, err := c.getHistoryTasks(context.Background(), &ask, base)
		require.NoError(t, err)
		require.LessOrEqual(t, len(resp.Tasks), max(req.BatchSize, 1),
			"a page longer than the caller's BatchSize: ExecutionMutableStateTaskSuite asserts this one itself")
		pages = append(pages, resp.Tasks)
		if len(resp.NextPageToken) == 0 {
			return pages
		}
		ask.NextPageToken = resp.NextPageToken
	}
	t.Fatal("the pagination did not terminate in 1000 pages")
	return nil
}

func taskIDs(pages [][]p.InternalHistoryTask) []int64 {
	var out []int64
	for _, page := range pages {
		for _, task := range page {
			out = append(out, task.Key.TaskID)
		}
	}
	return out
}

// TestATaskInTheWindowComesBackFromTheRangeThatWouldHaveFiredIt: notification
// carries nothing — both queues discard the payload and re-read persistence —
// so a reader that asked the cold store alone would find an empty range here,
// complete it, and ack past a key it never saw.
func TestATaskInTheWindowComesBackFromTheRangeThatWouldHaveFiredIt(t *testing.T) {
	e := taskEnv(t, func(c *Config) { c.Mutations = 1 << 20 })
	cold := coldtasks.New()
	cold.Hold(tasks.CategoryTransfer, immediate(10))

	require.NoError(t, e.add(t, mkTasks("ns", "wf", "run", 2, map[tasks.Category][]p.InternalHistoryTask{
		tasks.CategoryTransfer: {immediate(20)},
	})))

	minKey, maxKey := immediateRange()
	pages := paginate(t, e.c, cold.Read, taskReq(tasks.CategoryTransfer, minKey, maxKey, 100))
	require.Equal(t, []int64{10, 20}, taskIDs(pages))

	stats := e.c.Stats()
	require.Equal(t, 1, stats.TaskReads)
	require.Equal(t, 1, stats.TaskReadsMerged, "the window contributed a task and no page said so")
	require.Zero(t, stats.TaskCollisions)
}

// TestAnAddHistoryTasksRowDoesNotJumpTheQueue: AddHistoryTasks transits straight
// into the cold store while the mutation beside it waits in the window, and a
// shard hands out task ids before the write — so the cold store can hold a
// higher id than the window does. What keeps that honest is the merge's
// ordering, not its dedup: emitting the base's page first would hand the queue
// a descending pair, which queues/iterator.go silently skips.
func TestAnAddHistoryTasksRowDoesNotJumpTheQueue(t *testing.T) {
	e := taskEnv(t, func(c *Config) { c.Mutations = 1 << 20 })
	cold := coldtasks.New()
	cold.Hold(tasks.CategoryTransfer, immediate(200)) // already applied, higher id

	require.NoError(t, e.add(t, mkTasks("ns", "wf", "run", 2, map[tasks.Category][]p.InternalHistoryTask{
		tasks.CategoryTransfer: {immediate(100)}, // still in the window, lower id
	})))

	minKey, maxKey := immediateRange()
	pages := paginate(t, e.c, cold.Read, taskReq(tasks.CategoryTransfer, minKey, maxKey, 100))
	require.Equal(t, []int64{100, 200}, taskIDs(pages), "the merged read must order across the two sources")
}

// TestAnImmediateRangeIsComparedOnTaskIDAlone: one comparator serves both
// category types only because every immediate-category task's key is
// (tasks.DefaultFireTime, id) — but a request need not be. validateTaskRange
// (execution_manager.go:1163) accepts an immediate range whose fire times are
// the zero time, and compared whole, every window task would sit above such a
// range's maximum and the read would answer with the base's rows alone.
func TestAnImmediateRangeIsComparedOnTaskIDAlone(t *testing.T) {
	e := taskEnv(t, func(c *Config) { c.Mutations = 1 << 20 })
	cold := coldtasks.New()
	cold.Hold(tasks.CategoryTransfer, immediate(10))

	require.NoError(t, e.add(t, mkTasks("ns", "wf", "run", 2, map[tasks.Category][]p.InternalHistoryTask{
		tasks.CategoryTransfer: {immediate(5), immediate(20), immediate(40)},
	})))

	// A range with no fire time at all: what validateTaskRange permits, and what
	// tasks.Key's zero value is.
	pages := paginate(t, e.c, cold.Read,
		taskReq(tasks.CategoryTransfer, tasks.Key{TaskID: 6}, tasks.Key{TaskID: 30}, 100))
	require.Equal(t, []int64{10, 20}, taskIDs(pages),
		"the window's tasks must be filtered by task id, and 5 and 40 are outside [6, 30)")
}

// TestALookAheadSeesATailOnlyTimer: scheduledQueue.lookAheadTask asks for
// BatchSize 1 over [nonReadable.FireTime, +MaxPollInterval) and sets its timer
// gate from the first key it gets back (queue_scheduled.go:246-270). A timer
// living only in the window has to be that answer whenever it is the earliest,
// or the gate is set past its fire time and it fires late.
func TestALookAheadSeesATailOnlyTimer(t *testing.T) {
	e := taskEnv(t, func(c *Config) { c.Mutations = 1 << 20 })
	base := coldtasks.UnixNano(0).Add(9 * time.Hour)
	cold := coldtasks.New()
	cold.Hold(tasks.CategoryTimer, timer(base.Add(10*time.Minute), 70))

	require.NoError(t, e.add(t, mkTasks("ns", "wf", "run", 2, map[tasks.Category][]p.InternalHistoryTask{
		tasks.CategoryTimer: {timer(base.Add(time.Minute), 90)},
	})))

	// The look-ahead's own request: batch 1 over a five-minute window.
	req := taskReq(tasks.CategoryTimer,
		tasks.NewKey(base, 0), tasks.NewKey(base.Add(5*time.Minute), 0), 1)
	resp, err := e.c.getHistoryTasks(context.Background(), req, cold.Read)
	require.NoError(t, err)
	require.Len(t, resp.Tasks, 1)
	require.EqualValues(t, 90, resp.Tasks[0].Key.TaskID,
		"the gate must be set from the tail-only timer, not from the cold store's later one")

	// Without the merge the same look-ahead finds nothing and the gate goes to
	// lookAheadMaxTime — four minutes past the fire time.
	bare, err := cold.Read(context.Background(), req)
	require.NoError(t, err)
	require.Empty(t, bare.Tasks, "the cold store alone has nothing to fire in this window")
}

// TestATaskReadOnAShardThisNodeDoesNotOwnIsRefused: a task read has one caller,
// the owning shard's queue processors, and a page without the tail in it is one
// that caller completes and acks past. So it is refused — unlike the
// mutable-state reads, whose callers legitimately do not own the shard — with
// the store's own error unwrapped, since the read path switches on that type.
func TestATaskReadOnAShardThisNodeDoesNotOwnIsRefused(t *testing.T) {
	m := newManager(t, &fakeApplier{}, nil)
	cold := coldtasks.New()
	minKey, maxKey := immediateRange()

	_, err := m.GetHistoryTasks(context.Background(),
		taskReq(tasks.CategoryTransfer, minKey, maxKey, 100), cold.Read)
	lost, ok := err.(*p.ShardOwnershipLostError) //nolint:errorlint // the concrete type is the assertion
	require.True(t, ok, "expected the store's own ShardOwnershipLostError, got %T: %v", err, err)
	require.EqualValues(t, testShard, wal.ShardID(lost.ShardID))
	require.Zero(t, cold.Calls, "a refused read must not reach the store below either")
}

// TestAHaltedShardAnswersATaskReadByTheHaltAndThenTheTail: on halted-lost the
// state decides and the tail is not consulted; only on halted-invariant does
// the tail decide. An empty tail speaks for this cycle's completeness, which is
// the wrong question: halted-lost means another owner, whose acks are in
// neither this tail nor the cold store. The halted-invariant half with an empty
// tail is not buildable through a drain and lives in read_halt_test.go.
func TestAHaltedShardAnswersATaskReadByTheHaltAndThenTheTail(t *testing.T) {
	minKey, maxKey := immediateRange()

	t.Run("an empty tail on halted-lost is still refused", func(t *testing.T) {
		// The drain committed, so nothing of this cycle's is acked-and-unapplied;
		// then the shard is fenced on its next append. That is also sync mode's
		// shape at every call boundary.
		//
		// newEnv and not taskEnv: this window opens with a create, which asserts
		// the current row absent, so the cold store must not hold the workflow.
		e := newEnv(t, func(c *Config) { c.Mutations = 1 })
		require.NoError(t, e.add(t, mkCreate("ns", "wf", "run")))
		e.log.OnAppend(waltest.Once(wal.ErrFenced))
		require.Error(t, e.add(t, mkUpdate("ns", "wf", "run", 2)))
		require.Equal(t, StateHaltedLost, e.c.State())

		cold := coldtasks.New()
		cold.Hold(tasks.CategoryTransfer, immediate(10))
		_, err := e.c.getHistoryTasks(context.Background(),
			taskReq(tasks.CategoryTransfer, minKey, maxKey, 100), cold.Read)
		require.IsType(t, &p.ShardOwnershipLostError{}, err,
			"unwrapped: the shard's read path matches this one concrete type and nothing else, got %v", err)
		require.Zero(t, cold.Calls,
			"the cold store cannot be short of a tail it was never told about, and this one may be")
	})

	t.Run("a tail on halted-lost is ShardOwnershipLost", func(t *testing.T) {
		e := newEnv(t, nil)
		require.NoError(t, e.add(t, mkCreate("ns", "wf", "run")))
		e.apply.errs = []error{&p.ShardOwnershipLostError{ShardID: int32(testShard)}}
		require.Error(t, e.c.drainNow(context.Background()))
		require.Equal(t, StateHaltedLost, e.c.State())

		cold := coldtasks.New()
		_, err := e.c.getHistoryTasks(context.Background(),
			taskReq(tasks.CategoryTransfer, minKey, maxKey, 100), cold.Read)
		require.IsType(t, &p.ShardOwnershipLostError{}, err,
			"unwrapped: the shard's read path matches this one concrete type and nothing else, got %v", err)
		require.Zero(t, cold.Calls, "the layer knows the cold store is incomplete")
	})

	t.Run("a tail on halted-invariant stays unrecognised", func(t *testing.T) {
		e := newEnv(t, nil)
		require.NoError(t, e.add(t, mkCreate("ns", "wf", "run")))
		e.apply.errs = []error{&apply.InvariantViolationError{Cause: errors.New("a version assertion failed")}}
		require.Error(t, e.c.drainNow(context.Background()))
		require.Equal(t, StateHaltedInvariant, e.c.State())

		cold := coldtasks.New()
		_, err := e.c.getHistoryTasks(context.Background(),
			taskReq(tasks.CategoryTransfer, minKey, maxKey, 100), cold.Read)
		require.ErrorIs(t, err, ErrHalted)
		require.False(t, errors.As(err, new(*p.ShardOwnershipLostError)),
			"a divergence this process owns must not be handed on as an ordinary failover")
		require.Zero(t, cold.Calls)
	})
}

// TestATaskReadIsAnsweredByTheLoop: a read cannot be issued past a drain it
// shares a channel with, so what this pins is that a read arriving mid-drain
// gets the drain's outcome and not the interval inside it, where a task is in
// neither source.
func TestATaskReadIsAnsweredByTheLoop(t *testing.T) {
	e := taskEnv(t, func(c *Config) { c.Mutations = 1 })
	cold := coldtasks.New()

	blocking := &blockingApplier{
		started: make(chan struct{}),
		release: make(chan struct{}),
		commit: func() {
			cold.Hold(tasks.CategoryTransfer, immediate(20))
		},
	}
	e.c.deps.Writer = blocking

	go func() {
		_ = e.c.write(context.Background(), mkTasks("ns", "wf", "run", 2,
			map[tasks.Category][]p.InternalHistoryTask{
				tasks.CategoryTransfer: {immediate(20)},
			}), e.rows)
	}()
	<-blocking.started

	done := make(chan []int64, 1)
	go func() {
		minKey, maxKey := immediateRange()
		done <- taskIDs(paginate(t, e.c, cold.Read, taskReq(tasks.CategoryTransfer, minKey, maxKey, 100)))
	}()
	close(blocking.release)

	require.Equal(t, []int64{20}, <-done,
		"the task was in neither source for the length of that drain, and the read saw it exactly once")
}

// TestTheMergedPageDoesNotAliasTheAccumulator: fold's mergeTasks appends into a
// superseded request's own task map, so a page sharing that slice would grow —
// or be rewritten — after the reader was handed it.
func TestTheMergedPageDoesNotAliasTheAccumulator(t *testing.T) {
	e := taskEnv(t, func(c *Config) { c.Mutations = 1 << 20 })
	cold := coldtasks.New()

	require.NoError(t, e.add(t, mkTasks("ns", "wf", "run", 2, map[tasks.Category][]p.InternalHistoryTask{
		tasks.CategoryTransfer: {immediate(10), immediate(20)},
	})))
	minKey, maxKey := immediateRange()
	page := paginate(t, e.c, cold.Read, taskReq(tasks.CategoryTransfer, minKey, maxKey, 100))[0]

	// A set supersedes the pending update, which is the fold that rewrites a
	// run's task map wholesale.
	require.NoError(t, e.add(t, mutation.Mutation{Set: &p.InternalSetWorkflowExecutionRequest{
		ShardID: int32(testShard),
		SetWorkflowSnapshot: p.InternalWorkflowSnapshot{
			NamespaceID: "ns", WorkflowID: "wf", RunID: "run",
			DBRecordVersion: 3,
			Tasks: map[tasks.Category][]p.InternalHistoryTask{
				tasks.CategoryTransfer: {immediate(30)},
			},
		},
	}}))

	require.Equal(t, []int64{10, 20}, taskIDs([][]p.InternalHistoryTask{page}),
		"a fold after the read rewrote a page already handed out")
}

// TestTheBaseIsAskedForWhatTheWindowDoesNotFill: the window's tasks displace
// cold-store rows rather than adding to them, so a merged read scans no more
// rows than a passthrough read of the same page would.
func TestTheBaseIsAskedForWhatTheWindowDoesNotFill(t *testing.T) {
	e := taskEnv(t, func(c *Config) { c.Mutations = 1 << 20 })
	cold := coldtasks.New()
	for id := int64(100); id < 110; id++ {
		cold.Hold(tasks.CategoryTransfer, immediate(id))
	}
	require.NoError(t, e.add(t, mkTasks("ns", "wf", "run", 2, map[tasks.Category][]p.InternalHistoryTask{
		tasks.CategoryTransfer: {immediate(1), immediate(2), immediate(3)},
	})))

	minKey, maxKey := immediateRange()
	pages := paginate(t, e.c, cold.Read, taskReq(tasks.CategoryTransfer, minKey, maxKey, 5))
	require.Equal(t, []int{2, 5, 5}, cold.Asked,
		"the first page had three window tasks to place, so it asked the base for two")
	require.Equal(t, []int64{1, 2, 3, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109}, taskIDs(pages))
}

// TestTheCutIsNeverInsideABasePage: the page may not exceed BatchSize, the
// base's token is the base's own format and cannot be advanced by half a page,
// and a scheduled range cannot name a mid-fire-time resume. So the merge emits
// window tasks strictly below the base page's first key and discards that page
// unread, leaving its token untouched; a partially emitted base page would be
// lost rows in one category and duplicated rows in the other.
func TestTheCutIsNeverInsideABasePage(t *testing.T) {
	e := taskEnv(t, func(c *Config) { c.Mutations = 1 << 20 })
	cold := coldtasks.New()
	cold.Hold(tasks.CategoryTransfer, immediate(100), immediate(101))

	var window []p.InternalHistoryTask
	for id := int64(1); id <= 5; id++ {
		window = append(window, immediate(id))
	}
	require.NoError(t, e.add(t, mkTasks("ns", "wf", "run", 2,
		map[tasks.Category][]p.InternalHistoryTask{tasks.CategoryTransfer: window})))

	minKey, maxKey := immediateRange()
	pages := paginate(t, e.c, cold.Read, taskReq(tasks.CategoryTransfer, minKey, maxKey, 2))
	require.Equal(t, []int64{1, 2, 3, 4, 5, 100, 101}, taskIDs(pages),
		"nothing lost and nothing doubled across a cut the window forced")
	for _, page := range pages {
		require.LessOrEqual(t, len(page), 2)
	}
}

// TestAForeignPageTokenTransits: a pagination that began while the shard's
// cycle was retired was answered by the base alone and handed back the base's
// own token, which can then arrive at a running cycle. Continuing with the base
// alone is the consistent reading — the window cursor the merge needs was never
// handed out, and inventing one would re-emit keys the caller already has.
func TestAForeignPageTokenTransits(t *testing.T) {
	e := taskEnv(t, func(c *Config) { c.Mutations = 1 << 20 })
	cold := coldtasks.New()
	cold.Hold(tasks.CategoryTransfer, immediate(10), immediate(11))
	require.NoError(t, e.add(t, mkTasks("ns", "wf", "run", 2, map[tasks.Category][]p.InternalHistoryTask{
		tasks.CategoryTransfer: {immediate(1)},
	})))

	minKey, maxKey := immediateRange()
	req := taskReq(tasks.CategoryTransfer, minKey, maxKey, 1)
	req.NextPageToken = coldtasks.EncodeInt(11) // the base's own, from a page this layer never saw

	resp, err := e.c.getHistoryTasks(context.Background(), req, cold.Read)
	require.NoError(t, err)
	require.Equal(t, []int64{11}, taskIDs([][]p.InternalHistoryTask{resp.Tasks}),
		"a token this layer did not write is the base's, and the page continues from where the base was")
	require.Equal(t, 1, e.c.Stats().TaskReads)
	require.Zero(t, e.c.Stats().TaskReadsMerged)
}

// TestATaskReadFailsWithTheStoresOwnError: a base that cannot be read fails the
// read, unwrapped. The merge cannot answer a range without the store's rows,
// and a page that quietly omitted them is this mechanism's loss in the other
// direction.
func TestATaskReadFailsWithTheStoresOwnError(t *testing.T) {
	e := taskEnv(t, func(c *Config) { c.Mutations = 1 << 20 })
	cold := coldtasks.New()
	cold.Err = errors.New("the cold store is having a moment")

	minKey, maxKey := immediateRange()
	_, err := e.c.getHistoryTasks(context.Background(),
		taskReq(tasks.CategoryTransfer, minKey, maxKey, 100), cold.Read)
	require.True(t, err == cold.Err, //nolint:errorlint // identity is the assertion
		"the store's own error must not be rebuilt on the way out, got %v", err)
}

// TestABaseRowAndAWindowTaskWithOneKeyAreEmittedOnce: the dedup is a safety net
// — the two sources are disjoint by construction, since a task reaches the cold
// store only when the drain carrying it commits. The base's row wins, and the
// merge neither compares blobs nor halts: a read is the wrong place to discover
// an invariant violation.
func TestABaseRowAndAWindowTaskWithOneKeyAreEmittedOnce(t *testing.T) {
	e := taskEnv(t, func(c *Config) { c.Mutations = 1 << 20 })
	cold := coldtasks.New()
	durable := immediate(10)
	durable.Blob = &commonpb.DataBlob{Data: []byte("the cold store's copy")}
	cold.Hold(tasks.CategoryTransfer, durable)

	require.NoError(t, e.add(t, mkTasks("ns", "wf", "run", 2, map[tasks.Category][]p.InternalHistoryTask{
		tasks.CategoryTransfer: {immediate(10)},
	})))

	minKey, maxKey := immediateRange()
	pages := paginate(t, e.c, cold.Read, taskReq(tasks.CategoryTransfer, minKey, maxKey, 100))
	require.Equal(t, []int64{10}, taskIDs(pages))
	require.Equal(t, "the cold store's copy", string(pages[0][0].Blob.Data), "the base's row wins a tie")
	require.Equal(t, 1, e.c.Stats().TaskCollisions, "the collision is counted rather than acted on")
}

// TestASharedFirstKeyStillAdvancesThePagination is the one place a collision
// reaches the merge's arithmetic: a window that overflows the page on its own,
// whose first task is also the base's first row. Cutting strictly below that
// key would emit nothing, and a page of nothing with a token never ends.
func TestASharedFirstKeyStillAdvancesThePagination(t *testing.T) {
	e := taskEnv(t, func(c *Config) { c.Mutations = 1 << 20 })
	cold := coldtasks.New()
	cold.Hold(tasks.CategoryTransfer, immediate(1), immediate(9))

	require.NoError(t, e.add(t, mkTasks("ns", "wf", "run", 2, map[tasks.Category][]p.InternalHistoryTask{
		tasks.CategoryTransfer: {immediate(1), immediate(2), immediate(3)},
	})))

	minKey, maxKey := immediateRange()
	pages := paginate(t, e.c, cold.Read, taskReq(tasks.CategoryTransfer, minKey, maxKey, 2))
	require.Equal(t, []int64{1, 2, 3, 9}, taskIDs(pages))
}

// TestTheBaseIsAskedTheCallersOwnQuestion: the merge decides two fields of the
// base's request — the batch the cut needs and the cursor it carries — and
// states the caller's own question in every other. A request rebuilt here
// instead of copied drops whatever this layer does not know about, and the
// store below then answers a question nobody asked. Enumerated off the type, so
// a field upstream adds fails here by name rather than going silently missing.
func TestTheBaseIsAskedTheCallersOwnQuestion(t *testing.T) {
	e := taskEnv(t, func(c *Config) { c.Mutations = 1 << 20 })
	cold := coldtasks.New()
	cold.Hold(tasks.CategoryTransfer, immediate(10))
	require.NoError(t, e.add(t, mkTasks("ns", "wf", "run", 2, map[tasks.Category][]p.InternalHistoryTask{
		tasks.CategoryTransfer: {immediate(5)},
	})))

	var seen *p.GetHistoryTasksRequest
	var base BaseTasks = func(ctx context.Context, req *p.GetHistoryTasksRequest) (*p.InternalGetHistoryTasksResponse, error) {
		seen = req
		return cold.Read(ctx, req)
	}

	minKey, maxKey := immediateRange()
	req := taskReq(tasks.CategoryTransfer, minKey, maxKey, 100)
	_, err := e.c.getHistoryTasks(context.Background(), req, base)
	require.NoError(t, err)
	require.NotNil(t, seen, "the base was never asked, so this judges nothing")

	decided := map[string]bool{"BatchSize": true, "NextPageToken": true}
	asked, reached := reflect.ValueOf(*req), reflect.ValueOf(*seen)
	fields := reflect.TypeFor[p.GetHistoryTasksRequest]()
	require.NotZero(t, fields.NumField())
	for f := range fields.Fields() {
		if decided[f.Name] {
			continue
		}
		require.Equalf(t, asked.FieldByIndex(f.Index).Interface(), reached.FieldByIndex(f.Index).Interface(),
			"%s reached the store below changed: the merge decides the batch and the cursor, and "+
				"carries the caller's question in everything else", f.Name)
	}
}
