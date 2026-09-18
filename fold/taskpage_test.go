package fold_test

// The merged task page's rules one case at a time, at the door
// [fold.Accumulator.TaskPage]: the cut, the displacement, the token, the dedup,
// and the subtraction of what the window has deleted and the store below has
// not. taskpage_corpus_test.go checks the same rules over a stream; what a
// shard does with a page is cycle's.
//
// The base is the plugin's own pagination (internal/verify/coldtasks) rather than a stub,
// because the base's pagination is the merge's whole difficulty: a store that
// answered everything in one page would leave the cut untested.

import (
	"context"
	"encoding/base64"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"

	"github.com/aromanovich/waltz/fold"
	"github.com/aromanovich/waltz/internal/verify/coldtasks"
)

// immediateRange and scheduledRange are the widest ranges a queue could ask for,
// in the shape validateTaskRange demands: an immediate category bounded by task
// id, a scheduled one by fire time.
func immediateRange() (tasks.Key, tasks.Key) {
	return tasks.NewImmediateKey(0), tasks.NewImmediateKey(math.MaxInt64)
}

func scheduledRange() (tasks.Key, tasks.Key) {
	return tasks.NewKey(coldtasks.UnixNano(0), 0), tasks.NewKey(coldtasks.UnixNano(math.MaxInt64), 0)
}

func taskReq(category tasks.Category, minKey, maxKey tasks.Key, batch int) *p.GetHistoryTasksRequest {
	return &p.GetHistoryTasksRequest{
		ShardID:             int32(shard),
		TaskCategory:        category,
		InclusiveMinTaskKey: minKey,
		ExclusiveMaxTaskKey: maxKey,
		BatchSize:           batch,
	}
}

// basePage is the callback the cycle builds, written the same way: the caller's
// own request with the batch size and token overridden on a copy.
func basePage(cold *coldtasks.Store, req *p.GetHistoryTasksRequest) fold.BasePage {
	return func(batch int, token []byte) ([]p.InternalHistoryTask, []byte, error) {
		ask := *req
		ask.BatchSize, ask.NextPageToken = batch, token
		resp, err := cold.Read(context.Background(), &ask)
		if err != nil {
			return nil, nil, err
		}
		return resp.Tasks, resp.NextPageToken, nil
	}
}

// paginate drives one merged read to exhaustion the way collection.PagingIterator
// drives the store, returning the pages so their shape can be asserted too.
func paginate(
	t *testing.T, a *fold.Accumulator, cold *coldtasks.Store, req *p.GetHistoryTasksRequest,
) ([][]p.InternalHistoryTask, fold.TaskPageStats) {
	t.Helper()
	var pages [][]p.InternalHistoryTask
	var total fold.TaskPageStats
	ask := *req
	for range 100_000 {
		resp, stats, err := a.TaskPage(&ask, basePage(cold, &ask))
		require.NoError(t, err)
		require.LessOrEqual(t, len(resp.Tasks), max(req.BatchSize, 1),
			"a page longer than the caller's BatchSize: ExecutionMutableStateTaskSuite asserts this one itself")
		total.Add(stats)
		pages = append(pages, resp.Tasks)
		if len(resp.NextPageToken) == 0 {
			return pages, total
		}
		ask.NextPageToken = resp.NextPageToken
	}
	t.Fatal("the pagination did not terminate in 100 000 pages")
	return nil, total
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

// TestTheCutIsNeverInsideABasePage is the pagination rule in the case that
// forces it: a window holding more than one page of tasks below everything the
// cold store has. A page may not exceed BatchSize and the base's token cannot be
// advanced by half a page, so the merge emits window tasks strictly below the
// base page's first key and leaves that page unread with its token untouched.
// A partially emitted base page loses rows on one side and duplicates them on
// the other.
func TestTheCutIsNeverInsideABasePage(t *testing.T) {
	a := fold.New(shard)
	cold := coldtasks.New()
	cold.Hold(tasks.CategoryTransfer, keyed(100, "cold-100"), keyed(101, "cold-101"))

	var window []p.InternalHistoryTask
	for id := int64(1); id <= 5; id++ {
		window = append(window, keyed(id, "window"))
	}
	add(t, a, mkAddTasks(window...))

	minKey, maxKey := immediateRange()
	pages, _ := paginate(t, a, cold, taskReq(tasks.CategoryTransfer, minKey, maxKey, 2))
	require.Equal(t, []int64{1, 2, 3, 4, 5, 100, 101}, taskIDs(pages),
		"nothing lost and nothing doubled across a cut the window forced")
}

// TestTheBaseIsAskedForWhatTheWindowDoesNotFill: window tasks displace
// cold-store rows rather than adding to them, so a merged read scans no more
// rows than a passthrough read of the same page would.
func TestTheBaseIsAskedForWhatTheWindowDoesNotFill(t *testing.T) {
	a := fold.New(shard)
	cold := coldtasks.New()
	for id := int64(100); id < 110; id++ {
		cold.Hold(tasks.CategoryTransfer, keyed(id, "cold"))
	}
	add(t, a, mkAddTasks(keyed(1, "a"), keyed(2, "b"), keyed(3, "c")))

	minKey, maxKey := immediateRange()
	pages, _ := paginate(t, a, cold, taskReq(tasks.CategoryTransfer, minKey, maxKey, 5))
	require.Equal(t, []int{2, 5, 5}, cold.Asked,
		"the first page had three window tasks to place, so it asked the base for two")
	require.Equal(t, []int64{1, 2, 3, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109}, taskIDs(pages))
}

// TestASharedFirstKeyStillAdvancesThePagination: a window task and the base's
// first row on one key. The base page's token bounds the window's reach to that
// key, so the tie deduplicates to one row and the pagination still advances.
// mergePage's own tie-break — the cut at the base's first key, where an empty
// page with a token would never end — is reached by nothing in this file;
// taskpage_minimal_test.go's [TestATieAtTheCutIsEmittedOnce] is what drives it.
func TestASharedFirstKeyStillAdvancesThePagination(t *testing.T) {
	a := fold.New(shard)
	cold := coldtasks.New()
	cold.Hold(tasks.CategoryTransfer, keyed(1, "cold-1"), keyed(9, "cold-9"))
	add(t, a, mkAddTasks(keyed(1, "a"), keyed(2, "b"), keyed(3, "c")))

	minKey, maxKey := immediateRange()
	pages, _ := paginate(t, a, cold, taskReq(tasks.CategoryTransfer, minKey, maxKey, 2))
	require.Equal(t, []int64{1, 2, 3, 9}, taskIDs(pages))
}

// TestABaseRowAndAWindowTaskWithOneKeyAreEmittedOnce is the dedup, a safety net
// rather than a mechanism: the two sources are disjoint by construction, a task
// reaching the cold store only in the drain that stops the window holding it.
// The base's row wins, being the durable copy the queue will complete, and the
// merge neither compares blobs nor halts — a read is the wrong place to discover
// an invariant violation.
func TestABaseRowAndAWindowTaskWithOneKeyAreEmittedOnce(t *testing.T) {
	a := fold.New(shard)
	cold := coldtasks.New()
	cold.Hold(tasks.CategoryTransfer, keyed(10, "the cold store's copy"))
	add(t, a, mkAddTasks(keyed(10, "the window's copy")))

	minKey, maxKey := immediateRange()
	pages, stats := paginate(t, a, cold, taskReq(tasks.CategoryTransfer, minKey, maxKey, 100))
	require.Equal(t, []string{"the cold store's copy"}, names(pages[0]), "the base's row wins a tie")
	require.Len(t, taskIDs(pages), 1)
	require.Equal(t, 1, stats.Collisions, "the collision is counted rather than acted on")
}

// TestThePageHidesTheUndrainedRangesFromTheColdStoresHalfOnly is the
// subtraction: a range the window still holds has not reached the store, so the
// store's page still carries rows the caller has been told are gone. The
// window's own half needs none, a covered task having been dropped when the
// range folded in, and a task written after the range is kept by both paths.
func TestThePageHidesTheUndrainedRangesFromTheColdStoresHalfOnly(t *testing.T) {
	minKey, maxKey := immediateRange()

	t.Run("the rows a pending delete covers are gone from the page", func(t *testing.T) {
		a := fold.New(shard)
		cold := coldtasks.New()
		cold.Hold(tasks.CategoryTransfer, keyed(2, "doomed"), keyed(4, "doomed"), keyed(8, "kept"))
		// A task the caller wrote after the range: the sequential path keeps that
		// one, so the window shows it and the page carries it.
		add(t, a, mkRangeComplete(0, 6), mkAddTasks(keyed(3, "late")))

		pages, _ := paginate(t, a, cold, taskReq(tasks.CategoryTransfer, minKey, maxKey, 100))
		require.Equal(t, []int64{3, 8}, taskIDs(pages),
			"the store's rows inside an undrained range are rows the caller has been told are gone")
	})

	t.Run("it subtracts the ranges and not their maximum", func(t *testing.T) {
		a := fold.New(shard)
		cold := coldtasks.New()
		cold.Hold(tasks.CategoryTransfer, keyed(2, "covered"), keyed(5, "in the gap"), keyed(8, "covered"))
		add(t, a, mkRangeComplete(0, 4), mkRangeComplete(7, 9))

		pages, _ := paginate(t, a, cold, taskReq(tasks.CategoryTransfer, minKey, maxKey, 100))
		require.Equal(t, []int64{5}, taskIDs(pages),
			"a row in a gap between two pending ranges is one no delete covers, and hiding it is the leak")
	})

	t.Run("once the drain has applied them the page hides nothing", func(t *testing.T) {
		a := fold.New(shard)
		cold := coldtasks.New()
		cold.Hold(tasks.CategoryTransfer, keyed(2, "still there"))
		add(t, a, mkRangeComplete(0, 6))
		a.Drain() // the delete is the cold store's business now

		pages, _ := paginate(t, a, cold, taskReq(tasks.CategoryTransfer, minKey, maxKey, 100))
		require.Equal(t, []int64{2}, taskIDs(pages),
			"a page that kept hiding rows an applied range never removed would hide them for ever")
	})

	t.Run("a scheduled range hides at the store's resolution", func(t *testing.T) {
		// The read side of [TestAScheduledRangeComparesAtTheStoresResolution]:
		// A stored fire time is microseconds, so a maximum a nanosecond above a
		// row's fire time truncates to that fire time and the store's DELETE
		// removes nothing. A page that hid the row anyway would make it
		// invisible and present, and nobody would ever fire it.
		at := tasks.DefaultFireTime.Add(time.Hour)
		row := p.InternalHistoryTask{Key: tasks.NewKey(at, 4), Blob: blob("timer")}

		for _, tc := range []struct {
			name  string
			above time.Duration
			want  []int64
		}{
			{"a maximum inside the same microsecond covers nothing", time.Nanosecond, []int64{4}},
			{"a maximum a microsecond above covers it", time.Microsecond, nil},
		} {
			t.Run(tc.name, func(t *testing.T) {
				a := fold.New(shard)
				cold := coldtasks.New()
				cold.Hold(tasks.CategoryTimer, row)
				add(t, a, mkTimerRange(0, time.Hour+tc.above))

				from, to := scheduledRange()
				pages, _ := paginate(t, a, cold, taskReq(tasks.CategoryTimer, from, to, 100))
				require.Equal(t, tc.want, taskIDs(pages))
			})
		}
	})
}

// TestTheResumeTokenCarriesTheBasesOwnBytes is the token rule: this layer frames
// its own cursor around the base's token and never parses, rebuilds or
// interprets it. The base's format is the plugin's.
func TestTheResumeTokenCarriesTheBasesOwnBytes(t *testing.T) {
	a := fold.New(shard)
	cold := coldtasks.New()
	cold.Hold(tasks.CategoryTransfer, keyed(10, "a"), keyed(11, "b"), keyed(12, "c"))

	var handed [][]byte
	base := func(batch int, token []byte) ([]p.InternalHistoryTask, []byte, error) {
		handed = append(handed, token)
		ask := taskReq(tasks.CategoryTransfer, tasks.NewImmediateKey(0), tasks.NewImmediateKey(math.MaxInt64), batch)
		ask.NextPageToken = token
		resp, err := cold.Read(context.Background(), ask)
		if err != nil {
			return nil, nil, err
		}
		return resp.Tasks, resp.NextPageToken, nil
	}

	minKey, maxKey := immediateRange()
	req := taskReq(tasks.CategoryTransfer, minKey, maxKey, 2)
	first, _, err := a.TaskPage(req, base)
	require.NoError(t, err)
	require.Equal(t, []int64{10, 11}, taskIDs([][]p.InternalHistoryTask{first.Tasks}))
	require.NotEmpty(t, first.NextPageToken)
	require.NotEqual(t, coldtasks.EncodeInt(12), first.NextPageToken,
		"the layer's token is its own frame: handing the base's back would lose the window cursor")
	require.Contains(t, string(first.NextPageToken),
		base64.StdEncoding.EncodeToString(coldtasks.EncodeInt(12)),
		"the base's token has to travel inside ours, verbatim (JSON puts its bytes in base64)")

	req.NextPageToken = first.NextPageToken
	second, _, err := a.TaskPage(req, base)
	require.NoError(t, err)
	require.Equal(t, []int64{12}, taskIDs([][]p.InternalHistoryTask{second.Tasks}))
	require.Equal(t, [][]byte{nil, coldtasks.EncodeInt(12)}, handed,
		"the base is resumed with its own bytes and nothing else")
}

// TestABaseThatAnswersWithMoreRowsThanItWasAskedForIsRefused: the cut rests on
// the ask being a cap. Where the window alone overflows the page the base is
// asked for one row and exactly one is emitted, which is what lets that page's
// cursor advance — so a second row sent unasked is one the cursor moves past
// having emitted neither it nor anything above it. Measured before this was
// refused: an ask of one answered with keys 40, 41 and 42 emitted 40 alone and
// handed back a token resuming the base past 42, so 41 and 42 were returned by
// no page of that pagination and deleted by the range its reader completed.
func TestABaseThatAnswersWithMoreRowsThanItWasAskedForIsRefused(t *testing.T) {
	a := fold.New(shard)
	add(t, a, mkAddTasks(keyed(50, "a"), keyed(60, "b"), keyed(70, "c")))

	minKey, maxKey := immediateRange()
	req := taskReq(tasks.CategoryTransfer, minKey, maxKey, 2)

	overfilled := func(int, []byte) ([]p.InternalHistoryTask, []byte, error) {
		return []p.InternalHistoryTask{keyed(40, "x"), keyed(41, "y"), keyed(42, "z")},
			coldtasks.EncodeInt(42), nil
	}

	_, _, err := a.TaskPage(req, overfilled)
	require.ErrorIs(t, err, fold.ErrBasePageTooLarge)
}

// TestAForeignPageTokenIsRefused: the two token spaces never meet, because a
// task page is answered by a running cycle or not at all and every such page
// carries a token this layer wrote. So a token that is not one of ours is not a
// pagination to continue. Answering it from the base alone is the readable
// wrong answer — the window drops out of every remaining page, and the reader
// that reaches the end completes the range over rows it was never shown.
func TestAForeignPageTokenIsRefused(t *testing.T) {
	a := fold.New(shard)
	cold := coldtasks.New()
	cold.Hold(tasks.CategoryTransfer, keyed(10, "a"), keyed(11, "b"))
	add(t, a, mkAddTasks(keyed(1, "in the window")))

	minKey, maxKey := immediateRange()
	req := taskReq(tasks.CategoryTransfer, minKey, maxKey, 1)
	req.NextPageToken = coldtasks.EncodeInt(11) // the base's own, which no page of this layer's carries

	_, _, err := a.TaskPage(req, basePage(cold, req))
	require.ErrorIs(t, err, fold.ErrForeignPageToken)
	require.Zero(t, cold.Calls, "a refused page must not reach the store below either")
}

// TestABaseErrorFailsThePageUnwrapped: a base that cannot be read fails the
// read, unwrapped. The merge cannot answer a range without the store's rows, and
// a page that quietly omitted them would lose them.
func TestABaseErrorFailsThePageUnwrapped(t *testing.T) {
	a := fold.New(shard)
	cold := coldtasks.New()
	cold.Err = errors.New("the cold store is having a moment")

	minKey, maxKey := immediateRange()
	req := taskReq(tasks.CategoryTransfer, minKey, maxKey, 100)
	_, _, err := a.TaskPage(req, basePage(cold, req))
	require.True(t, err == cold.Err, //nolint:errorlint // identity is the assertion
		"the store's own error must not be rebuilt on the way out, got %v", err)
}
