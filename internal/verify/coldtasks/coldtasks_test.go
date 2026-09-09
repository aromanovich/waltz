package coldtasks

// The model's own two paginations, stated plainly. It stands in for the plugin
// at every merged-read call site in fold and cycle, so what it gets wrong those
// suites inherit — and a model that pages more strictly than the store it models
// makes them agree about pages the store never returns.
//
// This is the file to read against a real history-task store. Every claim here
// is one of that store's two queries or one of its two page tokens, and a base
// that answers differently is a base the merged read has not been judged over.

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
)

var epoch = time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)

func at(seconds int, id int64) p.InternalHistoryTask {
	return p.InternalHistoryTask{Key: tasks.NewKey(epoch.Add(time.Duration(seconds)*time.Second), id)}
}

func immediate(id int64) p.InternalHistoryTask {
	return p.InternalHistoryTask{Key: tasks.NewImmediateKey(id)}
}

// page reads one page and returns its ids and the token to carry.
func page(t *testing.T, c *Store, req *p.GetHistoryTasksRequest, token []byte) ([]int64, []byte) {
	t.Helper()
	req.NextPageToken = token
	resp, err := c.Read(context.Background(), req)
	require.NoError(t, err)
	ids := make([]int64, 0, len(resp.Tasks))
	for _, task := range resp.Tasks {
		ids = append(ids, task.Key.TaskID)
	}
	return ids, resp.NextPageToken
}

// drain pages until the store stops handing back a token, and reports every id
// in the order it was returned.
func drain(t *testing.T, c *Store, req *p.GetHistoryTasksRequest) []int64 {
	t.Helper()
	var all []int64
	var token []byte
	for range 20 {
		ids, next := page(t, c, req, token)
		all = append(all, ids...)
		if len(next) == 0 {
			return all
		}
		token = next
	}
	t.Fatal("the pagination did not terminate")
	return nil
}

func scheduledReq(from, to int, batch int) *p.GetHistoryTasksRequest {
	return &p.GetHistoryTasksRequest{
		TaskCategory:        tasks.CategoryTimer,
		InclusiveMinTaskKey: tasks.NewKey(epoch.Add(time.Duration(from)*time.Second), 0),
		ExclusiveMaxTaskKey: tasks.NewKey(epoch.Add(time.Duration(to)*time.Second), 0),
		BatchSize:           batch,
	}
}

func immediateReq(from, to int64, batch int) *p.GetHistoryTasksRequest {
	return &p.GetHistoryTasksRequest{
		TaskCategory:        tasks.CategoryTransfer,
		InclusiveMinTaskKey: tasks.NewImmediateKey(from),
		ExclusiveMaxTaskKey: tasks.NewImmediateKey(to),
		BatchSize:           batch,
	}
}

// TestAScheduledPageKeepsALowerIDFiringLater is the claim the scheduled
// condition is written for, and the one a range-scan predicate on task_id
// silently takes away: the token orders (fireTime, taskID) together, so a timer
// with a lower id than the page token's is still returned when it fires later.
//
// Read it against the plugin's own scheduled-paging test, wherever the
// deployment keeps it. If the two disagree the model is what is wrong, since
// only one of them is the store.
func TestAScheduledPageKeepsALowerIDFiringLater(t *testing.T) {
	c := New()
	c.Hold(tasks.CategoryTimer, at(60, 200), at(3600, 100))

	require.Equal(t, []int64{200, 100}, drain(t, c, scheduledReq(0, 7200, 1)),
		"both timers, in fire-time order, at the batch size that makes the first page full")
	require.Equal(t, 3, c.Calls, "two pages and the empty one that ends the pagination")
}

// TestAScheduledPageIsBoundedByFireTimeAlone: the upper bound is the exclusive
// maximum fire time, and it is not refined by a task id. A timer at or after it
// is outside the read whatever its id.
func TestAScheduledPageIsBoundedByFireTimeAlone(t *testing.T) {
	c := New()
	c.Hold(tasks.CategoryTimer, at(10, 5), at(20, 1), at(30, 9))

	require.Equal(t, []int64{5, 1}, drain(t, c, scheduledReq(0, 30, 10)),
		"the timer at the exclusive maximum is out, and the one before it is in whatever its id")
	require.Equal(t, []int64{1, 9}, drain(t, c, scheduledReq(20, 40, 10)),
		"and the inclusive minimum is in")
}

// TestAScheduledTokenCarriesTheSuccessorOfTheLastKey: {lastTaskID + 1,
// lastFireTime}. The successor is what makes a page boundary that falls inside
// one instant resume without repeating the row it ended on.
func TestAScheduledTokenCarriesTheSuccessorOfTheLastKey(t *testing.T) {
	c := New()
	c.Hold(tasks.CategoryTimer, at(10, 1), at(10, 2), at(10, 3))

	req := scheduledReq(0, 60, 2)
	ids, token := page(t, c, req, nil)
	require.Equal(t, []int64{1, 2}, ids)
	require.NotEmpty(t, token, "a full page carries one")
	require.Equal(t, tasks.NewKey(epoch.Add(10*time.Second), 3), DecodeKey(token))

	ids, token = page(t, c, req, token)
	require.Equal(t, []int64{3}, ids, "the successor resumes inside the instant, repeating nothing")
	require.Empty(t, token, "a short page ends it")
}

// TestAScheduledPageShorterThanTheBatchEndsThePagination: a token is emitted
// only for a full page. A short one is the end of the range, and a token on it
// would be a round trip that can only come back empty.
func TestAScheduledPageShorterThanTheBatchEndsThePagination(t *testing.T) {
	c := New()
	c.Hold(tasks.CategoryTimer, at(10, 1), at(20, 2))

	_, token := page(t, c, scheduledReq(0, 60, 5), nil)
	require.Empty(t, token)
	require.Equal(t, 1, c.Calls)
}

// TestAnImmediatePageIsTaskIDAloneAndReconstructsTheFireTime: an immediate
// task's fire time is not stored, so the column comes back NULL and the key is
// rebuilt with tasks.DefaultFireTime. A merge that compared fire times without
// that reconstruction would order the two sources differently.
func TestAnImmediatePageIsTaskIDAloneAndReconstructsTheFireTime(t *testing.T) {
	c := New()
	c.Hold(tasks.CategoryTransfer, immediate(1), immediate(2), immediate(3))

	resp, err := c.Read(context.Background(), immediateReq(0, 100, 10))
	require.NoError(t, err)
	require.Len(t, resp.Tasks, 3)
	for _, task := range resp.Tasks {
		require.Equal(t, tasks.DefaultFireTime, task.Key.FireTime)
	}
	require.Equal(t, []int64{1, 2, 3}, drain(t, c, immediateReq(0, 100, 10)))
	require.Equal(t, []int64{2}, drain(t, c, immediateReq(2, 3, 10)),
		"the minimum is inclusive and the maximum exclusive")
}

// TestAnImmediateTokenIsWithheldAtTheEndOfTheRange: the successor is emitted
// only while it is still inside the range, which is the one place the two
// tokens differ — the scheduled half has no such bound to check.
func TestAnImmediateTokenIsWithheldAtTheEndOfTheRange(t *testing.T) {
	c := New()
	c.Hold(tasks.CategoryTransfer, immediate(1), immediate(2))

	_, token := page(t, c, immediateReq(0, 4, 2), nil)
	require.Equal(t, int64(3), DecodeInt(token), "a full page, and its successor is still inside [0, 4)")

	_, token = page(t, c, immediateReq(0, 3, 2), nil)
	require.Empty(t, token,
		"a full page whose successor is the exclusive maximum: the next read could only come back empty")

	c2 := New()
	c2.Hold(tasks.CategoryTransfer, immediate(1), immediate(2), immediate(3))
	require.Equal(t, []int64{1, 2, 3}, drain(t, c2, immediateReq(0, 100, 2)))
}

// TestTheStoreCountsWhatItWasAsked: Calls, Returned and Asked are what a test
// about round trips reads — Calls and Asked count pages, Asked recording the
// batch size each was asked for, and Returned counts rows.
func TestTheStoreCountsWhatItWasAsked(t *testing.T) {
	c := New()
	c.Hold(tasks.CategoryTransfer, immediate(1), immediate(2), immediate(3))

	require.Equal(t, []int64{1, 2, 3}, drain(t, c, immediateReq(0, 100, 2)))
	require.Equal(t, 2, c.Calls, "a full page of two, then a short page of one")
	require.Equal(t, 3, c.Returned)
	require.Equal(t, []int{2, 2}, c.Asked)
}

// TestAStoreWithAnErrorAnswersNothingElse: Err is the whole answer, and it is
// still counted as a round trip — a caller that retried is a caller that asked
// twice.
func TestAStoreWithAnErrorAnswersNothingElse(t *testing.T) {
	c := New()
	c.Hold(tasks.CategoryTransfer, immediate(1))
	c.Err = context.DeadlineExceeded

	_, err := c.Read(context.Background(), immediateReq(0, 100, 10))
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, 1, c.Calls)
	require.Zero(t, c.Returned)
}

// TestCommitSortsWhatADrainWroteIntoTheStoresOwnOrder: a drain's tasks arrive
// in whatever order the batch held them and are read back in key order, because
// that is what an index gives a query and what both paginations assume.
func TestCommitSortsWhatADrainWroteIntoTheStoresOwnOrder(t *testing.T) {
	c := New()
	c.Commit(map[tasks.Category][]p.InternalHistoryTask{
		tasks.CategoryTimer: {at(30, 9), at(10, 5), at(20, 1)},
	})
	require.Equal(t, []int64{5, 1, 9}, drain(t, c, scheduledReq(0, 60, 10)))
}

// TestRemoveAppliesThePredicateItIsHanded: the model does not decide what a
// range covers. The caller hands it the layer's own predicate and this applies
// it row by row, the way the DELETE it stands in for would.
func TestRemoveAppliesThePredicateItIsHanded(t *testing.T) {
	c := New()
	c.Hold(tasks.CategoryTimer, at(10, 1), at(20, 2), at(30, 3))

	removed := c.Remove(tasks.CategoryTimer, func(k tasks.Key) bool {
		return k.TaskID == 2
	})
	require.Equal(t, 1, removed)
	require.Equal(t, []int64{1, 3}, drain(t, c, scheduledReq(0, 60, 10)))
}
