package fold_test

// The history-task path through the window (invariant I7): the rows an
// AddHistoryTasks puts in, the ranges a RangeCompleteHistoryTasks takes out,
// and the claims that decide whether the layer leaks a row or loses one.
//
// Both halves go through the log, so their order is the caller's: a task
// written after a range delete is one the sequential path keeps, and so must
// this ([TestATaskArrivingAfterARangeIsKept]).

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"

	"github.com/aromanovich/waltz/fold"
	"github.com/aromanovich/waltz/mutation"
)

// mkAddTasks builds one AddHistoryTasks over the given tasks.
func mkAddTasks(list ...p.InternalHistoryTask) mutation.Mutation {
	return mutation.Mutation{AddTasks: &p.InternalAddHistoryTasksRequest{
		ShardID:     int32(shard),
		NamespaceID: nsID,
		WorkflowID:  wfID,
		Tasks:       taskMap(list...),
	}}
}

// mkRangeComplete builds one RangeCompleteHistoryTasks over an immediate
// category.
func mkRangeComplete(from, upTo int64) mutation.Mutation {
	return mutation.Mutation{RangeCompleteTasks: &p.RangeCompleteHistoryTasksRequest{
		ShardID:             int32(shard),
		TaskCategory:        tasks.CategoryTransfer,
		InclusiveMinTaskKey: tasks.NewImmediateKey(from),
		ExclusiveMaxTaskKey: tasks.NewImmediateKey(upTo),
	}}
}

// mkTimerRange is the scheduled twin, ranged on fire time alone. The task ids
// are zeroed the way a queue's checkpoint zeroes them.
func mkTimerRange(from, upTo time.Duration) mutation.Mutation {
	return mutation.Mutation{RangeCompleteTasks: &p.RangeCompleteHistoryTasksRequest{
		ShardID:             int32(shard),
		TaskCategory:        tasks.CategoryTimer,
		InclusiveMinTaskKey: tasks.NewKey(tasks.DefaultFireTime.Add(from), 0),
		ExclusiveMaxTaskKey: tasks.NewKey(tasks.DefaultFireTime.Add(upTo), 0),
	}}
}

// TestTheWindowCarriesTheRowsAnAddPutInIt: the rows go in the drain's task
// work, and a reader sees them before the drain does.
func TestTheWindowCarriesTheRowsAnAddPutInIt(t *testing.T) {
	a := fold.New(shard)
	add(t, a, mkAddTasks(keyed(1, "a"), keyed(2, "b")))

	require.Equal(t, []string{"a", "b"}, names(a.Tasks(tasks.CategoryTransfer)),
		"a task in the window is readable before it reaches the store")

	batch := a.Drain()
	out, work := reqs(batch), batch.Tasks()
	require.Empty(t, out, "an AddHistoryTasks names no workflow, so it merges into no request")
	require.False(t, work.Empty())
	require.Equal(t, []string{"a", "b"}, names(work.Insert[tasks.CategoryTransfer]))
	require.EqualValues(t, 1, work.TailSeqno)
}

// TestARangeTakesOutWhatTheWindowAlreadyHeld: the plugin gathers every delete
// before every upsert, so a batch holding both a range and a row inside it
// would come out written. Both sources of a task are swept — an
// AddHistoryTasks' rows and a mutable-state write's task map — because both end
// up in the same UPSERT.
func TestARangeTakesOutWhatTheWindowAlreadyHeld(t *testing.T) {
	a := fold.New(shard)
	add(t, a,
		mkAddTasks(keyed(1, "a"), keyed(5, "b")),
		mkUpdate(runX, 2, withTasks(keyed(2, "c"), keyed(9, "d"))),
		mkRangeComplete(0, 6),
	)

	require.Equal(t, []string{"d"}, names(a.Tasks(tasks.CategoryTransfer)),
		"a reader must not see a row the caller has already declared garbage")

	batch := a.Drain()
	out, work := reqs(batch), batch.Tasks()
	require.Empty(t, work.Insert[tasks.CategoryTransfer], "the add's rows were both inside the range")
	require.Len(t, out, 1)
	require.Equal(t, []string{"d"}, taskNames(out[0].Request.Update.UpdateWorkflowMutation.Tasks),
		"the update keeps only the task above the range")
	require.Equal(t, []fold.TaskRange{{
		Category:     tasks.CategoryTransfer,
		InclusiveMin: tasks.NewImmediateKey(0),
		ExclusiveMax: tasks.NewImmediateKey(6),
	}}, work.Delete)
	require.Equal(t, map[string]int{tasks.CategoryTransfer.Name(): 3}, work.Dropped)
	require.Equal(t, map[string]int{tasks.CategoryTransfer.Name(): 1}, work.Written)
}

// TestATaskArrivingAfterARangeIsKept: the range has not been applied yet, and
// the log says the caller wrote this task after asking for it, so the
// sequential path keeps it. Dropping it would lose a task rather than leak one.
func TestATaskArrivingAfterARangeIsKept(t *testing.T) {
	a := fold.New(shard)
	add(t, a,
		mkRangeComplete(0, 6),
		mkAddTasks(keyed(3, "late")),
		mkUpdate(runX, 2, withTasks(keyed(4, "later"))),
	)

	require.Equal(t, []string{"late", "later"}, names(a.Tasks(tasks.CategoryTransfer)))

	batch := a.Drain()
	out, work := reqs(batch), batch.Tasks()
	require.Equal(t, []string{"late"}, names(work.Insert[tasks.CategoryTransfer]))
	require.Equal(t, []string{"later"}, taskNames(out[0].Request.Update.UpdateWorkflowMutation.Tasks))
	require.Empty(t, work.Dropped, "nothing was dropped: the range came first")
	require.Len(t, work.Delete, 1, "and the range is still applied")
}

// TestButtJoinedRangesMergeAndAGapDoesNot: a queue's checkpoints are
// `[old, new)` with old only rising, so a category costs one statement in
// practice. A gap stays two ranges: closing it would delete rows nobody asked
// to be gone, and the reader's subtraction would hide a row that is still
// there.
func TestButtJoinedRangesMergeAndAGapDoesNot(t *testing.T) {
	t.Run("butt-joined", func(t *testing.T) {
		a := fold.New(shard)
		add(t, a, mkRangeComplete(0, 4), mkRangeComplete(4, 9))

		work := a.Drain().Tasks()
		require.Equal(t, []fold.TaskRange{{
			Category:     tasks.CategoryTransfer,
			InclusiveMin: tasks.NewImmediateKey(0),
			ExclusiveMax: tasks.NewImmediateKey(9),
		}}, work.Delete)
	})

	t.Run("with a gap", func(t *testing.T) {
		a := fold.New(shard)
		add(t, a, mkRangeComplete(0, 4), mkRangeComplete(7, 9), mkAddTasks(keyed(5, "in the gap")))

		work := a.Drain().Tasks()
		require.Len(t, work.Delete, 2, "a range nobody asked for is a row nobody deletes")
		require.Equal(t, []string{"in the gap"}, names(work.Insert[tasks.CategoryTransfer]))
	})

	// Two producers of one category can issue a range below the last: a
	// reloaded queue restarts from its persisted checkpoint, and replication's
	// [0, minAcked+1) can sit under a queue's [old, new). The delete is acked
	// either way, so dropping it leaves pre-window rows in the cold store that
	// nobody deletes again.
	t.Run("below the last one", func(t *testing.T) {
		a := fold.New(shard)
		add(t, a, mkRangeComplete(100, 200), mkRangeComplete(10, 20))

		require.Equal(t, []fold.TaskRange{
			{Category: tasks.CategoryTransfer, InclusiveMin: tasks.NewImmediateKey(100), ExclusiveMax: tasks.NewImmediateKey(200)},
			{Category: tasks.CategoryTransfer, InclusiveMin: tasks.NewImmediateKey(10), ExclusiveMax: tasks.NewImmediateKey(20)},
		}, a.Drain().Tasks().Delete)
	})

	// The gap above is closed by a later request for exactly it, which joins
	// the range above it at that range's minimum rather than extending one at
	// its maximum. The task is added first, so it is a row the delete covers
	// rather than one the caller wrote afterwards.
	t.Run("a later range fills the gap", func(t *testing.T) {
		a := fold.New(shard)
		add(t, a, mkAddTasks(keyed(5, "in the gap")),
			mkRangeComplete(0, 4), mkRangeComplete(7, 9), mkRangeComplete(4, 7))

		work := a.Drain().Tasks()
		require.Empty(t, names(work.Insert[tasks.CategoryTransfer]),
			"the filler covers key 5, so the window's own row goes with it")

		var covered bool
		for _, r := range work.Delete {
			covered = covered || r.Covers(tasks.NewImmediateKey(5))
		}
		require.True(t, covered, "the range that closed the gap must reach the cold store's rows too")
	})
}

// TestTheRangesDoNotOutliveTheDrainThatAppliesThem: after a drain the ranges
// are the cold store's business, so a task arriving in the next window is
// written exactly as the sequential path writes it.
func TestTheRangesDoNotOutliveTheDrainThatAppliesThem(t *testing.T) {
	a := fold.New(shard)
	add(t, a, mkRangeComplete(0, 6))
	first := a.Drain().Tasks()
	require.Len(t, first.Delete, 1)

	require.NoError(t, a.Add(2, mkAddTasks(keyed(3, "next window"))))
	second := a.Drain().Tasks()
	require.Empty(t, second.Delete, "the range was applied by the drain that carried it")
	require.Equal(t, []string{"next window"}, names(second.Insert[tasks.CategoryTransfer]))
}

// TestAScheduledRangeIsFireTimeOnly: an immediate category is ranged on task_id
// and a scheduled one on task_visibility_ts, whose task ids the DELETE never
// looks at. A sweep narrower than the delete keeps a row the drain then writes
// under a delete the caller asked for; a wider one drops a task the DELETE
// leaves alone, and a dropped task is neither written nor deleted.
func TestAScheduledRangeIsFireTimeOnly(t *testing.T) {
	a := fold.New(shard)
	add(t, a,
		mkAddTasks(
			timerAt(time.Minute, 900, "inside, high id"),
			timerAt(2*time.Minute, 1, "outside, low id"),
		),
		mkTimerRange(0, 90*time.Second),
	)

	work := a.Drain().Tasks()
	require.Equal(t, []string{"outside, low id"}, names(work.Insert[tasks.CategoryTimer]),
		"the fire time decides and the task id does not, which is what the DELETE does")
}

// TestARangeReachesEveryHomeTheReadReaches: a task row lives in one of three
// places in a window, and the sweep must reach every one the read does. A row
// the read shows and the sweep misses is a leak — the caller declared it garbage
// and the drain writes it anyway — and a tombstone's orphans are the case that
// looks exempt: the collapse preserves them because I7 says a task is durable
// somewhere, while the range says the caller no longer wants it.
//
// Each home carries a row inside the range and one above it, so a sweep that
// took a whole home out would fail here rather than pass by emptying it.
func TestARangeReachesEveryHomeTheReadReaches(t *testing.T) {
	for _, home := range []struct {
		name   string
		window []mutation.Mutation
	}{
		{
			name:   "a pending request's task slot",
			window: []mutation.Mutation{mkUpdate(runX, 2, withTasks(keyed(1, "swept"), keyed(8, "kept")))},
		},
		{
			name: "a tombstone's orphaned tasks",
			window: []mutation.Mutation{
				mkUpdate(runX, 2, withTasks(keyed(1, "swept"), keyed(8, "kept"))),
				mkDelete(runX),
			},
		},
		{
			name:   "the rows an AddHistoryTasks put in",
			window: []mutation.Mutation{mkAddTasks(keyed(1, "swept"), keyed(8, "kept"))},
		},
	} {
		t.Run(home.name, func(t *testing.T) {
			a := fold.New(shard)
			add(t, a, home.window...)
			require.Equal(t, []string{"swept", "kept"}, names(a.Tasks(tasks.CategoryTransfer)),
				"the read reaches this home")

			// Above every seqno the window used, the homes being of unequal length.
			require.NoError(t, a.Add(9, mkRangeComplete(0, 5)))
			require.Equal(t, []string{"kept"}, names(a.Tasks(tasks.CategoryTransfer)),
				"and so must the sweep")

			batch := a.Drain()
			require.Equal(t, []string{"kept"}, names(written(batch, tasks.CategoryTransfer)),
				"a row the sweep missed is one the drain writes under a delete the caller asked for")
			require.Equal(t, map[string]int{tasks.CategoryTransfer.Name(): 1}, batch.Tasks().Dropped)
			require.Equal(t, map[string]int{tasks.CategoryTransfer.Name(): 1}, batch.Tasks().Written)
		})
	}
}

// TestASweepDoesNotWriteThroughASliceAReaderHolds: a page handed out before a
// range folded in must not change under its holder.
func TestASweepDoesNotWriteThroughASliceAReaderHolds(t *testing.T) {
	a := fold.New(shard)
	add(t, a, mkUpdate(runX, 2, withTasks(keyed(1, "a"), keyed(2, "b"), keyed(9, "c"))))

	page := a.Tasks(tasks.CategoryTransfer)
	require.Equal(t, []string{"a", "b", "c"}, names(page))

	require.NoError(t, a.Add(2, mkRangeComplete(0, 5)))
	require.Equal(t, []string{"a", "b", "c"}, names(page),
		"the page a reader already holds is not the accumulator's to edit")
	require.Equal(t, []string{"c"}, names(a.Tasks(tasks.CategoryTransfer)))
}

// TestAScheduledRangeComparesAtTheStoresResolution: comparison is at microsecond
// resolution, because that is what a timestamp column holds. A range whose exclusive
// maximum sits a nanosecond above a task's fire time truncates to that fire
// time, so `task_visibility_ts < max` excludes the row and the store deletes
// nothing; a window dropping it anyway loses a timer, since a dropped task is
// neither written nor deleted.
func TestAScheduledRangeComparesAtTheStoresResolution(t *testing.T) {
	at := tasks.DefaultFireTime.Add(time.Hour)

	t.Run("a maximum inside the same microsecond covers nothing", func(t *testing.T) {
		a := fold.New(shard)
		add(t, a,
			mkAddTasks(p.InternalHistoryTask{Key: tasks.NewKey(at, 4), Blob: blob("timer")}),
			mutation.Mutation{RangeCompleteTasks: &p.RangeCompleteHistoryTasksRequest{
				ShardID:             int32(shard),
				TaskCategory:        tasks.CategoryTimer,
				InclusiveMinTaskKey: tasks.NewKey(at.Add(-time.Hour), 0),
				ExclusiveMaxTaskKey: tasks.NewKey(at.Add(time.Nanosecond), 0),
			}},
		)

		work := a.Drain().Tasks()
		require.Equal(t, []string{"timer"}, names(work.Insert[tasks.CategoryTimer]),
			"the store's DELETE would not have removed this row, so the window may not either")
	})

	t.Run("a maximum a microsecond above covers it", func(t *testing.T) {
		a := fold.New(shard)
		add(t, a,
			mkAddTasks(p.InternalHistoryTask{Key: tasks.NewKey(at, 4), Blob: blob("timer")}),
			mutation.Mutation{RangeCompleteTasks: &p.RangeCompleteHistoryTasksRequest{
				ShardID:             int32(shard),
				TaskCategory:        tasks.CategoryTimer,
				InclusiveMinTaskKey: tasks.NewKey(at.Add(-time.Hour), 0),
				ExclusiveMaxTaskKey: tasks.NewKey(at.Add(time.Microsecond), 0),
			}},
		)

		work := a.Drain().Tasks()
		require.Empty(t, work.Insert[tasks.CategoryTimer])
	})
}

// TestTheRangesAReaderSubtractsAreTheUndrainedOnes: what a merged read hides
// from the cold store's page is exactly the ranges the drain has not applied.
// Asserted through the drain's [TaskWork], since the window's pending ranges
// are unexported and "undrained" and "carried by this drain" are the same set
// from the two sides. What the page does with them is
// [TestThePageHidesTheUndrainedRangesFromTheColdStoresHalfOnly].
func TestTheRangesAReaderSubtractsAreTheUndrainedOnes(t *testing.T) {
	a := fold.New(shard)
	untouched := a.Drain().Tasks()
	require.Empty(t, untouched.Delete, "an untouched category hides nothing")

	add(t, a, mkRangeComplete(0, 6))

	work := a.Drain().Tasks()
	require.Len(t, work.Delete, 1)
	require.True(t, work.Delete[0].Covers(tasks.NewImmediateKey(5)))
	require.False(t, work.Delete[0].Covers(tasks.NewImmediateKey(6)), "the maximum is exclusive")

	after := a.Drain().Tasks()
	require.Empty(t, after.Delete,
		"once applied, the rows really are gone from the store and hiding them would be wrong")
}
