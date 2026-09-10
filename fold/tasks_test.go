package fold_test

// What the window owes a task reader: the task set it shows, and the promise
// that it is the same set the drain will write. Merging that set with a cold
// store's page is taskpage_test.go's.

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"

	"github.com/aromanovich/waltz/fold"
	"github.com/aromanovich/waltz/mutation"
)

// keyed and timerAt build the two key shapes: an immediate key by task id, and a
// scheduled one at tasks.DefaultFireTime plus an offset.
func keyed(id int64, name string) p.InternalHistoryTask {
	return p.InternalHistoryTask{Key: tasks.NewImmediateKey(id), Blob: blob(name)}
}

func timerAt(d time.Duration, id int64, name string) p.InternalHistoryTask {
	return p.InternalHistoryTask{
		Key:  tasks.NewKey(tasks.DefaultFireTime.Add(d), id),
		Blob: blob(name),
	}
}

// withTasks and snapTasks put a whole task map on a request.
func withTasks(list ...p.InternalHistoryTask) func(*p.InternalWorkflowMutation) {
	return func(m *p.InternalWorkflowMutation) { m.Tasks = taskMap(list...) }
}

func snapTasks(list ...p.InternalHistoryTask) func(*p.InternalWorkflowSnapshot) {
	return func(s *p.InternalWorkflowSnapshot) { s.Tasks = taskMap(list...) }
}

// taskMap sorts tasks into two categories by key shape, the way the store does:
// a fire time equal to tasks.DefaultFireTime is immediate, anything else is
// scheduled.
func taskMap(list ...p.InternalHistoryTask) map[tasks.Category][]p.InternalHistoryTask {
	out := make(map[tasks.Category][]p.InternalHistoryTask)
	for _, t := range list {
		category := tasks.CategoryTimer
		if t.Key.FireTime.Equal(tasks.DefaultFireTime) {
			category = tasks.CategoryTransfer
		}
		out[category] = append(out[category], t)
	}
	return out
}

// names is each task's blob, in the order given.
func names(list []p.InternalHistoryTask) []string {
	out := make([]string, 0, len(list))
	for _, t := range list {
		out = append(out, string(t.Blob.Data))
	}
	return out
}

// multiset compares tasks ignoring order, which is what "the same tasks" means
// when one of the two sources is a map walk.
func multiset(list []p.InternalHistoryTask) map[string]int {
	out := make(map[string]int, len(list))
	for _, t := range list {
		out[string(t.Blob.Data)]++
	}
	return out
}

// TestTheWindowShowsATasksOwnOrder: the enumeration is sorted by key, not by
// arrival. A queue reads a range and expects it ascending, and the two orders do
// differ: a shard hands out task ids before the write, so a row already in the
// cold store can carry a higher id than a mutation still in the window.
func TestTheWindowShowsATasksOwnOrder(t *testing.T) {
	a := fold.New(shard)
	add(t, a,
		mkUpdate(runX, 2, withTasks(keyed(30, "third"))),
		mkUpdate(runX, 3, withTasks(keyed(10, "first"))),
		mkUpdate(runX, 4, withTasks(keyed(20, "second"))),
	)

	require.Equal(t, []string{"first", "second", "third"}, names(a.Tasks(tasks.CategoryTransfer)))
}

// TestTheWindowShowsOnlyTheCategoryAsked: a key from the wrong category is a key
// outside the requested range, and queues/slice.go panics the history service on
// exactly that.
func TestTheWindowShowsOnlyTheCategoryAsked(t *testing.T) {
	a := fold.New(shard)
	add(t, a, mkUpdate(runX, 2, withTasks(keyed(10, "transfer"), timerAt(time.Minute, 11, "timer"))))

	require.Equal(t, []string{"transfer"}, names(a.Tasks(tasks.CategoryTransfer)))
	require.Equal(t, []string{"timer"}, names(a.Tasks(tasks.CategoryTimer)))
	require.Empty(t, a.Tasks(tasks.CategoryVisibility))
}

// TestACategoryIsMatchedByItsID: the category reaching the accumulator comes
// from mutation.Decode's registry and the one reaching the read from the
// caller's request, and nothing makes those the same instance. Matching by value
// would answer a legitimate read with nothing, which reads like an empty window.
func TestACategoryIsMatchedByItsID(t *testing.T) {
	a := fold.New(shard)
	add(t, a, mkUpdate(runX, 2, withTasks(keyed(10, "transfer"))))

	// Differing in a field the id does not cover: a twin built from all three of
	// them is `==` to the original, so a match by value would pass this too.
	twin := tasks.NewCategory(
		tasks.CategoryTransfer.ID(), tasks.CategoryTransfer.Type(), "transfer@another-registry")
	require.Equal(t, []string{"transfer"}, names(a.Tasks(twin)))
}

// everyBarrier builds a window crossing every barrier a task could be lost at —
// a snapshot replacing a run's whole state (I8), a tombstone collapsing a run
// whose state carried tasks, and a fresh run behind that tombstone — and holding
// a task in each of the three homes.
func everyBarrier(t *testing.T) *fold.Accumulator {
	t.Helper()
	a := fold.New(shard)
	add(t, a,
		mkCreate(runX, snapTasks(keyed(10, "create"))),
		mkUpdate(runX, 2, withTasks(keyed(11, "update-before-set"), timerAt(time.Minute, 12, "timer"))),
		mkSet(runX, 3, snapTasks(keyed(13, "set"))),
		mkUpdate(runY, 2, withTasks(keyed(20, "doomed"))),
		mkDelete(runY),
		mkCreate(runY, snapTasks(keyed(21, "reborn"))),
		mkAddTasks(keyed(30, "added"), timerAt(2*time.Minute, 31, "added timer")),
	)
	return a
}

// written is every task of one category the batch will put in the cold store,
// gathered from each home a task can be written out of.
func written(b fold.Batch, category tasks.Category) []p.InternalHistoryTask {
	var out []p.InternalHistoryTask
	for _, e := range reqs(b) {
		out = append(out, requestTasks(e.Request)[category]...)
		out = append(out, e.OrphanedTasks()[category]...)
	}
	return append(out, b.Tasks().Insert[category]...)
}

// TestTheWindowShowsExactlyTheTasksItWillWrite: what a reader is shown is
// neither more nor less than what the drain will put in the cold store. One the
// window holds but does not show is one the reader acks past; one it shows but
// never writes is an acked task that reaches no store at all. Each side reaches
// its homes by its own route — the read through the accumulator, the write
// through the batch — so the equality is what says the two routes name the same
// set.
func TestTheWindowShowsExactlyTheTasksItWillWrite(t *testing.T) {
	for _, category := range []tasks.Category{tasks.CategoryTransfer, tasks.CategoryTimer} {
		t.Run(category.Name(), func(t *testing.T) {
			a := everyBarrier(t)
			shown := multiset(a.Tasks(category))
			require.NotEmpty(t, shown, "the window holds tasks of this category and showed none")

			// Drained last, because draining empties the window.
			require.Equal(t, shown, multiset(written(a.Drain(), category)),
				"the window showed a different set of tasks than the drain wrote")
		})
	}
}

// TestAContinueAsNewsSecondSlotIsShown: the new run's snapshot is adopted under
// the update's envelope, so a reader walking the runs instead of the request's
// task slots would show its tasks twice, or not at all.
func TestAContinueAsNewsSecondSlotIsShown(t *testing.T) {
	a := fold.New(shard)
	req := mkUpdate(runX, 2, withTasks(keyed(10, "current")))
	newRun := snapshot(runY, 1)
	newRun.Tasks = taskMap(keyed(11, "new-run"))
	req.Update.NewWorkflowSnapshot = &newRun
	add(t, a, req)

	require.Equal(t, []string{"current", "new-run"}, names(a.Tasks(tasks.CategoryTransfer)))
}

// TestAConflictResolvesThreeSlotsAreShown is the same at the request carrying
// the most slots: a reset snapshot, a new run and the current run's mutation.
func TestAConflictResolvesThreeSlotsAreShown(t *testing.T) {
	a := fold.New(shard)
	req := mkConflictResolve(runX, 2, snapTasks(keyed(10, "reset")))
	resolveNew := snapshot(runY, 1)
	resolveNew.Tasks = taskMap(keyed(11, "new-run"))
	req.ConflictResolve.NewWorkflowSnapshot = &resolveNew
	req.ConflictResolve.CurrentWorkflowMutation = &p.InternalWorkflowMutation{
		NamespaceID: nsID, WorkflowID: wfID, RunID: "run-z",
		ExecutionState: &persistencespb.WorkflowExecutionState{RunId: "run-z"},
		Tasks:          taskMap(keyed(12, "current")),
	}
	add(t, a, req)

	require.Equal(t, []string{"reset", "new-run", "current"}, names(a.Tasks(tasks.CategoryTransfer)))
}

// TestTheTaskViewIsReadOnlyOnTheAccumulator is this path's half of the rule the
// overlay and the condition authority also obey, with a second edge: no page
// handed to a reader may alias the accumulator's slices.
//
// The first sub-test probes that directly rather than through a scenario,
// because fold appends to a task list and reassigns it and never overwrites an
// element in place: an aliased page is invisible today and becomes a rewritten
// answer the first time some later rule writes one.
func TestTheTaskViewIsReadOnlyOnTheAccumulator(t *testing.T) {
	build := func() *fold.Accumulator {
		a := fold.New(shard)
		add(t, a,
			mkUpdate(runX, 2, withTasks(keyed(10, "first"))),
			mkUpdate(runX, 3, withTasks(keyed(20, "second"))),
			mkUpdate(runX, 4, withTasks(keyed(30, "third"))),
		)
		return a
	}
	// A set is the fold that rewrites a run's task map wholesale: the snapshot
	// supersedes the pending update, whose tasks merge into the snapshot's own.
	supersede := func(a *fold.Accumulator) {
		require.NoError(t, a.Add(4, mkSet(runX, 5, snapTasks(keyed(40, "fourth")))))
	}

	t.Run("the page is storage of its own", func(t *testing.T) {
		a := build()
		page := a.Tasks(tasks.CategoryTransfer)
		for i := range page {
			page[i].Blob = blob("scribbled on")
		}

		out := reqs(a.Drain())
		require.Equal(t, map[string]int{"first": 1, "second": 1, "third": 1},
			multiset(requestTasks(out[0].Request)[tasks.CategoryTransfer]),
			"writing into a page reached the window: the read handed out the accumulator's own slice")
	})

	t.Run("a later fold cannot rewrite a page already handed out", func(t *testing.T) {
		a := build()
		page := a.Tasks(tasks.CategoryTransfer)
		supersede(a)
		require.Equal(t, []string{"first", "second", "third"}, names(page))
	})

	t.Run("and so is the page the merge built", func(t *testing.T) {
		// [Accumulator.TaskPage] merges over the copy [Accumulator.Tasks] hands
		// it, so the page comes back as storage of its own whichever branch built
		// it. The base is empty here, so every row of the page came out of the
		// window.
		a := build()
		empty := func(int, []byte) ([]p.InternalHistoryTask, []byte, error) { return nil, nil, nil }
		resp, _, err := a.TaskPage(taskReq(tasks.CategoryTransfer,
			tasks.NewImmediateKey(0), tasks.NewImmediateKey(100), 100), empty)
		require.NoError(t, err)
		require.Equal(t, []string{"first", "second", "third"}, names(resp.Tasks))
		for i := range resp.Tasks {
			resp.Tasks[i].Blob = blob("scribbled on")
		}

		out := reqs(a.Drain())
		require.Equal(t, map[string]int{"first": 1, "second": 1, "third": 1},
			multiset(requestTasks(out[0].Request)[tasks.CategoryTransfer]),
			"writing into a merged page reached the window: the merge handed out the accumulator's own slice")
	})

	t.Run("and the window a drain emits does not know it was read", func(t *testing.T) {
		quiet := build()
		supersede(quiet)
		loud := build()
		loud.Tasks(tasks.CategoryTransfer)
		supersede(loud)

		quietOut := reqs(quiet.Drain())
		loudOut := reqs(loud.Drain())
		require.Equal(t,
			multiset(requestTasks(quietOut[0].Request)[tasks.CategoryTransfer]),
			multiset(requestTasks(loudOut[0].Request)[tasks.CategoryTransfer]),
			"the drain of a window that was read differs from the drain of one that was not")
	})
}

// requestTasks flattens every task slot of an emitted request into one map, the
// way apply hands them to the plugin. The slots are the record format's;
// mutation's TestEveryRequestShapesTaskSlotsAreNamed is what claims the
// enumeration names them all.
func requestTasks(m mutation.Mutation) map[tasks.Category][]p.InternalHistoryTask {
	out := make(map[tasks.Category][]p.InternalHistoryTask)
	for _, slot := range m.TaskSlots() {
		for category, list := range *slot {
			out[category] = append(out[category], list...)
		}
	}
	return out
}
