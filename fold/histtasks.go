package fold

// History tasks in the window: rows from AddHistoryTasks and ranges deleted by
// RangeCompleteHistoryTasks. A range drops every task the window already holds
// inside it; a task arriving after the range is kept, as the sequential path
// keeps it. Pending ranges are cleared by the drain that applies them.
//
// [TaskRange.Covers] is the store's DELETE predicate and the only answer to
// what the cold store loses, what the window drops and what a merged read hides.

import (
	"cmp"
	"iter"
	"maps"
	"time"

	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"

	"github.com/aromanovich/waltz/wal"
)

// TaskRange is one range delete the window carries, in the caller's terms.
type TaskRange struct {
	Category     tasks.Category
	InclusiveMin tasks.Key
	ExclusiveMax tasks.Key
}

// TaskWork is the window's shard-level history-task work: the rows one drain
// inserts and the ranges it deletes. Separate from [Emitted] because a task
// names no run, asserts nothing and is keyed by (shard, category, key) alone.
type TaskWork struct {
	// Insert is the AddHistoryTasks rows, by category. The workflow those
	// requests named is dropped; the store ignores it.
	Insert map[tasks.Category][]p.InternalHistoryTask
	// Delete is the range deletes, merged where they join; one statement each.
	Delete []TaskRange
	// TailSeqno is the last task mutation folded in; the drain's watermark must
	// be at or above it. There is no HeadSeqno: task work asserts nothing, so
	// a failed drain has no partial-apply cut.
	TailSeqno wal.Seqno
	// Counts has one row per category touched. One table rather than two maps,
	// so a consumer cannot mis-join a category present in only one.
	Counts map[string]TaskCounts
}

// Empty reports work a drain need not carry. Counts alone do not make it
// non-empty.
func (w TaskWork) Empty() bool { return len(w.Insert) == 0 && len(w.Delete) == 0 }

// TaskCounts is, for one category, the task rows a range dropped from the
// window and the rows the drain writes. Two counts, not a ratio, so "all
// dropped" differs from "no tasks".
type TaskCounts struct{ Dropped, Written int }

// Covers reports whether this range removes key under the store's predicate:
// immediate categories range on task_id, scheduled ones on task_visibility_ts
// ignoring task ids. The asymmetry is reproduced deliberately: covering less
// than the store's delete returns rows already gone, covering more hides rows
// nothing will delete.
func (r TaskRange) Covers(key tasks.Key) bool {
	return cmpBound(r.Category, r.InclusiveMin, key) <= 0 &&
		cmpBound(r.Category, key, r.ExclusiveMax) < 0
}

// cmpBound orders two keys as the store does for this category. Coverage,
// range joining and max selection all go through it.
func cmpBound(category tasks.Category, a, b tasks.Key) int {
	if category.Type() == tasks.CategoryTypeImmediate {
		return cmp.Compare(a.TaskID, b.TaskID)
	}
	return stored(a.FireTime).Compare(stored(b.FireTime))
}

// storedResolution is a stored fire time's resolution: microseconds, as in
// every store's timestamp column so far. Comparing finer would differ from the
// store: a range maximum a nanosecond above a task's fire time would cover it
// here but not in the store, losing a row the sequential path keeps.
//
// So it is a requirement on the store: scheduled fire times must round-trip at
// microsecond resolution or finer. If coarser, a range maximum truncated here
// covers a task the store's DELETE keeps; [Accumulator.sweepTasks] drops it
// from the window and [hideDeleted] hides its row, so the reader completes the
// range past a task that still exists.
const storedResolution = time.Microsecond

func stored(t time.Time) time.Time { return t.Truncate(storedResolution) }

// rangeAcc is one category's undrained range deletes, merged where they join.
// The drain that applies them empties it.
type rangeAcc struct {
	category tasks.Category
	ranges   []TaskRange
}

// taskRows yields every place a task row lives in the window: each pending
// request's task slots, its orphaned tasks, and the AddHistoryTasks rows. The
// read, a range's sweep and the drain's count must all see the same set: a row
// the read misses is one a queue completes past, a lost timer.
//
// It yields pointers so the sweep can replace the maps instead of writing
// through them; a slice already handed to a reader must not change.
func (a *Accumulator) taskRows() iter.Seq[*map[tasks.Category][]p.InternalHistoryTask] {
	return func(yield func(*map[tasks.Category][]p.InternalHistoryTask) bool) {
		for _, w := range a.workflows {
			for _, pr := range w.pending {
				// mutation.TaskSlots enumerates the slots, so a new request
				// shape is covered without an edit here.
				for _, slot := range pr.m.TaskSlots() {
					if !yield(slot) {
						return
					}
				}
				if !yield(&pr.orphanedTasks) {
					return
				}
			}
		}
		yield(&a.addedTasks)
	}
}

// rangeState returns the category's accumulator, creating it.
func (a *Accumulator) rangeState(category tasks.Category) *rangeAcc {
	id := int32(category.ID())
	t := a.ranges[id]
	if t == nil {
		t = &rangeAcc{category: category}
		a.ranges[id] = t
	}
	return t
}

// addTasks folds one AddHistoryTasks. Nothing is asserted and nothing can
// fail: the drain's transaction makes the epoch assertion anyway (I11).
func (a *Accumulator) addTasks(seqno wal.Seqno, req *p.InternalAddHistoryTasksRequest) {
	for category, list := range req.Tasks {
		// No key for an empty list: it would make [TaskWork.Empty] false and
		// send the store a transaction that writes nothing.
		if len(list) == 0 {
			continue
		}
		if a.addedTasks == nil {
			a.addedTasks = make(map[tasks.Category][]p.InternalHistoryTask, len(req.Tasks))
		}
		a.addedTasks[category] = append(a.addedTasks[category], list...)
	}
	a.markTaskSeqno(seqno)
}

// addRangeCompleteTasks folds one RangeCompleteHistoryTasks: the range is kept
// for the drain, and every window task inside it is dropped from every place
// [Accumulator.taskRows] names, not just the AddHistoryTasks rows (most live in
// mutable-state writes' task maps).
func (a *Accumulator) addRangeCompleteTasks(seqno wal.Seqno, req *p.RangeCompleteHistoryTasksRequest) {
	r := TaskRange{
		Category:     req.TaskCategory,
		InclusiveMin: req.InclusiveMinTaskKey,
		ExclusiveMax: req.ExclusiveMaxTaskKey,
	}
	a.rangeState(req.TaskCategory).addRange(r)
	a.sweepTasks(r)
	a.markTaskSeqno(seqno)
}

// addRange records a range, merging it with the last one when they join or
// overlap. A gap between ranges is kept: closing it would delete rows nobody
// asked to delete.
//
// Joining is tested at both ends. Ranges usually rise, but one wholly below
// the last would pass a min-only test, extend nothing, and drop an acked delete.
func (t *rangeAcc) addRange(r TaskRange) {
	if n := len(t.ranges); n > 0 {
		last := &t.ranges[n-1]
		if cmpBound(t.category, r.InclusiveMin, last.ExclusiveMax) <= 0 &&
			cmpBound(t.category, last.InclusiveMin, r.ExclusiveMax) <= 0 {
			if cmpBound(t.category, r.InclusiveMin, last.InclusiveMin) < 0 {
				last.InclusiveMin = r.InclusiveMin
			}
			if cmpBound(t.category, last.ExclusiveMax, r.ExclusiveMax) < 0 {
				last.ExclusiveMax = r.ExclusiveMax
			}
			return
		}
	}
	t.ranges = append(t.ranges, r)
}

// sweepTasks removes from the whole window every task the arriving range covers.
func (a *Accumulator) sweepTasks(r TaskRange) {
	covered := func(category tasks.Category, key tasks.Key) bool {
		return category.ID() == r.Category.ID() && r.Covers(key)
	}
	for home := range a.taskRows() {
		*home = filterTaskMap(*home, covered, a.countDropped)
	}
}

// countDropped counts every drop in this package. n == 0 is a no-op.
func (a *Accumulator) countDropped(category string, n int) {
	if n == 0 {
		return
	}
	if a.tasksDropped == nil {
		a.tasksDropped = make(map[string]int, 4)
	}
	a.tasksDropped[category] += n
}

// markTaskSeqno records the last seqno the window's task work reaches.
func (a *Accumulator) markTaskSeqno(seqno wal.Seqno) { a.taskTail = seqno }

// drainTasks empties the task half of the window and returns what the drain
// carries.
func (a *Accumulator) drainTasks() TaskWork {
	work := TaskWork{
		TailSeqno: a.taskTail,
		Insert:    a.addedTasks,
		// Counted here, before the window is emptied below.
		Counts: a.taskCounts(),
	}

	for _, t := range a.ranges {
		work.Delete = append(work.Delete, t.ranges...)
		t.ranges = nil
	}
	a.addedTasks = nil
	a.taskTail, a.tasksDropped = 0, nil
	return work
}

// filterTaskMap removes covered tasks from one map, replacing maps and slices
// rather than writing through them: a slice handed to a reader must not change.
func filterTaskMap(
	in map[tasks.Category][]p.InternalHistoryTask,
	covered func(tasks.Category, tasks.Key) bool,
	count func(string, int),
) map[tasks.Category][]p.InternalHistoryTask {
	if len(in) == 0 {
		return in
	}
	var out map[tasks.Category][]p.InternalHistoryTask
	for category, list := range in {
		kept, dropped := keepUncovered(list, func(key tasks.Key) bool { return covered(category, key) })
		if dropped == 0 {
			continue
		}
		count(category.Name(), dropped)
		if out == nil {
			out = make(map[tasks.Category][]p.InternalHistoryTask, len(in))
			maps.Copy(out, in)
		}
		if len(kept) == 0 {
			delete(out, category)
			continue
		}
		out[category] = kept
	}
	if out == nil {
		return in
	}
	return out
}

// keepUncovered returns list without the dropped rows, and how many were
// dropped. It copies only at the first dropped row and otherwise returns list
// itself, never writing through a slice a reader may hold.
func keepUncovered(
	list []p.InternalHistoryTask, drop func(tasks.Key) bool,
) ([]p.InternalHistoryTask, int) {
	var kept []p.InternalHistoryTask
	for i, task := range list {
		if !drop(task.Key) {
			if kept != nil {
				kept = append(kept, task)
			}
			continue
		}
		if kept == nil {
			kept = append(make([]p.InternalHistoryTask, 0, len(list)-1), list[:i]...)
		}
	}
	if kept == nil {
		return list, 0
	}
	return kept, len(list) - len(kept)
}

// taskCounts is the drain's count table: rows the transaction will write and
// rows ranges dropped. It reads the window, so [Accumulator.drainTasks] must
// call it before emptying the window, or Written silently falls short.
func (a *Accumulator) taskCounts() map[string]TaskCounts {
	counts := map[string]TaskCounts{}
	for home := range a.taskRows() {
		for category, list := range *home {
			c := counts[category.Name()]
			c.Written += len(list)
			counts[category.Name()] = c
		}
	}
	for name, dropped := range a.tasksDropped {
		c := counts[name]
		c.Dropped = dropped
		counts[name] = c
	}
	if len(counts) == 0 {
		return nil
	}
	return counts
}
