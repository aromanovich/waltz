package fold

// The history-task path through the window: the rows an AddHistoryTasks writes
// and the ranges a RangeCompleteHistoryTasks declares garbage. A range folding
// in drops every task the window already holds inside it; a task arriving
// after a range is kept, because the sequential path keeps it too. Pending
// ranges die at the drain that applies them.
//
// [TaskRange.Covers] is the store's own DELETE predicate, and it is the single
// answer to what the cold store loses, what the window drops and what a merged
// read hides.

import (
	"cmp"
	"iter"
	"maps"
	"time"

	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"

	"github.com/aromanovich/waltz/wal"
)

// TaskRange is one range delete the window carries, in the caller's own terms.
type TaskRange struct {
	Category     tasks.Category
	InclusiveMin tasks.Key
	ExclusiveMax tasks.Key
}

// TaskWork is the window's shard-level history-task work: the rows one drain
// inserts and the ranges it deletes. Separate from [Emitted] because a task
// names no run, asserts nothing and is keyed by (shard, category, key) alone.
type TaskWork struct {
	// Insert are the rows an AddHistoryTasks put into the window, by category.
	// The workflow those requests named is dropped; the store below ignores it.
	Insert map[tasks.Category][]p.InternalHistoryTask
	// Delete are the window's range deletes, merged where they join. Each costs
	// one statement at the drain.
	Delete []TaskRange
	// TailSeqno is the last task mutation folded in; the drain's watermark must
	// be at or above it. No HeadSeqno beside it, unlike [Emitted]: task work
	// asserts nothing, so a failed drain has no partial-apply cut to take.
	TailSeqno wal.Seqno
	// Dropped is how many task rows a range removed from this window and Written
	// how many the drain writes, per category. Two counts rather than a share, so
	// "everything was dropped" stays distinguishable from "there were no tasks".
	Dropped map[string]int
	Written map[string]int
}

// Empty reports work a drain need not carry. Counters alone do not make a
// [TaskWork] non-empty.
func (w TaskWork) Empty() bool { return len(w.Insert) == 0 && len(w.Delete) == 0 }

// Covers reports that this range removes the key under the store's own
// predicate: an immediate category is ranged on task_id, a scheduled one on
// task_visibility_ts with its task ids not looked at. The asymmetry is
// reproduced rather than repaired: covering less than the delete removes
// answers a read with a row that is already gone, covering more hides a row
// nothing will ever delete.
func (r TaskRange) Covers(key tasks.Key) bool {
	return cmpBound(r.Category, r.InclusiveMin, key) <= 0 &&
		cmpBound(r.Category, key, r.ExclusiveMax) < 0
}

// cmpBound orders two keys the way the store does for this category. What a
// range covers, whether two join and which maximum wins all go through it.
func cmpBound(category tasks.Category, a, b tasks.Key) int {
	if category.Type() == tasks.CategoryTypeImmediate {
		return cmp.Compare(a.TaskID, b.TaskID)
	}
	return stored(a.FireTime).Compare(stored(b.FireTime))
}

// storedResolution is the resolution a fire time has once it is a row:
// microseconds, which is what a timestamp column holds in every store this has
// been run against. Comparing finer is a different predicate, not a stricter
// one: a range maximum a nanosecond above a task's fire time covers that task
// here and nothing in the store, losing a row the sequential path keeps. A
// constant because this package names no store.
const storedResolution = time.Microsecond

func stored(t time.Time) time.Time { return t.Truncate(storedResolution) }

// rangeAcc is one category's undrained range deletes: the deletes this window
// still owes the cold store, merged where they join. The drain that applies
// them empties them.
type rangeAcc struct {
	category tasks.Category
	ranges   []TaskRange
}

// taskRows enumerates every place a task row lives in this window: each pending
// request's task slots, its orphaned tasks, and the rows an AddHistoryTasks put
// in beside the workflows. Three shapes, one walk, because the read, a range's
// sweep and the drain's count must reach the same set — a row the read misses is
// one a queue completes and acks past, so it is a lost timer rather than a stale
// answer, and a reader may not be able to tell which door a row came through.
//
// What it yields is addressable because the sweep replaces the maps rather than
// writing through them: a slice already handed to a reader is not this
// accumulator's to edit.
func (a *Accumulator) taskRows() iter.Seq[*map[tasks.Category][]p.InternalHistoryTask] {
	return func(yield func(*map[tasks.Category][]p.InternalHistoryTask) bool) {
		for _, w := range a.workflows {
			for _, pr := range w.pending {
				// The slots come from the record format's own enumeration
				// (mutation.TaskSlots), so a request shape added there is
				// reached here without an edit.
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
		// A category carrying no rows files no home: a key with an empty list
		// would make the drain's task work non-empty, and a transaction that
		// writes nothing is what [TaskWork.Empty] exists to spare the store.
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
// for the drain to apply, and everything the window already holds inside it
// goes — out of every home [Accumulator.taskRows] names and not just the rows
// an AddHistoryTasks put in, a mutable-state write's own task map holding most
// of them.
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

// addRange records a range to apply, merging it with the last one when the two
// join or overlap. A gap between two ranges is kept rather than closed:
// closing it would delete rows nobody asked to be gone.
//
// Joining is tested at both ends. A queue's checkpoints rise, so in practice a
// range extends the last one and nothing else — but a range lying wholly below
// it neither joins nor overlaps, and testing only its minimum against the last
// maximum would take it into the merge, find nothing to extend, and drop a
// delete that has already been acked.
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

// countDropped is where every drop in this package is counted, so no call site
// carries a per-category map of its own. n == 0 is a no-op.
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
		Dropped:   a.tasksDropped,
		Insert:    a.addedTasks,
	}

	for _, t := range a.ranges {
		work.Delete = append(work.Delete, t.ranges...)
		t.ranges = nil
	}
	a.addedTasks = nil
	a.taskTail, a.tasksDropped = 0, nil
	return work
}

// filterTaskMap removes the covered tasks from one home. Maps and slices are
// replaced rather than written through, since a slice handed to a reader must
// not change under it.
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
		var kept []p.InternalHistoryTask
		dropped := 0
		for i, task := range list {
			if !covered(category, task.Key) {
				if kept != nil {
					kept = append(kept, task)
				}
				continue
			}
			dropped++
			if kept == nil {
				kept = append(make([]p.InternalHistoryTask, 0, len(list)-1), list[:i]...)
			}
		}
		if kept == nil {
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

// countTasks is the drain's Written half: every task row the transaction will
// write, per category. Taken over the window and therefore before
// [Accumulator.drainTasks] empties it — after that the rows an AddHistoryTasks
// put in are the batch's, and the count would silently be short of them.
func (a *Accumulator) countTasks() map[string]int {
	written := map[string]int{}
	for home := range a.taskRows() {
		for category, list := range *home {
			written[category.Name()] += len(list)
		}
	}
	if len(written) == 0 {
		return nil
	}
	return written
}
