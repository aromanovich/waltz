package fold

// The window's tasks as a reader sees them. Read-only on the accumulator, and
// no slice of it is handed out: mergeTasks appends into these maps, so an
// aliased task list would change under the reader at the next fold.
//
// It shows every place [Accumulator.taskRows] names, the same slices
// [Accumulator.Drain] emits. That equality keeps I7 true across a window:
// tasks concatenate over the I8 snapshot barrier, and a tombstone keeps the
// dropped run's tasks as [Emitted.OrphanedTasks].
//
// No index: a window holds tens of tasks per category, and an index that
// missed a task would make it invisible to a reader and acked past.

import (
	"slices"

	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
)

// Tasks returns every task the window holds for one category, ascending by key,
// in a new slice.
//
// Categories match by [tasks.Category.ID], not by value: reader and writer may
// hold different instances from a registry. Blobs are shared with the
// accumulator, which is safe because fold never writes through a
// *commonpb.DataBlob.
func (a *Accumulator) Tasks(category tasks.Category) []p.InternalHistoryTask {
	var out []p.InternalHistoryTask
	for home := range a.taskRows() {
		out = appendCategory(out, category, *home)
	}
	slices.SortFunc(out, func(a, b p.InternalHistoryTask) int { return a.Key.CompareTo(b.Key) })
	return out
}

// taskRanges returns this category's undrained range deletes, which
// [Accumulator.TaskPage] applies to the cold store's rows only (covered window
// tasks were already dropped). The slice is the accumulator's: do not write it.
func (a *Accumulator) taskRanges(category tasks.Category) []TaskRange {
	t := a.ranges[int32(category.ID())]
	if t == nil {
		return nil
	}
	return t.ranges
}

// appendCategory copies one category's tasks out of a request's task map.
func appendCategory(
	dst []p.InternalHistoryTask,
	category tasks.Category,
	byCategory map[tasks.Category][]p.InternalHistoryTask,
) []p.InternalHistoryTask {
	for held, list := range byCategory {
		if held.ID() == category.ID() {
			dst = append(dst, list...)
		}
	}
	return dst
}
