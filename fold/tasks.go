package fold

// The window's tasks, as a reader sees them. Read-only on the accumulator, and
// no slice of it is handed out: mergeTasks appends into exactly these maps, so
// an aliased task list is a value the next fold changes under the reader.
//
// What it shows is every home [Accumulator.taskRows] names, and the slices are
// the ones [Accumulator.Drain] emits. That equality is what keeps I7 true across
// a window: tasks concatenate over the I8 snapshot barrier, and a tombstone's
// collapse preserves the dropped run's tasks as [Emitted.OrphanedTasks].
//
// There is no index. A window holds tens of tasks per category, so the scan is
// the cost, and an index would have the window's whole lifecycle to reproduce
// where one mistake is a task invisible to a reader and therefore acked past.

import (
	"slices"

	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
)

// Tasks returns every task the window holds for one category, ascending by key,
// in a slice of its own.
//
// The category is matched by [tasks.Category.ID], not by value: a category
// value reaches this package through a registry, so the reader's and the
// writer's need not be the same instance to mean the same queue. The blobs are
// shared with the accumulator, as everywhere on the read path: fold replaces a
// task list, it never writes through a [commonpb.DataBlob].
func (a *Accumulator) Tasks(category tasks.Category) []p.InternalHistoryTask {
	var out []p.InternalHistoryTask
	for home := range a.taskRows() {
		out = appendCategory(out, category, *home)
	}
	slices.SortFunc(out, func(a, b p.InternalHistoryTask) int { return a.Key.CompareTo(b.Key) })
	return out
}

// taskRanges is this category's undrained range deletes, which
// [Accumulator.TaskPage] subtracts from the cold store's half of its answer.
// The window's own half needs none: a covered task was dropped when the range
// folded in. The returned slice is the accumulator's own and must not be
// written to.
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
