package fold_test

// The merged task read against a stream: the rules taskpage_test.go states one
// case at a time, checked at every drain point of a 20 000-mutation corpus, for
// all four categories, at three batch sizes.
//
// Four properties, each mapping to a failure upstream:
//
//   - every key inside the requested range. queues/slice.go:373 is a literal
//     panic on a key outside it, with no recover in the reader loop;
//   - strictly ascending across page boundaries. collection.PagingIterator hands
//     pages straight on, and queues/iterator.go silently skips what does not
//     ascend, so that task is never asked for again;
//   - the pages concatenate to the sorted union of the two sources. Less is a
//     task that never fires, more is one that fires twice;
//   - no page longer than BatchSize, which ExecutionMutableStateTaskSuite
//     asserts itself.
//
// The union is computed from what the two sources hold, never from what the
// base's own pagination returns. The merge carries the base's token verbatim,
// so a claim built out of the observed pages would assert this layer against
// whatever the store below does with one — and would go green by matching a
// store that dropped a row, which is the direction that matters here.
//
// The reader is owed the two sources minus the ranges the window has not applied
// yet, so the cold store here applies a range when the drain does.

import (
	"fmt"
	"math"
	"os"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"

	"github.com/aromanovich/waltz/fold"
	"github.com/aromanovich/waltz/internal/verify/coldtasks"
	"github.com/aromanovich/waltz/internal/verify/mutgen"
	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/wal"
)

const (
	// The drain policy the numbers are measured at: [cycle.Defaults]' window.
	taskCorpusWindow    = 256
	taskCorpusBytes     = 256 << 10
	taskCorpusMutations = 20_000
	taskCorpusSeed      = 20260801
	// envTaskCorpusMutations raises the stream length for a longer run.
	envTaskCorpusMutations = "WAL_TASK_MERGE_MUTATIONS"
	// batchSize is history.transferTaskBatchSize and history.timerTaskBatchSize,
	// both 100 in temporal's dynamic config.
	batchSize = 100
)

var taskCorpusCategories = []tasks.Category{
	tasks.CategoryTransfer,
	tasks.CategoryTimer,
	tasks.CategoryVisibility,
	tasks.CategoryReplication,
}

// taskCorpus is one pass of a stream through the accumulator with the cold store
// following one drain behind it, and a simulated queue reading at every drain
// point, the moment a read has the most to merge.
type taskCorpus struct {
	acc  *fold.Accumulator
	cold *coldtasks.Store

	mutations int
	windows   int
	refusals  int
	// readPoints is how many times the queue paginated a category, and pages how
	// many pages that took.
	readPoints int
	pages      int
	// cost is the page stats summed over the run.
	cost fold.TaskPageStats
	// perWindow[categoryID] is the task count of every window that had any.
	perWindow map[int32][]int
	// surfaced counts window tasks a read answered before their row existed.
	surfaced int
	// pending[categoryID] are the range deletes this window still owes the cold
	// store, the set [Accumulator.TaskPage] subtracts from the base's page.
	// Tracked from the stream rather than read off the accumulator, so the
	// expectation is not computed by the code under test.
	pending map[int32][]fold.TaskRange
}

func newTaskCorpus() *taskCorpus {
	return &taskCorpus{
		acc:       fold.New(shard),
		cold:      coldtasks.New(),
		perWindow: make(map[int32][]int),
		pending:   make(map[int32][]fold.TaskRange),
	}
}

// drive folds the stream, calling at() just before every drain.
func (r *taskCorpus) drive(t *testing.T, cfg mutgen.Config, n int, at func()) {
	t.Helper()
	g, err := mutgen.New(cfg)
	require.NoError(t, err)
	registry := tasks.NewDefaultTaskCategoryRegistry()

	inWindow, bytesIn := 0, 0
	drain := func() {
		if at != nil {
			at()
		}
		for _, category := range taskCorpusCategories {
			if held := r.acc.Tasks(category); len(held) > 0 {
				r.perWindow[int32(category.ID())] = append(r.perWindow[int32(category.ID())], len(held))
			}
		}
		batch := r.acc.Drain()
		out, work := reqs(batch), batch.Tasks()
		// What the drain's transaction does, in the plugin's own order: every
		// delete before every upsert.
		for _, rng := range work.Delete {
			r.cold.Remove(rng.Category, rng.Covers)
		}
		landing := make(map[tasks.Category][]p.InternalHistoryTask)
		for _, e := range out {
			for category, list := range emittedTasks(e) {
				landing[category] = append(landing[category], list...)
			}
		}
		r.cold.Commit(landing)
		clear(r.pending)
		r.windows++
		inWindow, bytesIn = 0, 0
	}

	for i := range n {
		m, err := g.Next()
		require.NoError(t, err, "generating mutation %d of seed %d", i, cfg.Seed)
		payload, err := mutation.Encode(m)
		require.NoError(t, err, "encoding mutation %d of seed %d", i, cfg.Seed)
		decoded, err := mutation.Decode(payload, registry)
		require.NoError(t, err, "decoding mutation %d of seed %d", i, cfg.Seed)

		seqno := wal.Seqno(i + 1)
		refusal, err := r.acc.AddOrDrain(seqno, decoded, func() error { drain(); return nil })
		require.NoError(t, err, "mutation %d", i)
		if refusal.Drained {
			r.refusals++
		}
		// Recorded after the fold and not before it: a refusal drains, and a range
		// noted before that drain would be one this window no longer owes.
		if rc := decoded.RangeCompleteTasks; rc != nil {
			id := int32(rc.TaskCategory.ID())
			r.pending[id] = append(r.pending[id], fold.TaskRange{
				Category:     rc.TaskCategory,
				InclusiveMin: rc.InclusiveMinTaskKey,
				ExclusiveMax: rc.ExclusiveMaxTaskKey,
			})
		}
		r.mutations++
		inWindow++
		bytesIn += len(payload)
		if inWindow >= taskCorpusWindow || bytesIn >= taskCorpusBytes {
			drain()
		}
	}
	drain()
}

// hidden reports that an undrained range covers this key, which is what the
// reader is owed and the cold store does not know yet.
func (r *taskCorpus) hidden(category tasks.Category, key tasks.Key) bool {
	for _, rng := range r.pending[int32(category.ID())] {
		if rng.Covers(key) {
			return true
		}
	}
	return false
}

// emittedTasks is what apply hands the plugin for one merged request: the
// request's own task slots plus a tombstone collapse's orphans.
func emittedTasks(e *fold.Emitted) map[tasks.Category][]p.InternalHistoryTask {
	out := requestTasks(e.Request)
	for category, list := range e.OrphanedTasks() {
		out[category] = append(out[category], list...)
	}
	return out
}

// widestRange is the widest range this category's queue could ask for, in the
// shape validateTaskRange demands.
func widestRange(category tasks.Category) (tasks.Key, tasks.Key) {
	if category.Type() == tasks.CategoryTypeImmediate {
		return immediateRange()
	}
	return scheduledRange()
}

// resumeFrom turns a queue's cursor into a bound a request may actually carry.
// validateTaskRange rejects a task id on a scheduled request, so a scheduled
// reader resumes at a fire time and re-reads whatever else shares it, which
// scheduledQueue trims off its own front (queue_scheduled.go:76-78).
func resumeFrom(category tasks.Category, cursor tasks.Key) tasks.Key {
	if category.Type() == tasks.CategoryTypeImmediate {
		return cursor
	}
	return tasks.NewKey(cursor.FireTime, 0)
}

// sortedUnion is what a merged read must equal: the cold store's rows in the
// range, less the ones an undrained range covers, plus the window's own. From
// the rows and not the pagination, for the reason the file comment gives.
func (r *taskCorpus) sortedUnion(
	window []p.InternalHistoryTask, category tasks.Category, minKey, maxKey tasks.Key,
) []p.InternalHistoryTask {
	inRange := func(k tasks.Key) bool {
		return k.CompareTo(minKey) >= 0 && k.CompareTo(maxKey) < 0
	}
	var all []p.InternalHistoryTask
	for _, row := range r.cold.Rows[int32(category.ID())] {
		key := row.Key
		if category.Type() == tasks.CategoryTypeImmediate {
			key = tasks.NewImmediateKey(row.Key.TaskID)
		}
		if inRange(key) && !r.hidden(category, key) {
			all = append(all, p.InternalHistoryTask{Key: key, Blob: row.Blob})
		}
	}
	for _, task := range window {
		if inRange(task.Key) {
			all = append(all, task)
		}
	}
	slices.SortFunc(all, func(a, b p.InternalHistoryTask) int { return a.Key.CompareTo(b.Key) })
	return all
}

func keysOf(list []p.InternalHistoryTask) [][2]int64 {
	out := make([][2]int64, 0, len(list))
	for _, task := range list {
		out = append(out, [2]int64{task.Key.FireTime.UnixNano(), task.Key.TaskID})
	}
	return out
}

func taskCorpusConfig() mutgen.Config {
	cfg := mutgen.Default()
	cfg.Seed = taskCorpusSeed
	cfg.ShardID = int32(shard)
	// The locality cap: 32 concurrently-hot workflows is a shard whose hot set
	// turns over faster than it drains, which is what puts anything in a window.
	cfg.Workflows = 32
	return cfg
}

func taskCorpusLength(t *testing.T) int {
	t.Helper()
	if s := os.Getenv(envTaskCorpusMutations); s != "" {
		parsed, err := strconv.Atoi(s)
		require.NoError(t, err, "%s must be a number, got %q", envTaskCorpusMutations, s)
		require.Positive(t, parsed)
		return parsed
	}
	return taskCorpusMutations
}

// TestTheMergedReadIsTheSortedUnion is the correctness claim, checked at every
// drain point of the stream, for every category, at three batch sizes: 1 because
// scheduledQueue's look-ahead asks for it, 100 because both queues poll with it,
// and 7 because a divisor nobody's arithmetic is tuned to is where an off-by-one
// in the cut shows up. The reader acks everything it reads, which is what makes
// these read points cost what a queue's do.
func TestTheMergedReadIsTheSortedUnion(t *testing.T) {
	cfg := taskCorpusConfig()
	n := taskCorpusLength(t)

	for _, batch := range []int{1, 7, batchSize} {
		t.Run(fmt.Sprintf("batch=%d", batch), func(t *testing.T) {
			r := newTaskCorpus()
			cursor := make(map[int32]tasks.Key)
			r.drive(t, cfg, n, func() {
				for _, category := range taskCorpusCategories {
					id := int32(category.ID())
					minKey, maxKey := widestRange(category)
					if k, ok := cursor[id]; ok {
						minKey = resumeFrom(category, k)
					}
					req := taskReq(category, minKey, maxKey, batch)
					window := r.acc.Tasks(category)
					pages, c := paginate(t, r.acc, r.cold, req)
					r.cost.Add(c)
					r.readPoints++
					r.pages += len(pages)
					for _, task := range window {
						if task.Key.CompareTo(minKey) >= 0 {
							r.surfaced++
						}
					}

					var got []p.InternalHistoryTask
					for _, page := range pages {
						require.LessOrEqual(t, len(page), batch, "%s: a page longer than BatchSize", category.Name())
						got = append(got, page...)
					}
					for i, task := range got {
						require.GreaterOrEqual(t, task.Key.CompareTo(minKey), 0,
							"%s: a key below the requested range", category.Name())
						require.Less(t, task.Key.CompareTo(maxKey), 0,
							"%s: a key at or above the requested range", category.Name())
						if i > 0 {
							require.Positive(t, task.Key.CompareTo(got[i-1].Key),
								"%s: keys must strictly ascend across pages", category.Name())
						}
					}
					require.Equal(t,
						keysOf(r.sortedUnion(window, category, minKey, maxKey)), keysOf(got),
						"%s: the merged read is not the sorted union of the two sources", category.Name())

					// The next read of this category starts above the last key
					// returned.
					if len(got) > 0 {
						cursor[id] = got[len(got)-1].Key.Next()
					}
				}
			})

			require.NotZero(t, r.cost.FromWindow,
				"not one task was answered out of the window: this run proves only that the base still works")
			t.Logf("%d mutations, %d windows (%d refusals), %d read points, %d pages: "+
				"%d base calls (%d rows, %d discarded), %d window entries touched, "+
				"%d comparisons, %d collisions, %d tasks answered from the window "+
				"(%d surfaced ahead of their rows)",
				r.mutations, r.windows, r.refusals, r.readPoints, r.pages,
				r.cost.BaseCalls, r.cost.BaseRows, r.cost.BaseDiscarded, r.cost.WindowTouched,
				r.cost.Comparisons, r.cost.Collisions, r.cost.FromWindow, r.surfaced)
		})
	}
}

// TestEveryImmediateKeyIsNormalised pins the upstream fact that lets one
// comparator serve both category types: every immediate-category task type's
// GetKey() returns NewImmediateKey, whose fire time is the constant
// tasks.DefaultFireTime, and the plugin reconstructs that on the way out because
// the column is NULL for immediate rows. The merge normalises the range and
// takes tasks as they come, so a type putting a real fire time on an immediate
// key would hand the queue a key outside the range asked for, which
// queues/slice.go:373 panics on.
func TestEveryImmediateKeyIsNormalised(t *testing.T) {
	r := newTaskCorpus()
	seen := 0
	r.drive(t, taskCorpusConfig(), taskCorpusMutations/4, func() {
		for _, category := range taskCorpusCategories {
			if category.Type() != tasks.CategoryTypeImmediate {
				continue
			}
			for _, task := range r.acc.Tasks(category) {
				require.True(t, task.Key.FireTime.Equal(tasks.DefaultFireTime),
					"%s task %d carries fire time %v", category.Name(), task.Key.TaskID, task.Key.FireTime)
				seen++
			}
		}
	})
	require.Positive(t, seen)
	t.Logf("%d immediate-category window tasks, every one keyed (DefaultFireTime, TaskID)", seen)
}

// TestWhatAMergedReadCosts is counts only: a sandbox timing would describe the
// sandbox. It answers the "is a heap warranted" question — a window holds tens
// of tasks per category, so the sort is nothing and the scan is the cost, while
// an index would be a second structure reproducing the window's whole lifecycle.
// The numbers are printed rather than asserted, except the one below.
func TestWhatAMergedReadCosts(t *testing.T) {
	r := newTaskCorpus()
	cursor := make(map[int32]tasks.Key)
	r.drive(t, taskCorpusConfig(), taskCorpusLength(t), func() {
		for _, category := range taskCorpusCategories {
			id := int32(category.ID())
			minKey, maxKey := widestRange(category)
			if k, ok := cursor[id]; ok {
				minKey = resumeFrom(category, k)
			}
			pages, c := paginate(t, r.acc, r.cold, taskReq(category, minKey, maxKey, batchSize))
			r.cost.Add(c)
			r.readPoints++
			r.pages += len(pages)
			if last := pages[len(pages)-1]; len(last) > 0 {
				cursor[id] = last[len(last)-1].Key.Next()
			}
		}
	})

	t.Logf("\n=== %d mutations, %d windows (%d refusals), %d read points at BatchSize %d",
		r.mutations, r.windows, r.refusals, r.readPoints, batchSize)
	for _, category := range taskCorpusCategories {
		list := r.perWindow[int32(category.ID())]
		if len(list) == 0 {
			continue
		}
		sorted := slices.Clone(list)
		slices.Sort(sorted)
		sum := 0
		for _, v := range sorted {
			sum += v
		}
		t.Logf("%-11s window tasks: n=%d min=%d p50=%d p95=%d max=%d mean=%.1f | sort at p50 ≈ %.0f comparisons",
			category.Name(), len(sorted), sorted[0], sorted[len(sorted)/2],
			sorted[len(sorted)*95/100], sorted[len(sorted)-1],
			float64(sum)/float64(len(sorted)),
			float64(sorted[len(sorted)/2])*math.Log2(float64(sorted[len(sorted)/2]+1)))
	}
	t.Logf("per page: %.2f base calls, %.1f base rows (%d discarded over the run), "+
		"%.1f window entries touched, %.2f comparisons; %d collisions, %d tasks answered from the window",
		float64(r.cost.BaseCalls)/float64(r.pages), float64(r.cost.BaseRows)/float64(r.pages),
		r.cost.BaseDiscarded, float64(r.cost.WindowTouched)/float64(r.pages),
		float64(r.cost.Comparisons)/float64(r.pages), r.cost.Collisions, r.cost.FromWindow)

	// A run where the window answered nothing would report the same shape as a
	// merge that does not work.
	require.NotZero(t, r.cost.FromWindow)
}
