package fold_test

// A base that conforms minimally rather than realistically.
//
// internal/verify/coldtasks models the pagination of the plugins this layer is
// actually run against. That is what makes taskpage_corpus_test.go's judgement a
// judgement about a real store, and it is also what makes it blind in one
// direction: a model built from real behaviour satisfies every requirement the
// merge has, including the ones nobody had written down, so a requirement
// missing from [fold.BasePage]'s doc could not be discovered by running against
// it. Three were missing.
//
// This base honours exactly what that doc states and is adversarial in
// everything it is left free to do: it parts with one row at a time whatever it
// was asked for, it hands a token back beside its last row and reports the range
// exhausted only with an empty page, and it can be seeded so that a window task
// ties the last key of a base page. If the merge is correct over every base like
// it, the stated requirements are *sufficient* — which is the property the
// corpus run cannot state, however long it runs.

import (
	"fmt"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"

	"github.com/aromanovich/waltz/fold"
)

// minimalBase answers from a fixed ascending set of rows, under
// [fold.BasePage]'s three requirements and nothing more.
type minimalBase struct {
	// rows is this base's whole content for the category, ascending by key.
	rows []p.InternalHistoryTask
	// page caps how many rows it will part with at once, under whatever it was
	// asked for. One is the adversarial setting and the legal one: nothing
	// requires a page to be full, only that an empty one means the range is
	// exhausted.
	page int
	// trailingToken hands a token back beside the last row rather than reporting
	// exhaustion with it, so the pagination has to survive one more call
	// answering nothing. Real plugins emit a token only on a full page, which is
	// why this shape has never been driven.
	trailingToken bool

	// The three requirements, each with a way to break it. Off, this base keeps
	// all three; on, one of them is broken and everything else is still kept, so
	// what the reader then sees is that requirement's own cost
	// ([TestWhatABaseThatBreaksTheRequirementsCosts]).
	ignoresRange bool
	// repeatsOnce resumes one row back, exactly once, so the pagination still
	// terminates and one key comes back twice.
	repeatsOnce bool
	repeated    bool
	// emptyAfter ends a page with a token and no rows once this many rows have
	// been handed out, while rows remain. Zero is off.
	emptyAfter int
	handed     int
}

// get is the callback the cycle builds. The token is this base's own bytes —
// the merge may not parse them — so an index will do.
func (b *minimalBase) get(req *p.GetHistoryTasksRequest) fold.BasePage {
	return func(batch int, token []byte) ([]p.InternalHistoryTask, []byte, error) {
		from := 0
		if len(token) > 0 {
			parsed, err := strconv.Atoi(string(token))
			if err != nil {
				// Unreachable: the only tokens handed to this base are its own.
				return nil, nil, err
			}
			from = parsed
		}

		if b.emptyAfter > 0 && b.handed >= b.emptyAfter && from < len(b.rows) {
			// Requirement 3 broken: a token that promises more, beside a page that
			// carried nothing.
			return nil, []byte(strconv.Itoa(from)), nil
		}

		var page []p.InternalHistoryTask
		i := from
		for ; i < len(b.rows); i++ {
			// Requirement 1: nothing outside the range the request names. Rows
			// are ascending, so the range is a window over them and skipping one
			// below the minimum keeps the rest ascending — requirement 2.
			if !b.ignoresRange && !inTaskRange(req, b.rows[i].Key) {
				if b.rows[i].Key.CompareTo(req.ExclusiveMaxTaskKey) >= 0 {
					i = len(b.rows)
					break
				}
				continue
			}
			if len(page) == min(b.page, max(batch, 1)) {
				break
			}
			page = append(page, b.rows[i])
		}
		b.handed += len(page)

		// Requirement 3: no rows means the range is exhausted, so a token may only
		// ride a page that carried some.
		if len(page) == 0 {
			return nil, nil, nil
		}
		if i >= len(b.rows) && !b.trailingToken {
			return page, nil, nil
		}
		next := i
		if b.repeatsOnce && !b.repeated && next > 0 {
			// Requirement 2 broken: the next page opens on a key this one already
			// returned. Once, so the pagination still ends.
			b.repeated, next = true, next-1
		}
		return page, []byte(strconv.Itoa(next)), nil
	}
}

func inTaskRange(req *p.GetHistoryTasksRequest, key tasks.Key) bool {
	return key.CompareTo(req.InclusiveMinTaskKey) >= 0 && key.CompareTo(req.ExclusiveMaxTaskKey) < 0
}

// TestTheMergeIsCorrectOverAMinimallyConformingBase drives every arrangement of
// the two sources through every page size and batch size, and holds each
// pagination to the four things a reader is owed.
func TestTheMergeIsCorrectOverAMinimallyConformingBase(t *testing.T) {
	arrangements := map[string]struct{ base, window []int64 }{
		"the base alone":                  {base: []int64{1, 2, 3, 4, 5}},
		"the window alone":                {window: []int64{1, 2, 3, 4, 5}},
		"the window below the base":       {base: []int64{10, 11, 12}, window: []int64{1, 2, 3, 4}},
		"the window above the base":       {base: []int64{1, 2, 3}, window: []int64{10, 11, 12, 13}},
		"interleaved":                     {base: []int64{2, 4, 6, 8}, window: []int64{1, 3, 5, 7, 9}},
		"a tie on the base's first key":   {base: []int64{5, 6, 7}, window: []int64{5, 8}},
		"a tie on every key":              {base: []int64{1, 2, 3}, window: []int64{1, 2, 3}},
		"one row and one window task":     {base: []int64{2}, window: []int64{1}},
		"a long window over a short base": {base: []int64{50}, window: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9}},
	}

	for name, arrangement := range arrangements {
		for _, page := range []int{1, 2} {
			for _, batch := range []int{1, 2, 3, 7} {
				for _, trailing := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/page %d/batch %d/trailing %v", name, page, batch, trailing), func(t *testing.T) {
						a := fold.New(shard)
						if len(arrangement.window) > 0 {
							var held []p.InternalHistoryTask
							for _, id := range arrangement.window {
								held = append(held, keyed(id, "window"))
							}
							add(t, a, mkAddTasks(held...))
						}
						base := &minimalBase{page: page, trailingToken: trailing}
						for _, id := range arrangement.base {
							base.rows = append(base.rows, keyed(id, "cold"))
						}

						minKey, maxKey := immediateRange()
						req := taskReq(tasks.CategoryTransfer, minKey, maxKey, batch)
						pages := paginateOver(t, a, base, req)

						got := assertPagesWellFormed(t, tasks.CategoryTransfer, batch, minKey, maxKey, pages)
						require.Equal(t, sortedTaskIDs(arrangement.base, arrangement.window),
							taskIDs([][]p.InternalHistoryTask{got}),
							"the merged read is not the sorted union of the two sources")
					})
				}
			}
		}
	}
}

// paginateOver drives one merged read to exhaustion over a base of the caller's,
// where [paginate] drives one over the plugin model.
func paginateOver(
	t *testing.T, a *fold.Accumulator, base *minimalBase, req *p.GetHistoryTasksRequest,
) [][]p.InternalHistoryTask {
	t.Helper()
	var pages [][]p.InternalHistoryTask
	ask := *req
	for range 1000 {
		resp, _, err := a.TaskPage(&ask, base.get(&ask))
		require.NoError(t, err)
		pages = append(pages, resp.Tasks)
		if len(resp.NextPageToken) == 0 {
			return pages
		}
		ask.NextPageToken = resp.NextPageToken
	}
	t.Fatal("the pagination did not terminate in 1000 pages")
	return nil
}

// sortedTaskIDs is the union of the two sources' ids, which is what the reader
// is owed: computed from what each source holds, never from what the pagination
// returned.
func sortedTaskIDs(base, window []int64) []int64 {
	seen := map[int64]struct{}{}
	for _, id := range slices.Concat(base, window) {
		seen[id] = struct{}{}
	}
	ids := make([]int64, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

// TestWhatABaseThatBreaksTheRequirementsCosts turns [fold.BasePage]'s three
// requirements from prose into the failure each is there to prevent. The base
// above keeps all three; each base here keeps the other two and breaks one, so
// what reaches the reader is that requirement's own cost and nothing else's.
//
// The merge does not defend against any of them, and the alternatives are worse
// than the panic upstream already raises: dropping a row the store returned
// loses a task, and refusing the page turns a store's defect into a read that
// fails. So the requirement is stated and this is what it is worth.
func TestWhatABaseThatBreaksTheRequirementsCosts(t *testing.T) {
	rows := func(ids ...int64) []p.InternalHistoryTask {
		var out []p.InternalHistoryTask
		for _, id := range ids {
			out = append(out, keyed(id, "cold"))
		}
		return out
	}
	minKey, maxKey := immediateRange()

	t.Run("a row outside the range reaches the reader", func(t *testing.T) {
		base := &minimalBase{rows: rows(1, 2, 3, 20, 21), page: 1, ignoresRange: true}
		// A range that ends below what the base holds, which is every range a
		// queue actually asks for: it reads up to its own checkpoint.
		req := taskReq(tasks.CategoryTransfer, minKey, tasks.NewImmediateKey(10), 2)
		var got []int64
		for _, page := range paginateOver(t, fold.New(shard), base, req) {
			for _, task := range page {
				got = append(got, task.Key.TaskID)
			}
		}
		require.Contains(t, got, int64(20),
			"the merge filtered the base's page by the range after all, and this requirement is not one")
	})

	t.Run("a page repeating an earlier key breaks the ascent", func(t *testing.T) {
		base := &minimalBase{rows: rows(1, 2, 3, 4, 5), page: 1, repeatsOnce: true}
		req := taskReq(tasks.CategoryTransfer, minKey, maxKey, 2)
		var got []int64
		for _, page := range paginateOver(t, fold.New(shard), base, req) {
			for _, task := range page {
				got = append(got, task.Key.TaskID)
			}
		}
		require.False(t, slices.IsSorted(got) && len(slices.Compact(slices.Clone(got))) == len(got),
			"the keys still ascend strictly, so this requirement is not one: %v", got)
	})

	t.Run("a token beside an empty page ends the pagination early", func(t *testing.T) {
		base := &minimalBase{rows: rows(1, 2, 3, 4, 5), page: 1, emptyAfter: 2}
		req := taskReq(tasks.CategoryTransfer, minKey, maxKey, 2)
		var got []int64
		for _, page := range paginateOver(t, fold.New(shard), base, req) {
			for _, task := range page {
				got = append(got, task.Key.TaskID)
			}
		}
		// Which is a queue completing a range over three rows it was never shown.
		require.Equal(t, []int64{1, 2}, got,
			"the pagination went on past the empty page, so this requirement is not one")
	})
}

// TestATieAtTheCutIsEmittedOnce drives the one shape the tie-break at the cut
// exists for, and which nothing else reaches: a base exhausted with rows, a
// window that overflows the page on its own, and a window key equal to the base
// page's first. A cut taken at that key rather than strictly below it emits the
// window's copy and leaves the base's row unread, so the base hands the same key
// back on a later page.
func TestATieAtTheCutIsEmittedOnce(t *testing.T) {
	a := fold.New(shard)
	add(t, a, mkAddTasks(keyed(5, "window"), keyed(6, "window"), keyed(7, "window"), keyed(8, "window")))
	base := &minimalBase{page: 1, rows: []p.InternalHistoryTask{keyed(5, "cold")}}

	minKey, maxKey := immediateRange()
	req := taskReq(tasks.CategoryTransfer, minKey, maxKey, 2)
	got := assertPagesWellFormed(t, tasks.CategoryTransfer, 2, minKey, maxKey,
		paginateOver(t, a, base, req))
	require.Equal(t, []int64{5, 6, 7, 8}, taskIDs([][]p.InternalHistoryTask{got}))
}
