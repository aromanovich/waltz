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
	// the merge's refusal is about that requirement alone
	// ([TestABaseThatBreaksTheRequirementsIsRefused]).
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

// TestABaseThatBreaksTheRequirementsIsRefused turns [fold.BasePage]'s first
// three requirements from prose into a refusal each. Every base here keeps the
// other two, so what is refused is that requirement alone.
//
// Refusing them reverses what this file used to say, and the third one is the
// reason. Judged in general a failing read does look worse than a store that
// pages oddly — but the third breach does not cost a failing read. The merge
// reads the empty page as the end, stops calling the base, and hands back a
// pagination that is over, so the queue completes the range over rows it was
// never shown and deletes acked task rows. Against that a refusal is the cheap
// outcome, and once the page is walked at all the other two cost nothing further
// and name the store instead of panicking in somebody else's reader.
func TestABaseThatBreaksTheRequirementsIsRefused(t *testing.T) {
	rows := func(ids ...int64) []p.InternalHistoryTask {
		var out []p.InternalHistoryTask
		for _, id := range ids {
			out = append(out, keyed(id, "cold"))
		}
		return out
	}
	minKey, maxKey := immediateRange()

	for _, tt := range []struct {
		name   string
		base   *minimalBase
		maxKey tasks.Key
		want   error
		cost   string
	}{
		{
			name: "a row outside the range",
			base: &minimalBase{rows: rows(1, 2, 3, 20, 21), page: 1, ignoresRange: true},
			// A range ending below what the base holds, which is every range a
			// queue actually asks for: it reads up to its own checkpoint.
			maxKey: tasks.NewImmediateKey(10),
			want:   fold.ErrBaseRowOutsideRange,
			cost:   "the row reaches the reader, where the queue panics on it with no recover in the loop",
		},
		{
			name:   "a page repeating a key the pagination has passed",
			base:   &minimalBase{rows: rows(1, 2, 3, 4, 5), page: 1, repeatsOnce: true},
			maxKey: maxKey,
			want:   fold.ErrBasePageNotAscending,
			cost:   "the reader's iterator skips what does not ascend without saying so, and that task is never asked for again",
		},
		{
			// The boundary of the same requirement, which the case above does not
			// reach: a key repeated *within* one page rather than across two. The
			// ascent is strict, so the comparison is at-or-below and not below.
			name: "a page carrying one key twice",
			// Both copies in the *first* page: with the duplicate split across two
			// pages the cross-page rule catches it instead, and the within-page
			// comparison stays unjudged.
			base:   &minimalBase{rows: rows(1, 1, 2), page: 2},
			maxKey: maxKey,
			want:   fold.ErrBasePageNotAscending,
			cost:   "the reader is handed the same task twice in one page",
		},
		{
			// The boundary of the range requirement: the maximum is exclusive, so a
			// row *at* it is already outside. A case that sends a row far outside
			// passes with the comparison off by one.
			name:   "a row at the range's exclusive maximum",
			base:   &minimalBase{rows: rows(1, 2, 10), page: 1, ignoresRange: true},
			maxKey: tasks.NewImmediateKey(10),
			want:   fold.ErrBaseRowOutsideRange,
			cost:   "a key the range excludes reaches the reader, where the queue panics on it",
		},
		{
			name:   "a token beside an empty page",
			base:   &minimalBase{rows: rows(1, 2, 3, 4, 5), page: 1, emptyAfter: 2},
			maxKey: maxKey,
			want:   fold.ErrBasePageEmptyBesideAToken,
			cost:   "the pagination ends three rows early and the queue completes the range over rows it was never shown",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req := taskReq(tasks.CategoryTransfer, minKey, tt.maxKey, 2)
			err := paginateToRefusal(t, fold.New(shard), tt.base, req)
			require.ErrorIs(t, err, tt.want, "unrefused, %s", tt.cost)
		})
	}
}

// paginateToRefusal drives a merged read until the merge refuses the base it is
// reading, and fails the test if the pagination runs to exhaustion instead.
func paginateToRefusal(
	t *testing.T, a *fold.Accumulator, base *minimalBase, req *p.GetHistoryTasksRequest,
) error {
	t.Helper()
	ask := *req
	for range 1000 {
		resp, _, err := a.TaskPage(&ask, base.get(&ask))
		if err != nil {
			return err
		}
		if len(resp.NextPageToken) == 0 {
			t.Fatal("the pagination ran to exhaustion, so the base's breach was not refused")
		}
		ask.NextPageToken = resp.NextPageToken
	}
	t.Fatal("the pagination did not terminate in 1000 pages")
	return nil
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
