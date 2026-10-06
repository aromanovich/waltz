package fold

// Merge-on-read for history tasks: one page of GetHistoryTasks built from the
// cold store's rows and the window's tasks together, ascending, deduplicated,
// inside the requested range, at most BatchSize long.
//
// Read-only on the accumulator. The page must not alias a window task slice:
// mergeTasks appends into a superseded request's map, and a shared backing
// array would rewrite an answer already given. The base page is a callback so
// the merge picks the batch size and token while the caller does the round trip.
//
// # The pagination rule
//
// The base's token is the plugin's format, which this layer may not parse or
// build, and a scheduled range can resume only at a fire time. So a page is
// cut at the end of a base page or below its first row, never inside it:
// either the whole base page is emitted and its token advances, or none of it
// is and the incoming token is returned untouched. A partial base page would
// lose rows on one side and duplicate them on the other. The base is asked for
// BatchSize minus the window's share, so window tasks displace cold-store rows.
//
// No state is held between calls; the token carries the base's bytes, whether
// the base is exhausted, and the last key emitted. Undrained range deletes are
// applied to the cold store's page only: the window's own tasks were swept
// when each range folded in.

import (
	"encoding/json"
	"fmt"
	"slices"
	"time"

	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
)

// BasePage fetches one page of the cold store's answer for the requested range.
// It takes only a batch size and a token because those are all the merge
// decides; range, category and shard stay the caller's. The token is the
// base's own bytes, passed through unparsed. An empty returned token means the
// base is exhausted.
//
// The merge requires four things of it and refuses a page that breaks one,
// because the damage otherwise lands in a reader that cannot name the store
// (a panicking queue, an iterator that skips silently, a range completed over
// unseen rows). Temporal's SQL and Cassandra plugins satisfy all four.
//
//  1. Every row is inside the requested range. Rows are filtered only by the
//     undrained deletes, so an outside row reaches queues/slice.go, which
//     panics on it. [ErrBaseRowOutsideRange].
//  2. Rows ascend within a page, and no later page has a key at or below the
//     last key of an earlier one. That last key bounds the window's share and
//     goes into the token; queues/iterator.go silently skips a key that does
//     not ascend, and the task is never read again. [ErrBasePageNotAscending].
//  3. An empty page means the range is exhausted. A token beside an empty page
//     would end the pagination here, and the reader's range completion would
//     delete rows never read. [ErrBasePageEmptyBesideAToken].
//  4. A page holds at most the batch asked for. When the window alone fills a
//     page the ask is one row, emitted to move the base's cursor past it; an
//     extra row would be passed unemitted and later deleted by range
//     completion. [ErrBasePageTooLarge].
type BasePage func(batch int, token []byte) ([]p.InternalHistoryTask, []byte, error)

// TaskPageStats is an instrument rather than a contract.
type TaskPageStats struct {
	BaseCalls     int // cold-store round trips
	BaseRows      int // rows the cold store returned
	BaseDiscarded int // rows it returned that this page did not carry
	WindowTouched int // window tasks examined
	Comparisons   int // key comparisons the merge itself made
	Collisions    int // keys both sources carried
	FromWindow    int // tasks this page took from the window
}

// Add sums one page's stats into a run's.
func (s *TaskPageStats) Add(o TaskPageStats) {
	s.BaseCalls += o.BaseCalls
	s.BaseRows += o.BaseRows
	s.BaseDiscarded += o.BaseDiscarded
	s.WindowTouched += o.WindowTouched
	s.Comparisons += o.Comparisons
	s.Collisions += o.Collisions
	s.FromWindow += o.FromWindow
}

// TaskPage answers one page of a task read from the window and the base, under
// the pagination rule above. The base is called at most once per page, and not
// at all once exhausted. Its error is returned unwrapped, never swallowed: a
// page silently missing the store's rows would lose them.
func (a *Accumulator) TaskPage(
	req *p.GetHistoryTasksRequest, base BasePage,
) (*p.InternalGetHistoryTasksResponse, TaskPageStats, error) {
	token, ours := decodeTaskToken(req.NextPageToken)
	if !ours {
		return nil, TaskPageStats{}, ErrForeignPageToken
	}

	page, next, stats, err := mergePage(
		req, base, a.Tasks(req.TaskCategory), token,
		hideDeleted{ranges: a.taskRanges(req.TaskCategory)})
	if err != nil {
		return nil, stats, err
	}
	return &p.InternalGetHistoryTasksResponse{Tasks: page, NextPageToken: encodeTaskToken(next)}, stats, nil
}

// taskPageToken is a merged read's token: the base's token verbatim plus an
// exact cursor over the window.
type taskPageToken struct {
	Base []byte `json:"base,omitempty"`
	// BaseDone means the base is exhausted for this range. A flag, because an
	// empty Base is also what the first page carries.
	BaseDone bool `json:"baseDone,omitempty"`
	// AfterFireTime and AfterTaskID are the last key emitted, exclusive. Stored
	// as components rather than a tasks.Key so the encoding is exact and has no
	// time zone.
	AfterFireTime int64 `json:"afterFireTime"`
	AfterTaskID   int64 `json:"afterTaskId"`
	// After means the two fields above are set; (0, 0) is a valid stop key.
	After bool `json:"after,omitempty"`
}

func (t *taskPageToken) after() (tasks.Key, bool) {
	if t == nil || !t.After {
		return tasks.Key{}, false
	}
	return tasks.NewKey(time.Unix(0, t.AfterFireTime).UTC(), t.AfterTaskID), true
}

func (t *taskPageToken) setAfter(k tasks.Key) {
	t.After, t.AfterFireTime, t.AfterTaskID = true, k.FireTime.UnixNano(), k.TaskID
}

// taskTokenMagic prefixes this layer's token so a token we did not issue is
// refused ([ErrForeignPageToken]) rather than resumed against a window cursor
// nobody handed out.
var taskTokenMagic = [4]byte{'w', 'a', 'l', '1'}

func encodeTaskToken(t *taskPageToken) []byte {
	if t == nil {
		return nil
	}
	body, err := json.Marshal(t)
	if err != nil {
		// Unreachable: the struct is four scalars and a byte slice.
		panic(fmt.Sprintf("fold: encoding a task page token: %v", err))
	}
	return append(taskTokenMagic[:], body...)
}

func decodeTaskToken(raw []byte) (*taskPageToken, bool) {
	if len(raw) == 0 {
		return nil, true
	}
	if len(raw) < len(taskTokenMagic) || [4]byte(raw[:4]) != taskTokenMagic {
		return nil, false
	}
	var t taskPageToken
	if err := json.Unmarshal(raw[len(taskTokenMagic):], &t); err != nil {
		return nil, false
	}
	return &t, true
}

// hideDeleted is the window's undrained range deletes for the category read:
// rows the cold store still holds that the caller has already deleted. Applied
// to the base's page only. The zero value hides nothing. It keeps the ranges,
// not their maximum: a row in a gap between two ranges is not deleted and must
// stay visible.
type hideDeleted struct {
	ranges []TaskRange
}

// hides reports whether a base row is inside an undrained range, by
// [TaskRange.Covers] and nothing else.
func (d hideDeleted) hides(key tasks.Key) bool {
	return slices.ContainsFunc(d.ranges, func(r TaskRange) bool { return r.Covers(key) })
}

// keep returns the base page without hidden rows, and how many were removed.
// Where nothing is hidden the store's slice comes back, as in [keepUncovered].
func (d hideDeleted) keep(page []p.InternalHistoryTask) ([]p.InternalHistoryTask, int) {
	if len(d.ranges) == 0 {
		return page, 0
	}
	return keepUncovered(page, d.hides)
}

// mergePage answers one page from the two sources. window is this category's
// window tasks, ascending; token is nil on the first call.
func mergePage(
	req *p.GetHistoryTasksRequest,
	base BasePage,
	window []p.InternalHistoryTask,
	token *taskPageToken,
	hidden hideDeleted,
) ([]p.InternalHistoryTask, *taskPageToken, TaskPageStats, error) {
	var c TaskPageStats
	// Floored at one: zero-row pages would never end the pagination.
	batch := max(req.BatchSize, 1)
	minKey, maxKey := taskBounds(req.TaskCategory, req.InclusiveMinTaskKey, req.ExclusiveMaxTaskKey)

	// What the window still owes this pagination.
	from := minKey
	if after, ok := token.after(); ok {
		from = after.Next()
	}
	var tail []p.InternalHistoryTask
	for _, t := range window {
		c.WindowTouched++
		if t.Key.CompareTo(from) >= 0 && t.Key.CompareTo(maxKey) < 0 {
			tail = append(tail, t)
		}
	}

	var rawPage, basePage []p.InternalHistoryTask
	var nextBase []byte
	baseDone := token != nil && token.BaseDone
	if !baseDone {
		// Ask for what the window does not fill, at least one: asking for nothing
		// would end the pagination with rows left in the store.
		ask := max(batch-len(tail), 1)
		var carried []byte
		if token != nil {
			carried = token.Base
		}
		var err error
		rawPage, nextBase, err = base(ask, carried)
		if err != nil {
			return nil, nil, c, err
		}
		if len(rawPage) > ask {
			return nil, nil, c, fmt.Errorf("%w: asked for %d, got %d", ErrBasePageTooLarge, ask, len(rawPage))
		}
		if err := refuseBasePage(rawPage, nextBase, minKey, from, maxKey); err != nil {
			return nil, nil, c, err
		}
		c.BaseCalls, c.BaseRows = 1, len(rawPage)
		// The undrained deletes are subtracted here and nowhere else.
		var hiddenRows int
		basePage, hiddenRows = hidden.keep(rawPage)
		c.BaseDiscarded += hiddenRows
	}

	// How far this page may reach. A base page with a token says nothing above
	// its last key; without a token the base is exhausted. Use the raw page, not
	// the kept one: the token resumes after hidden rows too.
	bounded := len(nextBase) > 0 && len(rawPage) > 0
	var upTo tasks.Key
	inReach := tail
	if bounded {
		upTo = rawPage[len(rawPage)-1].Key
		inReach = nil
		for _, t := range tail {
			c.Comparisons++
			if t.Key.CompareTo(upTo) <= 0 {
				inReach = append(inReach, t)
			}
		}
	}

	merged, collisions, comparisons := mergeSorted(basePage, inReach)
	c.Collisions, c.Comparisons = collisions, c.Comparisons+comparisons

	if len(merged) <= batch {
		c.FromWindow = len(inReach) - collisions
		if !bounded {
			// Base exhausted and every remaining window task is here: done.
			return merged, nil, c, nil
		}
		next := &taskPageToken{Base: nextBase}
		next.setAfter(upTo)
		return merged, next, c, nil
	}

	// The window alone overflows the page. Emit window tasks strictly below the
	// base page's first key and leave the base page unread, token untouched, so
	// it comes back next time. Here len(tail) >= batch, so the ask was one and
	// at most one base row is discarded.
	c.BaseDiscarded += len(basePage)
	cut := len(inReach)
	if len(basePage) > 0 {
		cut = 0
		for _, t := range inReach {
			c.Comparisons++
			if t.Key.CompareTo(basePage[0].Key) >= 0 {
				break
			}
			cut++
		}
	}
	next := &taskPageToken{BaseDone: baseDone || len(rawPage) == 0}
	if cut == 0 {
		// The base's first row is at or below the window's first, so cutting
		// below it would emit empty pages forever. That row is merged[0] (a tie
		// deduplicates to it) and is the whole base page here, so emitting it
		// alone obeys the cut rule.
		c.BaseDiscarded = 0
		c.FromWindow = 0
		next.Base, next.BaseDone = nextBase, !bounded
		next.setAfter(merged[0].Key)
		return merged[:1], next, c, nil
	}
	cut = min(cut, batch)
	only := inReach[:cut]
	c.FromWindow = cut
	// Nothing emitted from the base: its cursor stays.
	if token != nil {
		next.Base = token.Base
	}
	next.setAfter(only[len(only)-1].Key)
	return only, next, c, nil
}

// refuseBasePage checks [BasePage]'s first three requirements; the caller,
// which holds the ask, checks the fourth.
//
// from is where this pagination resumes, inclusive: minKey on the first page,
// then the key after the last one emitted. A conforming store never answers
// below it: its token resumes after its last row, and the branch that keeps a
// token unchanged emits only window keys strictly below that page's first row.
func refuseBasePage(page []p.InternalHistoryTask, token []byte, minKey, from, maxKey tasks.Key) error {
	if len(page) == 0 {
		if len(token) > 0 {
			return fmt.Errorf("%w: a token of %d bytes", ErrBasePageEmptyBesideAToken, len(token))
		}
		return nil
	}
	for i, row := range page {
		switch {
		case row.Key.CompareTo(minKey) < 0 || row.Key.CompareTo(maxKey) >= 0:
			return fmt.Errorf("%w: row %d at %v, for the range [%v, %v)",
				ErrBaseRowOutsideRange, i, row.Key, minKey, maxKey)
		case row.Key.CompareTo(from) < 0:
			return fmt.Errorf("%w: row %d at %v, which this pagination passed at %v",
				ErrBasePageNotAscending, i, row.Key, from)
		case i > 0 && row.Key.CompareTo(page[i-1].Key) <= 0:
			return fmt.Errorf("%w: row %d at %v does not ascend from %v",
				ErrBasePageNotAscending, i, row.Key, page[i-1].Key)
		}
	}
	return nil
}

// mergeSorted merges two ascending runs and returns the page, the collisions
// and the comparisons. On a shared key the base row wins, as the durable copy
// the queue will complete. The sources are disjoint by construction (a drain
// removes from the window what it writes to the store), so a collision is
// counted, not raised.
func mergeSorted(base, tail []p.InternalHistoryTask) ([]p.InternalHistoryTask, int, int) {
	out := make([]p.InternalHistoryTask, 0, len(base)+len(tail))
	collisions, comparisons := 0, 0
	i, j := 0, 0
	for i < len(base) && j < len(tail) {
		comparisons++
		switch c := base[i].Key.CompareTo(tail[j].Key); {
		case c == 0:
			out = append(out, base[i])
			i, j = i+1, j+1
			collisions++
		case c < 0:
			out = append(out, base[i])
			i++
		default:
			out = append(out, tail[j])
			j++
		}
	}
	out = append(out, base[i:]...)
	return append(out, tail[j:]...), collisions, comparisons
}

// taskBounds normalises the requested range as the store does: immediate
// categories filter on task id alone, scheduled ones on fire time. Upstream
// lets an immediate range use a zero fire time or tasks.DefaultFireTime;
// unnormalised, a zero-time maximum drops every window task, and a zero-time
// minimum returns unrequested keys, which panics the history service.
//
// It is the range's half of [TaskRange.Covers]. Window keys need no
// normalisation: every immediate task type's GetKey() already returns
// NewImmediateKey (TestEveryImmediateKeyIsNormalised).
func taskBounds(category tasks.Category, minKey, maxKey tasks.Key) (tasks.Key, tasks.Key) {
	if category.Type() == tasks.CategoryTypeImmediate {
		return tasks.NewImmediateKey(minKey.TaskID), tasks.NewImmediateKey(maxKey.TaskID)
	}
	return minKey, maxKey
}
