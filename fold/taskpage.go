package fold

// Merge-on-read for history tasks: one page of GetHistoryTasks answered from
// the cold store's rows and the window's tasks at once, ascending,
// deduplicated, inside the requested range, no longer than BatchSize.
//
// Read-only on the accumulator, and no slice of it is handed out: the page may
// not alias a task slice, because mergeTasks appends into a superseded
// request's own map and a shared backing array would rewrite an answer already
// given. The base page arrives as a callback because the merge picks its own
// batch size and token while the round trip stays the caller's.
//
// # The pagination rule
//
// A page may not exceed BatchSize, the base's token is the plugin's own format
// that this layer may not parse or synthesise, and a scheduled range can name
// only a fire time as a resume point. So the cut is at the end of a base page
// or below its first row, never inside one: either the whole base page is
// emitted and its token advances, or none of it is and the incoming token is
// handed back untouched. A partially emitted base page means lost rows on one
// side and duplicates on the other. The base is asked for BatchSize minus what
// the window contributes, so window tasks displace cold-store rows.
//
// No state is held between calls: the token is the base's own bytes, whether
// the base is exhausted, and the last key emitted. The window's undrained range
// deletes come off the cold store's page and nothing else, the window's own
// half having been swept when each range folded in.

import (
	"encoding/json"
	"fmt"
	"slices"
	"time"

	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
)

// BasePage is one page of the cold store's own answer for the range the request
// names, as the caller reaches it. A batch size and a token rather than a
// request, since those are the only two things the merge decides; the range,
// the category and the shard stay the caller's. The token is the base's own
// bytes, passed through unparsed, which keeps the merge backend-independent. A
// zero-length returned token means the base is exhausted.
//
// Three things are required of it, all three because this merge builds a page's
// reach out of what the base last returned rather than out of a cursor of its
// own. Temporal's SQL and Cassandra plugins satisfy every one, so no run here has
// had to; a store that pages differently breaks a queue rather than this package,
// which is why they are written down.
//
//  1. Every row is inside the range the request names. What comes back is
//     filtered against the window's undrained deletes and by nothing else, so a
//     row outside the range reaches the reader — where queues/slice.go panics on
//     one, with no recover in the loop.
//  2. Rows ascend within a page, and no later page holds a key at or below the
//     last key of an earlier one. That last key is what bounds the window's half
//     of the page and what goes into the token, so a base row arriving under it
//     breaks the ascent across the page boundary — and queues/iterator.go skips
//     what does not ascend without saying so, which is a task nobody asks for
//     again.
//  3. No rows means the range is exhausted. A token beside an empty page is read
//     here as the end of one: the merge stops calling the base and hands back a
//     pagination that is over, so rows the store still held are never read.
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

// TaskPage answers one page of a task read from the window and the base at
// once, under the rule this file opens with. The base is called at most once
// per page, and not at all once its token says it is exhausted. Its error is
// returned unwrapped and never swallowed: a page that quietly omitted the
// store's rows would lose them.
func (a *Accumulator) TaskPage(
	req *p.GetHistoryTasksRequest, base BasePage,
) (*p.InternalGetHistoryTasksResponse, TaskPageStats, error) {
	token, ours := decodeTaskToken(req.NextPageToken)
	if !ours {
		// A token this layer did not write: an earlier page was answered by the
		// base alone, on a shard whose cycle was retired mid pagination. Carry on
		// with the base alone, since the window cursor the merge needs was never
		// handed out and inventing one would re-emit keys the caller has.
		page, next, err := base(req.BatchSize, req.NextPageToken)
		if err != nil {
			return nil, TaskPageStats{}, err
		}
		return &p.InternalGetHistoryTasksResponse{Tasks: page, NextPageToken: next},
			TaskPageStats{BaseCalls: 1, BaseRows: len(page)}, nil
	}

	page, next, stats, err := mergePage(
		req, base, a.Tasks(req.TaskCategory), token,
		hideDeleted{ranges: a.taskRanges(req.TaskCategory)})
	if err != nil {
		return nil, stats, err
	}
	return &p.InternalGetHistoryTasksResponse{Tasks: page, NextPageToken: encodeTaskToken(next)}, stats, nil
}

// taskPageToken is what a merged read hands back: the base store's own token
// verbatim, plus this layer's exact cursor over the window.
type taskPageToken struct {
	// Base is the cold store's token for this range, as the cold store wrote it.
	Base []byte `json:"base,omitempty"`
	// BaseDone reports that the base is exhausted for this range: do not call it
	// again. A flag rather than an empty Base, since an empty Base is also what
	// the first page carries.
	BaseDone bool `json:"baseDone,omitempty"`
	// AfterFireTime and AfterTaskID are the last key this pagination emitted,
	// exclusive. Stored as the key's two components rather than as a tasks.Key,
	// so the encoding is exact and carries no time zone.
	AfterFireTime int64 `json:"afterFireTime"`
	AfterTaskID   int64 `json:"afterTaskId"`
	// After reports that the two fields above are set, since (0, 0) is a key a
	// pagination can legitimately have stopped at.
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

// taskTokenMagic frames this layer's token so a store's own can be told apart
// from it. The two meet when a pagination began while this shard's cycle was
// retired; mistaking one for the other fails the read or resumes the window
// from the wrong place.
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

// hideDeleted is the window's undrained range deletes for the category being
// read: rows the cold store still holds and the caller has already declared
// garbage. Applied to the base's page only, since the window's own tasks were
// swept when each range folded in. The zero value hides nothing. The ranges and
// not their maximum: a row in a gap between two of them is one no pending
// delete covers, and hiding it would make it invisible and present.
type hideDeleted struct {
	ranges []TaskRange
}

// hides reports that a row of the base's page is inside an undrained range,
// under [TaskRange.Covers] and no other predicate.
func (d hideDeleted) hides(key tasks.Key) bool {
	return slices.ContainsFunc(d.ranges, func(r TaskRange) bool { return r.Covers(key) })
}

// keep is the base page with the hidden rows removed, and how many went. The
// store's own slice comes back where nothing hides, under [keepUncovered]'s rule.
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
	// A batch of zero is floored at one: a page of zero rows would make the
	// pagination endless.
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
		// The base is asked for what the window does not already fill, floored at
		// one: asking for nothing would end the pagination with rows left in the
		// store.
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
		c.BaseCalls, c.BaseRows = 1, len(rawPage)
		// The undrained deletes are subtracted here and nowhere else.
		var hiddenRows int
		basePage, hiddenRows = hidden.keep(rawPage)
		c.BaseDiscarded += hiddenRows
	}

	// How far up the key space this page may reach. A base page with a token
	// says nothing above its last key; without one the base is exhausted. Read
	// off the raw page and not the kept one, since the base's token resumes
	// after what the base returned, hidden rows included.
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
			// The base is exhausted and every remaining window task is in this
			// page: the pagination is over.
			return merged, nil, c, nil
		}
		next := &taskPageToken{Base: nextBase}
		next.setAfter(upTo)
		return merged, next, c, nil
	}

	// The window alone overflows the page. Emit window tasks strictly below the
	// base page's first key and leave that page unread: its token is untouched,
	// so the same rows come back next time. This branch implies len(tail) >=
	// batch, hence an ask of one, so at most a single base row is discarded.
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
		// The base's first row ties the window's first, so nothing is strictly
		// below it and cutting there would emit an empty page for ever. The tie
		// deduplicates to one entry and this branch holds at most one base row,
		// so emitting that entry emits the base page whole, which the cut rule
		// allows.
		c.BaseDiscarded = 0
		c.FromWindow = 0
		next.Base, next.BaseDone = nextBase, !bounded
		next.setAfter(merged[0].Key)
		return merged[:1], next, c, nil
	}
	cut = min(cut, batch)
	only := inReach[:cut]
	c.FromWindow = cut
	// Nothing was emitted from the base, so its cursor stays where it was.
	if token != nil {
		next.Base = token.Base
	}
	next.setAfter(only[len(only)-1].Key)
	return only, next, c, nil
}

// mergeSorted merges two ascending runs, dropping a key the two share; the
// base's row wins, being the durable copy the queue will complete. It returns
// the merged page, the collisions and the comparisons made. The sources are
// disjoint by construction, the window dropping its tasks in the drain that
// puts them in the store, so a collision is counted rather than raised.
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

// taskBounds normalises the requested range so one comparator serves both
// category types, mirroring the store: immediate queries filter on task id
// alone, scheduled ones on fire time. Upstream lets an immediate range name
// either a zero fire time or tasks.DefaultFireTime, so an unnormalised
// immediate range puts every window task above a zero-time maximum and drops
// the lot, or below a zero-time minimum and returns keys the caller did not ask
// for, which panics the history service.
//
// It is the range's half of what [TaskRange.Covers] does per row, spelled out
// because this package names no store. The window's own keys
// need no normalisation: every immediate-category task type's GetKey() already
// returns NewImmediateKey (TestEveryImmediateKeyIsNormalised).
func taskBounds(category tasks.Category, minKey, maxKey tasks.Key) (tasks.Key, tasks.Key) {
	if category.Type() == tasks.CategoryTypeImmediate {
		return tasks.NewImmediateKey(minKey.TaskID), tasks.NewImmediateKey(maxKey.TaskID)
	}
	return minKey, maxKey
}
