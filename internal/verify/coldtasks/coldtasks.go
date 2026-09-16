// Package coldtasks is a model of the store below, for the merged task read's
// tests.
//
// It is not a stub: the merge's whole difficulty is the base's *pagination*, so
// a fake that answered everything in one page would leave every rule in
// fold/taskpage.go untested. This one is written against the two queries and
// two page tokens of a real Temporal persistence plugin's history-task store,
// which is where the shapes below come from:
//
//   - an immediate page is [minTaskID, maxTaskID) by task id alone, and its token
//     is {lastTaskID + 1} — emitted only when the page came back full *and* that
//     successor is still inside the range;
//   - a scheduled page is refined by (fireTime, taskID) from the token and bounded
//     above by fireTime alone, and its token is {lastTaskID + 1, lastFireTime}.
//
// The scheduled query's condition is the refinement alone — every row at or
// after the token's (fireTime, taskID) — so a timer whose id is below the
// token's is returned when it fires later, and a full page does not hide it.
//
// # What can make it a lie
//
// It models a store this repository does not contain, so what makes it wrong is
// that store paging differently rather than anything written here.
// coldtasks_test.go states both paginations plainly for that reason: it is
// where the modelled behaviour is written down, and a base that pages another
// way makes the suites above agree about pages it never returns.
//
// # Why it is a package and not a test file
//
// Because two packages' tests need it and neither may have the other's helpers:
// `fold` owns what a page holds and `cycle` owns who may answer one, so the
// model that stands in for the plugin is asked the same questions from both
// sides. A second copy would be two models of one store, and the first
// divergence between them would be a rule tested against a store the other half
// does not have. It sits under internal/verify/ for the reason
// internal/verify/mutgen does: it is an instrument, and the layer may not import
// it outside a test.
package coldtasks

import (
	"context"
	"encoding/binary"
	"math"
	"slices"
	"time"

	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
)

// UnixNano is the reconstruction the store does on the way out, and the one the
// merge's own token does: a fire time is stored as an instant and comes back in
// UTC.
func UnixNano(ns int64) time.Time { return time.Unix(0, ns).UTC() }

// Store is one shard's task rows, by category id, with the plugin's pagination
// over them.
type Store struct {
	Rows map[int32][]p.InternalHistoryTask

	// Err, when set, is what every call answers with.
	Err error

	Calls    int // round trips
	Returned int // rows handed back
	Asked    []int
}

func New() *Store {
	return &Store{Rows: make(map[int32][]p.InternalHistoryTask)}
}

// Commit is what a drain's transaction does: every task lands at once.
func (c *Store) Commit(byCategory map[tasks.Category][]p.InternalHistoryTask) {
	for category, list := range byCategory {
		id := int32(category.ID())
		c.Rows[id] = append(c.Rows[id], list...)
		slices.SortFunc(c.Rows[id], func(a, b p.InternalHistoryTask) int {
			return a.Key.CompareTo(b.Key)
		})
	}
}

// Hold puts rows in without a drain around them.
func (c *Store) Hold(category tasks.Category, list ...p.InternalHistoryTask) {
	c.Commit(map[tasks.Category][]p.InternalHistoryTask{category: list})
}

// Remove is the other half of what a drain's transaction does: the range deletes
// it carries, applied before the rows it inserts, which is the order a drain's
// transaction owes: a task that arrived after a range is one fold deliberately
// keeps, and a delete running after that insert would take it away.
//
// The predicate is passed in rather than named here, so that this model does not
// have to agree with the layer about what a range covers — the caller hands it
// [fold.TaskRange.Covers] and the store applies it row by row, the way a DELETE
// would.
func (c *Store) Remove(category tasks.Category, covered func(tasks.Key) bool) int {
	id := int32(category.ID())
	kept := c.Rows[id][:0]
	for _, row := range c.Rows[id] {
		if covered(row.Key) {
			continue
		}
		kept = append(kept, row)
	}
	removed := len(c.Rows[id]) - len(kept)
	c.Rows[id] = kept
	return removed
}

// Read is the base task read the wrapper would hand down.
func (c *Store) Read(
	_ context.Context, req *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {
	c.Calls++
	c.Asked = append(c.Asked, req.BatchSize)
	if c.Err != nil {
		return nil, c.Err
	}
	if req.TaskCategory.Type() == tasks.CategoryTypeImmediate {
		return c.immediate(req), nil
	}
	return c.scheduled(req), nil
}

func (c *Store) immediate(req *p.GetHistoryTasksRequest) *p.InternalGetHistoryTasksResponse {
	minID, maxID := req.InclusiveMinTaskKey.TaskID, req.ExclusiveMaxTaskKey.TaskID
	if len(req.NextPageToken) > 0 {
		minID = DecodeInt(req.NextPageToken)
	}

	var page []p.InternalHistoryTask
	for _, row := range c.Rows[int32(req.TaskCategory.ID())] {
		if row.Key.TaskID < minID || row.Key.TaskID >= maxID {
			continue
		}
		// The key the plugin reconstructs on the way out: an immediate task's
		// fire time is not stored (the column is NULL) and comes back as
		// tasks.DefaultFireTime.
		page = append(page, p.InternalHistoryTask{
			Key: tasks.NewImmediateKey(row.Key.TaskID), Blob: row.Blob,
		})
		if len(page) == req.BatchSize {
			break
		}
	}
	c.Returned += len(page)

	resp := &p.InternalGetHistoryTasksResponse{Tasks: page}
	if len(page) == req.BatchSize {
		if next := page[len(page)-1].Key.TaskID + 1; next < maxID {
			resp.NextPageToken = EncodeInt(next)
		}
	}
	return resp
}

func (c *Store) scheduled(req *p.GetHistoryTasksRequest) *p.InternalGetHistoryTasksResponse {
	from := tasks.NewKey(req.InclusiveMinTaskKey.FireTime, math.MinInt64)
	if len(req.NextPageToken) > 0 {
		from = DecodeKey(req.NextPageToken)
	}

	var page []p.InternalHistoryTask
	for _, row := range c.Rows[int32(req.TaskCategory.ID())] {
		ts, id := row.Key.FireTime, row.Key.TaskID
		if ts.Before(from.FireTime) || !ts.Before(req.ExclusiveMaxTaskKey.FireTime) {
			continue
		}
		if !(ts.After(from.FireTime) || (ts.Equal(from.FireTime) && id >= from.TaskID)) {
			continue
		}
		page = append(page, row)
		if len(page) == req.BatchSize {
			break
		}
	}
	c.Returned += len(page)

	resp := &p.InternalGetHistoryTasksResponse{Tasks: page}
	if len(page) == req.BatchSize {
		last := page[len(page)-1].Key
		resp.NextPageToken = EncodeKey(tasks.NewKey(last.FireTime, last.TaskID+1))
	}
	return resp
}

// The model's own token encoding. It is deliberately *not* the layer's — these
// bytes stand for the plugin's opaque format, and a test in which the two are
// the same shape could not tell a merge that carried the base's token from one
// that rebuilt it.

func EncodeInt(v int64) []byte {
	return binary.BigEndian.AppendUint64([]byte("base:"), uint64(v))
}

func DecodeInt(b []byte) int64 {
	return int64(binary.BigEndian.Uint64(b[len("base:"):]))
}

func EncodeKey(k tasks.Key) []byte {
	return append(EncodeInt(k.FireTime.UnixNano()), EncodeInt(k.TaskID)...)
}

func DecodeKey(b []byte) tasks.Key {
	const one = len("base:") + 8
	return tasks.NewKey(UnixNano(DecodeInt(b[:one])), DecodeInt(b[one:]))
}
