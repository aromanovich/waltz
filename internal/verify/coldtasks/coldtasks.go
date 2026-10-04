// Package coldtasks models a base history-task store, with its pagination,
// for the merged task read's tests. A one-page fake would leave the paging
// rules in fold/taskpage.go untested.
//
// It follows a real Temporal persistence plugin's two queries and tokens:
//
//   - an immediate page is [minTaskID, maxTaskID) by task id; its token is
//     {lastTaskID + 1}, emitted only when the page is full and that id is
//     still inside the range;
//   - a scheduled page starts at the token's (fireTime, taskID), is bounded
//     above by fireTime alone, and its token is {lastTaskID + 1, lastFireTime}.
//     A timer with a lower id but a later fire time is still returned.
//
// If the real store pages differently, this model is wrong and the suites
// agree about pages it never returns; coldtasks_test.go states both
// paginations for that reason.
//
// It is a package so the fold and cycle tests share one model of the store.
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

// UnixNano rebuilds a stored fire time as the store and the merge's token do:
// an instant, in UTC.
func UnixNano(ns int64) time.Time { return time.Unix(0, ns).UTC() }

// Store is one shard's task rows by category id, paged as the plugin pages.
type Store struct {
	Rows map[int32][]p.InternalHistoryTask

	// Err, when set, fails every Read.
	Err error

	Calls    int // round trips
	Returned int // rows handed back
	Asked    []int
}

func New() *Store {
	return &Store{Rows: make(map[int32][]p.InternalHistoryTask)}
}

// Commit inserts every task at once, as a drain's transaction does.
func (c *Store) Commit(byCategory map[tasks.Category][]p.InternalHistoryTask) {
	for category, list := range byCategory {
		id := int32(category.ID())
		c.Rows[id] = append(c.Rows[id], list...)
		slices.SortFunc(c.Rows[id], func(a, b p.InternalHistoryTask) int {
			return a.Key.CompareTo(b.Key)
		})
	}
}

// Hold inserts rows outside any drain.
func (c *Store) Hold(category tasks.Category, list ...p.InternalHistoryTask) {
	c.Commit(map[tasks.Category][]p.InternalHistoryTask{category: list})
}

// Remove applies a range delete row by row, as a DELETE would, and returns how
// many rows it removed. Call it before Commit for the same drain: a task added
// after the range is one fold keeps, and deleting after the insert would lose
// it. The caller passes the predicate (normally [fold.TaskRange.Covers]) so
// the model need not define range coverage.
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

// Read answers a base task read with one page.
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
		// An immediate task's fire time is not stored, so it comes back as
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

// The model's token encoding, deliberately unlike the layer's, so a test can
// tell a merge that passed the base's token through from one that rebuilt it.

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
