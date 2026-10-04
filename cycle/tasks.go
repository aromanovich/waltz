package cycle

// Merge-on-read for history tasks: who may answer a task page, and when. The
// page's contents (cut, token, batching, dedup, undrained ranges) are
// [fold.Accumulator.TaskPage].
//
// The cold store alone cannot answer: a queue that finds nothing in its range
// completes the range and acks past a key still in the window, losing the
// task. The read runs on the cycle's goroutine because between a drain's start
// and its commit a mutation is in neither source, and a task read there would
// skip a key, not merely read stale.

import (
	"context"
	"fmt"

	p "go.temporal.io/server/common/persistence"

	"github.com/aromanovich/waltz/wal"
)

// BaseTasks is the cold store's own task read, handed down by the wrapper. It
// takes a request because the merge sends its own batch size and page token.
// It is an alias so that [wrapper.ShardReader], which cannot import this
// package, can spell the same function type.
type BaseTasks = func(context.Context, *p.GetHistoryTasksRequest) (*p.InternalGetHistoryTasksResponse, error)

// GetHistoryTasks answers a task read for the shard the request names. With no
// cycle for the shard it is refused ([noCycleRoute]), unlike the mutable-state
// reads.
func (m *Manager) GetHistoryTasks(
	ctx context.Context,
	req *p.GetHistoryTasksRequest,
	base BaseTasks,
) (*p.InternalGetHistoryTasksResponse, error) {
	shard := wal.ShardID(req.ShardID)
	return m.taskPage(ctx, shard, m.Shard(shard), req, base)
}

// taskPage tells [supersededRoute] what no cycle can know: whether the cycle
// that answered is still the registry's for the shard. An acquire can land
// between resolving the cycle and its answer, so the check runs after.
func (m *Manager) taskPage(
	ctx context.Context,
	shard wal.ShardID,
	c *Cycle,
	req *p.GetHistoryTasksRequest,
	base BaseTasks,
) (*p.InternalGetHistoryTasksResponse, error) {
	for attempt := 0; ; attempt++ {
		if c == nil {
			route, refusal := noCycleRoute(taskRead, shard)
			return offLoop(ctx, route, refusal,
				func(ctx context.Context) (*p.InternalGetHistoryTasksResponse, error) {
					return base(ctx, req)
				})
		}
		resp, err := c.getHistoryTasks(ctx, req, base)
		// Resolved after the answer, so it covers the whole call; it is also
		// the cycle a retry goes to. Errors are checked too: a superseded
		// cycle's refusal is no more the shard's answer than its page.
		current := m.Shard(shard)
		switch route, refusal := supersededRoute(current == c, attempt > 0, shard); route {
		case merge:
			return resp, err
		case retryOnSuccessor:
			c = current
		default:
			return nil, refusal
		}
	}
}

// getHistoryTasks asks this cycle's goroutine for one merged page. A stopped
// cycle refuses; [Manager.taskPage] then re-issues on the successor if any.
func (c *Cycle) getHistoryTasks(
	ctx context.Context,
	req *p.GetHistoryTasksRequest,
	base BaseTasks,
) (*p.InternalGetHistoryTasksResponse, error) {
	resp, stopped, err := ask(ctx, c, func(s *state) (*p.InternalGetHistoryTasksResponse, error) {
		return c.readTasks(ctx, s, req, base)
	})
	if stopped {
		route, refusal := c.stoppedRead(taskRead, err)
		return offLoop(ctx, route, refusal,
			func(ctx context.Context) (*p.InternalGetHistoryTasksResponse, error) {
				return base(ctx, req)
			})
	}
	return resp, err
}

// readTasks is the loop's half.
func (c *Cycle) readTasks(
	ctx context.Context,
	s *state,
	req *p.GetHistoryTasksRequest,
	base BaseTasks,
) (*p.InternalGetHistoryTasksResponse, error) {
	// Counted before [Cycle.prelude] because TaskReads is pages routed: one the
	// readiness gate fails still counts. There is no takeView, since the page
	// merges over the window rather than rendering a row from it.
	// Under DrainOnRead the merge still runs over the emptied window, so
	// pagination tokens stay the merge's and only TaskReadsMerged stops.
	s.TaskReads++

	switch pass, err := c.prelude(ctx, s, taskRead, nil); {
	case err != nil:
		return nil, err
	case pass:
		// Unreachable: every route merges or refuses a task read. Kept so a
		// change in decide.go cannot make it pass through: the base's token is
		// unreadable by a successor (fold.ErrForeignPageToken), and a page from
		// the base alone drops the window, so the reader's range completion
		// deletes acked rows.
		return nil, fmt.Errorf(
			"cycle: shard %d: a task page may not be answered by the cold store alone", c.shard)
	}

	// A copy of the caller's request, so fields this layer does not know about
	// survive; only batch size and token are overridden.
	basePage := func(batch int, token []byte) ([]p.InternalHistoryTask, []byte, error) {
		ask := *req
		ask.BatchSize, ask.NextPageToken = batch, token
		resp, err := base(ctx, &ask)
		if err != nil {
			// Unwrapped, like every other error on this path.
			return nil, nil, err
		}
		return resp.Tasks, resp.NextPageToken, nil
	}

	// TaskReadsMerged counts pages with at least one window task.
	resp, stats, err := s.acc.TaskPage(req, basePage)
	if err != nil {
		return nil, err
	}
	if stats.FromWindow > 0 {
		s.TaskReadsMerged++
	}
	s.TaskCollisions += stats.Collisions
	c.deps.Metrics.TaskCollisions(stats.Collisions)

	return resp, nil
}
