package cycle

// Merge-on-read for history tasks: who may answer a task page, when, and out of
// which window. The page's contents are not here — the cut, the token, the
// batch arithmetic, the dedup and the subtraction of undrained ranges are
// [fold.Accumulator.TaskPage], beside the window they read.
//
// A task read cannot be left to the store below. Notification carries no
// payload (both queues re-read persistence) and the server's own hold-back is
// above this boundary, so a reader that asks the cold store alone finds nothing
// in its range, completes that range, and acks past a key it never saw: the
// task is lost rather than late.
//
// The cycle's goroutine answers it for the same reason the overlay's reads live
// there, only sharper: in the interval where a mutation is in neither source, a
// mutable-state read is merely stale while a task read skips a key.

import (
	"context"
	"fmt"

	p "go.temporal.io/server/common/persistence"

	"github.com/aromanovich/waltz/wal"
)

// BaseTasks is the cold store's own task read, handed down by the wrapper. It
// takes a request rather than closing over one, because the merge issues a
// different request than the caller's: its own batch size, and the base's own
// page token.
//
// An alias and not a defined type: the two packages that must agree on this
// signature may not import each other, so [wrapper.ShardReader] spells the
// function type out and a defined type here would not satisfy it.
type BaseTasks = func(context.Context, *p.GetHistoryTasksRequest) (*p.InternalGetHistoryTasksResponse, error)

// GetHistoryTasks answers a task read for the shard the request names. A shard
// this node holds no cycle for is [noCycleRoute]'s, which refuses this reader
// where it passes the two mutable-state reads through.
func (m *Manager) GetHistoryTasks(
	ctx context.Context,
	req *p.GetHistoryTasksRequest,
	base BaseTasks,
) (*p.InternalGetHistoryTasksResponse, error) {
	shard := wal.ShardID(req.ShardID)
	return m.taskPage(ctx, shard, m.Shard(shard), req, base)
}

// taskPage supplies [supersededRoute] with the one value no cycle can read:
// whether the cycle that answered is still the one the registry holds for the
// shard. Both ends of the call are put to that rule, the resolve and the answer
// being two moments with room for an acquire between them.
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
		// One resolve, read after the answer so the window it covers is the
		// whole call, and it is both the rule's input and the cycle a retry
		// re-issues on. The answer is inside the check, error included: a
		// refusal from a superseded cycle is no more that shard's answer than a
		// page from one is.
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

// getHistoryTasks asks this cycle's goroutine for one merged page. Its one
// caller is [Manager.taskPage], which is where the refusal a stopped cycle
// gives — this cycle saying it is not the one to answer — is resolved by
// re-issuing on the successor.
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
	// Counted here rather than through [Cycle.prelude]'s takeView, because it counts
	// pages routed: a page the readiness gate fails is still one this shard was
	// asked for. The page holds no view of the window either — it merges over the
	// window rather than rendering one row out of it — so nothing has to be
	// re-taken after a drain.
	//
	// DrainOnRead therefore changes nothing here but the sources: the merge still
	// runs, over a window the drain just emptied, so the page is the base's page
	// and task-reads-merged stops counting. Answering from the base directly
	// would change the pagination too, and that arm takes out the merge, not the
	// tokens.
	s.TaskReads++

	switch pass, err := c.prelude(ctx, s, taskRead, nil); {
	case err != nil:
		return nil, err
	case pass:
		// No route answers a task read this way — every one of them merges or
		// refuses — and this arm is here to keep it that way rather than to run.
		// What it would do is hand the caller the cold store's own page *token*,
		// which the cycle that replaces this one cannot read: the pagination
		// finishes on the base alone with the window dropped out of it, and the
		// range its reader completes deletes the acked rows that were in it. A
		// rule held by three functions in decide.go and nothing at the site that
		// would carry out the loss is a rule one edit away from being gone.
		return nil, fmt.Errorf(
			"cycle: shard %d: a task page may not be answered by the cold store alone", c.shard)
	}

	// A copy of the caller's request with the two fields the merge decides
	// overridden. A copy and not a fresh request, so a field this layer does not
	// know about is not dropped from a read it is only resizing.
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

	// TaskReadsMerged is pages that carried at least one task out of the window
	// — the honest witness that the merge ran.
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
