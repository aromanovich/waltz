package cycle

// Merge-on-read for event history: who may answer a branch page, and when.
// The page's contents (cut, token, order, dedup) are
// [fold.Accumulator.HistoryPage].
//
// It routes like the mutable-state reads, not the task page: a history reader
// deletes nothing, so a short page is staleness, which [tailRoute] decides.
// Like the task page its token is ours, so every route that answers without
// the window unwraps it first ([fold.BaseHistoryToken]); our token reaching
// the plugin's parser fails the pagination.

import (
	"context"

	p "go.temporal.io/server/common/persistence"

	"github.com/aromanovich/waltz/fold"
	"github.com/aromanovich/waltz/wal"
)

// BaseHistory is the cold store's own branch read, handed down by the wrapper.
// It takes a request because the merge sends its own page size and token. An
// alias, like [BaseTasks], so [wrapper.ShardReader] can spell the same type.
type BaseHistory = func(context.Context, *p.InternalReadHistoryBranchRequest) (*p.InternalReadHistoryBranchResponse, error)

// ReadHistoryBranch answers one page of a branch for the shard the request
// names. The wrapper parses treeID from the branch token, whose codec is the
// store's.
func (m *Manager) ReadHistoryBranch(
	ctx context.Context,
	req *p.InternalReadHistoryBranchRequest,
	treeID string,
	base BaseHistory,
) (*p.InternalReadHistoryBranchResponse, error) {
	shard := wal.ShardID(req.ShardID)
	c := m.Shard(shard)
	if c == nil {
		route, refusal := noCycleRoute(mutableStateRead, shard)
		return offLoop(ctx, route, refusal, baseAlone(req, base))
	}
	return c.readHistoryBranch(ctx, req, treeID, base)
}

func (c *Cycle) readHistoryBranch(
	ctx context.Context,
	req *p.InternalReadHistoryBranchRequest,
	treeID string,
	base BaseHistory,
) (*p.InternalReadHistoryBranchResponse, error) {
	resp, stopped, err := ask(ctx, c, func(s *state) (*p.InternalReadHistoryBranchResponse, error) {
		return c.readHistoryPage(ctx, s, req, treeID, base)
	})
	if stopped {
		route, refusal := c.stoppedRead(mutableStateRead, err)
		return offLoop(ctx, route, refusal, baseAlone(req, base))
	}
	return resp, err
}

// baseAlone reads a page from the cold store alone, unwrapping the caller's
// token in case this layer wrote it. That drops the window's half of the page,
// which the pass-through route has already judged safe (a non-empty tail is
// refused instead).
func baseAlone(
	req *p.InternalReadHistoryBranchRequest, base BaseHistory,
) func(context.Context) (*p.InternalReadHistoryBranchResponse, error) {
	return func(ctx context.Context) (*p.InternalReadHistoryBranchResponse, error) {
		ask := *req
		ask.NextPageToken = fold.BaseHistoryToken(req.NextPageToken)
		return base(ctx, &ask)
	}
}

// readHistoryPage is the loop's half. The page is built after [Cycle.prelude],
// so a replay that resets the accumulator is not read around.
func (c *Cycle) readHistoryPage(
	ctx context.Context,
	s *state,
	req *p.InternalReadHistoryBranchRequest,
	treeID string,
	base BaseHistory,
) (*p.InternalReadHistoryBranchResponse, error) {
	// No takeView, so Reads and ReadsHeld stay the overlay's: witnesses read
	// ReadsHeld as "the overlay crossed a held workflow", and a branch page
	// raising it would hide an overlay that did nothing.
	switch pass, err := c.prelude(ctx, s, mutableStateRead, nil); {
	case err != nil:
		return nil, err
	case pass:
		return baseAlone(req, base)(ctx)
	}

	return s.acc.HistoryPage(req, treeID, func(pageSize int, token []byte) ([]p.InternalHistoryNode, []byte, error) {
		ask := *req
		ask.PageSize, ask.NextPageToken = pageSize, token
		resp, err := base(ctx, &ask)
		if err != nil {
			return nil, nil, err
		}
		return resp.Nodes, resp.NextPageToken, nil
	})
}
