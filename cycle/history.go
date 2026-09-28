package cycle

// Merge-on-read for event history: who may answer a branch page, when, and out
// of which window. The page's contents are not here — the cut, the token, the
// order and the dedup are [fold.Accumulator.HistoryPage], beside the window they
// read.
//
// It is the third merged read and it routes like the two mutable-state ones
// rather than like the task page. A history reader does not delete what it read,
// so a page short a node is a workflow rebuilt short its newest events rather
// than a row nobody asks for again — which is the staleness [tailRoute] already
// decides who may pay. What it does share with the task page is a token this
// layer wrote, so every route that answers without the window unwraps it first
// ([fold.BaseHistoryToken]): one of ours reaching a plugin's parser fails a
// pagination in the middle.

import (
	"context"

	p "go.temporal.io/server/common/persistence"

	"github.com/aromanovich/waltz/fold"
	"github.com/aromanovich/waltz/wal"
)

// BaseHistory is the cold store's own branch read, handed down by the wrapper.
// It takes a request rather than closing over one, because the merge issues a
// different one than the caller's: its own page size, and the base's own token.
//
// An alias for [wrapper.ShardReader]'s reason: the two packages that must agree
// on this signature may not import each other.
type BaseHistory = func(context.Context, *p.InternalReadHistoryBranchRequest) (*p.InternalReadHistoryBranchResponse, error)

// ReadHistoryBranch answers one page of a branch for the shard the request
// names. treeID is parsed from the branch token by the wrapper, the branch-token
// codec being the store's.
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

// baseAlone is the cold store's answer for a page this layer is not merging
// into: the caller's own request with its page token unwrapped, since the token
// it holds may be one this layer wrote on an earlier page.
//
// Unwrapping loses the window's half of that page, which is exactly what the
// route deciding to pass through has already decided is acceptable — a
// non-empty tail is refused rather than passed through.
func baseAlone(
	req *p.InternalReadHistoryBranchRequest, base BaseHistory,
) func(context.Context) (*p.InternalReadHistoryBranchResponse, error) {
	return func(ctx context.Context) (*p.InternalReadHistoryBranchResponse, error) {
		ask := *req
		ask.NextPageToken = fold.BaseHistoryToken(req.NextPageToken)
		return base(ctx, &ask)
	}
}

// readHistoryPage is the loop's half. The window's view is taken inside
// [Cycle.prelude] so a replay that resets the accumulator is not read around,
// and the page itself is built after it.
func (c *Cycle) readHistoryPage(
	ctx context.Context,
	s *state,
	req *p.InternalReadHistoryBranchRequest,
	treeID string,
	base BaseHistory,
) (*p.InternalReadHistoryBranchResponse, error) {
	// No view is taken, for the reason the task page takes none: Reads and
	// ReadsHeld are the mutable-state overlay's numbers, and three witness rules
	// read ReadsHeld as "the overlay crossed a held workflow". A branch page
	// raising it would let that claim be satisfied by a read that never touched
	// the overlay at all — the silent pass those rules exist to catch.
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
