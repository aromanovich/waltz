package fold

// The window's half of event history: the batches a mutable-state record
// carried, kept until the drain writes them.
//
// Two shapes of the same nodes, because two callers want different things. The
// drain wants the requests, in WAL order, because what it writes is a row per
// request and a tree row beside a new branch. A read wants the nodes of one
// branch, because a page is merged per branch.
//
// Nothing here folds. Two appends of one node are two rows the store dedupes on
// the key it writes them under, and a window that dropped the second would
// answer a read with the first — a node's blob is the caller's, not a state this
// layer may collapse.

import (
	p "go.temporal.io/server/common/persistence"

	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/wal"
)

// historyBranch is what a page is read by and what the window keys its nodes
// on. The tree is part of it because a branch id is unique only inside one.
type historyBranch struct {
	treeID   string
	branchID string
}

// addHistory takes the mutation's batches into the window. A mutation whose
// batches were written through the store before the append carries none, so this
// is the mode's fork and there is no flag here: what the accumulator holds is
// what the record held.
func (a *Accumulator) addHistory(seqno wal.Seqno, m mutation.Mutation) {
	for _, slot := range m.EventSlots() {
		for _, r := range slot {
			branch := historyBranch{treeID: r.BranchInfo.TreeId, branchID: r.BranchInfo.BranchId}
			a.history[branch] = append(a.history[branch], r.Node)
			a.historyWrites = append(a.historyWrites, r)
			a.historyTail = seqno
		}
	}
}

// heldHistory reports whether the window holds a node of the branch inside the
// range a read names. It is the read's "did the window have anything for this"
// — [Accumulator.HistoryPage] is what decides the page.
func (a *Accumulator) heldHistory(treeID, branchID string, minNodeID, maxNodeID int64) bool {
	for _, node := range a.history[historyBranch{treeID: treeID, branchID: branchID}] {
		if node.NodeID >= minNodeID && node.NodeID < maxNodeID {
			return true
		}
	}
	return false
}

// History is every event batch this window carries, in WAL order: what a drain
// writes before it publishes the mutable state pointing at it.
//
// The order is the caller's own and matters for one row of it — a batch opening
// a branch carries the tree info that branch's later batches do not.
func (b Batch) History() []*p.InternalAppendHistoryNodesRequest { return b.history }
