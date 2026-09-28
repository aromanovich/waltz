package fold

// The window's half of event history: the batches a mutable-state record
// carried, kept until the drain writes them.
//
// One slice, in WAL order, because that is what the drain writes: a row per
// request and a tree row beside a new branch. A read wants one branch's nodes
// and derives them from it — an index kept beside the slice would be a second
// thing to append to and a second thing to reset, and both divergences are
// silent in opposite directions (a node in one and not the other is either
// history no read shows or history no drain writes).
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

// branchNodes is the window's nodes for one branch, in WAL order. The tree is
// part of the key because a branch id is unique only inside one.
func (a *Accumulator) branchNodes(treeID, branchID string) []p.InternalHistoryNode {
	var out []p.InternalHistoryNode
	for _, r := range a.historyWrites {
		if r.BranchInfo.TreeId == treeID && r.BranchInfo.BranchId == branchID {
			out = append(out, r.Node)
		}
	}
	return out
}

// addHistory takes the mutation's batches into the window. A mutation whose
// batches were written through the store before the append carries none, so this
// is the cold store's fork and there is no flag here: what the accumulator holds is
// what the record held.
func (a *Accumulator) addHistory(seqno wal.Seqno, m mutation.Mutation) {
	for _, slot := range m.EventSlots() {
		for _, r := range slot {
			a.historyWrites = append(a.historyWrites, r)
			a.historyTail = seqno
		}
	}
}

// History is every event batch this window carries, in WAL order: what a drain
// writes before it publishes the mutable state pointing at it.
//
// The order is the caller's own and matters for one row of it — a batch opening
// a branch carries the tree info that branch's later batches do not.
func (b Batch) History() []*p.InternalAppendHistoryNodesRequest { return b.history }
