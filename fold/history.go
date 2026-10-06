package fold

// The window's event history: batches carried by mutable-state records, held
// until the drain writes them.
//
// They live in one slice in WAL order, which is what the drain writes. Reads
// scan it per branch rather than keep an index, so no node can be in one and
// missing from the other.
//
// History is never folded. Two appends of one node stay two rows; the store
// dedupes them by key. Dropping the second would make a read return the first.

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

// addHistory adds the mutation's batches to the window. Batches already written
// through the store before the append are not in the record, so the window
// holds exactly what the record held. Open hole (DURABILITY.md): a replayed
// tail with batches hands them to the applier even if it does not declare
// cold.HistoryApplier.
func (a *Accumulator) addHistory(seqno wal.Seqno, m mutation.Mutation) {
	for _, slot := range m.EventSlots() {
		for _, r := range slot {
			a.historyWrites = append(a.historyWrites, r)
			a.historyTail = seqno
		}
	}
}

// History is every event batch in the window, in WAL order. The applier must
// make each durable no later than the mutable state that points at it, and
// must keep the order: a branch's first batch carries the tree info its later
// batches lack.
func (b Batch) History() []*p.InternalAppendHistoryNodesRequest { return b.history }
