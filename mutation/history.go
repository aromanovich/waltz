package mutation

import (
	"errors"
	"fmt"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"
)

// The event batches a mutable-state request carries, into the mirror and back.
//
// The codec carries whatever the mutation still holds and decides nothing: a
// record written with none is byte-for-byte what the codec wrote before these
// fields existed.

// ErrMalformedHistory is what [Encode] and [Decode] answer a request
// whose event batches are missing something the fold keys on or the applier
// writes: a nil batch, one belonging to another shard, one with no branch, one
// whose node carries no events, or one opening a branch with no tree info.
//
// Encode is the last point that can refuse one. Past the append the entry is
// acked and every owner inherits it, so a batch first judged where it is
// applied leaves the choice between a crash loop and a silent hole — the same
// reasoning [ErrUncarriedProto] carries, at the other half of the request.
// Decode judges again so that a payload this build did not write is named
// rather than dereferenced.
var ErrMalformedHistory = errors.New("mutation: history batch is missing a field the fold or the applier needs")

// validateHistory holds the mutation's batches against what reads them. Every
// clause is a dereference somewhere else: the shard id keys the row the drain
// of this mutation's shard writes, the branch is the fold's key, the node's blob
// is what the applier writes, and the tree info is the row a new branch needs
// beside it.
func validateHistory(m Mutation) error {
	shard := m.ShardID()
	for _, slot := range m.EventSlots() {
		for _, r := range slot {
			var why string
			switch {
			case r == nil:
				why = "a nil batch"
			case r.ShardID != shard:
				why = fmt.Sprintf("a batch for shard %d", r.ShardID)
			case r.BranchInfo == nil:
				why = "a batch with no branch info"
			case r.Node.Events == nil:
				why = fmt.Sprintf("node %d with no events", r.Node.NodeID)
			case r.IsNewBranch && r.TreeInfo == nil:
				why = fmt.Sprintf("a new branch %q with no tree info", r.BranchInfo.BranchId)
			default:
				continue
			}
			return fmt.Errorf("mutation: %s carries %s: %w", m.Kind(), why, ErrMalformedHistory)
		}
	}
	return nil
}

// encodeHistoryRequests mirrors one slot. An empty slot encodes to an absent
// field, which is the absent-vs-empty rule the rest of this codec keeps and what
// makes a batch-less mutation's bytes the ones written before these fields
// existed.
func encodeHistoryRequests(rs []*p.InternalAppendHistoryNodesRequest) []*AppendHistoryNodesRequest {
	if len(rs) == 0 {
		return nil
	}
	out := make([]*AppendHistoryNodesRequest, 0, len(rs))
	for _, r := range rs {
		out = append(out, &AppendHistoryNodesRequest{
			BranchToken: r.BranchToken,
			IsNewBranch: r.IsNewBranch,
			Info:        r.Info,
			BranchInfo:  encodeHistoryBranch(r.BranchInfo),
			TreeInfo:    encodeBlob(r.TreeInfo),
			Node: &HistoryNode{
				NodeId:            r.Node.NodeID,
				TransactionId:     r.Node.TransactionID,
				PrevTransactionId: r.Node.PrevTransactionID,
				Events:            encodeBlob(r.Node.Events),
			},
			ShardId: r.ShardID,
		})
	}
	return out
}

func encodeHistoryBranch(b *persistencespb.HistoryBranch) *HistoryBranch {
	if b == nil {
		return nil
	}
	out := &HistoryBranch{TreeId: b.TreeId, BranchId: b.BranchId}
	for _, a := range b.Ancestors {
		if a == nil {
			out.Ancestors = append(out.Ancestors, nil)
			continue
		}
		out.Ancestors = append(out.Ancestors, &HistoryBranchRange{
			BranchId:    a.BranchId,
			BeginNodeId: a.BeginNodeId,
			EndNodeId:   a.EndNodeId,
		})
	}
	return out
}

func decodeHistoryRequests(rs []*AppendHistoryNodesRequest) []*p.InternalAppendHistoryNodesRequest {
	if len(rs) == 0 {
		return nil
	}
	out := make([]*p.InternalAppendHistoryNodesRequest, 0, len(rs))
	for _, r := range rs {
		if r == nil {
			// Refused at both ends, so this is a payload no encoder here wrote.
			// It travels to validateHistory, which names it.
			out = append(out, nil)
			continue
		}
		var node p.InternalHistoryNode
		if r.Node != nil {
			node = p.InternalHistoryNode{
				NodeID:            r.Node.NodeId,
				TransactionID:     r.Node.TransactionId,
				PrevTransactionID: r.Node.PrevTransactionId,
				Events:            decodeBlob(r.Node.Events),
			}
		}
		out = append(out, &p.InternalAppendHistoryNodesRequest{
			BranchToken: r.BranchToken,
			IsNewBranch: r.IsNewBranch,
			Info:        r.Info,
			BranchInfo:  decodeHistoryBranch(r.BranchInfo),
			TreeInfo:    decodeBlob(r.TreeInfo),
			Node:        node,
			ShardID:     r.ShardId,
		})
	}
	return out
}

func decodeHistoryBranch(b *HistoryBranch) *persistencespb.HistoryBranch {
	if b == nil {
		return nil
	}
	out := &persistencespb.HistoryBranch{TreeId: b.TreeId, BranchId: b.BranchId}
	for _, a := range b.Ancestors {
		if a == nil {
			out.Ancestors = append(out.Ancestors, nil)
			continue
		}
		out.Ancestors = append(out.Ancestors, &persistencespb.HistoryBranchRange{
			BranchId:    a.BranchId,
			BeginNodeId: a.BeginNodeId,
			EndNodeId:   a.EndNodeId,
		})
	}
	return out
}
