package mutation

import (
	"errors"
	"fmt"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"
)

// The event batches a mutable-state request carries, into the mirror and back.
//
// The codec carries whatever batches the mutation holds. A record with none
// encodes exactly as it would without these fields.

// ErrMalformedHistory is returned by [Encode] and [Decode] for event batches
// missing something the fold or applier needs: a nil batch, another shard's
// batch, no branch, a node with no events, or a new branch without tree info.
//
// Encode is the last chance to refuse: after the ack, a bad entry leaves only
// a crash loop or a silent hole (as with [ErrUncarriedProto]). Decode checks
// again so a foreign payload is reported rather than dereferenced.
var ErrMalformedHistory = errors.New("mutation: history batch is missing a field the fold or the applier needs")

// validateHistory checks each batch has what its readers dereference: the
// shard id (the drain's row key), the branch (fold's key), the node's blob
// (what the applier writes), and tree info for a new branch.
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

// encodeHistoryRequests mirrors one slot. An empty slot encodes as an absent
// field, like every other collection in this codec.
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
			// Only a foreign payload gets here; validateHistory reports it.
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
