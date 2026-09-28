package mutation

import (
	"testing"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"
	"google.golang.org/protobuf/proto"
)

func TestEncodeWithHistoryCarriesTheBatchesAndEncodeDropsThem(t *testing.T) {
	m := createWithHistory()

	carried, err := EncodeWithHistory(m)
	require.NoError(t, err)
	got, err := Decode(carried, registry())
	require.NoError(t, err)
	require.Len(t, got.Create.NewWorkflowNewEvents, 2,
		"an entry written with history decodes holding it, or the drain writes a state over nodes nobody wrote")
	require.Equal(t, []byte("e2"), got.Create.NewWorkflowNewEvents[1].Node.Events.Data)
	require.Equal(t, "branch-1", got.Create.NewWorkflowNewEvents[1].BranchInfo.BranchId)

	dropped, err := Encode(m)
	require.NoError(t, err)
	got, err = Decode(dropped, registry())
	require.NoError(t, err)
	require.Nil(t, got.Create.NewWorkflowNewEvents,
		"the default mode's writer puts the events down itself, so carrying them too would write them twice")
}

// The compatibility claim the new fields rest on: a record this mode does not
// carry history in is the record the codec wrote before these fields existed.
// Held by comparing the two encoders rather than against recorded bytes, which
// record_format_test.go already does for the rest of the format.
func TestAMutationWithNoBatchesEncodesTheSameBytesEitherWay(t *testing.T) {
	m := createWithHistory()
	m.Create.NewWorkflowNewEvents = nil

	plain, err := Encode(m)
	require.NoError(t, err)
	withHistory, err := EncodeWithHistory(m)
	require.NoError(t, err)
	require.Equal(t, plain, withHistory,
		"a mutation carrying no batches must encode identically in both modes")
}

// Each shape is something the fold or the applier dereferences, and each is
// refused where refusing writes nothing. Past the append the entry is acked and
// every owner inherits it, so the choice would be a crash loop or a silent hole.
func TestTheEncoderRefusesAHistoryBatchNothingCouldApply(t *testing.T) {
	cases := map[string]func(*p.InternalAppendHistoryNodesRequest) *p.InternalAppendHistoryNodesRequest{
		"nil batch": func(*p.InternalAppendHistoryNodesRequest) *p.InternalAppendHistoryNodesRequest { return nil },
		"another shard": func(r *p.InternalAppendHistoryNodesRequest) *p.InternalAppendHistoryNodesRequest {
			r.ShardID = 99
			return r
		},
		"no branch info": func(r *p.InternalAppendHistoryNodesRequest) *p.InternalAppendHistoryNodesRequest {
			r.BranchInfo = nil
			return r
		},
		"no node events": func(r *p.InternalAppendHistoryNodesRequest) *p.InternalAppendHistoryNodesRequest {
			r.Node.Events = nil
			return r
		},
		"new branch, no tree info": func(r *p.InternalAppendHistoryNodesRequest) *p.InternalAppendHistoryNodesRequest {
			r.IsNewBranch, r.TreeInfo = true, nil
			return r
		},
	}
	for name, breakIt := range cases {
		t.Run(name, func(t *testing.T) {
			m := createWithHistory()
			m.Create.NewWorkflowNewEvents[1] = breakIt(m.Create.NewWorkflowNewEvents[1])

			_, err := EncodeWithHistory(m)
			require.ErrorIs(t, err, ErrMalformedHistory)

			_, err = Encode(m)
			require.NoError(t, err,
				"the default mode does not carry the batch, so it has nothing to refuse it for")
		})
	}
}

// The same judgement at the other end. A payload this build did not write
// reaches the fold's key and the applier's blob the same way, so the refusal has
// to be the decoder's as well as the encoder's — a halted replay rather than a
// panicking one.
func TestDecodeRefusesAHistoryBatchNothingCouldApply(t *testing.T) {
	m := createWithHistory()
	payload, err := EncodeWithHistory(m)
	require.NoError(t, err)

	var pb Payload
	require.NoError(t, proto.Unmarshal(payload, &pb))
	pb.GetCreate().NewWorkflowNewEvents[1].BranchInfo = nil

	_, err = Decode(mustMarshal(t, &pb), registry())
	require.ErrorIs(t, err, ErrMalformedHistory)
}

func TestClearEventsEmptiesEverySlot(t *testing.T) {
	for _, m := range []Mutation{createWithHistory(), updateWithHistory(), conflictResolveWithHistory()} {
		t.Run(m.Kind().String(), func(t *testing.T) {
			require.NotEmpty(t, m.EventSlots(), "the fixture must carry batches for the clear to be worth asserting")
			m.ClearEvents()
			for _, slot := range m.EventSlots() {
				require.Empty(t, slot, "a slot left filled is a batch the drain writes a second time")
			}
		})
	}
}

// ---------------------------------------------------------------- fixtures

func historyBatch(shard int32, branch string, nodeID int64, events string) *p.InternalAppendHistoryNodesRequest {
	return &p.InternalAppendHistoryNodesRequest{
		ShardID:     shard,
		BranchToken: []byte(branch),
		Info:        "test",
		BranchInfo: &persistencespb.HistoryBranch{
			TreeId:    "tree-1",
			BranchId:  branch,
			Ancestors: []*persistencespb.HistoryBranchRange{{BranchId: "root", BeginNodeId: 1, EndNodeId: 4}},
		},
		Node: p.InternalHistoryNode{
			NodeID:            nodeID,
			TransactionID:     100 + nodeID,
			PrevTransactionID: 99 + nodeID,
			Events:            blob(events),
		},
	}
}

func createWithHistory() Mutation {
	return Mutation{Create: &p.InternalCreateWorkflowExecutionRequest{
		ShardID:             2,
		Mode:                p.CreateWorkflowModeUpdateCurrent,
		NewWorkflowSnapshot: sampleSnapshot(),
		NewWorkflowNewEvents: []*p.InternalAppendHistoryNodesRequest{
			historyBatch(2, "branch-1", 4, "e1"),
			historyBatch(2, "branch-1", 5, "e2"),
		},
	}}
}

func updateWithHistory() Mutation {
	return Mutation{Update: &p.InternalUpdateWorkflowExecutionRequest{
		ShardID:                 3,
		UpdateWorkflowMutation:  sampleMutation(),
		UpdateWorkflowNewEvents: []*p.InternalAppendHistoryNodesRequest{historyBatch(3, "branch-1", 6, "e3")},
		NewWorkflowNewEvents:    []*p.InternalAppendHistoryNodesRequest{historyBatch(3, "branch-2", 1, "e4")},
	}}
}

func conflictResolveWithHistory() Mutation {
	return Mutation{ConflictResolve: &p.InternalConflictResolveWorkflowExecutionRequest{
		ShardID:                        4,
		ResetWorkflowSnapshot:          sampleSnapshot(),
		CurrentWorkflowEventsNewEvents: []*p.InternalAppendHistoryNodesRequest{historyBatch(4, "branch-1", 7, "e5")},
		ResetWorkflowEventsNewEvents:   []*p.InternalAppendHistoryNodesRequest{historyBatch(4, "branch-2", 2, "e6")},
		NewWorkflowEventsNewEvents:     []*p.InternalAppendHistoryNodesRequest{historyBatch(4, "branch-3", 1, "e7")},
	}}
}
