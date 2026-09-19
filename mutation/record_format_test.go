package mutation

// The wire format's pin. Everything else in this package encodes and decodes
// with one build, so a slot swapped on *both* sides of the mirror round-trips
// perfectly — and that is the shape a rolling restart produces, since
// Payload.format is the only version there is and there is no migration path.
// A node coming back on a new binary replays a tail the old one wrote.

import (
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"google.golang.org/protobuf/proto"
)

func fixBlob(s string) *commonpb.DataBlob {
	return &commonpb.DataBlob{Data: []byte(s), EncodingType: enumspb.ENCODING_TYPE_PROTO3}
}

func recordedFixture() Mutation {
	state := &persistencespb.WorkflowExecutionState{
		RunId:           "run-of-the-record",
		CreateRequestId: "create-request",
		State:           enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		Status:          enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}
	blob, err := serialization.WorkflowExecutionStateToBlob(state)
	if err != nil {
		panic(err)
	}
	raw, err := proto.Marshal(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: "namespace-of-the-record",
		WorkflowId:  "workflow-of-the-record",
		TaskQueue:   "the-task-queue",
	})
	if err != nil {
		panic(err)
	}
	info := &commonpb.DataBlob{Data: raw, EncodingType: enumspb.ENCODING_TYPE_PROTO3}
	return Mutation{Update: &p.InternalUpdateWorkflowExecutionRequest{
		ShardID: 7,
		Mode:    p.UpdateWorkflowModeUpdateCurrent,
		UpdateWorkflowMutation: p.InternalWorkflowMutation{
			NamespaceID:        "namespace-of-the-record",
			WorkflowID:         "workflow-of-the-record",
			RunID:              "run-of-the-record",
			ExecutionInfoBlob:  info,
			ExecutionStateBlob: blob,

			NextEventID:      71,
			StartVersion:     72,
			LastWriteVersion: 73,
			DBRecordVersion:  74,
			Condition:        75,

			UpsertActivityInfos:       map[int64]*commonpb.DataBlob{1: fixBlob("the activity")},
			UpsertTimerInfos:          map[string]*commonpb.DataBlob{"t": fixBlob("the timer")},
			UpsertChildExecutionInfos: map[int64]*commonpb.DataBlob{2: fixBlob("the child")},
			UpsertRequestCancelInfos:  map[int64]*commonpb.DataBlob{3: fixBlob("the cancel")},
			UpsertSignalInfos:         map[int64]*commonpb.DataBlob{4: fixBlob("the signal")},
			UpsertSignalRequestedIDs:  map[string]struct{}{"wanted": {}},

			DeleteActivityInfos:       map[int64]struct{}{11: {}},
			DeleteTimerInfos:          map[string]struct{}{"dt": {}},
			DeleteChildExecutionInfos: map[int64]struct{}{12: {}},
			DeleteRequestCancelInfos:  map[int64]struct{}{13: {}},
			DeleteSignalInfos:         map[int64]struct{}{14: {}},
			DeleteSignalRequestedIDs:  map[string]struct{}{"unwanted": {}},

			Checksum: fixBlob("the checksum"),
		},
	}}
}

// recordedUpdate is what a build wrote for [recordedFixture], kept as bytes so
// that this case is about an entry already in somebody's log rather than about
// what this build happens to produce. Re-recording it from a changed encoder
// would assert nothing: if the values below stop matching, the question to ask
// is whether an inherited tail still means what its writer meant.
const recordedUpdate = "CAEa6AIIBxrjAgoXbmFtZXNwYWNlLW9mLXRoZS1yZWNvcmQSFndvcmtmbG93LW9mLXRoZS1yZWNv" +
	"cmQaEXJ1bi1vZi10aGUtcmVjb3JkIkUKQQoXbmFtZXNwYWNlLW9mLXRoZS1yZWNvcmQSFndvcmtm" +
	"bG93LW9mLXRoZS1yZWNvcmRKDnRoZS10YXNrLXF1ZXVlEAEqKwonCg5jcmVhdGUtcmVxdWVzdBIR" +
	"cnVuLW9mLXRoZS1yZWNvcmQYAiABEAEwRzhIQElISlBLWhQIARIQCgx0aGUgYWN0aXZpdHkQAWIB" +
	"C2oSCgF0Eg0KCXRoZSB0aW1lchABcgJkdHoRCAISDQoJdGhlIGNoaWxkEAGCAQEMigESCAMSDgoK" +
	"dGhlIGNhbmNlbBABkgEBDZoBEggEEg4KCnRoZSBzaWduYWwQAaIBAQ66AQZ3YW50ZWTCAQh1bndh" +
	"bnRlZOIBEAoMdGhlIGNoZWNrc3VtEAE="

// TestARecordedEntryStillMeansWhatItsWriterMeant decodes those bytes and names
// every slot two same-typed fields could have been swapped between. Five of the
// mutation's scalars are int64, four of its upsert collections are
// map[int64]*DataBlob and five of its deletes are map[int64]struct{} or
// map[string]struct{} — so each of those lines can be crossed with its
// neighbours and still compile.
//
// The round-trip cases cannot see it. Swapping next-event-id with
// db-record-version in the encoder *and* the decoder together left the whole of
// `go test ./...` green, and so did swapping the child executions with the
// request cancels: both arms share the defect, so it cancels. Only a record
// this build did not write can tell.
func TestARecordedEntryStillMeansWhatItsWriterMeant(t *testing.T) {
	t.Run("the bytes an earlier build wrote", func(t *testing.T) {
		raw, err := base64.StdEncoding.DecodeString(recordedUpdate)
		require.NoError(t, err)
		m, err := Decode(raw, registry())
		require.NoError(t, err)
		requireRecordMeaning(t, m)
	})

	// The same claims through this build's own encoder. Both arms failing says
	// the meaning moved; this one alone says the recorded constant is stale,
	// which is the only reason to touch it.
	t.Run("the bytes this build writes", func(t *testing.T) {
		raw, err := Encode(recordedFixture())
		require.NoError(t, err)
		m, err := Decode(raw, registry())
		require.NoError(t, err)
		requireRecordMeaning(t, m)
	})
}

// requireRecordMeaning names every slot two same-typed fields could have been
// swapped between. Five of the mutation's scalars are int64, four of its upsert
// collections are map[int64]*DataBlob, and its delete sets share two key types,
// so each of those lines can be crossed with its neighbours and still compile.
func requireRecordMeaning(t *testing.T, m Mutation) {
	t.Helper()
	require.Equal(t, KindUpdate, m.Kind())

	req := m.Update
	require.EqualValues(t, 7, req.ShardID)
	require.Equal(t, p.UpdateWorkflowModeUpdateCurrent, req.Mode)

	mut := req.UpdateWorkflowMutation
	require.Equal(t, "namespace-of-the-record", mut.NamespaceID)
	require.Equal(t, "workflow-of-the-record", mut.WorkflowID)
	require.Equal(t, "run-of-the-record", mut.RunID)

	require.EqualValues(t, 71, mut.NextEventID, "next-event-id moved slot")
	require.EqualValues(t, 72, mut.StartVersion, "start-version moved slot")
	require.EqualValues(t, 73, mut.LastWriteVersion, "last-write-version moved slot")
	require.EqualValues(t, 74, mut.DBRecordVersion, "db-record-version moved slot")
	require.EqualValues(t, 75, mut.Condition, "condition moved slot")

	require.Equal(t, "the-task-queue", mut.ExecutionInfo.TaskQueue,
		"the execution info comes back off its own blob")
	require.Equal(t, "run-of-the-record", mut.ExecutionState.RunId)
	require.Equal(t, "the checksum", string(mut.Checksum.Data))

	require.Equal(t, "the activity", string(mut.UpsertActivityInfos[1].Data))
	require.Equal(t, "the timer", string(mut.UpsertTimerInfos["t"].Data))
	require.Equal(t, "the child", string(mut.UpsertChildExecutionInfos[2].Data))
	require.Equal(t, "the cancel", string(mut.UpsertRequestCancelInfos[3].Data))
	require.Equal(t, "the signal", string(mut.UpsertSignalInfos[4].Data))
	require.Contains(t, mut.UpsertSignalRequestedIDs, "wanted")

	require.Equal(t, map[int64]struct{}{11: {}}, mut.DeleteActivityInfos)
	require.Equal(t, map[string]struct{}{"dt": {}}, mut.DeleteTimerInfos)
	require.Equal(t, map[int64]struct{}{12: {}}, mut.DeleteChildExecutionInfos)
	require.Equal(t, map[int64]struct{}{13: {}}, mut.DeleteRequestCancelInfos)
	require.Equal(t, map[int64]struct{}{14: {}}, mut.DeleteSignalInfos)
	require.Equal(t, map[string]struct{}{"unwanted": {}}, mut.DeleteSignalRequestedIDs)
}
