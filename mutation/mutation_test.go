package mutation

import (
	"slices"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func registry() tasks.TaskCategoryRegistry { return tasks.NewDefaultTaskCategoryRegistry() }

// compareOptions ignores what no encoding preserves: proto internal state, and
// the monotonic reading a time.Time read from a clock carries. A category is
// compared by id because that is all the payload carries — the type and the name
// come back from the registry this process built.
var compareOptions = []cmp.Option{
	protocmp.Transform(),
	cmp.Comparer(func(a, b time.Time) bool {
		return a.Equal(b) && a.IsZero() == b.IsZero()
	}),
	cmp.Comparer(func(a, b tasks.Category) bool { return a.ID() == b.ID() }),
}

func TestRoundTripUpdate(t *testing.T) {
	original := Mutation{Update: &p.InternalUpdateWorkflowExecutionRequest{
		ShardID:                1,
		RangeID:                77, // dropped by the codec
		Mode:                   p.UpdateWorkflowModeUpdateCurrent,
		UpdateWorkflowMutation: sampleMutation(),
		NewWorkflowSnapshot:    new(sampleSnapshot()),
	}}

	payload, err := Encode(original)
	require.NoError(t, err)
	decoded, err := Decode(payload, registry())
	require.NoError(t, err)

	// RangeID is the epoch (I11) and travels with the entry, so the codec drops
	// it and apply refills it. The only difference a round trip may have.
	require.Zero(t, decoded.Update.RangeID, "RangeID must not survive the codec: it is the epoch")
	original.Update.RangeID = 0

	require.Equal(t, KindUpdate, decoded.Kind())
	if diff := cmp.Diff(original, decoded, compareOptions...); diff != "" {
		t.Fatalf("round trip changed the mutation (-want +got):\n%s", diff)
	}
}

func TestRoundTripEveryKind(t *testing.T) {
	snapshot := sampleSnapshot()
	mutation := sampleMutation()
	rangeStart := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)

	cases := map[string]Mutation{
		"create": {Create: &p.InternalCreateWorkflowExecutionRequest{
			ShardID:                  2,
			Mode:                     p.CreateWorkflowModeUpdateCurrent,
			PreviousRunID:            "prev-run",
			PreviousLastWriteVersion: -24, // EmptyVersion: negative, and legal
			NewWorkflowSnapshot:      snapshot,
		}},
		"update": {Update: &p.InternalUpdateWorkflowExecutionRequest{
			ShardID:                3,
			Mode:                   p.UpdateWorkflowModeBypassCurrent,
			UpdateWorkflowMutation: mutation,
		}},
		"conflict-resolve": {ConflictResolve: &p.InternalConflictResolveWorkflowExecutionRequest{
			ShardID:                 4,
			Mode:                    p.ConflictResolveWorkflowModeUpdateCurrent,
			ResetWorkflowSnapshot:   snapshot,
			NewWorkflowSnapshot:     new(sampleSnapshot()),
			CurrentWorkflowMutation: new(sampleMutation()),
		}},
		// The zero value of ConflictResolveWorkflowMode is UpdateCurrent, so the
		// case above round-trips identically whether the mode is carried or
		// dropped. This one carries the other mode, in the shape that goes with
		// it: bypassing the current row leaves no current mutation to send.
		"conflict-resolve bypassing the current row": {
			ConflictResolve: &p.InternalConflictResolveWorkflowExecutionRequest{
				ShardID:               10,
				Mode:                  p.ConflictResolveWorkflowModeBypassCurrent,
				ResetWorkflowSnapshot: snapshot,
			}},
		"set": {Set: &p.InternalSetWorkflowExecutionRequest{
			ShardID:             5,
			SetWorkflowSnapshot: snapshot,
		}},
		"delete": {Delete: &p.DeleteWorkflowExecutionRequest{
			ShardID: 6, NamespaceID: "ns", WorkflowID: "wf", RunID: "run",
		}},
		"delete-current": {DeleteCurrent: &p.DeleteCurrentWorkflowExecutionRequest{
			ShardID: 7, NamespaceID: "ns", WorkflowID: "wf", RunID: "run",
		}},
		"add-tasks": {AddTasks: &p.InternalAddHistoryTasksRequest{
			ShardID:     8,
			NamespaceID: "ns-add-tasks",
			WorkflowID:  "order-8123",
			Tasks:       sampleTaskGroups(),
		}},
		"range-complete-tasks": {RangeCompleteTasks: &p.RangeCompleteHistoryTasksRequest{
			ShardID:      9,
			TaskCategory: tasks.CategoryTimer,
			// Two keys of the same shape and different in both fields, so a
			// bound delivered to the other's slot is a changed range rather
			// than an equal one.
			InclusiveMinTaskKey: tasks.NewKey(rangeStart, 4001),
			ExclusiveMaxTaskKey: tasks.NewKey(rangeStart.Add(time.Hour), 4002),
		}},
	}

	covered := map[Kind]bool{}
	for name, original := range cases {
		t.Run(name, func(t *testing.T) {
			payload, err := Encode(original)
			require.NoError(t, err)
			decoded, err := Decode(payload, registry())
			require.NoError(t, err)
			require.Equal(t, original.Kind(), decoded.Kind())
			require.Equal(t, original.ShardID(), decoded.ShardID())
			if diff := cmp.Diff(original, decoded, compareOptions...); diff != "" {
				t.Fatalf("round trip changed the mutation (-want +got):\n%s", diff)
			}
		})
		covered[original.Kind()] = true
	}

	// Every kind must appear above, so a new one fails here by name rather than
	// on the first replay of an entry encode had no arm for.
	for k := KindInvalid + 1; int(k) < KindCount; k++ {
		require.True(t, covered[k], "kind %s has no case round-tripping it through the codec", k)
	}
}

// The two delete requests carry the same four fields and must still decode as
// different kinds: fold makes one a tombstone for the workflow and the other
// for the current-run pointer.
func TestDeleteKindsAreNotInterchangeable(t *testing.T) {
	del, err := Encode(Mutation{Delete: &p.DeleteWorkflowExecutionRequest{ShardID: 1, RunID: "r"}})
	require.NoError(t, err)
	cur, err := Encode(Mutation{DeleteCurrent: &p.DeleteCurrentWorkflowExecutionRequest{ShardID: 1, RunID: "r"}})
	require.NoError(t, err)
	require.NotEqual(t, del, cur)

	decoded, err := Decode(cur, registry())
	require.NoError(t, err)
	require.Equal(t, KindDeleteCurrent, decoded.Kind())
}

// Encode must be a function of its argument, which Go map iteration is not:
// every collection has to travel in sorted key order.
func TestEncodeIsDeterministic(t *testing.T) {
	m := Mutation{Update: &p.InternalUpdateWorkflowExecutionRequest{
		ShardID:                1,
		UpdateWorkflowMutation: sampleMutation(),
	}}

	first, err := Encode(m)
	require.NoError(t, err)
	for i := range 200 {
		again, err := Encode(m)
		require.NoError(t, err)
		require.Equal(t, first, again, "encoding is not a function of the mutation (run %d)", i)
	}
}

// The two fire times a bare nanosecond count cannot tell apart.
func TestFireTimeZeroIsNotTheEpoch(t *testing.T) {
	m := Mutation{Update: &p.InternalUpdateWorkflowExecutionRequest{
		ShardID: 1,
		UpdateWorkflowMutation: p.InternalWorkflowMutation{
			RunID: "run",
			Tasks: map[tasks.Category][]p.InternalHistoryTask{
				tasks.CategoryTransfer: {
					// tasks.DefaultFireTime, an immediate task's key.
					{Key: tasks.NewImmediateKey(1), Blob: blob("a")},
					// The zero Time, which UnixNano cannot express.
					{Key: tasks.Key{TaskID: 2}, Blob: blob("b")},
				},
			},
		},
	}}

	payload, err := Encode(m)
	require.NoError(t, err)
	decoded, err := Decode(payload, registry())
	require.NoError(t, err)

	got := decoded.Update.UpdateWorkflowMutation.Tasks[tasks.CategoryTransfer]
	require.Len(t, got, 2)
	require.True(t, got[0].Key.FireTime.Equal(tasks.DefaultFireTime), "immediate key lost its fire time")
	require.False(t, got[0].Key.FireTime.IsZero(), "the epoch is not the zero time")
	require.True(t, got[1].Key.FireTime.IsZero(), "the zero time did not survive as zero")
}

// Category ids are re-resolved through the process's own registry, so an id
// this node lacks must be an error: a fallback would drop tasks silently.
func TestUnknownCategoryIsAnError(t *testing.T) {
	reg := tasks.NewDefaultTaskCategoryRegistry()
	unknown := tasks.NewCategory(9999, tasks.CategoryTypeImmediate, "made-up")
	reg.AddCategory(unknown)

	payload, err := Encode(Mutation{Update: &p.InternalUpdateWorkflowExecutionRequest{
		ShardID: 1,
		UpdateWorkflowMutation: p.InternalWorkflowMutation{
			RunID: "run",
			Tasks: map[tasks.Category][]p.InternalHistoryTask{
				unknown: {{Key: tasks.NewImmediateKey(1), Blob: blob("x")}},
			},
		},
	}})
	require.NoError(t, err)

	_, err = Decode(payload, tasks.NewDefaultTaskCategoryRegistry())
	require.ErrorIs(t, err, ErrUnknownCategory)
	require.Contains(t, err.Error(), "9999")

	_, err = Decode(payload, reg)
	require.NoError(t, err)
}

// Every collection travels as repeated entries, and a key repeated among them
// would decode as the last value alone — one activity's blob or a whole
// category's tasks gone, with nothing said. Encode cannot write such a payload,
// so each case builds one.
func TestDuplicateKeysAreRefused(t *testing.T) {
	cases := map[string]struct {
		m    *WorkflowMutation
		says string
	}{
		"int-keyed blobs": {
			m: &WorkflowMutation{UpsertActivityInfos: []*Int64BlobEntry{
				{Key: 5, Blob: &Blob{Data: []byte("first")}},
				{Key: 5, Blob: &Blob{Data: []byte("second")}},
			}},
			says: `duplicate blob key 5`,
		},
		"string-keyed blobs": {
			m: &WorkflowMutation{UpsertTimerInfos: []*StringBlobEntry{
				{Key: "timer-a", Blob: &Blob{Data: []byte("first")}},
				{Key: "timer-a", Blob: &Blob{Data: []byte("second")}},
			}},
			says: `duplicate blob key "timer-a"`,
		},
		"task groups": {
			m: &WorkflowMutation{Tasks: []*TaskGroup{
				{CategoryId: int32(tasks.CategoryTimer.ID()), Tasks: []*Task{{TaskId: 1}}},
				{CategoryId: int32(tasks.CategoryTimer.ID()), Tasks: []*Task{{TaskId: 2}}},
			}},
			says: `duplicate task category id 2`,
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			payload := mustMarshal(t, &Payload{
				Format: formatVersion,
				Request: &Payload_Update{Update: &UpdateRequest{
					ShardId:  1,
					Mutation: c.m,
				}},
			})
			_, err := Decode(payload, registry())
			require.ErrorContains(t, err, c.says)
		})
	}
}

// Encode refuses a CassandraBlob rather than dropping the state quietly.
func TestCassandraBlobIsRefused(t *testing.T) {
	_, err := Encode(Mutation{Update: &p.InternalUpdateWorkflowExecutionRequest{
		ShardID: 1,
		UpdateWorkflowMutation: p.InternalWorkflowMutation{
			RunID: "run",
			UpsertChasmNodes: map[string]p.InternalChasmNode{
				"n": {CassandraBlob: blob("cassandra")},
			},
		},
	}})
	require.ErrorIs(t, err, ErrCassandraBlob)
}

// Unknown fields are refused rather than skipped, at the top level and nested:
// an entry from a newer codec would otherwise replay with a piece missing.
func TestUnknownFieldIsRefused(t *testing.T) {
	payload, err := Encode(Mutation{Delete: &p.DeleteWorkflowExecutionRequest{
		ShardID: 1, NamespaceID: "ns", WorkflowID: "wf", RunID: "run",
	}})
	require.NoError(t, err)

	t.Run("top level", func(t *testing.T) {
		fromTheFuture := protowire.AppendTag(slices.Clone(payload), 999, protowire.VarintType)
		fromTheFuture = protowire.AppendVarint(fromTheFuture, 1)
		_, err := Decode(fromTheFuture, registry())
		require.ErrorContains(t, err, "unknown field")
	})

	t.Run("nested", func(t *testing.T) {
		inner := protowire.AppendTag(nil, fieldDeleteShardID, protowire.VarintType)
		inner = protowire.AppendVarint(inner, 1)
		inner = protowire.AppendTag(inner, 998, protowire.VarintType)
		inner = protowire.AppendVarint(inner, 7)

		outer := protowire.AppendTag(nil, fieldPayloadFormat, protowire.VarintType)
		outer = protowire.AppendVarint(outer, formatVersion)
		outer = protowire.AppendTag(outer, fieldPayloadDelete, protowire.BytesType)
		outer = protowire.AppendBytes(outer, inner)

		_, err := Decode(outer, registry())
		require.ErrorContains(t, err, "unknown field")
	})
}

// Adding the provisional bit needs no format bump: a build without the field
// reads a non-provisional entry unchanged and refuses a provisional one.
func TestTheProvisionalBitNeedsNoFormatBump(t *testing.T) {
	m := Mutation{Delete: &p.DeleteWorkflowExecutionRequest{
		ShardID: 1, NamespaceID: "ns", WorkflowID: "wf", RunID: "run",
	}}

	plain, err := Encode(m)
	require.NoError(t, err)
	var decoded Payload
	require.NoError(t, proto.Unmarshal(plain, &decoded))
	require.False(t, decoded.ProtoReflect().Has(
		decoded.ProtoReflect().Descriptor().Fields().ByName("provisional")),
		"a false bool is not on the wire, so these are the bytes the previous codec wrote")
	require.EqualValues(t, formatVersion, decoded.Format, "and the format they carry is unchanged")

	provisional, err := EncodeProvisional(m)
	require.NoError(t, err)
	require.NotEqual(t, plain, provisional)
	// An older build rejects the entry whole rather than reading it as verified.
	require.ErrorContains(t, rejectUnknownFields(unknownTo(t, provisional, "provisional")), "unknown field")

	_, gotProvisional, err := DecodeEntry(provisional, registry())
	require.NoError(t, err)
	require.True(t, gotProvisional, "and this build reads it back")
}

// unknownTo re-parses the payload with the named field moved to unknown, as an
// older build's descriptor would have parsed it.
func unknownTo(t *testing.T, payload []byte, field string) protoreflect.Message {
	t.Helper()
	var pb Payload
	require.NoError(t, proto.Unmarshal(payload, &pb))
	msg := pb.ProtoReflect()
	fd := msg.Descriptor().Fields().ByName(protoreflect.Name(field))
	require.NotNil(t, fd)
	value := protowire.AppendTag(nil, protowire.Number(fd.Number()), protowire.VarintType)
	value = protowire.AppendVarint(value, 1)
	clone := proto.Clone(&pb).(*Payload)
	clone.Provisional = false
	out := clone.ProtoReflect()
	out.SetUnknown(protoreflect.RawFields(value))
	return out
}

func TestFormatVersionIsChecked(t *testing.T) {
	other := mustMarshal(t, &Payload{
		Format:  formatVersion + 1,
		Request: &Payload_Delete{Delete: &DeleteRequest{ShardId: 1}},
	})
	_, err := Decode(other, registry())
	require.ErrorContains(t, err, "format")
}

func TestEmptyAndAmbiguousMutationsAreRefused(t *testing.T) {
	_, err := Encode(Mutation{})
	require.Error(t, err)

	_, err = Encode(Mutation{
		Delete:        &p.DeleteWorkflowExecutionRequest{ShardID: 1},
		DeleteCurrent: &p.DeleteCurrentWorkflowExecutionRequest{ShardID: 1},
	})
	require.Error(t, err)

	require.Equal(t, KindInvalid, Mutation{}.Kind())
}

func TestDecodeNeedsARegistry(t *testing.T) {
	payload, err := Encode(Mutation{Delete: &p.DeleteWorkflowExecutionRequest{ShardID: 1}})
	require.NoError(t, err)
	_, err = Decode(payload, nil)
	require.Error(t, err)
}

// The proto is derived from the blob with a plain proto.Unmarshal: Temporal's
// own helper back-fills RequestIds, which would change the bytes apply writes.
func TestExecutionStateIsDerivedFromTheBlobVerbatim(t *testing.T) {
	state := &persistencespb.WorkflowExecutionState{
		RunId:           "run-1",
		CreateRequestId: "req-1",
		State:           enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		Status:          enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}
	stateBlob, err := serialization.WorkflowExecutionStateToBlob(state)
	require.NoError(t, err)

	m := Mutation{Update: &p.InternalUpdateWorkflowExecutionRequest{
		ShardID: 1,
		UpdateWorkflowMutation: p.InternalWorkflowMutation{
			RunID:              "run-1",
			ExecutionState:     state,
			ExecutionStateBlob: stateBlob,
		},
	}}
	payload, err := Encode(m)
	require.NoError(t, err)
	decoded, err := Decode(payload, registry())
	require.NoError(t, err)

	got := decoded.Update.UpdateWorkflowMutation
	require.Equal(t, stateBlob.Data, got.ExecutionStateBlob.Data, "the stored bytes must be the same bytes")
	require.Empty(t, got.ExecutionState.RequestIds, "nothing may back-fill the derived proto")
}

// ---------------------------------------------------------------- fixtures

func blob(s string) *commonpb.DataBlob {
	return &commonpb.DataBlob{Data: []byte(s), EncodingType: enumspb.ENCODING_TYPE_PROTO3}
}

// Field numbers for the tests that build a payload Encode cannot produce.
const (
	fieldPayloadFormat = 1
	fieldPayloadDelete = 6
	fieldDeleteShardID = 1
)

func mustMarshal(t *testing.T, m *Payload) []byte {
	t.Helper()
	out, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
	require.NoError(t, err)
	return out
}

// sampleMutation builds a mutation carrying every shape the codec must survive:
// blob/proto pairs, upsert and delete maps, CHASM nodes, two task categories.
func sampleMutation() p.InternalWorkflowMutation {
	now := time.Date(2026, 7, 28, 10, 0, 0, 0, time.UTC)
	info, infoBlob, state, stateBlob := sampleState(now)

	return p.InternalWorkflowMutation{
		NamespaceID: "ns-0000-0000-0000",
		WorkflowID:  "order-4711",
		RunID:       "11111111-2222-3333-4444-555555555555",

		ExecutionInfo:      info,
		ExecutionInfoBlob:  infoBlob,
		ExecutionState:     state,
		ExecutionStateBlob: stateBlob,

		NextEventID:      42,
		StartVersion:     1,
		LastWriteVersion: 7,
		DBRecordVersion:  9,
		Condition:        8,

		UpsertActivityInfos: map[int64]*commonpb.DataBlob{
			5: blob("activity-5-payload"),
			6: blob("activity-6-payload"),
		},
		DeleteActivityInfos: map[int64]struct{}{4: {}, 3: {}},
		UpsertTimerInfos: map[string]*commonpb.DataBlob{
			"timer-a": blob("timer-a-payload"),
			"timer-c": blob("timer-c-payload"),
		},
		DeleteTimerInfos:          map[string]struct{}{"timer-b": {}},
		UpsertChildExecutionInfos: map[int64]*commonpb.DataBlob{8: blob("child-8")},
		DeleteChildExecutionInfos: map[int64]struct{}{7: {}},
		UpsertRequestCancelInfos:  map[int64]*commonpb.DataBlob{9: blob("cancel-9")},
		DeleteRequestCancelInfos:  map[int64]struct{}{10: {}},
		UpsertSignalInfos:         map[int64]*commonpb.DataBlob{11: blob("signal-11")},
		DeleteSignalInfos:         map[int64]struct{}{12: {}},
		UpsertChasmNodes: map[string]p.InternalChasmNode{
			"node/1": {Metadata: blob("chasm-meta"), Data: blob("chasm-data")},
			"node/3": {Metadata: blob("chasm-meta-3")},
		},
		DeleteChasmNodes:         map[string]struct{}{"node/2": {}},
		UpsertSignalRequestedIDs: map[string]struct{}{"sig-req-1": {}, "sig-req-2": {}},
		DeleteSignalRequestedIDs: map[string]struct{}{"sig-req-0": {}},
		NewBufferedEvents:        blob("buffered-events-blob"),
		ClearBufferedEvents:      true,

		Tasks: map[tasks.Category][]p.InternalHistoryTask{
			tasks.CategoryTransfer: {
				{Key: tasks.NewImmediateKey(1001), Blob: blob("transfer-1001")},
				{Key: tasks.NewImmediateKey(1002), Blob: blob("transfer-1002")},
			},
			tasks.CategoryTimer: {
				{Key: tasks.NewKey(now.Add(time.Hour), 2001), Blob: blob("timer-2001")},
			},
		},

		Checksum: blob("checksum-blob"),
	}
}

// sampleTaskGroups is the rows an AddTasks carries in its own right, belonging
// to no run: two categories rather than one, an immediate and a scheduled key,
// and a blob naming its row — so a group written under the wrong category, or a
// row landing in the wrong group, differs rather than merely counts the same.
func sampleTaskGroups() map[tasks.Category][]p.InternalHistoryTask {
	fire := time.Date(2026, 7, 28, 13, 0, 0, 0, time.UTC)
	return map[tasks.Category][]p.InternalHistoryTask{
		tasks.CategoryTransfer: {
			{Key: tasks.NewImmediateKey(7001), Blob: blob("add-transfer-7001")},
			{Key: tasks.NewImmediateKey(7002), Blob: blob("add-transfer-7002")},
		},
		tasks.CategoryTimer: {
			{Key: tasks.NewKey(fire, 7003), Blob: blob("add-timer-7003")},
		},
	}
}

func sampleSnapshot() p.InternalWorkflowSnapshot {
	now := time.Date(2026, 7, 28, 11, 0, 0, 0, time.UTC)
	info, infoBlob, state, stateBlob := sampleState(now)

	return p.InternalWorkflowSnapshot{
		NamespaceID: "ns-0000-0000-0000",
		WorkflowID:  "order-4711",
		RunID:       "99999999-2222-3333-4444-555555555555",

		ExecutionInfo:      info,
		ExecutionInfoBlob:  infoBlob,
		ExecutionState:     state,
		ExecutionStateBlob: stateBlob,

		StartVersion:     1,
		LastWriteVersion: 7,
		NextEventID:      3,
		DBRecordVersion:  1,
		Condition:        0,

		ActivityInfos:       map[int64]*commonpb.DataBlob{1: blob("activity-1")},
		TimerInfos:          map[string]*commonpb.DataBlob{"t": blob("timer-t")},
		ChildExecutionInfos: map[int64]*commonpb.DataBlob{2: blob("child-2")},
		RequestCancelInfos:  map[int64]*commonpb.DataBlob{3: blob("cancel-3")},
		SignalInfos:         map[int64]*commonpb.DataBlob{4: blob("signal-4")},
		ChasmNodes:          map[string]p.InternalChasmNode{"n": {Data: blob("chasm")}},
		SignalRequestedIDs:  map[string]struct{}{"sig-a": {}, "sig-b": {}},

		Tasks: map[tasks.Category][]p.InternalHistoryTask{
			tasks.CategoryTimer: {
				{Key: tasks.NewKey(now.Add(time.Minute), 3001), Blob: blob("timer-3001")},
			},
		},

		Checksum: blob("snapshot-checksum"),
	}
}

func sampleState(now time.Time) (
	*persistencespb.WorkflowExecutionInfo, *commonpb.DataBlob,
	*persistencespb.WorkflowExecutionState, *commonpb.DataBlob,
) {
	info := &persistencespb.WorkflowExecutionInfo{
		NamespaceId:          "ns-0000-0000-0000",
		WorkflowId:           "order-4711",
		TaskQueue:            "orders",
		WorkflowTypeName:     "OrderWorkflow",
		StartTime:            timestamppb.New(now),
		LastUpdateTime:       timestamppb.New(now.Add(time.Second)),
		ExecutionStats:       &persistencespb.ExecutionStats{HistorySize: 4096},
		StateTransitionCount: 41,
	}
	state := &persistencespb.WorkflowExecutionState{
		RunId:           "11111111-2222-3333-4444-555555555555",
		CreateRequestId: "req-1",
		State:           enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		Status:          enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}

	serializer := serialization.NewSerializer()
	infoBlob, err := serializer.WorkflowExecutionInfoToBlob(info)
	if err != nil {
		panic(err)
	}
	stateBlob, err := serialization.WorkflowExecutionStateToBlob(state)
	if err != nil {
		panic(err)
	}
	return info, infoBlob, state, stateBlob
}

// A request the write path folds and the replay path cannot. Only the blob is
// carried, so a parsed proto with no blob behind it decodes back as nothing:
// the state's absence panics the fold on every owner that replays the entry,
// and the info's is a row committed without one. Both are past the ack by then,
// so [Encode] is the last place that can refuse, and it refuses rather than
// writing an entry nobody can fold.
func TestARequestThatCannotRoundTripIsRefused(t *testing.T) {
	now := time.Date(2026, 7, 28, 10, 0, 0, 0, time.UTC)
	info, infoBlob, state, stateBlob := sampleState(now)

	// Each case drops exactly one blob from an otherwise well-formed request, so
	// what is being refused is the missing blob and not the fixture.
	for _, tt := range []struct {
		name  string
		drop  func(*p.InternalWorkflowMutation, *p.InternalWorkflowSnapshot)
		field string
	}{
		{"execution info", func(m *p.InternalWorkflowMutation, s *p.InternalWorkflowSnapshot) {
			m.ExecutionInfoBlob, s.ExecutionInfoBlob = nil, nil
		}, "execution info"},
		{"execution state", func(m *p.InternalWorkflowMutation, s *p.InternalWorkflowSnapshot) {
			m.ExecutionStateBlob, s.ExecutionStateBlob = nil, nil
		}, "execution state"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pair := func() (p.InternalWorkflowMutation, p.InternalWorkflowSnapshot) {
				m := p.InternalWorkflowMutation{
					RunID:              "run-1",
					ExecutionInfo:      info,
					ExecutionInfoBlob:  infoBlob,
					ExecutionState:     state,
					ExecutionStateBlob: stateBlob,
				}
				s := p.InternalWorkflowSnapshot{
					RunID:              "run-1",
					ExecutionInfo:      info,
					ExecutionInfoBlob:  infoBlob,
					ExecutionState:     state,
					ExecutionStateBlob: stateBlob,
				}
				tt.drop(&m, &s)
				return m, s
			}

			// Every kind that carries either a mutation or a snapshot, since the
			// two encoders are separate functions and a check on one says nothing
			// about the other.
			mut, snap := pair()
			kinds := map[string]Mutation{
				"update": {Update: &p.InternalUpdateWorkflowExecutionRequest{
					ShardID: 1, UpdateWorkflowMutation: mut,
				}},
				"create": {Create: &p.InternalCreateWorkflowExecutionRequest{
					ShardID: 1, NewWorkflowSnapshot: snap,
				}},
				"conflict resolve": {ConflictResolve: &p.InternalConflictResolveWorkflowExecutionRequest{
					ShardID: 1, ResetWorkflowSnapshot: snap,
				}},
				"set": {Set: &p.InternalSetWorkflowExecutionRequest{
					ShardID: 1, SetWorkflowSnapshot: snap,
				}},
			}
			for name, m := range kinds {
				t.Run(name, func(t *testing.T) {
					_, err := Encode(m)
					require.ErrorIs(t, err, ErrUncarriedProto,
						"a %s carrying a parsed %s with no blob must be refused at the write: "+
							"encoded, it is acked into the log and every owner inherits an entry "+
							"that decodes back without it", name, tt.field)
					require.ErrorContains(t, err, tt.field, "the refusal must name which of the two is missing")
				})
			}
		})
	}
}

// The other direction is the ordinary case: bytes with no parsed proto beside
// them is what Decode produces, and re-encoding one must not be refused.
func TestABlobWithNoParsedProtoIsNotRefused(t *testing.T) {
	now := time.Date(2026, 7, 28, 10, 0, 0, 0, time.UTC)
	_, infoBlob, _, stateBlob := sampleState(now)

	_, err := Encode(Mutation{Update: &p.InternalUpdateWorkflowExecutionRequest{
		ShardID: 1,
		UpdateWorkflowMutation: p.InternalWorkflowMutation{
			RunID:              "run-1",
			ExecutionInfoBlob:  infoBlob,
			ExecutionStateBlob: stateBlob,
		},
	}})
	require.NoError(t, err)
}
