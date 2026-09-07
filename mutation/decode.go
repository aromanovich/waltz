package mutation

import (
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/proto"
)

// Decoding: the mirror back into Temporal's structs.
//
// The mirror carries only the blob of each blob/proto pair, so the proto is
// derived with a plain [proto.Unmarshal], never with Temporal's serialization
// helpers: WorkflowExecutionStateFromBlob back-fills RequestIds for old
// records, and apply would then write the grown blob.

func decodeCreate(r *CreateRequest, registry tasks.TaskCategoryRegistry) (*p.InternalCreateWorkflowExecutionRequest, error) {
	snapshot, err := decodeSnapshot(r.Snapshot, registry)
	if err != nil {
		return nil, err
	}
	out := &p.InternalCreateWorkflowExecutionRequest{
		ShardID:                  r.ShardId,
		Mode:                     p.CreateWorkflowMode(r.Mode),
		PreviousRunID:            r.PreviousRunId,
		PreviousLastWriteVersion: r.PreviousLastWriteVersion,
	}
	if snapshot != nil {
		out.NewWorkflowSnapshot = *snapshot
	}
	return out, nil
}

func decodeUpdate(r *UpdateRequest, registry tasks.TaskCategoryRegistry) (*p.InternalUpdateWorkflowExecutionRequest, error) {
	mutation, err := decodeMutation(r.Mutation, registry)
	if err != nil {
		return nil, err
	}
	out := &p.InternalUpdateWorkflowExecutionRequest{
		ShardID: r.ShardId,
		Mode:    p.UpdateWorkflowMode(r.Mode),
	}
	if mutation != nil {
		out.UpdateWorkflowMutation = *mutation
	}
	if out.NewWorkflowSnapshot, err = decodeSnapshot(r.NewSnapshot, registry); err != nil {
		return nil, err
	}
	return out, nil
}

func decodeConflictResolve(r *ConflictResolveRequest, registry tasks.TaskCategoryRegistry) (*p.InternalConflictResolveWorkflowExecutionRequest, error) {
	reset, err := decodeSnapshot(r.ResetSnapshot, registry)
	if err != nil {
		return nil, err
	}
	out := &p.InternalConflictResolveWorkflowExecutionRequest{
		ShardID: r.ShardId,
		Mode:    p.ConflictResolveWorkflowMode(r.Mode),
	}
	if reset != nil {
		out.ResetWorkflowSnapshot = *reset
	}
	if out.NewWorkflowSnapshot, err = decodeSnapshot(r.NewSnapshot, registry); err != nil {
		return nil, err
	}
	if out.CurrentWorkflowMutation, err = decodeMutation(r.CurrentMutation, registry); err != nil {
		return nil, err
	}
	return out, nil
}

func decodeSet(r *SetRequest, registry tasks.TaskCategoryRegistry) (*p.InternalSetWorkflowExecutionRequest, error) {
	snapshot, err := decodeSnapshot(r.Snapshot, registry)
	if err != nil {
		return nil, err
	}
	out := &p.InternalSetWorkflowExecutionRequest{ShardID: r.ShardId}
	if snapshot != nil {
		out.SetWorkflowSnapshot = *snapshot
	}
	return out, nil
}

func decodeMutation(m *WorkflowMutation, registry tasks.TaskCategoryRegistry) (*p.InternalWorkflowMutation, error) {
	if m == nil {
		return nil, nil
	}
	chasm, err := decodeChasmNodes(m.UpsertChasmNodes)
	if err != nil {
		return nil, err
	}
	taskGroups, err := decodeTasks(m.Tasks, registry)
	if err != nil {
		return nil, err
	}
	activityInfos, err := decodeBlobsInt(m.UpsertActivityInfos)
	if err != nil {
		return nil, err
	}
	timerInfos, err := decodeBlobsStr(m.UpsertTimerInfos)
	if err != nil {
		return nil, err
	}
	childInfos, err := decodeBlobsInt(m.UpsertChildExecutionInfos)
	if err != nil {
		return nil, err
	}
	cancelInfos, err := decodeBlobsInt(m.UpsertRequestCancelInfos)
	if err != nil {
		return nil, err
	}
	signalInfos, err := decodeBlobsInt(m.UpsertSignalInfos)
	if err != nil {
		return nil, err
	}
	out := &p.InternalWorkflowMutation{
		NamespaceID: m.NamespaceId,
		WorkflowID:  m.WorkflowId,
		RunID:       m.RunId,

		ExecutionInfoBlob:  decodeBlob(m.ExecutionInfo),
		ExecutionStateBlob: decodeBlob(m.ExecutionState),

		NextEventID:      m.NextEventId,
		StartVersion:     m.StartVersion,
		LastWriteVersion: m.LastWriteVersion,
		DBRecordVersion:  m.DbRecordVersion,
		Condition:        m.Condition,

		UpsertActivityInfos:       activityInfos,
		DeleteActivityInfos:       setOf(m.DeleteActivityInfos),
		UpsertTimerInfos:          timerInfos,
		DeleteTimerInfos:          setOf(m.DeleteTimerInfos),
		UpsertChildExecutionInfos: childInfos,
		DeleteChildExecutionInfos: setOf(m.DeleteChildExecutionInfos),
		UpsertRequestCancelInfos:  cancelInfos,
		DeleteRequestCancelInfos:  setOf(m.DeleteRequestCancelInfos),
		UpsertSignalInfos:         signalInfos,
		DeleteSignalInfos:         setOf(m.DeleteSignalInfos),
		UpsertChasmNodes:          chasm,
		DeleteChasmNodes:          setOf(m.DeleteChasmNodes),
		UpsertSignalRequestedIDs:  setOf(m.UpsertSignalRequestedIds),
		DeleteSignalRequestedIDs:  setOf(m.DeleteSignalRequestedIds),

		NewBufferedEvents:   decodeBlob(m.NewBufferedEvents),
		ClearBufferedEvents: m.ClearBufferedEvents,

		Tasks:    taskGroups,
		Checksum: decodeBlob(m.Checksum),
	}
	if out.ExecutionInfo, err = deriveExecutionInfo(out.ExecutionInfoBlob); err != nil {
		return nil, err
	}
	if out.ExecutionState, err = deriveExecutionState(out.ExecutionStateBlob); err != nil {
		return nil, err
	}
	return out, nil
}

func decodeSnapshot(s *WorkflowSnapshot, registry tasks.TaskCategoryRegistry) (*p.InternalWorkflowSnapshot, error) {
	if s == nil {
		return nil, nil
	}
	chasm, err := decodeChasmNodes(s.ChasmNodes)
	if err != nil {
		return nil, err
	}
	taskGroups, err := decodeTasks(s.Tasks, registry)
	if err != nil {
		return nil, err
	}
	activityInfos, err := decodeBlobsInt(s.ActivityInfos)
	if err != nil {
		return nil, err
	}
	timerInfos, err := decodeBlobsStr(s.TimerInfos)
	if err != nil {
		return nil, err
	}
	childInfos, err := decodeBlobsInt(s.ChildExecutionInfos)
	if err != nil {
		return nil, err
	}
	cancelInfos, err := decodeBlobsInt(s.RequestCancelInfos)
	if err != nil {
		return nil, err
	}
	signalInfos, err := decodeBlobsInt(s.SignalInfos)
	if err != nil {
		return nil, err
	}
	out := &p.InternalWorkflowSnapshot{
		NamespaceID: s.NamespaceId,
		WorkflowID:  s.WorkflowId,
		RunID:       s.RunId,

		ExecutionInfoBlob:  decodeBlob(s.ExecutionInfo),
		ExecutionStateBlob: decodeBlob(s.ExecutionState),

		StartVersion:     s.StartVersion,
		LastWriteVersion: s.LastWriteVersion,
		NextEventID:      s.NextEventId,
		DBRecordVersion:  s.DbRecordVersion,
		Condition:        s.Condition,

		ActivityInfos:       activityInfos,
		TimerInfos:          timerInfos,
		ChildExecutionInfos: childInfos,
		RequestCancelInfos:  cancelInfos,
		SignalInfos:         signalInfos,
		ChasmNodes:          chasm,
		SignalRequestedIDs:  setOf(s.SignalRequestedIds),

		Tasks:    taskGroups,
		Checksum: decodeBlob(s.Checksum),
	}
	if out.ExecutionInfo, err = deriveExecutionInfo(out.ExecutionInfoBlob); err != nil {
		return nil, err
	}
	if out.ExecutionState, err = deriveExecutionState(out.ExecutionStateBlob); err != nil {
		return nil, err
	}
	return out, nil
}

// ---------------------------------------------------------------- pieces

func decodeBlob(b *Blob) *commonpb.DataBlob {
	if b == nil {
		return nil
	}
	return &commonpb.DataBlob{Data: b.Data, EncodingType: enumspb.EncodingType(b.Encoding)}
}

func deriveExecutionInfo(b *commonpb.DataBlob) (*persistencespb.WorkflowExecutionInfo, error) {
	if b == nil {
		return nil, nil
	}
	info := &persistencespb.WorkflowExecutionInfo{}
	if err := unmarshalBlob(b, info); err != nil {
		return nil, fmt.Errorf("execution info: %w", err)
	}
	return info, nil
}

func deriveExecutionState(b *commonpb.DataBlob) (*persistencespb.WorkflowExecutionState, error) {
	if b == nil {
		return nil, nil
	}
	state := &persistencespb.WorkflowExecutionState{}
	if err := unmarshalBlob(b, state); err != nil {
		return nil, fmt.Errorf("execution state: %w", err)
	}
	return state, nil
}

// unmarshalBlob is the inverse of Temporal's ProtoEncode and nothing more.
func unmarshalBlob(b *commonpb.DataBlob, into proto.Message) error {
	if b.EncodingType != enumspb.ENCODING_TYPE_PROTO3 {
		return fmt.Errorf("unexpected blob encoding %v", b.EncodingType)
	}
	return proto.Unmarshal(b.Data, into)
}

// A repeated key is refused rather than overwritten, here and in the three
// decoders below it: keeping the last value would drop one run's blob, or a
// whole category's tasks, on replay and say nothing — the failure
// [ErrUnknownCategory] exists to prevent. [Encode] emits each collection once
// out of a Go map, so a duplicate is an entry this codec did not write.

func decodeBlobsInt(entries []*Int64BlobEntry) (map[int64]*commonpb.DataBlob, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	out := make(map[int64]*commonpb.DataBlob, len(entries))
	for _, e := range entries {
		if _, dup := out[e.Key]; dup {
			return nil, fmt.Errorf("mutation: decode: duplicate blob key %d", e.Key)
		}
		out[e.Key] = decodeBlob(e.Blob)
	}
	return out, nil
}

func decodeBlobsStr(entries []*StringBlobEntry) (map[string]*commonpb.DataBlob, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	out := make(map[string]*commonpb.DataBlob, len(entries))
	for _, e := range entries {
		if _, dup := out[e.Key]; dup {
			return nil, fmt.Errorf("mutation: decode: duplicate blob key %q", e.Key)
		}
		out[e.Key] = decodeBlob(e.Blob)
	}
	return out, nil
}

func decodeChasmNodes(entries []*ChasmNodeEntry) (map[string]p.InternalChasmNode, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	out := make(map[string]p.InternalChasmNode, len(entries))
	for _, e := range entries {
		if _, dup := out[e.Key]; dup {
			return nil, fmt.Errorf("mutation: decode: duplicate CHASM node key %q", e.Key)
		}
		out[e.Key] = p.InternalChasmNode{
			Metadata: decodeBlob(e.Metadata),
			Data:     decodeBlob(e.Data),
		}
	}
	return out, nil
}

func decodeTasks(groups []*TaskGroup, registry tasks.TaskCategoryRegistry) (map[tasks.Category][]p.InternalHistoryTask, error) {
	if len(groups) == 0 {
		return nil, nil
	}
	out := make(map[tasks.Category][]p.InternalHistoryTask, len(groups))
	for _, g := range groups {
		category, ok := registry.GetCategoryByID(int(g.CategoryId))
		if !ok {
			return nil, fmt.Errorf("%w: %d", ErrUnknownCategory, g.CategoryId)
		}
		group := make([]p.InternalHistoryTask, 0, len(g.Tasks))
		for _, t := range g.Tasks {
			key := tasks.Key{TaskID: t.TaskId}
			if t.FireTime != nil {
				key.FireTime = t.FireTime.AsTime()
			}
			group = append(group, p.InternalHistoryTask{Key: key, Blob: decodeBlob(t.Blob)})
		}
		if _, dup := out[category]; dup {
			return nil, fmt.Errorf("mutation: decode: duplicate task category id %d", g.CategoryId)
		}
		out[category] = group
	}
	return out, nil
}

// decodeTaskKey is [encodeTaskKey]'s inverse. A nil key decodes to the zero
// key, which is what a range naming no bound means.
func decodeTaskKey(k *TaskKey) tasks.Key {
	if k == nil {
		return tasks.Key{}
	}
	key := tasks.Key{TaskID: k.TaskId}
	if k.FireTime != nil {
		key.FireTime = k.FireTime.AsTime()
	}
	return key
}

// setOf is the decode half of the absent-vs-empty rule, inverse to
// [sortedKeys]: an absent field decodes to a nil set, not an empty one.
func setOf[K comparable](keys []K) map[K]struct{} {
	if len(keys) == 0 {
		return nil
	}
	out := make(map[K]struct{}, len(keys))
	for _, k := range keys {
		out[k] = struct{}{}
	}
	return out
}
