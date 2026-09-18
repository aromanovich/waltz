package mutation

import (
	"cmp"
	"fmt"
	"maps"
	"slices"

	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Encoding: Temporal's structs into the mirror, one struct at a time.
//
// Every collection is written in sorted key order. That alone is what makes
// [Encode] a function of its argument: callers compare the bytes of two
// encodings of the same mutation, and Go's map iteration order would give one
// mutation several byte strings.

func encodeCreate(r *p.InternalCreateWorkflowExecutionRequest) (*CreateRequest, error) {
	snapshot, err := encodeSnapshot(&r.NewWorkflowSnapshot)
	if err != nil {
		return nil, err
	}
	return &CreateRequest{
		ShardId:                  r.ShardID,
		Mode:                     int32(r.Mode),
		PreviousRunId:            r.PreviousRunID,
		PreviousLastWriteVersion: r.PreviousLastWriteVersion,
		Snapshot:                 snapshot,
	}, nil
}

func encodeUpdate(r *p.InternalUpdateWorkflowExecutionRequest) (*UpdateRequest, error) {
	mutation, err := encodeMutation(&r.UpdateWorkflowMutation)
	if err != nil {
		return nil, err
	}
	out := &UpdateRequest{
		ShardId:  r.ShardID,
		Mode:     int32(r.Mode),
		Mutation: mutation,
	}
	if r.NewWorkflowSnapshot != nil {
		if out.NewSnapshot, err = encodeSnapshot(r.NewWorkflowSnapshot); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func encodeConflictResolve(r *p.InternalConflictResolveWorkflowExecutionRequest) (*ConflictResolveRequest, error) {
	reset, err := encodeSnapshot(&r.ResetWorkflowSnapshot)
	if err != nil {
		return nil, err
	}
	out := &ConflictResolveRequest{
		ShardId:       r.ShardID,
		Mode:          int32(r.Mode),
		ResetSnapshot: reset,
	}
	if r.NewWorkflowSnapshot != nil {
		if out.NewSnapshot, err = encodeSnapshot(r.NewWorkflowSnapshot); err != nil {
			return nil, err
		}
	}
	if r.CurrentWorkflowMutation != nil {
		if out.CurrentMutation, err = encodeMutation(r.CurrentWorkflowMutation); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func encodeSet(r *p.InternalSetWorkflowExecutionRequest) (*SetRequest, error) {
	snapshot, err := encodeSnapshot(&r.SetWorkflowSnapshot)
	if err != nil {
		return nil, err
	}
	return &SetRequest{ShardId: r.ShardID, Snapshot: snapshot}, nil
}

func encodeMutation(m *p.InternalWorkflowMutation) (*WorkflowMutation, error) {
	if err := refuseUncarried(m.ExecutionInfo, m.ExecutionInfoBlob, m.ExecutionState, m.ExecutionStateBlob); err != nil {
		return nil, err
	}
	chasm, err := encodeChasmNodes(m.UpsertChasmNodes)
	if err != nil {
		return nil, err
	}
	return &WorkflowMutation{
		NamespaceId: m.NamespaceID,
		WorkflowId:  m.WorkflowID,
		RunId:       m.RunID,

		// Only the blob is carried; [Decode] derives the parsed proto back from
		// it, and derives nil from a blob that is not there — which is why a
		// request holding the struct without the blob is refused above rather
		// than encoded to something that decodes back as neither.
		ExecutionInfo:  encodeBlob(m.ExecutionInfoBlob),
		ExecutionState: encodeBlob(m.ExecutionStateBlob),

		NextEventId:      m.NextEventID,
		StartVersion:     m.StartVersion,
		LastWriteVersion: m.LastWriteVersion,
		DbRecordVersion:  m.DBRecordVersion,
		Condition:        m.Condition,

		UpsertActivityInfos:       encodeBlobsInt(m.UpsertActivityInfos),
		DeleteActivityInfos:       sortedKeys(m.DeleteActivityInfos),
		UpsertTimerInfos:          encodeBlobsStr(m.UpsertTimerInfos),
		DeleteTimerInfos:          sortedKeys(m.DeleteTimerInfos),
		UpsertChildExecutionInfos: encodeBlobsInt(m.UpsertChildExecutionInfos),
		DeleteChildExecutionInfos: sortedKeys(m.DeleteChildExecutionInfos),
		UpsertRequestCancelInfos:  encodeBlobsInt(m.UpsertRequestCancelInfos),
		DeleteRequestCancelInfos:  sortedKeys(m.DeleteRequestCancelInfos),
		UpsertSignalInfos:         encodeBlobsInt(m.UpsertSignalInfos),
		DeleteSignalInfos:         sortedKeys(m.DeleteSignalInfos),
		UpsertChasmNodes:          chasm,
		DeleteChasmNodes:          sortedKeys(m.DeleteChasmNodes),
		UpsertSignalRequestedIds:  sortedKeys(m.UpsertSignalRequestedIDs),
		DeleteSignalRequestedIds:  sortedKeys(m.DeleteSignalRequestedIDs),

		NewBufferedEvents:   encodeBlob(m.NewBufferedEvents),
		ClearBufferedEvents: m.ClearBufferedEvents,

		Tasks:    encodeTasks(m.Tasks),
		Checksum: encodeBlob(m.Checksum),
	}, nil
}

func encodeSnapshot(s *p.InternalWorkflowSnapshot) (*WorkflowSnapshot, error) {
	if err := refuseUncarried(s.ExecutionInfo, s.ExecutionInfoBlob, s.ExecutionState, s.ExecutionStateBlob); err != nil {
		return nil, err
	}
	chasm, err := encodeChasmNodes(s.ChasmNodes)
	if err != nil {
		return nil, err
	}
	return &WorkflowSnapshot{
		NamespaceId: s.NamespaceID,
		WorkflowId:  s.WorkflowID,
		RunId:       s.RunID,

		ExecutionInfo:  encodeBlob(s.ExecutionInfoBlob),
		ExecutionState: encodeBlob(s.ExecutionStateBlob),

		StartVersion:     s.StartVersion,
		LastWriteVersion: s.LastWriteVersion,
		NextEventId:      s.NextEventID,
		DbRecordVersion:  s.DBRecordVersion,
		Condition:        s.Condition,

		ActivityInfos:       encodeBlobsInt(s.ActivityInfos),
		TimerInfos:          encodeBlobsStr(s.TimerInfos),
		ChildExecutionInfos: encodeBlobsInt(s.ChildExecutionInfos),
		RequestCancelInfos:  encodeBlobsInt(s.RequestCancelInfos),
		SignalInfos:         encodeBlobsInt(s.SignalInfos),
		ChasmNodes:          chasm,
		SignalRequestedIds:  sortedKeys(s.SignalRequestedIDs),

		Tasks:    encodeTasks(s.Tasks),
		Checksum: encodeBlob(s.Checksum),
	}, nil
}

// ---------------------------------------------------------------- pieces

// refuseUncarried holds the blob-is-authoritative rule to what the record can
// actually carry: a parsed proto whose blob is absent has no home here, and
// encoding it anyway hands the log an entry that decodes back to neither the
// struct nor the bytes. Both pairs are checked, because the two failures differ
// and neither is visible from above — a missing state panics the fold on replay,
// a missing info commits a row without one.
//
// The other direction is the ordinary case and not an error: a blob with no
// parsed proto beside it is exactly what [Decode] produces before the derive,
// and what a caller holding only bytes legitimately has.
func refuseUncarried(
	info *persistencespb.WorkflowExecutionInfo, infoBlob *commonpb.DataBlob,
	state *persistencespb.WorkflowExecutionState, stateBlob *commonpb.DataBlob,
) error {
	switch {
	case info != nil && infoBlob == nil:
		return fmt.Errorf("%w: execution info", ErrUncarriedProto)
	case state != nil && stateBlob == nil:
		return fmt.Errorf("%w: execution state", ErrUncarriedProto)
	}
	return nil
}

func encodeBlob(b *commonpb.DataBlob) *Blob {
	if b == nil {
		return nil
	}
	return &Blob{Data: b.Data, Encoding: int32(b.EncodingType)}
}

func encodeBlobsInt(m map[int64]*commonpb.DataBlob) []*Int64BlobEntry {
	if len(m) == 0 {
		return nil
	}
	out := make([]*Int64BlobEntry, 0, len(m))
	for _, k := range orderedKeys(m) {
		out = append(out, &Int64BlobEntry{Key: k, Blob: encodeBlob(m[k])})
	}
	return out
}

func encodeBlobsStr(m map[string]*commonpb.DataBlob) []*StringBlobEntry {
	if len(m) == 0 {
		return nil
	}
	out := make([]*StringBlobEntry, 0, len(m))
	for _, k := range orderedKeys(m) {
		out = append(out, &StringBlobEntry{Key: k, Blob: encodeBlob(m[k])})
	}
	return out
}

func encodeChasmNodes(m map[string]p.InternalChasmNode) ([]*ChasmNodeEntry, error) {
	if len(m) == 0 {
		return nil, nil
	}
	out := make([]*ChasmNodeEntry, 0, len(m))
	for _, k := range orderedKeys(m) {
		node := m[k]
		if node.CassandraBlob != nil {
			return nil, ErrCassandraBlob
		}
		out = append(out, &ChasmNodeEntry{
			Key:      k,
			Metadata: encodeBlob(node.Metadata),
			Data:     encodeBlob(node.Data),
		})
	}
	return out, nil
}

// encodeTasks writes the groups in category-id order and keeps the caller's
// slice order inside a group, which is generation order and not necessarily key
// order — a scheduled category's fire times need not ascend with it. Only the
// categories are sorted, because only their order came out of a map, and
// pinning that is all determinism needs.
func encodeTasks(groups map[tasks.Category][]p.InternalHistoryTask) []*TaskGroup {
	if len(groups) == 0 {
		return nil
	}
	ordered := slices.SortedFunc(maps.Keys(groups), func(a, b tasks.Category) int {
		return cmp.Compare(a.ID(), b.ID())
	})
	out := make([]*TaskGroup, 0, len(groups))
	for _, category := range ordered {
		rows := groups[category]
		group := &TaskGroup{CategoryId: int32(category.ID())}
		if len(rows) > 0 {
			group.Tasks = make([]*Task, 0, len(rows))
		}
		for _, t := range rows {
			task := &Task{TaskId: t.Key.TaskID, Blob: encodeBlob(t.Blob)}
			// A zero fire time travels as an absent field, so that the
			// immediate/scheduled distinction survives the round trip.
			if !t.Key.FireTime.IsZero() {
				task.FireTime = timestamppb.New(t.Key.FireTime)
			}
			group.Tasks = append(group.Tasks, task)
		}
		out = append(out, group)
	}
	return out
}

// encodeTaskKey carries one tasks.Key, under the same zero-fire-time rule the
// tasks inside a group follow.
func encodeTaskKey(k tasks.Key) *TaskKey {
	out := &TaskKey{TaskId: k.TaskID}
	if !k.FireTime.IsZero() {
		out.FireTime = timestamppb.New(k.FireTime)
	}
	return out
}

// orderedKeys is [slices.Sorted] over a map's keys, presized. Not spelled
// `slices.Sorted(maps.Keys(m))`: an [iter.Seq] carries no length, so that form
// grows the slice up from nil while the map has known len(m) all along.
func orderedKeys[K cmp.Ordered, V any](m map[K]V) []K {
	keys := slices.AppendSeq(make([]K, 0, len(m)), maps.Keys(m))
	slices.Sort(keys)
	return keys
}

// sortedKeys is the encode half of the absent-vs-empty rule, inverse to
// [setOf]: an empty set encodes as an absent field, not a present empty one.
func sortedKeys[K cmp.Ordered](set map[K]struct{}) []K {
	if len(set) == 0 {
		return nil
	}
	return orderedKeys(set)
}
