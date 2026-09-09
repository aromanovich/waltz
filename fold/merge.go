package fold

import (
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
)

// mergeItems folds one mutation's upsert/delete pair for a single collection
// into the accumulator's, resolving upsert-vs-delete per key: the later
// operation wins and the key leaves the other set. Emitted unresolved, the
// store issues every upsert before every delete, so a key re-upserted after
// being deleted would be written and then deleted again — an acked write gone.
func mergeItems[K comparable, V any](
	ups map[K]V, dels map[K]struct{},
	srcUps map[K]V, srcDels map[K]struct{},
) (map[K]V, map[K]struct{}) {
	// A key one source mutation both upserts and deletes ends up upserted here
	// and deleted in the store, which orders the two statements the other way
	// round.
	for k := range srcDels {
		if dels == nil {
			dels = make(map[K]struct{})
		}
		dels[k] = struct{}{}
		delete(ups, k)
	}
	for k, v := range srcUps {
		if ups == nil {
			ups = make(map[K]V)
		}
		ups[k] = v
		delete(dels, k)
	}
	return ups, dels
}

// applyDelta folds one mutation's upsert/delete pair onto a snapshot's
// collection, which holds whole state and needs no resolution.
func applyDelta[K comparable, V any](state map[K]V, ups map[K]V, dels map[K]struct{}) map[K]V {
	for k := range dels {
		delete(state, k)
	}
	for k, v := range ups {
		if state == nil {
			state = make(map[K]V)
		}
		state[k] = v
	}
	return state
}

// mergeTasks concatenates task groups in arrival order. Tasks are queue entries
// rather than workflow state: no snapshot or tombstone barrier collapses them
// (I8). A range delete still does, out of these slots as out of every other
// home.
func mergeTasks(dst, src map[tasks.Category][]p.InternalHistoryTask) map[tasks.Category][]p.InternalHistoryTask {
	for category, list := range src {
		if dst == nil {
			dst = make(map[tasks.Category][]p.InternalHistoryTask)
		}
		dst[category] = append(dst[category], list...)
	}
	return dst
}

// mergeMutation folds src into dst: collections merge with per-key resolution,
// tasks concatenate, every scalar comes from src. The buffered-events fields
// are left alone — they do not merge, and runState.foldBuffered owns them.
func mergeMutation(dst, src *p.InternalWorkflowMutation) {
	dst.UpsertActivityInfos, dst.DeleteActivityInfos = mergeItems(
		dst.UpsertActivityInfos, dst.DeleteActivityInfos, src.UpsertActivityInfos, src.DeleteActivityInfos)
	dst.UpsertTimerInfos, dst.DeleteTimerInfos = mergeItems(
		dst.UpsertTimerInfos, dst.DeleteTimerInfos, src.UpsertTimerInfos, src.DeleteTimerInfos)
	dst.UpsertChildExecutionInfos, dst.DeleteChildExecutionInfos = mergeItems(
		dst.UpsertChildExecutionInfos, dst.DeleteChildExecutionInfos, src.UpsertChildExecutionInfos, src.DeleteChildExecutionInfos)
	dst.UpsertRequestCancelInfos, dst.DeleteRequestCancelInfos = mergeItems(
		dst.UpsertRequestCancelInfos, dst.DeleteRequestCancelInfos, src.UpsertRequestCancelInfos, src.DeleteRequestCancelInfos)
	dst.UpsertSignalInfos, dst.DeleteSignalInfos = mergeItems(
		dst.UpsertSignalInfos, dst.DeleteSignalInfos, src.UpsertSignalInfos, src.DeleteSignalInfos)
	dst.UpsertSignalRequestedIDs, dst.DeleteSignalRequestedIDs = mergeItems(
		dst.UpsertSignalRequestedIDs, dst.DeleteSignalRequestedIDs, src.UpsertSignalRequestedIDs, src.DeleteSignalRequestedIDs)
	dst.UpsertChasmNodes, dst.DeleteChasmNodes = mergeItems(
		dst.UpsertChasmNodes, dst.DeleteChasmNodes, src.UpsertChasmNodes, src.DeleteChasmNodes)
	dst.Tasks = mergeTasks(dst.Tasks, src.Tasks)

	dst.ExecutionInfo, dst.ExecutionInfoBlob = src.ExecutionInfo, src.ExecutionInfoBlob
	dst.ExecutionState, dst.ExecutionStateBlob = src.ExecutionState, src.ExecutionStateBlob
	dst.NextEventID = src.NextEventID
	dst.StartVersion = src.StartVersion
	dst.LastWriteVersion = src.LastWriteVersion
	dst.DBRecordVersion = src.DBRecordVersion
	dst.Condition = src.Condition
	dst.Checksum = src.Checksum
}

// applyMutationToSnapshot folds a delta onto whole state: collections absorb
// the upserts and deletes, tasks concatenate, scalars come from src.
func applyMutationToSnapshot(dst *p.InternalWorkflowSnapshot, src *p.InternalWorkflowMutation) {
	dst.ActivityInfos = applyDelta(dst.ActivityInfos, src.UpsertActivityInfos, src.DeleteActivityInfos)
	dst.TimerInfos = applyDelta(dst.TimerInfos, src.UpsertTimerInfos, src.DeleteTimerInfos)
	dst.ChildExecutionInfos = applyDelta(dst.ChildExecutionInfos, src.UpsertChildExecutionInfos, src.DeleteChildExecutionInfos)
	dst.RequestCancelInfos = applyDelta(dst.RequestCancelInfos, src.UpsertRequestCancelInfos, src.DeleteRequestCancelInfos)
	dst.SignalInfos = applyDelta(dst.SignalInfos, src.UpsertSignalInfos, src.DeleteSignalInfos)
	dst.SignalRequestedIDs = applyDelta(dst.SignalRequestedIDs, src.UpsertSignalRequestedIDs, src.DeleteSignalRequestedIDs)
	dst.ChasmNodes = applyDelta(dst.ChasmNodes, src.UpsertChasmNodes, src.DeleteChasmNodes)
	dst.Tasks = mergeTasks(dst.Tasks, src.Tasks)

	dst.ExecutionInfo, dst.ExecutionInfoBlob = src.ExecutionInfo, src.ExecutionInfoBlob
	dst.ExecutionState, dst.ExecutionStateBlob = src.ExecutionState, src.ExecutionStateBlob
	dst.NextEventID = src.NextEventID
	dst.StartVersion = src.StartVersion
	dst.LastWriteVersion = src.LastWriteVersion
	dst.DBRecordVersion = src.DBRecordVersion
	dst.Condition = src.Condition
	dst.Checksum = src.Checksum
}

// replaceSnapshot puts src's whole state into dst, a snapshot superseding a
// snapshot, keeping the tasks accumulated so far.
func replaceSnapshot(dst, src *p.InternalWorkflowSnapshot) {
	prior := dst.Tasks
	*dst = *src
	dst.Tasks = mergeTasks(prior, src.Tasks)
}
