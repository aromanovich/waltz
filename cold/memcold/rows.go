package memcold

// Derived from go.temporal.io/server v1.29.6, common/persistence/sql, which is
// copyright Temporal Technologies Inc. and Uber Technologies, Inc. and licensed
// under the MIT licence. NOTICE at the repository root has that licence and
// what this file takes.

// One merged request's rows, mirrored from upstream's applyWorkflowMutationTx,
// applyWorkflowSnapshotTxAsReset and applyWorkflowSnapshotTxAsNew. They are
// unexported, so this is their statement sequence copied rather than called;
// the order a table's own statements come in is theirs and is the
// specification. Where one table's statements sit relative to another's is not:
// a reset clears all seven maps and then writes all seven, where upstream
// interleaves the clear with the write per table. Nothing here reads what
// another statement of the same request wrote, so that regrouping is the same
// write.
//
// What is missing from two of them is the lock-and-check they open with. A
// drain stands on fold's head-of-window assertions, which apply.go registers
// before the request runs, and the check here would assert the version the
// merged request writes — the tail's — against a row still holding the head's.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/service/history/tasks"

	"github.com/aromanovich/waltz/fold"
)

// applyMutation writes one run's delta: the executions row, the request's
// tasks, and each collection's upserts and deletes.
func applyMutation(
	ctx context.Context, tx sqlplugin.Tx, shardID int32, m *p.InternalWorkflowMutation,
) error {
	ns, run, err := runKeys(m.NamespaceID, m.ExecutionState.RunId)
	if err != nil {
		return err
	}

	if err := updateExecution(ctx, tx, shardID, ns, run, m.WorkflowID,
		m.ExecutionInfoBlob, m.ExecutionState, m.NextEventID, m.LastWriteVersion, m.DBRecordVersion,
	); err != nil {
		return err
	}
	if err := applyTasks(ctx, tx, shardID, m.Tasks); err != nil {
		return err
	}
	if err := applyCollections(ctx, tx, shardID, ns, m.WorkflowID, run, collections{
		activities:     m.UpsertActivityInfos,
		timers:         m.UpsertTimerInfos,
		children:       m.UpsertChildExecutionInfos,
		requestCancels: m.UpsertRequestCancelInfos,
		signals:        m.UpsertSignalInfos,
		signalsWanted:  m.UpsertSignalRequestedIDs,
		chasm:          m.UpsertChasmNodes,
	}, deletions{
		activities:     m.DeleteActivityInfos,
		timers:         m.DeleteTimerInfos,
		children:       m.DeleteChildExecutionInfos,
		requestCancels: m.DeleteRequestCancelInfos,
		signals:        m.DeleteSignalInfos,
		signalsWanted:  m.DeleteSignalRequestedIDs,
		chasm:          m.DeleteChasmNodes,
	}); err != nil {
		return err
	}

	if m.ClearBufferedEvents {
		if err := deleteBufferedEvents(ctx, tx, shardID, ns, m.WorkflowID, run); err != nil {
			return err
		}
	}
	return insertBufferedEvents(ctx, tx, shardID, ns, m.WorkflowID, run, m.NewBufferedEvents)
}

// applySnapshotAsReset replaces one existing run's whole state: every
// collection is cleared before the snapshot's own rows go in, and the buffered
// events go with them.
func applySnapshotAsReset(
	ctx context.Context, tx sqlplugin.Tx, shardID int32, s *p.InternalWorkflowSnapshot,
) error {
	ns, run, err := runKeys(s.NamespaceID, s.ExecutionState.RunId)
	if err != nil {
		return err
	}

	if err := updateExecution(ctx, tx, shardID, ns, run, s.WorkflowID,
		s.ExecutionInfoBlob, s.ExecutionState, s.NextEventID, s.LastWriteVersion, s.DBRecordVersion,
	); err != nil {
		return err
	}
	if err := applyTasks(ctx, tx, shardID, s.Tasks); err != nil {
		return err
	}
	if err := clearCollections(ctx, tx, shardID, ns, s.WorkflowID, run); err != nil {
		return err
	}
	if err := applySnapshotCollections(ctx, tx, shardID, ns, s.WorkflowID, run, s); err != nil {
		return err
	}
	return deleteBufferedEvents(ctx, tx, shardID, ns, s.WorkflowID, run)
}

// applySnapshotAsNew writes a run that does not exist yet: the executions row
// is an insert, and there is nothing to clear.
func applySnapshotAsNew(
	ctx context.Context, tx sqlplugin.Tx, shardID int32, snap *p.InternalWorkflowSnapshot,
) error {
	ns, run, err := runKeys(snap.NamespaceID, snap.ExecutionState.RunId)
	if err != nil {
		return err
	}

	if err := createExecution(ctx, tx, shardID, ns, run, snap.WorkflowID,
		snap.ExecutionInfoBlob, snap.ExecutionState, snap.NextEventID, snap.LastWriteVersion, snap.DBRecordVersion,
	); err != nil {
		return err
	}
	if err := applyTasks(ctx, tx, shardID, snap.Tasks); err != nil {
		return err
	}
	return applySnapshotCollections(ctx, tx, shardID, ns, snap.WorkflowID, run, snap)
}

// deleteRun removes every row one run owns, in upstream's
// DeleteWorkflowExecution order: the collections, the buffered events, then the
// executions row.
func deleteRun(
	ctx context.Context, tx sqlplugin.Tx, shardID int32, namespaceID, workflowID, runID string,
) error {
	ns, run, err := runKeys(namespaceID, runID)
	if err != nil {
		return err
	}
	if err := clearCollections(ctx, tx, shardID, ns, workflowID, run); err != nil {
		return err
	}
	if err := deleteBufferedEvents(ctx, tx, shardID, ns, workflowID, run); err != nil {
		return err
	}
	if _, err := tx.DeleteFromExecutions(ctx, sqlplugin.ExecutionsFilter{
		ShardID: shardID, NamespaceID: ns, WorkflowID: workflowID, RunID: run,
	}); err != nil {
		return serviceerror.NewUnavailablef("deleting the executions row of run %s: %v", runID, err)
	}
	return nil
}

// collections is one write's upserts across the seven keyed tables a run owns,
// and deletions its removals. Two structs rather than fourteen parameters,
// because a mutation names both halves and a snapshot only the first.
type collections struct {
	activities     map[int64]*commonpb.DataBlob
	timers         map[string]*commonpb.DataBlob
	children       map[int64]*commonpb.DataBlob
	requestCancels map[int64]*commonpb.DataBlob
	signals        map[int64]*commonpb.DataBlob
	signalsWanted  map[string]struct{}
	chasm          map[string]p.InternalChasmNode
}

type deletions struct {
	activities     map[int64]struct{}
	timers         map[string]struct{}
	children       map[int64]struct{}
	requestCancels map[int64]struct{}
	signals        map[int64]struct{}
	signalsWanted  map[string]struct{}
	chasm          map[string]struct{}
}

func applySnapshotCollections(
	ctx context.Context, tx sqlplugin.Tx, shardID int32,
	ns primitives.UUID, workflowID string, run primitives.UUID, s *p.InternalWorkflowSnapshot,
) error {
	return applyCollections(ctx, tx, shardID, ns, workflowID, run, collections{
		activities:     s.ActivityInfos,
		timers:         s.TimerInfos,
		children:       s.ChildExecutionInfos,
		requestCancels: s.RequestCancelInfos,
		signals:        s.SignalInfos,
		signalsWanted:  s.SignalRequestedIDs,
		chasm:          s.ChasmNodes,
	}, deletions{})
}

// applyCollections writes the seven keyed tables. Upserts precede deletes, as
// they do upstream; fold resolves an upsert and a delete of one key to whichever
// came last, so the two sets are disjoint and the order decides nothing.
func applyCollections(
	ctx context.Context, tx sqlplugin.Tx, shardID int32,
	ns primitives.UUID, workflowID string, run primitives.UUID,
	up collections, del deletions,
) error {
	if err := writeKeyed("activity info", up.activities, del.activities,
		func(id int64, blob *commonpb.DataBlob) sqlplugin.ActivityInfoMapsRow {
			return sqlplugin.ActivityInfoMapsRow{
				ShardID: shardID, NamespaceID: ns, WorkflowID: workflowID, RunID: run,
				ScheduleID: id, Data: blob.Data, DataEncoding: blob.EncodingType.String(),
			}
		},
		func(rows []sqlplugin.ActivityInfoMapsRow) (sql.Result, error) {
			return tx.ReplaceIntoActivityInfoMaps(ctx, rows)
		},
		func(ids []int64) (sql.Result, error) {
			return tx.DeleteFromActivityInfoMaps(ctx, sqlplugin.ActivityInfoMapsFilter{
				ShardID: shardID, NamespaceID: ns, WorkflowID: workflowID, RunID: run, ScheduleIDs: ids,
			})
		}); err != nil {
		return err
	}

	if err := writeKeyed("timer info", up.timers, del.timers,
		func(id string, blob *commonpb.DataBlob) sqlplugin.TimerInfoMapsRow {
			return sqlplugin.TimerInfoMapsRow{
				ShardID: shardID, NamespaceID: ns, WorkflowID: workflowID, RunID: run,
				TimerID: id, Data: blob.Data, DataEncoding: blob.EncodingType.String(),
			}
		},
		func(rows []sqlplugin.TimerInfoMapsRow) (sql.Result, error) {
			return tx.ReplaceIntoTimerInfoMaps(ctx, rows)
		},
		func(ids []string) (sql.Result, error) {
			return tx.DeleteFromTimerInfoMaps(ctx, sqlplugin.TimerInfoMapsFilter{
				ShardID: shardID, NamespaceID: ns, WorkflowID: workflowID, RunID: run, TimerIDs: ids,
			})
		}); err != nil {
		return err
	}

	if err := writeKeyed("child execution info", up.children, del.children,
		func(id int64, blob *commonpb.DataBlob) sqlplugin.ChildExecutionInfoMapsRow {
			return sqlplugin.ChildExecutionInfoMapsRow{
				ShardID: shardID, NamespaceID: ns, WorkflowID: workflowID, RunID: run,
				InitiatedID: id, Data: blob.Data, DataEncoding: blob.EncodingType.String(),
			}
		},
		func(rows []sqlplugin.ChildExecutionInfoMapsRow) (sql.Result, error) {
			return tx.ReplaceIntoChildExecutionInfoMaps(ctx, rows)
		},
		func(ids []int64) (sql.Result, error) {
			return tx.DeleteFromChildExecutionInfoMaps(ctx, sqlplugin.ChildExecutionInfoMapsFilter{
				ShardID: shardID, NamespaceID: ns, WorkflowID: workflowID, RunID: run, InitiatedIDs: ids,
			})
		}); err != nil {
		return err
	}

	if err := writeKeyed("request cancel info", up.requestCancels, del.requestCancels,
		func(id int64, blob *commonpb.DataBlob) sqlplugin.RequestCancelInfoMapsRow {
			return sqlplugin.RequestCancelInfoMapsRow{
				ShardID: shardID, NamespaceID: ns, WorkflowID: workflowID, RunID: run,
				InitiatedID: id, Data: blob.Data, DataEncoding: blob.EncodingType.String(),
			}
		},
		func(rows []sqlplugin.RequestCancelInfoMapsRow) (sql.Result, error) {
			return tx.ReplaceIntoRequestCancelInfoMaps(ctx, rows)
		},
		func(ids []int64) (sql.Result, error) {
			return tx.DeleteFromRequestCancelInfoMaps(ctx, sqlplugin.RequestCancelInfoMapsFilter{
				ShardID: shardID, NamespaceID: ns, WorkflowID: workflowID, RunID: run, InitiatedIDs: ids,
			})
		}); err != nil {
		return err
	}

	if err := writeKeyed("signal info", up.signals, del.signals,
		func(id int64, blob *commonpb.DataBlob) sqlplugin.SignalInfoMapsRow {
			return sqlplugin.SignalInfoMapsRow{
				ShardID: shardID, NamespaceID: ns, WorkflowID: workflowID, RunID: run,
				InitiatedID: id, Data: blob.Data, DataEncoding: blob.EncodingType.String(),
			}
		},
		func(rows []sqlplugin.SignalInfoMapsRow) (sql.Result, error) {
			return tx.ReplaceIntoSignalInfoMaps(ctx, rows)
		},
		func(ids []int64) (sql.Result, error) {
			return tx.DeleteFromSignalInfoMaps(ctx, sqlplugin.SignalInfoMapsFilter{
				ShardID: shardID, NamespaceID: ns, WorkflowID: workflowID, RunID: run, InitiatedIDs: ids,
			})
		}); err != nil {
		return err
	}

	if err := writeKeyed("signals requested", up.signalsWanted, del.signalsWanted,
		func(id string, _ struct{}) sqlplugin.SignalsRequestedSetsRow {
			return sqlplugin.SignalsRequestedSetsRow{
				ShardID: shardID, NamespaceID: ns, WorkflowID: workflowID, RunID: run, SignalID: id,
			}
		},
		func(rows []sqlplugin.SignalsRequestedSetsRow) (sql.Result, error) {
			return tx.ReplaceIntoSignalsRequestedSets(ctx, rows)
		},
		func(ids []string) (sql.Result, error) {
			return tx.DeleteFromSignalsRequestedSets(ctx, sqlplugin.SignalsRequestedSetsFilter{
				ShardID: shardID, NamespaceID: ns, WorkflowID: workflowID, RunID: run, SignalIDs: ids,
			})
		}); err != nil {
		return err
	}

	return writeKeyed("CHASM node", up.chasm, del.chasm,
		func(path string, node p.InternalChasmNode) sqlplugin.ChasmNodeMapsRow {
			row := sqlplugin.ChasmNodeMapsRow{
				ShardID: shardID, NamespaceID: ns, WorkflowID: workflowID, RunID: run,
				ChasmPath:        path,
				Metadata:         node.Metadata.Data,
				MetadataEncoding: node.Metadata.EncodingType.String(),
			}
			if node.Data != nil {
				row.Data, row.DataEncoding = node.Data.Data, node.Data.EncodingType.String()
			}
			return row
		},
		func(rows []sqlplugin.ChasmNodeMapsRow) (sql.Result, error) {
			return tx.ReplaceIntoChasmNodeMaps(ctx, rows)
		},
		func(paths []string) (sql.Result, error) {
			return tx.DeleteFromChasmNodeMaps(ctx, sqlplugin.ChasmNodeMapsFilter{
				ShardID: shardID, NamespaceID: ns, WorkflowID: workflowID, RunID: run, ChasmPaths: paths,
			})
		})
}

// clearCollections empties the seven tables for one run, which is what a reset
// does before writing its whole state and what a tombstone does instead of it.
func clearCollections(
	ctx context.Context, tx sqlplugin.Tx, shardID int32,
	ns primitives.UUID, workflowID string, run primitives.UUID,
) error {
	clears := []struct {
		what  string
		clear func() (sql.Result, error)
	}{
		{"activity info", func() (sql.Result, error) {
			return tx.DeleteAllFromActivityInfoMaps(ctx, sqlplugin.ActivityInfoMapsAllFilter{
				ShardID: shardID, NamespaceID: ns, WorkflowID: workflowID, RunID: run})
		}},
		{"timer info", func() (sql.Result, error) {
			return tx.DeleteAllFromTimerInfoMaps(ctx, sqlplugin.TimerInfoMapsAllFilter{
				ShardID: shardID, NamespaceID: ns, WorkflowID: workflowID, RunID: run})
		}},
		{"child execution info", func() (sql.Result, error) {
			return tx.DeleteAllFromChildExecutionInfoMaps(ctx, sqlplugin.ChildExecutionInfoMapsAllFilter{
				ShardID: shardID, NamespaceID: ns, WorkflowID: workflowID, RunID: run})
		}},
		{"request cancel info", func() (sql.Result, error) {
			return tx.DeleteAllFromRequestCancelInfoMaps(ctx, sqlplugin.RequestCancelInfoMapsAllFilter{
				ShardID: shardID, NamespaceID: ns, WorkflowID: workflowID, RunID: run})
		}},
		{"signal info", func() (sql.Result, error) {
			return tx.DeleteAllFromSignalInfoMaps(ctx, sqlplugin.SignalInfoMapsAllFilter{
				ShardID: shardID, NamespaceID: ns, WorkflowID: workflowID, RunID: run})
		}},
		{"signals requested", func() (sql.Result, error) {
			return tx.DeleteAllFromSignalsRequestedSets(ctx, sqlplugin.SignalsRequestedSetsAllFilter{
				ShardID: shardID, NamespaceID: ns, WorkflowID: workflowID, RunID: run})
		}},
		{"CHASM node", func() (sql.Result, error) {
			return tx.DeleteAllFromChasmNodeMaps(ctx, sqlplugin.ChasmNodeMapsAllFilter{
				ShardID: shardID, NamespaceID: ns, WorkflowID: workflowID, RunID: run})
		}},
	}
	for _, c := range clears {
		if _, err := c.clear(); err != nil {
			return serviceerror.NewUnavailablef("clearing the %s map: %v", c.what, err)
		}
	}
	return nil
}

// writeKeyed is one keyed table's upserts and deletes. All seven differ only in
// their row type and their two statements, so those are parameters: a
// transcription per table is seven places for one of them to drift.
func writeKeyed[K comparable, V any, R any](
	what string,
	upserts map[K]V,
	deletes map[K]struct{},
	row func(K, V) R,
	replace func([]R) (sql.Result, error),
	remove func([]K) (sql.Result, error),
) error {
	if len(upserts) > 0 {
		rows := make([]R, 0, len(upserts))
		for k, v := range upserts {
			rows = append(rows, row(k, v))
		}
		if _, err := replace(rows); err != nil {
			return serviceerror.NewUnavailablef("upserting %s rows: %v", what, err)
		}
	}
	if len(deletes) > 0 {
		if _, err := remove(slices.Collect(maps.Keys(deletes))); err != nil {
			return serviceerror.NewUnavailablef("deleting %s rows: %v", what, err)
		}
	}
	return nil
}

// createExecution inserts a run's row. A duplicate key is the store's own
// condition failure and everything else is a failure of the database: the first
// halts the shard, the second leaves the drain's outcome unknown, and rounding
// one to the other is either a shard halted for a blip or a blip mistaken for a
// broken invariant.
func createExecution(
	ctx context.Context, tx sqlplugin.Tx, shardID int32,
	ns, run primitives.UUID, workflowID string,
	info *commonpb.DataBlob, state *persistencespb.WorkflowExecutionState,
	nextEventID, lastWriteVersion, dbRecordVersion int64,
) error {
	row, err := executionRow(shardID, ns, run, workflowID, info, state, nextEventID, lastWriteVersion, dbRecordVersion)
	if err != nil {
		return err
	}
	result, err := tx.InsertIntoExecutions(ctx, row)
	if err != nil {
		// Not translated into a condition failure, which is what upstream's own
		// create does with a duplicate-key error. A drain asserts every run row
		// it touches under this transaction's lock a statement earlier
		// (assertRuns), so a run that is already there is answered there, named
		// — TestACreateOfARunThatExistsFailsAtItsAssertion holds that — and
		// fencing leaves nobody to have put it in between. So an insert that
		// fails here is infrastructure, and its transaction may yet commit.
		//
		// The plugin cannot tell the difference anyway: sqlite's
		// IsDupEntryError masks the error code against the constraint codes
		// with a bitwise AND rather than comparing it, and 3603 &
		// SQLITE_FULL, SQLITE_IOERR, SQLITE_CORRUPT, SQLITE_NOMEM,
		// SQLITE_INTERRUPT and SQLITE_BUSY are all non-zero. Every one of those
		// has an unknown outcome, and [cold.Applier] says not to round one down.
		return serviceerror.NewUnavailablef("inserting the executions row of run %s: %v", state.RunId, err)
	}
	return exactlyOneRow(result, "insert", workflowID, state.RunId)
}

func updateExecution(
	ctx context.Context, tx sqlplugin.Tx, shardID int32,
	ns, run primitives.UUID, workflowID string,
	info *commonpb.DataBlob, state *persistencespb.WorkflowExecutionState,
	nextEventID, lastWriteVersion, dbRecordVersion int64,
) error {
	row, err := executionRow(shardID, ns, run, workflowID, info, state, nextEventID, lastWriteVersion, dbRecordVersion)
	if err != nil {
		return err
	}
	result, err := tx.UpdateExecutions(ctx, row)
	if err != nil {
		return serviceerror.NewUnavailablef("updating the executions row of run %s: %v", state.RunId, err)
	}
	return exactlyOneRow(result, "update", workflowID, state.RunId)
}

// exactlyOneRow is upstream's post-check on both execution-row writes. It is a
// condition failure here rather than upstream's not-found: the row is keyed by
// (shard, namespace, workflow, run) and this drain asserted it a statement
// earlier, so a count other than one is a claim about the row that no retry can
// make true.
func exactlyOneRow(result sql.Result, what, workflowID, runID string) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return serviceerror.NewUnavailablef("counting the rows the executions %s affected: %v", what, err)
	}
	if affected != 1 {
		return &p.ConditionFailedError{
			Msg: fmt.Sprintf("the executions %s of workflow %s run %s affected %d rows instead of 1",
				what, workflowID, runID, affected),
		}
	}
	return nil
}

func executionRow(
	shardID int32, ns, run primitives.UUID, workflowID string,
	info *commonpb.DataBlob, state *persistencespb.WorkflowExecutionState,
	nextEventID, lastWriteVersion, dbRecordVersion int64,
) (*sqlplugin.ExecutionsRow, error) {
	stateBlob, err := serialization.WorkflowExecutionStateToBlob(state)
	if err != nil {
		return nil, serviceerror.NewUnavailablef("serialising the execution state of run %s: %v", state.RunId, err)
	}
	return &sqlplugin.ExecutionsRow{
		ShardID:          shardID,
		NamespaceID:      ns,
		WorkflowID:       workflowID,
		RunID:            run,
		NextEventID:      nextEventID,
		LastWriteVersion: lastWriteVersion,
		Data:             info.Data,
		DataEncoding:     info.EncodingType.String(),
		State:            stateBlob.Data,
		StateEncoding:    stateBlob.EncodingType.String(),
		DBRecordVersion:  dbRecordVersion,
	}, nil
}

// insertBufferedEvents adds the batch as a row of its own: batches never merge,
// so a caller holding several calls once per batch. The id column upstream's
// read sorts on is never written — the v3 SQLite schema declares it BIGINT
// AUTO_INCREMENT, which SQLite takes for a type name and leaves NULL — so what
// orders the rows for a reader is the scan.
func insertBufferedEvents(
	ctx context.Context, tx sqlplugin.Tx, shardID int32,
	ns primitives.UUID, workflowID string, run primitives.UUID, batch *commonpb.DataBlob,
) error {
	if batch == nil {
		return nil
	}
	rows := []sqlplugin.BufferedEventsRow{{
		ShardID: shardID, NamespaceID: ns, WorkflowID: workflowID, RunID: run,
		Data: batch.Data, DataEncoding: batch.EncodingType.String(),
	}}
	if _, err := tx.InsertIntoBufferedEvents(ctx, rows); err != nil {
		return serviceerror.NewUnavailablef("inserting buffered events for workflow %s: %v", workflowID, err)
	}
	return nil
}

func deleteBufferedEvents(
	ctx context.Context, tx sqlplugin.Tx, shardID int32,
	ns primitives.UUID, workflowID string, run primitives.UUID,
) error {
	if _, err := tx.DeleteFromBufferedEvents(ctx, sqlplugin.BufferedEventsFilter{
		ShardID: shardID, NamespaceID: ns, WorkflowID: workflowID, RunID: run,
	}); err != nil {
		return serviceerror.NewUnavailablef("clearing buffered events for workflow %s: %v", workflowID, err)
	}
	return nil
}

// applyTasks writes history-task rows. The four categories that predate the
// general tables keep their own, which is upstream's compatibility rule and not
// a choice available here: a queue reads the table its category is stored in.
func applyTasks(
	ctx context.Context, tx sqlplugin.Tx, shardID int32,
	byCategory map[tasks.Category][]p.InternalHistoryTask,
) error {
	for category, list := range byCategory {
		if len(list) == 0 {
			continue
		}
		var err error
		switch category.Type() {
		case tasks.CategoryTypeImmediate:
			err = insertImmediateTasks(ctx, tx, shardID, category.ID(), list)
		case tasks.CategoryTypeScheduled:
			err = insertScheduledTasks(ctx, tx, shardID, category.ID(), list)
		default:
			err = serviceerror.NewInternalf("unknown task category type: %v", category)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func insertImmediateTasks(
	ctx context.Context, tx sqlplugin.Tx, shardID int32, categoryID int, list []p.InternalHistoryTask,
) error {
	switch categoryID {
	case tasks.CategoryIDTransfer:
		rows := make([]sqlplugin.TransferTasksRow, 0, len(list))
		for _, t := range list {
			rows = append(rows, sqlplugin.TransferTasksRow{
				ShardID: shardID, TaskID: t.Key.TaskID,
				Data: t.Blob.Data, DataEncoding: t.Blob.EncodingType.String(),
			})
		}
		return insertedAll(len(rows), "transfer_tasks", func() (sql.Result, error) {
			return tx.InsertIntoTransferTasks(ctx, rows)
		})

	case tasks.CategoryIDVisibility:
		rows := make([]sqlplugin.VisibilityTasksRow, 0, len(list))
		for _, t := range list {
			rows = append(rows, sqlplugin.VisibilityTasksRow{
				ShardID: shardID, TaskID: t.Key.TaskID,
				Data: t.Blob.Data, DataEncoding: t.Blob.EncodingType.String(),
			})
		}
		return insertedAll(len(rows), "visibility_tasks", func() (sql.Result, error) {
			return tx.InsertIntoVisibilityTasks(ctx, rows)
		})

	case tasks.CategoryIDReplication:
		rows := make([]sqlplugin.ReplicationTasksRow, 0, len(list))
		for _, t := range list {
			rows = append(rows, sqlplugin.ReplicationTasksRow{
				ShardID: shardID, TaskID: t.Key.TaskID,
				Data: t.Blob.Data, DataEncoding: t.Blob.EncodingType.String(),
			})
		}
		return insertedAll(len(rows), "replication_tasks", func() (sql.Result, error) {
			return tx.InsertIntoReplicationTasks(ctx, rows)
		})

	default:
		rows := make([]sqlplugin.HistoryImmediateTasksRow, 0, len(list))
		for _, t := range list {
			rows = append(rows, sqlplugin.HistoryImmediateTasksRow{
				ShardID: shardID, CategoryID: int32(categoryID), TaskID: t.Key.TaskID,
				Data: t.Blob.Data, DataEncoding: t.Blob.EncodingType.String(),
			})
		}
		return insertedAll(len(rows), "history_immediate_tasks", func() (sql.Result, error) {
			return tx.InsertIntoHistoryImmediateTasks(ctx, rows)
		})
	}
}

func insertScheduledTasks(
	ctx context.Context, tx sqlplugin.Tx, shardID int32, categoryID int, list []p.InternalHistoryTask,
) error {
	if categoryID == tasks.CategoryIDTimer {
		rows := make([]sqlplugin.TimerTasksRow, 0, len(list))
		for _, t := range list {
			rows = append(rows, sqlplugin.TimerTasksRow{
				ShardID: shardID, VisibilityTimestamp: t.Key.FireTime, TaskID: t.Key.TaskID,
				Data: t.Blob.Data, DataEncoding: t.Blob.EncodingType.String(),
			})
		}
		return insertedAll(len(rows), "timer_tasks", func() (sql.Result, error) {
			return tx.InsertIntoTimerTasks(ctx, rows)
		})
	}

	rows := make([]sqlplugin.HistoryScheduledTasksRow, 0, len(list))
	for _, t := range list {
		rows = append(rows, sqlplugin.HistoryScheduledTasksRow{
			ShardID: shardID, CategoryID: int32(categoryID),
			VisibilityTimestamp: t.Key.FireTime, TaskID: t.Key.TaskID,
			Data: t.Blob.Data, DataEncoding: t.Blob.EncodingType.String(),
		})
	}
	return insertedAll(len(rows), "history_scheduled_tasks", func() (sql.Result, error) {
		return tx.InsertIntoHistoryScheduledTasks(ctx, rows)
	})
}

// insertedAll runs a task insert and checks the count, which is upstream's own
// guard: a task row silently not inserted is a timer that never fires.
func insertedAll(want int, table string, insert func() (sql.Result, error)) error {
	result, err := insert()
	if err != nil {
		return serviceerror.NewUnavailablef("inserting into %s: %v", table, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return serviceerror.NewUnavailablef("counting the rows inserted into %s: %v", table, err)
	}
	if int(affected) != want {
		return serviceerror.NewUnavailablef("inserted %d rows into %s instead of %d", affected, table, want)
	}
	return nil
}

// rangeDeleteTasks removes one category's range under the store's own predicate
// for that category: an immediate one on task id, a scheduled one on the
// visibility timestamp with the task ids not looked at.
func rangeDeleteTasks(ctx context.Context, tx sqlplugin.Tx, shardID int32, r fold.TaskRange) error {
	categoryID := r.Category.ID()
	var err error
	switch {
	case r.Category.Type() == tasks.CategoryTypeImmediate && categoryID == tasks.CategoryIDTransfer:
		_, err = tx.RangeDeleteFromTransferTasks(ctx, sqlplugin.TransferTasksRangeFilter{
			ShardID:            shardID,
			InclusiveMinTaskID: r.InclusiveMin.TaskID,
			ExclusiveMaxTaskID: r.ExclusiveMax.TaskID,
		})
	case r.Category.Type() == tasks.CategoryTypeImmediate && categoryID == tasks.CategoryIDVisibility:
		_, err = tx.RangeDeleteFromVisibilityTasks(ctx, sqlplugin.VisibilityTasksRangeFilter{
			ShardID:            shardID,
			InclusiveMinTaskID: r.InclusiveMin.TaskID,
			ExclusiveMaxTaskID: r.ExclusiveMax.TaskID,
		})
	case r.Category.Type() == tasks.CategoryTypeImmediate && categoryID == tasks.CategoryIDReplication:
		_, err = tx.RangeDeleteFromReplicationTasks(ctx, sqlplugin.ReplicationTasksRangeFilter{
			ShardID:            shardID,
			InclusiveMinTaskID: r.InclusiveMin.TaskID,
			ExclusiveMaxTaskID: r.ExclusiveMax.TaskID,
		})
	case r.Category.Type() == tasks.CategoryTypeImmediate:
		_, err = tx.RangeDeleteFromHistoryImmediateTasks(ctx, sqlplugin.HistoryImmediateTasksRangeFilter{
			ShardID:            shardID,
			CategoryID:         int32(categoryID),
			InclusiveMinTaskID: r.InclusiveMin.TaskID,
			ExclusiveMaxTaskID: r.ExclusiveMax.TaskID,
		})
	case r.Category.Type() == tasks.CategoryTypeScheduled && categoryID == tasks.CategoryIDTimer:
		_, err = tx.RangeDeleteFromTimerTasks(ctx, sqlplugin.TimerTasksRangeFilter{
			ShardID:                         shardID,
			InclusiveMinVisibilityTimestamp: r.InclusiveMin.FireTime,
			ExclusiveMaxVisibilityTimestamp: r.ExclusiveMax.FireTime,
		})
	case r.Category.Type() == tasks.CategoryTypeScheduled:
		_, err = tx.RangeDeleteFromHistoryScheduledTasks(ctx, sqlplugin.HistoryScheduledTasksRangeFilter{
			ShardID:                         shardID,
			CategoryID:                      int32(categoryID),
			InclusiveMinVisibilityTimestamp: r.InclusiveMin.FireTime,
			ExclusiveMaxVisibilityTimestamp: r.ExclusiveMax.FireTime,
		})
	default:
		err = serviceerror.NewInternalf("unknown task category type: %v", r.Category)
	}
	if err != nil {
		return serviceerror.NewUnavailablef("range-deleting category %d: %v", categoryID, err)
	}
	return nil
}

// runKeys parses the two uuids every row of a run is keyed by. A malformed one
// is the caller's and not the database's.
func runKeys(namespaceID, runID string) (primitives.UUID, primitives.UUID, error) {
	ns, err := parseNamespace(namespaceID)
	if err != nil {
		return nil, nil, err
	}
	run, err := parseRun(runID)
	if err != nil {
		return nil, nil, err
	}
	return ns, run, nil
}

// parseNamespace and parseRun are the halves of [runKeys], for the paths that
// have one id in hand and not the other. Split rather than copied because the
// sentence a malformed id is reported with is the same sentence wherever it is
// found.
func parseNamespace(namespaceID string) (primitives.UUID, error) {
	ns, err := primitives.ParseUUID(namespaceID)
	if err != nil {
		return nil, serviceerror.NewInternalf("namespace id %q is not a uuid: %v", namespaceID, err)
	}
	return ns, nil
}

func parseRun(runID string) (primitives.UUID, error) {
	run, err := primitives.ParseUUID(runID)
	if err != nil {
		return nil, serviceerror.NewInternalf("run id %q is not a uuid: %v", runID, err)
	}
	return run, nil
}

// lockRun reads a run row's db_record_version under the transaction's lock, and
// reports absence as a nil row rather than as an error, which is the shape
// fold's assertion is stated over.
//
// Upstream's lockAndCheckExecution has a second arm this one does not: where a
// request carries db_record_version 0 it is judged on next_event_id against the
// request's condition instead. Fold derives every run assertion as
// DBRecordVersion−1 and has no second form, so a request that would have taken
// that arm asserts −1 here. Carrying the fallback would mean a second assertion
// shape reaching fold, which decides the same condition before the ack.
func lockRun(
	ctx context.Context, tx sqlplugin.Tx, shardID int32,
	ns primitives.UUID, workflowID string, run primitives.UUID,
) (*p.InternalGetWorkflowExecutionResponse, error) {
	version, _, err := tx.WriteLockExecutions(ctx, sqlplugin.ExecutionsFilter{
		ShardID: shardID, NamespaceID: ns, WorkflowID: workflowID, RunID: run,
	})
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, serviceerror.NewUnavailablef("locking the executions row of workflow %s: %v", workflowID, err)
	}
	return &p.InternalGetWorkflowExecutionResponse{DBRecordVersion: version}, nil
}

// lockCurrent reads a workflow's current-execution row under the transaction's
// lock, nil when there is none.
func lockCurrent(
	ctx context.Context, tx sqlplugin.Tx, shardID int32, ns primitives.UUID, workflowID string,
) (*sqlplugin.CurrentExecutionsRow, error) {
	row, err := tx.LockCurrentExecutions(ctx, sqlplugin.CurrentExecutionsFilter{
		ShardID: shardID, NamespaceID: ns, WorkflowID: workflowID,
	})
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, serviceerror.NewUnavailablef("locking the current row of workflow %s: %v", workflowID, err)
	}
	return row, nil
}

// startTimeOf is the column upstream derives from the execution state, nil
// meaning the state carries none.
func startTimeOf(state *persistencespb.WorkflowExecutionState) *time.Time {
	if state == nil || state.StartTime == nil {
		return nil
	}
	t := state.StartTime.AsTime()
	return &t
}
