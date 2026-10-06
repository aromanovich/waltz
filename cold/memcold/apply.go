package memcold

// The current-row write here is derived from go.temporal.io/server v1.29.6,
// common/persistence/sql, which is copyright Temporal Technologies Inc. and
// Uber Technologies, Inc. and licensed under the MIT licence. NOTICE at the
// repository root has that licence and what this file takes.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"slices"

	"go.temporal.io/api/serviceerror"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"

	"github.com/aromanovich/waltz/apply"
	"github.com/aromanovich/waltz/baserow"
	"github.com/aromanovich/waltz/cold"
	"github.com/aromanovich/waltz/fold"
	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/wal"
)

var (
	_ cold.Applier        = (*Store)(nil)
	_ cold.HistoryApplier = (*Store)(nil)
)

// AppliesHistory declares that this store writes the batch's event history. It
// is never called; see [cold.HistoryApplier].
func (*Store) AppliesHistory() {}

// Apply implements the cold package's contract and is the reference for a
// client implementing it over another database.
//
// The transaction, in order:
//
//  1. The epoch, as a locked compare of the shard's rangeID. First, so a drain
//     that lost the shard reports a lost shard, not the stale-version failure
//     the new owner's writes would cause.
//  2. The event-history rows, before anything that points at them. Here they
//     share the transaction; the contract also allows writing them first.
//  3. The task range deletes, before any task insert. Otherwise a delete would
//     remove a task fold deliberately kept: a timer that never fires.
//  4. The merged requests, in tail-seqno order. Each starts with the
//     current-execution row where the workflow's record rides it (fold's
//     assertion, then the window's write, once per workflow), then the
//     db_record_version assertion on every run row it touches, then its rows.
//  5. The shard-level task rows, and the watermark.
//
// An ExecutionStore cannot express a transaction spanning many workflows, so
// a client must build this beside it, on what its driver offers.
//
// One transaction on one connection applies statements in issue order, so an
// assertion sees earlier requests of the same batch, and a run deleted and
// recreated in one window needs no special case. An engine that reorders
// statements (e.g. by table) must preserve orders 1–3 some other way.
//
// Outcome: nil is committed; [apply.Refuse] means nothing was written;
// *p.ShardOwnershipLostError is the epoch; a condition failure is attributed
// by [apply.Attribute]; anything else is an unknown outcome, and the caller
// must read the watermark before doing anything else.
func (s *Store) Apply(ctx context.Context, shard wal.ShardID, epoch wal.Epoch, batch fold.Batch) error {
	if err := refusals(shard, epoch, batch); err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx)
	if err != nil {
		// Not a refusal: we cannot prove nothing was written, and a caller told
		// "refused" would not read the watermark.
		return serviceerror.NewUnavailablef("memcold: opening the drain's transaction on shard %d: %v", shard, err)
	}

	if err := s.drain(ctx, tx, shard, epoch, batch); err != nil {
		if rerr := tx.Rollback(); rerr != nil {
			// Without a rollback the issued statements may still land, so the
			// outcome is unknown. [cold.Applier] forbids reporting it as a
			// definite failure; Unavailable makes the cycle read the watermark.
			return serviceerror.NewUnavailablef(
				"memcold: the drain of shard %d failed (%v) and its transaction would not roll back: %v",
				shard, err, rerr)
		}
		// Read back only after the rollback: there is one connection, and a
		// read while the transaction holds it would deadlock.
		if apply.Classify(err) == apply.ClassInvariantViolated {
			return apply.Attribute(ctx, baserow.New(s), err, shard, batch)
		}
		return err
	}
	if err := tx.Commit(); err != nil {
		return serviceerror.NewUnavailablef("memcold: committing the drain of shard %d: %v", shard, err)
	}
	return nil
}

// refusals are the checks made before the transaction opens, so a failure is
// [apply.ClassRefused]. Batch consistency is [fold.Accumulator.Drain]'s
// postcondition and is not rechecked. What remains is the shard/epoch pairing
// and two switches whose unknown arm would otherwise commit silently: an
// unhandled request kind would write nothing, and an unknown current-row
// assertion kind would be confirmed unchecked.
func refusals(shard wal.ShardID, epoch wal.Epoch, batch fold.Batch) error {
	if epoch == 0 {
		return apply.Refuse(errors.New("memcold: epoch 0 is not an epoch to write under"))
	}
	if batch.Empty() {
		return apply.Refuse(errors.New("memcold: the drain carries no request and no task work"))
	}
	if got := batch.Shard(); got != shard {
		return apply.Refuse(fmt.Errorf(
			"memcold: the drain folded shard %d, this call writes shard %d", got, shard))
	}
	for e := range batch.Each() {
		switch e.Request.Kind() {
		case mutation.KindCreate, mutation.KindUpdate, mutation.KindConflictResolve,
			mutation.KindSet, mutation.KindDelete, mutation.KindDeleteCurrent:
		default:
			return apply.Refuse(fmt.Errorf("memcold: request kind %s reaches no write path", e.Request.Kind()))
		}
		if cur := e.Workflow().Current; cur != nil && e.FirstOfWorkflow() {
			switch cur.Kind {
			case fold.CurrentMustNotExist, fold.CurrentEquals,
				fold.CurrentNotEquals, fold.CurrentEqualsWithVersion:
			default:
				return apply.Refuse(fmt.Errorf(
					"memcold: current-row assertion kind %d of workflow %s is one nothing here evaluates",
					cur.Kind, e.WorkflowID))
			}
		}
	}
	return nil
}

// drain issues the transaction's statements in the order listed on [Store.Apply].
// It does not commit: the caller does, so a failure here always leaves an open
// transaction to roll back.
func (s *Store) drain(
	ctx context.Context, tx sqlplugin.Tx, shard wal.ShardID, epoch wal.Epoch, batch fold.Batch,
) error {
	shardID := int32(shard)

	// The epoch must precede the request loop
	// (TestAStaleEpochShadowsTheVersionFailureUnderIt); the deletes do not assert.
	if err := assertEpoch(ctx, tx, shardID, int64(epoch)); err != nil {
		return err
	}

	// History must be durable no later than the mutable state naming it; one
	// transaction guarantees that.
	if err := applyHistory(ctx, tx, batch.History()); err != nil {
		return err
	}

	work := batch.Tasks()
	for _, r := range work.Delete {
		if err := rangeDeleteTasks(ctx, tx, shardID, r); err != nil {
			return err
		}
	}

	for e := range batch.Each() {
		if err := s.applyRequest(ctx, tx, shardID, e); err != nil {
			return err
		}
	}

	if err := applyTasks(ctx, tx, shardID, work.Insert); err != nil {
		return err
	}
	return SetWatermark(ctx, tx, shard, batch.Watermark())
}

// assertEpoch is the drain's fence: the shard's rangeID must still be this
// writer's. A missing shard row is a lost shard: an unknown outcome would have
// the caller retry a drain that can never land.
func assertEpoch(ctx context.Context, tx sqlplugin.Tx, shardID int32, epoch int64) error {
	rangeID, err := tx.ReadLockShards(ctx, sqlplugin.ShardsFilter{ShardID: shardID})
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return &p.ShardOwnershipLostError{
			ShardID: shardID,
			Msg:     fmt.Sprintf("shard %d has no row to write under", shardID),
		}
	case err != nil:
		return serviceerror.NewUnavailablef("locking shard %d: %v", shardID, err)
	case rangeID != epoch:
		return &p.ShardOwnershipLostError{
			ShardID: shardID,
			Msg: fmt.Sprintf("the drain writes under range id %d, shard %d holds %d",
				epoch, shardID, rangeID),
		}
	}
	return nil
}

// applyRequest writes one merged request: the current row if this request
// carries it, then the run assertions, then the rows.
func (s *Store) applyRequest(
	ctx context.Context, tx sqlplugin.Tx, shardID int32, e *fold.Emitted,
) error {
	ns, err := parseNamespace(e.NamespaceID)
	if err != nil {
		return err
	}

	if e.FirstOfWorkflow() {
		if err := applyCurrentRow(ctx, tx, shardID, ns, e); err != nil {
			return err
		}
	}
	if err := assertRuns(ctx, tx, shardID, ns, e); err != nil {
		return err
	}

	switch e.Request.Kind() {
	case mutation.KindCreate:
		if err := applySnapshotAsNew(ctx, tx, shardID, &e.Request.Create.NewWorkflowSnapshot); err != nil {
			return err
		}

	case mutation.KindUpdate:
		req := e.Request.Update
		if err := applyMutation(ctx, tx, shardID, &req.UpdateWorkflowMutation); err != nil {
			return err
		}
		if added := req.NewWorkflowSnapshot; added != nil {
			if err := applySnapshotAsNew(ctx, tx, shardID, added); err != nil {
				return err
			}
		}

	case mutation.KindConflictResolve:
		req := e.Request.ConflictResolve
		if err := applySnapshotAsReset(ctx, tx, shardID, &req.ResetWorkflowSnapshot); err != nil {
			return err
		}
		if cur := req.CurrentWorkflowMutation; cur != nil {
			if err := applyMutation(ctx, tx, shardID, cur); err != nil {
				return err
			}
		}
		if added := req.NewWorkflowSnapshot; added != nil {
			if err := applySnapshotAsNew(ctx, tx, shardID, added); err != nil {
				return err
			}
		}

	case mutation.KindSet:
		if err := applySnapshotAsReset(ctx, tx, shardID, &e.Request.Set.SetWorkflowSnapshot); err != nil {
			return err
		}

	case mutation.KindDelete:
		req := e.Request.Delete
		if err := deleteRun(ctx, tx, shardID, req.NamespaceID, req.WorkflowID, req.RunID); err != nil {
			return err
		}
		// Tasks orphaned by the collapse: a Delete has no task slot, and a task
		// dropped here is lost for good.
		if err := applyTasks(ctx, tx, shardID, e.OrphanedTasks()); err != nil {
			return err
		}

	case mutation.KindDeleteCurrent:
		if err := deleteCurrentRow(ctx, tx, shardID, ns, e); err != nil {
			return err
		}
	}

	// Batches never merge: one row each, written after the request, which may
	// have cleared existing rows.
	for _, b := range e.BufferedBatches {
		run, err := parseRun(b.RunID)
		if err != nil {
			return err
		}
		if err := insertBufferedEvents(ctx, tx, shardID, ns, e.WorkflowID, run, b.Blob); err != nil {
			return err
		}
	}
	return nil
}

// applyCurrentRow checks fold's assertion on the current-execution row and
// writes the window's last current-row write, once per workflow, on the request
// [fold.Emitted.FirstOfWorkflow] marks. Not hoisted to the front of the batch:
// that would change which failure a mixed drain reports first.
func applyCurrentRow(
	ctx context.Context, tx sqlplugin.Tx, shardID int32, ns primitives.UUID, e *fold.Emitted,
) error {
	wf := e.Workflow()
	if wf.Current == nil && wf.CurrentWrite == nil {
		return nil
	}

	row, err := lockCurrent(ctx, tx, shardID, ns, e.WorkflowID)
	if err != nil {
		return err
	}
	if cur := wf.Current; cur != nil {
		// Same predicate and row shape as the pre-ack check, so the drain asks
		// exactly the question the layer confirmed.
		base, version, err := currentRowResponse(row)
		if err != nil {
			return err
		}
		if err := cur.VerifyRow(base, version); err != nil {
			return err
		}
	}

	cw := wf.CurrentWrite
	if cw == nil {
		return nil
	}
	return writeCurrentRow(ctx, tx, shardID, ns, e.WorkflowID, cw, row != nil)
}

// writeCurrentRow writes the window's last current-row write, inserting or
// updating according to the locked read just made (the plugin has no upsert).
//
// Create request id, status and start time exist only inside the blob, so they
// are decoded from it; row and blob agree by construction.
func writeCurrentRow(
	ctx context.Context, tx sqlplugin.Tx, shardID int32,
	ns primitives.UUID, workflowID string, cw *fold.CurrentWrite, exists bool,
) error {
	state, err := serialization.WorkflowExecutionStateFromBlob(cw.StateBlob)
	if err != nil {
		return serviceerror.NewUnavailablef(
			"deserialising the current-row state of run %s: %v", cw.RunID, err)
	}
	run, err := parseRun(cw.RunID)
	if err != nil {
		return err
	}

	row := sqlplugin.CurrentExecutionsRow{
		ShardID:          shardID,
		NamespaceID:      ns,
		WorkflowID:       workflowID,
		RunID:            run,
		CreateRequestID:  state.CreateRequestId,
		State:            cw.State,
		Status:           state.Status,
		LastWriteVersion: cw.LastWriteVersion,
		StartTime:        startTimeOf(state),
		Data:             cw.StateBlob.Data,
		DataEncoding:     cw.StateBlob.EncodingType.String(),
	}

	if !exists {
		if _, err := tx.InsertIntoCurrentExecutions(ctx, &row); err != nil {
			return serviceerror.NewUnavailablef(
				"inserting the current row of workflow %s: %v", workflowID, err)
		}
		return nil
	}
	result, err := tx.UpdateCurrentExecutions(ctx, &row)
	if err != nil {
		return serviceerror.NewUnavailablef("updating the current row of workflow %s: %v", workflowID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return serviceerror.NewUnavailablef("counting the current rows updated: %v", err)
	}
	if affected != 1 {
		return &p.CurrentWorkflowConditionFailedError{
			Msg: fmt.Sprintf("the current-row update of workflow %s affected %d rows instead of 1",
				workflowID, affected),
		}
	}
	return nil
}

// deleteCurrentRow applies a folded DeleteCurrent, guarded by the request's
// run. The guard is not an assertion: naming a run that is no longer current
// is a no-op. If the window removed a row it wrote itself (CurrentRemoved),
// the stored row is the pre-window one the request cannot name, so the delete
// takes whatever is there.
func deleteCurrentRow(
	ctx context.Context, tx sqlplugin.Tx, shardID int32, ns primitives.UUID, e *fold.Emitted,
) error {
	guard := e.Request.DeleteCurrent.RunID
	if e.Workflow().CurrentRemoved {
		row, err := lockCurrent(ctx, tx, shardID, ns, e.WorkflowID)
		if err != nil {
			return err
		}
		if row == nil {
			return nil
		}
		guard = row.RunID.String()
	}
	run, err := parseRun(guard)
	if err != nil {
		return err
	}
	if _, err := tx.DeleteFromCurrentExecutions(ctx, sqlplugin.CurrentExecutionsFilter{
		ShardID: shardID, NamespaceID: ns, WorkflowID: e.WorkflowID, RunID: run,
	}); err != nil {
		return serviceerror.NewUnavailablef("deleting the current row of workflow %s: %v", e.WorkflowID, err)
	}
	return nil
}

// assertRuns checks one request's run assertions in run-id order, not map
// order, so the first failure reported is the same on every run of a batch.
func assertRuns(
	ctx context.Context, tx sqlplugin.Tx, shardID int32, ns primitives.UUID, e *fold.Emitted,
) error {
	runs := e.RunAssertions()
	if len(runs) == 0 {
		return nil
	}

	for _, runID := range slices.Sorted(maps.Keys(runs)) {
		run, err := parseRun(runID)
		if err != nil {
			return err
		}
		base, err := lockRun(ctx, tx, shardID, ns, e.WorkflowID, run)
		if err != nil {
			return err
		}
		if err := runs[runID].VerifyRow(e.WorkflowID, base); err != nil {
			return err
		}
	}
	return nil
}

// applyHistory writes the window's event batches: one history_node row each, and
// a history_tree row beside a batch that opens a branch.
//
// Both are upserts (the plugin's REPLACE / ON CONFLICT), so a repeated drain
// rewrites the same immutable row instead of failing on a duplicate key the
// shard could never get past. A store without upsert must solve this itself.
func applyHistory(ctx context.Context, tx sqlplugin.Tx, batches []*p.InternalAppendHistoryNodesRequest) error {
	for _, r := range batches {
		treeID, err := parseTree(r.BranchInfo.TreeId)
		if err != nil {
			return err
		}
		branchID, err := parseBranch(r.BranchInfo.BranchId)
		if err != nil {
			return err
		}
		if _, err := tx.InsertIntoHistoryNode(ctx, &sqlplugin.HistoryNodeRow{
			ShardID:      r.ShardID,
			TreeID:       treeID,
			BranchID:     branchID,
			NodeID:       r.Node.NodeID,
			PrevTxnID:    r.Node.PrevTransactionID,
			TxnID:        r.Node.TransactionID,
			Data:         r.Node.Events.Data,
			DataEncoding: r.Node.Events.EncodingType.String(),
		}); err != nil {
			return fmt.Errorf("memcold: history node %d of branch %s: %w", r.Node.NodeID, r.BranchInfo.BranchId, err)
		}
		if !r.IsNewBranch {
			continue
		}
		if _, err := tx.InsertIntoHistoryTree(ctx, &sqlplugin.HistoryTreeRow{
			ShardID:      r.ShardID,
			TreeID:       treeID,
			BranchID:     branchID,
			Data:         r.TreeInfo.Data,
			DataEncoding: r.TreeInfo.EncodingType.String(),
		}); err != nil {
			return fmt.Errorf("memcold: history tree %s: %w", r.BranchInfo.TreeId, err)
		}
	}
	return nil
}
