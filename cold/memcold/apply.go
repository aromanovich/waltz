package memcold

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

var _ cold.Applier = (*Store)(nil)

// Apply is the cold package's contract implemented, and the reference for a
// client implementing it over another database.
//
// The order of the transaction is the contract, statement for statement:
//
//  1. the epoch, as a compare-and-set on the shard's range id. First, so a
//     drain that lost the shard reports a lost shard rather than the version
//     failure a fenced writer would find underneath it — the shard's new owner
//     has been writing, and every version this drain stands on is stale for a
//     reason that is not this shard's to halt over.
//  2. the task range deletes, before any task row this drain writes. A task
//     that arrived after a range is one fold deliberately kept, and a delete
//     running after that insert would take it away — a timer that never fires
//     rather than a row left behind.
//  3. the merged requests, in the batch's own tail-seqno order, each preceded
//     by the assertions fold registered for it: the head-of-window
//     db_record_version on every run row it touches and, once per workflow, the
//     current-execution row's.
//  4. the shard-level task rows, and the watermark.
//
// A client owes the same four things and gets none of them from an
// ExecutionStore: that interface has nowhere to declare a transaction spanning
// many workflows, so the write path has to be built beside it, on whatever the
// driver offers below. An implementer whose driver offers nothing below cannot
// satisfy the contract by trying harder inside it.
//
// What this one gets for free from being one transaction on one connection is
// worth naming, because a client on a different engine will not have it: the
// statements take effect in the order they are issued. So an assertion reads
// the rows as every earlier request of this same batch left them, a run
// tombstoned and recreated inside one window needs no special case, and the
// only ordering rules left are the two stated above — the epoch first and the
// range deletes before the inserts. An engine that gathers a transaction's
// statements and reorders them by table has to reproduce those orderings some
// other way, and a batch that lands in a different order is not the same batch.
//
// The outcome: nil is committed; [apply.Refuse] is a drain nothing was written
// for, refused before the transaction opened; *p.ShardOwnershipLostError is the
// epoch; a condition failure comes back attributed by [apply.Attribute], naming
// the rows that diverged; anything else is an unknown outcome and the caller
// must read the watermark before it does anything else.
func (s *Store) Apply(ctx context.Context, shard wal.ShardID, epoch wal.Epoch, batch fold.Batch) error {
	if err := refusals(shard, epoch, batch); err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx)
	if err != nil {
		// Not a refusal: nothing was written, but nothing establishes that from
		// here, and a caller told "refused" would not read the watermark back.
		return serviceerror.NewUnavailablef("memcold: opening the drain's transaction on shard %d: %v", shard, err)
	}

	if err := s.drain(ctx, tx, shard, epoch, batch); err != nil {
		_ = tx.Rollback()
		// The readback runs after the rollback and not before it: the database
		// is served by one connection, and a read taken while the drain's
		// transaction still holds it would wait for a transaction waiting for
		// the read.
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

// refusals are the checks that must fail before the transaction opens, so that
// what they answer is [apply.ClassRefused] — an input to fix, with no outcome to
// recover. Everything a batch is internally consistent about is
// [fold.Accumulator.Drain]'s postcondition and is not re-derived here; what is
// left is this call's own pairing, and the two fan-outs whose default arm would
// otherwise commit a transaction that wrote nothing for the request it could
// not read.
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

// drain is everything the transaction carries, in the order Apply's doc states.
// It does not commit: the caller does, so that a failure here is always a
// transaction still open and always rolled back.
func (s *Store) drain(
	ctx context.Context, tx sqlplugin.Tx, shard wal.ShardID, epoch wal.Epoch, batch fold.Batch,
) error {
	shardID := int32(shard)

	if err := assertEpoch(ctx, tx, shardID, int64(epoch)); err != nil {
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

// assertEpoch is the drain's fence: the shard's range id must still be the one
// this writer holds. A shard with no row is a lost shard rather than a failure —
// this writer cannot own a shard that is not there, and reporting an unknown
// outcome would leave the caller retrying a drain that can never land.
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

// applyRequest drives one merged request: the workflow's current-row facts if
// this is where they belong, then the run assertions, then the rows.
func (s *Store) applyRequest(
	ctx context.Context, tx sqlplugin.Tx, shardID int32, e *fold.Emitted,
) error {
	if e.FirstOfWorkflow() {
		if err := applyCurrentRow(ctx, tx, shardID, e); err != nil {
			return err
		}
	}
	if err := assertRuns(ctx, tx, shardID, e); err != nil {
		return err
	}

	ns, err := primitives.ParseUUID(e.NamespaceID)
	if err != nil {
		return serviceerror.NewInternalf("namespace id %q is not a uuid: %v", e.NamespaceID, err)
	}

	switch e.Request.Kind() {
	case mutation.KindCreate:
		if err := s.applySnapshotAsNew(ctx, tx, shardID, &e.Request.Create.NewWorkflowSnapshot); err != nil {
			return err
		}

	case mutation.KindUpdate:
		req := e.Request.Update
		if err := applyMutation(ctx, tx, shardID, &req.UpdateWorkflowMutation); err != nil {
			return err
		}
		if added := req.NewWorkflowSnapshot; added != nil {
			if err := s.applySnapshotAsNew(ctx, tx, shardID, added); err != nil {
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
			if err := s.applySnapshotAsNew(ctx, tx, shardID, added); err != nil {
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
		// The tasks the collapse orphaned: the Delete has no task slot of its
		// own, and a task lost here is lost in the tail as well.
		if err := applyTasks(ctx, tx, shardID, e.OrphanedTasks()); err != nil {
			return err
		}

	case mutation.KindDeleteCurrent:
		if err := deleteCurrentRow(ctx, tx, shardID, ns, e); err != nil {
			return err
		}
	}

	// Batches never merge, so each is a row of its own, written after the
	// request that may have cleared the rows already there.
	for _, b := range e.BufferedBatches {
		run, err := primitives.ParseUUID(b.RunID)
		if err != nil {
			return serviceerror.NewInternalf("run id %q is not a uuid: %v", b.RunID, err)
		}
		if err := insertBufferedEvents(ctx, tx, shardID, ns, e.WorkflowID, run, b.Blob); err != nil {
			return err
		}
	}
	return nil
}

// applyCurrentRow settles the workflow's current-execution row once per
// workflow record: fold's head-of-window assertion, and then the write its tail
// left. Both are the workflow's rather than any one request's, which is why
// they ride the request [fold.Emitted.FirstOfWorkflow] marks — the store reports
// the first failing assertion, and hoisting every workflow's to the front of
// the batch would change which failure a mixed drain reports.
func applyCurrentRow(ctx context.Context, tx sqlplugin.Tx, shardID int32, e *fold.Emitted) error {
	wf := e.Workflow()
	if wf.Current == nil && wf.CurrentWrite == nil {
		return nil
	}
	ns, err := primitives.ParseUUID(e.NamespaceID)
	if err != nil {
		return serviceerror.NewInternalf("namespace id %q is not a uuid: %v", e.NamespaceID, err)
	}

	row, err := lockCurrent(ctx, tx, shardID, ns, e.WorkflowID)
	if err != nil {
		return err
	}
	if cur := wf.Current; cur != nil {
		// Judged through fold's own predicate, against the row shaped the way
		// this store's versioned read returns it: what the layer confirmed
		// before the ack and what the drain asserts have to be the same
		// question asked twice.
		base, version := currentRowResponse(row)
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

// writeCurrentRow puts the window's last current-row write into the store,
// inserting or updating according to what the assertion read found: the plugin
// offers the two statements and no upsert, and the row was read a statement ago
// under this transaction's lock.
//
// The columns beside the blob are recovered from it. [fold.CurrentWrite] carries
// the run, the state and the last write version, and the create request id,
// status and start time live only inside the serialised state — so the row's
// scalars and its blob agree by construction rather than by two derivations
// staying in step.
func writeCurrentRow(
	ctx context.Context, tx sqlplugin.Tx, shardID int32,
	ns primitives.UUID, workflowID string, cw *fold.CurrentWrite, exists bool,
) error {
	state, err := serialization.WorkflowExecutionStateFromBlob(cw.StateBlob)
	if err != nil {
		return serviceerror.NewUnavailablef(
			"deserialising the current-row state of run %s: %v", cw.RunID, err)
	}
	run, err := primitives.ParseUUID(cw.RunID)
	if err != nil {
		return serviceerror.NewInternalf("run id %q is not a uuid: %v", cw.RunID, err)
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

// deleteCurrentRow applies a folded DeleteCurrent. The guard is the request's
// run and must never become an assertion — a delete-current naming a run that is
// no longer current is an ordinary no-op — except where fold reports the window
// removed the row it wrote itself: the row in the store is then the pre-window
// one, which the request cannot name, so the delete takes whatever is there.
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
	run, err := primitives.ParseUUID(guard)
	if err != nil {
		return serviceerror.NewInternalf("run id %q is not a uuid: %v", guard, err)
	}
	if _, err := tx.DeleteFromCurrentExecutions(ctx, sqlplugin.CurrentExecutionsFilter{
		ShardID: shardID, NamespaceID: ns, WorkflowID: e.WorkflowID, RunID: run,
	}); err != nil {
		return serviceerror.NewUnavailablef("deleting the current row of workflow %s: %v", e.WorkflowID, err)
	}
	return nil
}

// assertRuns places one request's head-of-window run assertions, in run-id
// order. The order is not the map's: the drain reports the first assertion that
// fails, and a map's iteration would make which failure a caller sees differ
// between two runs of the same batch.
func assertRuns(ctx context.Context, tx sqlplugin.Tx, shardID int32, e *fold.Emitted) error {
	runs := e.RunAssertions()
	if len(runs) == 0 {
		return nil
	}
	ns, err := primitives.ParseUUID(e.NamespaceID)
	if err != nil {
		return serviceerror.NewInternalf("namespace id %q is not a uuid: %v", e.NamespaceID, err)
	}

	for _, runID := range slices.Sorted(maps.Keys(runs)) {
		run, err := primitives.ParseUUID(runID)
		if err != nil {
			return serviceerror.NewInternalf("run id %q is not a uuid: %v", runID, err)
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
