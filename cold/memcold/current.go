package memcold

// The read below is derived from go.temporal.io/server v1.29.6,
// common/persistence/sql, which is copyright Temporal Technologies Inc. and
// Uber Technologies, Inc. and licensed under the MIT licence. NOTICE at the
// repository root has that licence and what this file takes.

import (
	"context"
	"database/sql"
	"errors"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// GetCurrentExecutionWithLastWriteVersion is the one read this store adds to
// the embedded one. Upstream's GetCurrentExecution selects the whole
// current_executions row and then drops last_write_version, because
// [p.InternalGetCurrentExecutionResponse] has nowhere to hold it; a create
// asserts on that column, so a layer that cannot see it can confirm the
// assertion and never refuse it.
//
// Everything else is upstream's read, down to the error classes it hands back:
// an absent row is a *serviceerror.NotFound and a broken database is a
// *serviceerror.Unavailable. The caller separates them — the first is "no such
// workflow", the second is a failure — and a store answering absence any other
// way makes the layer treat a live workflow as unstarted.
func (s *Store) GetCurrentExecutionWithLastWriteVersion(
	ctx context.Context,
	request *p.GetCurrentExecutionRequest,
) (*p.InternalGetCurrentExecutionResponse, int64, error) {
	row, err := s.db.SelectFromCurrentExecutions(ctx, sqlplugin.CurrentExecutionsFilter{
		ShardID:     request.ShardID,
		NamespaceID: primitives.MustParseUUID(request.NamespaceID),
		WorkflowID:  request.WorkflowID,
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, 0, serviceerror.NewNotFound(err.Error())
		}
		return nil, 0, serviceerror.NewUnavailablef("GetCurrentExecution operation failed. Error: %v", err)
	}

	return currentRowResponse(row)
}

// currentRowResponse is one current_executions row as every assertion in this
// package is judged against it, absence included. The drain reads the row under
// its own lock and the layer reads it through the method above; one derivation,
// so the two cannot disagree about what the store holds.
func currentRowResponse(row *sqlplugin.CurrentExecutionsRow) (*p.InternalGetCurrentExecutionResponse, int64, error) {
	if row == nil {
		return nil, 0, nil
	}
	state, err := executionStateOf(row)
	if err != nil {
		return nil, 0, err
	}
	return &p.InternalGetCurrentExecutionResponse{
		RunID:          row.RunID.String(),
		ExecutionState: state,
	}, row.LastWriteVersion, nil
}

// executionStateOf is the row's serialised state, and the columns beside it only
// for a record written before that blob existed — upstream's own order, because
// the two carry different amounts: the blob has the run id and the request ids
// and the columns do not. What stands on those two is the conflict error the
// layer refuses a write with, whose run id decides whether the history service
// resolves the conflict at all and whose request ids are what a retried start
// deduplicates on.
//
// A blob that will not deserialise is reported rather than quietly answered out
// of the columns: the caller would get a row that names no run and no way to
// tell it from one that never had them.
func executionStateOf(row *sqlplugin.CurrentExecutionsRow) (*persistencespb.WorkflowExecutionState, error) {
	if len(row.Data) > 0 && row.DataEncoding != "" {
		state, err := serialization.WorkflowExecutionStateFromBlob(p.NewDataBlob(row.Data, row.DataEncoding))
		if err != nil {
			return nil, serviceerror.NewUnavailablef(
				"memcold: deserialising the current-execution state of run %s: %v", row.RunID, err)
		}
		return state, nil
	}

	state := &persistencespb.WorkflowExecutionState{
		CreateRequestId: row.CreateRequestID,
		RunId:           row.RunID.String(),
		State:           row.State,
		Status:          row.Status,
		RequestIds: map[string]*persistencespb.RequestIDInfo{
			row.CreateRequestID: {
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
				EventId:   common.FirstEventID,
			},
		},
	}
	if row.StartTime != nil {
		state.StartTime = timestamppb.New(*row.StartTime)
	}
	return state, nil
}
