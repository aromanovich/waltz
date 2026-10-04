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
	"google.golang.org/protobuf/types/known/timestamppb"
)

// GetCurrentExecutionWithLastWriteVersion is GetCurrentExecution plus the
// row's last_write_version, which [p.InternalGetCurrentExecutionResponse]
// cannot hold. A create asserts on that column; without it the layer could
// confirm an assertion it should refuse.
//
// Errors match upstream: an absent row is *serviceerror.NotFound, a broken
// database *serviceerror.Unavailable. Answering absence any other way makes
// the layer treat a live workflow as unstarted.
//
// Unlike upstream (primitives.MustParseUUID), a malformed namespace id is an
// error, not a panic: this read runs on every delegated current-row check, and
// a panic would take down the process.
func (s *Store) GetCurrentExecutionWithLastWriteVersion(
	ctx context.Context,
	request *p.GetCurrentExecutionRequest,
) (*p.InternalGetCurrentExecutionResponse, int64, error) {
	ns, err := parseNamespace(request.NamespaceID)
	if err != nil {
		return nil, 0, err
	}
	row, err := s.db.SelectFromCurrentExecutions(ctx, sqlplugin.CurrentExecutionsFilter{
		ShardID:     request.ShardID,
		NamespaceID: ns,
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

// currentRowResponse converts a current_executions row (or its absence) into
// what assertions are judged against. Both the drain's locked read and the
// layer's read above use it, so they cannot disagree.
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

// executionStateOf returns the row's serialised state, falling back to the
// columns only for a record written before the blob existed (upstream's
// order). The blob has every request id, the columns only the creating one;
// the conflict error built from this needs the run id (so history resolves the
// conflict) and the request ids (so a retried start deduplicates).
//
// A blob that fails to deserialise is an error, not a silent fallback to the
// columns, which would drop request ids undetectably.
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
