package memcold

// The read below is derived from go.temporal.io/server v1.29.6,
// common/persistence/sql, which is copyright Temporal Technologies Inc. and
// Uber Technologies, Inc. and licensed under the MIT licence. NOTICE at the
// repository root has that licence and what this file takes.

import (
	"context"
	"database/sql"
	"errors"

	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
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

	base, version := currentRowResponse(row)
	return base, version, nil
}

// currentRowResponse is one current_executions row as every assertion in this
// package is judged against it, absence included. The drain reads the row under
// its own lock and the layer reads it through the method above; one derivation,
// so the two cannot disagree about what the store holds.
func currentRowResponse(row *sqlplugin.CurrentExecutionsRow) (*p.InternalGetCurrentExecutionResponse, int64) {
	if row == nil {
		return nil, 0
	}
	return &p.InternalGetCurrentExecutionResponse{
		RunID: row.RunID.String(),
		ExecutionState: &persistencespb.WorkflowExecutionState{
			CreateRequestId: row.CreateRequestID,
			State:           row.State,
			Status:          row.Status,
		},
	}, row.LastWriteVersion
}
