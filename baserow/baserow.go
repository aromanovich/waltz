// Package baserow provides the cold store's two mutable-state reads the write
// path needs: one run's row, and a workflow's current-execution row with its
// last_write_version.
//
// The wrapper, the cycle and apply all need these reads and may not import one
// another, so this package imports only upstream Temporal.
//
// It stamps the shard onto each request, returns absence as a nil row rather
// than an error, and returns the current row's version beside it.
package baserow

import (
	"context"
	"errors"
	"fmt"

	"go.temporal.io/api/serviceerror"
	p "go.temporal.io/server/common/persistence"
)

// Store is the pair of reads, in Temporal's signatures. The cold store must
// satisfy it or intercept mode does not start ([ErrNoVersionedRead]).
//
// The current-row read is a store extension because
// [p.InternalGetCurrentExecutionResponse] cannot hold last_write_version.
// Judging the assertion through the plain read could wrongly confirm it.
type Store interface {
	GetWorkflowExecution(
		context.Context, *p.GetWorkflowExecutionRequest,
	) (*p.InternalGetWorkflowExecutionResponse, error)
	GetCurrentExecutionWithLastWriteVersion(
		context.Context, *p.GetCurrentExecutionRequest,
	) (*p.InternalGetCurrentExecutionResponse, int64, error)
}

// ErrNoVersionedRead is returned by [Of] for a store that does not implement
// [Store]; intercept mode cannot run over it.
var ErrNoVersionedRead = errors.New("baserow: the store below cannot read a current row's last_write_version")

// Rows reads the pre-window rows a shard's write path depends on. A nil *Rows
// means no store; callers with a delegated assertion must handle that.
type Rows struct {
	store Store
}

// New wraps a store that answers both reads.
func New(store Store) *Rows { return &Rows{store: store} }

// Of is [New] for a store that arrives from the factory as [p.ExecutionStore],
// which cannot declare the version-carrying read.
func Of(store p.ExecutionStore) (*Rows, error) {
	versioned, ok := store.(Store)
	if !ok {
		return nil, fmt.Errorf("%w: %T", ErrNoVersionedRead, store)
	}
	return New(versioned), nil
}

// Run reads one run's row, or nil if there is none.
func (r *Rows) Run(
	ctx context.Context, shard int32, namespaceID, workflowID, runID string,
) (*p.InternalGetWorkflowExecutionResponse, error) {
	row, err := r.store.GetWorkflowExecution(ctx, &p.GetWorkflowExecutionRequest{
		ShardID:     shard,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	})
	if err != nil {
		if absent(err) {
			return nil, nil
		}
		return nil, err
	}
	return row, nil
}

// Current reads a workflow's current-execution row and its last_write_version,
// or nil and zero if there is none.
//
// The store must put the run in RunID (as upstream does, leaving the execution
// state's copy empty) and the request ids in the state: a refused write's
// conflict error is built from this row. Without a run, history will not
// resolve the conflict; without request ids, a retried start does not
// deduplicate.
func (r *Rows) Current(
	ctx context.Context, shard int32, namespaceID, workflowID string,
) (*p.InternalGetCurrentExecutionResponse, int64, error) {
	row, version, err := r.store.GetCurrentExecutionWithLastWriteVersion(ctx, &p.GetCurrentExecutionRequest{
		ShardID:     shard,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
	})
	if err != nil {
		if absent(err) {
			return nil, 0, nil
		}
		return nil, 0, err
	}
	return row, version, nil
}

// absent reports whether err means "no such row"; stores signal it as an error.
func absent(err error) bool {
	return errors.As(err, new(*serviceerror.NotFound))
}
