// Package baserow is the cold store's two mutable-state reads, as the write
// path needs them: one run's row, and a workflow's current-execution row with
// the last_write_version beside it.
//
// It exists because three packages need the same pair and none of them may name
// each other's version of it. The wrapper holds a store and may import nothing
// that reaches the plugin; the cycle stands a delegated assertion on a
// pre-window row and may not name a store at all; apply reads the same two rows
// back to attribute a condition failure. What it imports is upstream Temporal
// and nothing of this layer, so all three may reach it.
//
// What it owns is the three things every caller of those reads was repeating:
// the shard is stamped onto the request, absence arrives as a nil row rather
// than as an error, and the current row's version travels beside it.
package baserow

import (
	"context"
	"errors"
	"fmt"

	"go.temporal.io/api/serviceerror"
	p "go.temporal.io/server/common/persistence"
)

// Store is the pair of reads as Temporal's own store spells them. The
// current-row read is the one that carries last_write_version
// through the version-carrying store extension. The store asserts on that
// column and [p.InternalGetCurrentExecutionResponse] has nowhere to hold it, so a layer
// deciding the same condition through the plain read could confirm it and never
// refuse it.
type Store interface {
	GetWorkflowExecution(
		context.Context, *p.GetWorkflowExecutionRequest,
	) (*p.InternalGetWorkflowExecutionResponse, error)
	GetCurrentExecutionWithLastWriteVersion(
		context.Context, *p.GetCurrentExecutionRequest,
	) (*p.InternalGetCurrentExecutionResponse, int64, error)
}

// ErrNoVersionedRead is what [Of] answers for a store that does not implement
// [Store]: the cold store below has not been extended with the
// version-carrying current-row read, so intercept mode cannot be served over
// it.
var ErrNoVersionedRead = errors.New("baserow: the store below cannot read a current row's last_write_version")

// Rows reads the pre-window rows one shard's write path stands on. A nil *Rows
// is a caller that brought no store, which every caller of a delegated
// assertion has to answer for itself.
type Rows struct {
	store Store
}

// New wraps a store that answers both reads.
func New(store Store) *Rows { return &Rows{store: store} }

// Of is [New] for a caller holding Temporal's own interface, which cannot
// declare the version-carrying read. It is a conversion and not a parameter
// type because that is how the store arrives: through the factory, as
// [p.ExecutionStore].
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

// absent is how "there is no such row" arrives from a store: an error and not
// an empty answer, on both reads.
func absent(err error) bool {
	return errors.As(err, new(*serviceerror.NotFound))
}
