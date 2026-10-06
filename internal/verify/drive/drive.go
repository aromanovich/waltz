// Package drive is the writing half of a run: it turns one mutation into one
// ExecutionStore call, takes a shard so calls carry an epoch, and [Stream]s a
// generated stream through the codec. It judges nothing; the checker does.
//
// Nothing here takes a *testing.T: everything returns an error, so a binary
// driving a stream under kill -9 runs the same code a test fixture does.
//
// It depends on mutgen, never the reverse: a generator that could see its
// driver would end up tuned to it.
package drive

import (
	"context"

	p "go.temporal.io/server/common/persistence"

	"github.com/aromanovich/waltz/mutation"
)

// Apply drives one mutation through store, stamping epoch as the RangeID. The
// codec drops RangeID (it is the epoch, carried by the WAL entry; invariant
// I11), so the caller that took the shard supplies it. The three unstamped
// requests have no RangeID field.
//
// A mutation with zero or several requests returns
// [mutation.ErrNotExactlyOneRequest], so a new kind without an arm here fails
// loudly instead of applying nothing.
func Apply(ctx context.Context, store p.ExecutionStore, epoch int64, m mutation.Mutation) error {
	switch m.Kind() {
	case mutation.KindCreate:
		m.Create.RangeID = epoch
		_, err := store.CreateWorkflowExecution(ctx, m.Create)
		return err
	case mutation.KindUpdate:
		m.Update.RangeID = epoch
		return store.UpdateWorkflowExecution(ctx, m.Update)
	case mutation.KindConflictResolve:
		m.ConflictResolve.RangeID = epoch
		return store.ConflictResolveWorkflowExecution(ctx, m.ConflictResolve)
	case mutation.KindSet:
		m.Set.RangeID = epoch
		return store.SetWorkflowExecution(ctx, m.Set)
	case mutation.KindDelete:
		return store.DeleteWorkflowExecution(ctx, m.Delete)
	case mutation.KindDeleteCurrent:
		return store.DeleteCurrentWorkflowExecution(ctx, m.DeleteCurrent)
	case mutation.KindAddTasks:
		m.AddTasks.RangeID = epoch
		return store.AddHistoryTasks(ctx, m.AddTasks)
	case mutation.KindRangeCompleteTasks:
		return store.RangeCompleteHistoryTasks(ctx, m.RangeCompleteTasks)
	default:
		return mutation.ErrNotExactlyOneRequest
	}
}
