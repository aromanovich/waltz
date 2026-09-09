// Package drive is the half of a run that *writes*: one mutation into one
// ExecutionStore call, a shard taken so those calls have an epoch to carry, and
// [Stream] — a generated stream behind the codec, for the callers that want what
// came back out of a log. It judges nothing.
//
// # Why it knows nothing about testing
//
// A fixture is a `*testing.T` API: it fails tests and skips when no cluster is
// reachable. Everything here returns an error instead, so a driver with no test
// to fail — a binary driving a stream under `kill -9` — runs the same code a
// fixture does, and the driving half stays one implementation both are judged
// through.
//
// It reaches for the persistence interface, for [mutation.Kind] and for the
// generator, and, in [Recorder], for `wal`'s two id types beside the checker's
// record — and for nothing else of the layer: what a call *means* is the
// checker's business. The dependency on verify/mutgen only points this way — a
// generator that could reach the thing driving it would be tuned to it.
package drive

import (
	"context"

	p "go.temporal.io/server/common/persistence"

	"github.com/aromanovich/waltz/mutation"
)

// Apply drives one mutation through an ExecutionStore, stamping the epoch the
// store asserts. The mutation carries no RangeID — the codec drops it, because
// it is the epoch and travels with the WAL entry (invariant I11) — so filling it
// in is the caller's job here exactly as it is apply's. The three requests that
// are handed over unstamped have no RangeID field of their own.
//
// A mutation holding no request, or several, is refused with
// [mutation.ErrNotExactlyOneRequest] rather than ignored, so a ninth kind added
// to [mutation.Mutation] without an arm here fails loudly instead of silently
// applying nothing.
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
