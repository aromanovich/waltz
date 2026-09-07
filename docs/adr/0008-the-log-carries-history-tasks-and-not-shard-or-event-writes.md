# 8. The log carries history tasks, and not shard or event writes

Date: 2026-08-02

## Status

Accepted, and implemented: two record kinds for the two history-task calls, and no bound kept
beside the apply loop.

## Context

Before this the write path was split. Six mutable-state writes went through the WAL; both
history-task calls went straight to the cold store. That is not a difference in importance, it
is an **inversion of order**: `RangeCompleteHistoryTasks` took effect immediately while
`AddHistoryTasks` — and every task carried by a mutable-state write — waited for a drain.

The compensation was invariant I7's drop: a bound per (shard, epoch, category), filled from
whichever queue's goroutine checkpointed, snapshotted per drain, and applied to task rows the
batch was about to write. It worked, and it had a residual of its own — a checkpoint landing
between a drain's snapshot and its commit leaks that drain's rows.

The question this ADR answers is not "should the deletes be deferred too" but **where the log's
boundary is**, since the same argument reaches shard writes and event history.

### Why both halves of the task path, and not only the deletes

Deferring the delete while the write stays immediate is worse than either. A store deletes a
**scheduled** category's range by fire time and ignores the task ids entirely. A delete applied
late over a write applied immediately therefore covers a timer created *after* the checkpoint whose
fire time falls inside the passed window. That is not a garbage row: it is a lost timer, and a
workflow that never wakes. There is no intermediate state between "both halves in the log" and
"neither".

### Why not shard writes

`GetOrCreateShard`, `UpdateShard` and `AssertShardOwnership` stay out, and the reason is that
rangeID is two things at once:

* the **fencing token** every drain compares against (invariants I4 and I11);
* the **task-id allocator**, handed to `taskKeyManager.setRangeID` immediately after
  `UpdateShard` succeeds (`shard/context_impl.go`'s `renewRangeLocked`).

Deferring it would reopen the double-write window the shard-acquire ordering exists to forbid, and
would hand one range of task ids to two owners. It is also circular: the log entry carrying the
bump would have to be appended under the epoch that bump establishes.

### Why not event history

Decision D3, unchanged: the question is revisited after mutable state **and tasks**, which is where
this leaves it. Two things make it the wrong next step rather than merely a later one — I10's
budget is denominated in bytes and event blobs are the bulk of them, and an intercepted write
already puts its own events down through the base store before it acks, so the tree is never behind
the tail.

The replication DLQ is out for the ordinary reason: its writer is not one of the intercepted
methods and nothing in the window can change its answer.

## Decision

**`AddHistoryTasks` and `RangeCompleteHistoryTasks` are record kinds.** Eight kinds, and the
partition at the wrapper becomes intercepted / answered / **refused**.

**`CompleteHistoryTask` is refused in intercept mode**, with
`wrapper.ErrCompleteHistoryTaskUnsupported`. The log's deletion record is a range per category
because that is what a queue's own checkpoints are — `[old, new)`, butt-joined, only rising —
and it is what makes the rule cheap enough to live in the accumulator and be rebuilt by replay.
A single key is neither, and a second deletion shape would be a second thing every reader, every
drain and every replay has to agree about. Its one caller in the whole server is the admin
handler's `RemoveTask`, which today, on a task still in the window, deletes a row that is not
there and reports success. Passthrough still transits it.

**The answer is given at the append**, like every other intercepted write (I2), not after the
drain. The decoupling the old bound bought — a queue checkpoint never parked behind a stuck
apply — is lost either way once the checkpoint enters the loop; what waiting for the transaction
would add is the one place this layer blocks for a long time. Upstream survives a refusal:
`queue_base.go` logs it and resets the checkpoint timer without advancing its deletion watermark.

**The resolution is the accumulator's**, exactly as it is for the current-execution row: a range
folding in removes the tasks the window already holds inside it, because a store that gathers every
delete before every upsert would come out *written* whatever the window meant.

## Consequences

**I7's bound stops being side state.** There is no mutex, no snapshot per drain, and no counter for
a race that can no longer happen: a range delete is an entry, so it moves in log order and is
replayed with the rest of the window. `cycle/acked.go` and a task filter in the write path are
gone.

**One thing about the old bound is reversed, and it is worth stating outright.** It outlived every
drain and dropped whatever fell below it whenever it arrived, because the delete had already
happened. With both halves in the log the order is the caller's own, so a task arriving *after* a
range delete is one the caller wrote after it — which the sequential path writes and keeps.
Dropping it would be a divergence. The pending ranges therefore die with the drain that applies
them.

**The predicate is compared at the store's resolution, and a differential run against the
sequential path is what said so.** The first generated range delete judged that way produced one
row present on the sequential path and absent on the folded one: the range's exclusive maximum sat
a nanosecond above a task's fire time, which covers it in memory and truncates to that same fire
time in a store whose timestamps are coarser, where `fire_time < max` excludes it. A dropped task
is never written and never deleted, so that was a **lost timer** — the exact failure this decision
exists to prevent, found on the first run. `storedResolution` in `fold/histtasks.go` is where the
resolution is stated, and it is a constant there because this layer names no store.

**Merge-on-read subtracts the undrained ranges from the cold store's own page.** Without it the
layer's correctness would rest on a property of the caller — that a queue never reads below its
own deletion watermark — which is not this layer's property to rely on.

**`temporal admin ... remove-task` stops working on a deployment running intercept mode.**

**A store's own paged fallback for a large range delete is unavailable inside the drain's
transaction.** A store that catches an intermediate-data-materialization limit and re-runs the
delete in pages cannot do so as a statement inside somebody else's transaction, because the pages
would be separate transactions. The exposure is the same size as the standalone call's — the same
range, the same predicate — but where that one degrades into paging, this one fails the drain it
rides and halts the shard. Named rather than closed, and it is a property of the `cold.Applier`
a deployment supplies rather than of this library.

**Every task write and every queue checkpoint is now answered by the shard's apply loop**, which
is the decoupling the old bound bought and this ADR gives up knowingly (see "The answer is given at
the append" above). It is a real cost under load and it is unquantified here: this repository
measures no performance, and what a given cold store does with the shape is the deployment's to
measure.
