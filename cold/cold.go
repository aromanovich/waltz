// Package cold is the cold store contract: the seam a drain lands on, as [wal]
// is the seam an ack lands on. A deployment implements [Store]; no package of
// the layer writes to a store of its own. cold/memcold, the one implementation
// here, sits under this seam as a deployment's store would.
//
// A cold store holds a Temporal history shard's mutable state, history tasks,
// history events and replication DLQ (persistence.ExecutionStore and
// ShardStore). A drain folds many acked mutations into one batch and hands it
// to the [Applier] once. The store owes four things; breaking any of them loses
// acked data:
//
//  1. One drain is one publication. The merged requests, the task work and the
//     watermark commit in one transaction. A half-applied batch cannot be
//     repaired by replay: replay re-drives the same folded window against rows
//     the half that landed has already moved.
//
//     [fold.Batch.History] may be written outside that transaction, by any
//     means, but every history row must be durable no later than the mutable
//     state that names it: inside the transaction or before it starts. History
//     nodes are immutable and keyed by (tree, branch, node, transaction), so
//     writing them twice is harmless and a failed drain leaves only
//     unreferenced orphans. The reverse order publishes state pointing at
//     history nobody wrote.
//
//     Inside the transaction, the batch's range deletes run before its task
//     inserts. The window keeps a task that arrived after a range even if the
//     range covers its key, so inserting first and deleting second deletes an
//     acked task.
//
//  2. The watermark (the batch's seqno) commits inside that transaction.
//     [Watermarker] reads it back as the only record of what a drain did; never
//     derive it from the rows. A watermark written outside the transaction
//     makes the shard replay what it applied or trim what it did not.
//
//  3. The epoch is asserted first, and the whole batch is refused if it has
//     moved. Fencing makes the layer a shard's single writer. Nothing else in
//     the transaction catches a stale owner for every batch: run rows carry a
//     version, but task work asserts nothing, so a range completion under a
//     stale epoch deletes rows the real owner acked.
//
//  4. The outcome comes back in [apply]'s five classes: committed, refused,
//     shard lost, invariant violated, unknown outcome. Reporting an ambiguous
//     result as a failure instead of unknown gets the batch applied twice.
//
// An [Applier] must also bound its own calls. Most drains run on a context with
// no deadline, because they carry earlier writers' acked mutations and must not
// fail because one caller's deadline expired. An Apply that hangs blocks the
// shard's loop, which serves all of its writes and reads, and graceful shutdown
// waits on that loop without a bound. The layer imposes no timeout because a
// cut-short drain is an unknown outcome, which stalls the shard. The store's
// own driver, statement or request timeout is the only bound.
//
// Intercept mode also asserts on two pre-window rows: a run's mutable state,
// and a workflow's current-execution row with its last_write_version. That pair
// is [baserow.Store]; it is part of this layer's contract because Temporal's
// response type has no field for that version. A store that cannot answer the
// versioned read is refused at server start, not run in a reduced mode.
//
// [apply]: ../apply
// [baserow.Store]: ../baserow
package cold

import (
	"context"

	"github.com/aromanovich/waltz/fold"
	"github.com/aromanovich/waltz/wal"
)

// Store is the cold store a deployment hands the layer. Both halves must be one
// value: a watermark read from a store other than the one the drains wrote to
// makes the layer trim entries that store never saw, or replay ones it holds.
type Store interface {
	Applier
	Watermarker
}

// Applier is the write path of one drain.
type Applier interface {
	// Apply commits everything batch carries (the merged request per dirty
	// workflow, the history-task work, the range completions) and
	// batch.Watermark() in one transaction, after a compare-and-set on epoch,
	// with batch.History() durable no later than the commit. The package doc
	// says why.
	//
	// The cycle reads the error through apply.Classify. Return nil only if the
	// transaction committed; *persistence.ShardOwnershipLostError if the epoch
	// had moved; a condition failure if an assertion did not hold; apply.Refuse
	// for input this store cannot express. Anything else, including every
	// ambiguous timeout, dropped connection or context deadline, is an unknown
	// outcome, which the cycle resolves by reading the watermark. Never report
	// an ambiguous result as a failure: the cycle gives up on a failed batch.
	//
	// ctx often has no deadline (size, age, refusal and storage-pressure drains
	// run detached), so Apply must bound itself; see the package doc.
	Apply(ctx context.Context, shard wal.ShardID, epoch wal.Epoch, batch fold.Batch) error
}

// HistoryApplier is declared by an [Applier] that writes [fold.Batch.History]
// itself, inside Apply. Over an applier that does not declare it, the layer
// writes each live write's events through the store below before appending,
// and its batches carry none.
//
// Replay does not check the declaration: a tail written over a declaring store
// carries event batches whatever the successor declares (an Open entry in
// DURABILITY.md). So any applier handed a non-empty [fold.Batch.History] must
// write it under obligation 1, declared or not; ignoring it commits mutable
// state over events nobody wrote.
type HistoryApplier interface {
	Applier

	// AppliesHistory is never called. Declaring it is the claim.
	AppliesHistory()
}

// Watermarker is the recovery half of the seam.
type Watermarker interface {
	// Watermark returns the seqno written by the last committed [Applier.Apply]
	// for this shard: the value that transaction stored, never one derived from
	// the rows or cached in this process.
	//
	//   - (seqno, true, nil): every entry up to seqno is in this store; a new
	//     owner replays from seqno+1. After an unknown outcome, the drain
	//     committed if seqno equals exactly the one it carried; a higher seqno
	//     means another owner wrote, and the cycle treats it as such.
	//   - (_, false, nil): no drain has ever committed for this shard; a new
	//     owner replays the whole log. After an unknown outcome the cycle reads
	//     this as "did not commit" and halts the shard.
	//   - (_, _, err): the answer could not be read, which is not the same as
	//     false. The cycle stalls the tail and retries.
	//
	// Never report a seqno above what committed: the layer would trim entries
	// the store never received. When unsure, return the error or what is
	// durably recorded.
	Watermark(ctx context.Context, shard wal.ShardID) (wal.Seqno, bool, error)
}
