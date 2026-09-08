// Package cold is the cold store contract: the seam a drain lands on, as [wal]
// is the seam an ack lands on. Two interfaces, and in production both are the
// caller's to satisfy — no package of the layer writes to a store of its own.
// The one implementation here is cold/memcold, which is not a package of the
// layer: it sits under this seam where a deployment's store sits.
//
// A cold store here is whatever holds a Temporal history shard's mutable state,
// its history tasks, its history events and its replication DLQ: Temporal's own
// persistence.ExecutionStore and persistence.ShardStore, reached through the
// [Applier] a drain hands its batch to. The layer folds many acked mutations
// into one batch and hands it over once; what the store owes back is four
// things, and each is a way the acked-is-never-lost rule can be broken from
// below.
//
//  1. One drain is one transaction. A batch that lands half-applied leaves rows
//     no replay can reconstruct: the mutations behind it were acked, folded and
//     collapsed, so there is no per-mutation record left to re-drive.
//  2. The watermark commits inside that transaction. It is the seqno the batch
//     carries ([Applier]), and [Watermarker] reads it back — the only witness to
//     what a drain did, and the reason a store may never derive that answer from
//     the rows themselves. A watermark written beside the transaction rather than
//     in it is a shard that either replays what it applied or trims what it did
//     not.
//  3. The epoch is asserted first, and the store refuses the whole batch if it
//     has moved. Fencing is what makes the layer a shard's single writer, and an
//     applier that writes under a stale epoch has two.
//  4. The outcome comes back in [apply]'s five classes. Committed, refused,
//     shard lost, invariant violated, unknown outcome: the cycle branches on
//     them, and the fifth is the one a store gets wrong by rounding an ambiguous
//     code down to a failure. That is a batch applied twice.
//
// Intercept mode asks for one read besides: the current-execution row with its
// last_write_version, which is [baserow.Store] and is asserted on rather than
// returned to a caller. A store that cannot answer it can still be written to;
// it just cannot settle the assertions the fold hands on.
//
// [apply]: ../apply
// [baserow.Store]: ../baserow
package cold

import (
	"context"

	"github.com/aromanovich/waltz/fold"
	"github.com/aromanovich/waltz/wal"
)

// Applier is the write path one drain goes through, and the caller's to supply.
// An interface so a test can vary a drain's outcome without a cluster, and so
// the class of store can change without an invariant moving.
type Applier interface {
	// Apply commits everything batch carries — the merged request per dirty
	// workflow, the history-task work, the range completions — and
	// batch.Watermark(), in one transaction, under an epoch it compare-and-sets
	// first. The four obligations in this package's doc say why each of those
	// is not negotiable.
	//
	// The error is the whole of what the cycle learns, and it is read through
	// apply.Classify rather than compared: return nil only if the transaction
	// committed, a *persistence.ShardOwnershipLostError if the epoch had moved,
	// a condition failure if an assertion did not hold, and apply.Refuse for
	// input this store cannot express. Anything else is an unknown outcome, and
	// that is the right answer for every ambiguous transport code — a timeout,
	// a dropped connection, a context deadline. Do not round one down to a
	// failure: the cycle answers an unknown outcome by reading the watermark,
	// and answers a failure by giving up on the batch.
	Apply(ctx context.Context, shard wal.ShardID, epoch wal.Epoch, batch fold.Batch) error
}

// Watermarker is the recovery half of the same seam: the only read the layer
// makes through this contract, though not its only read of the store — intercept
// mode also asserts on baserow.Store's current-execution row.
type Watermarker interface {
	// Watermark reads back the seqno of the last [Applier.Apply] that committed
	// for this shard — the value that transaction wrote inside itself, never a
	// value derived from the rows and never one cached in this process.
	//
	// The three results are three different answers and the cycle acts on each
	// differently:
	//
	//   - (seqno, true, nil) — every entry up to and including seqno is in this
	//     store. A new owner replays from seqno+1; a drain whose outcome was
	//     unknown committed if seqno is at or above the one it carried.
	//   - (_, false, nil) — no drain has ever committed for this shard. The
	//     seqno is ignored, and a new owner replays the log from the bottom.
	//   - (_, _, err) — the answer could not be read. This is not "false", and
	//     the difference is the point: after an unknown outcome, false means the
	//     drain did not commit and an error means nobody knows yet. The cycle
	//     stalls the tail on an error and retries; it halts the shard on false.
	//
	// Reporting a seqno above what actually committed is the one failure that
	// loses data: the layer would trim entries the store never received and
	// replay from above them. Under an ambiguous write, answer with the error
	// or with what is durably recorded — never with what was probably written.
	Watermark(ctx context.Context, shard wal.ShardID) (wal.Seqno, bool, error)
}
