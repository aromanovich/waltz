// Package wal is the write-ahead log contract in front of a Temporal history
// shard's cold store. The layer depends on these five guarantees and nothing
// else, so the log is replaceable ([ADR 0002], [chapter 04], [CONTEXT.md]).
//
//  1. Total order per shard: the single writer (fencing keeps it single)
//     assigns seqnos.
//  2. [Log.Fence] atomically cuts off appends of all lower epochs.
//  3. Cumulative ack: a successful [Log.Append] at seqno n means every entry
//     ≤ n is durable in storage, not just in the process that acked. Same for
//     the fenced epoch. The suite cannot see this; waltest.CheckReopen can.
//  4. Gap-freedom: an append never skips a seqno, so the log is one unbroken
//     run between its trimmed lower end and its appended upper end.
//  5. Readback: [Log.ReadFrom] returns every acked entry no trim removed, in
//     seqno order. Trim is the only removal allowed: retention, TTLs and
//     compaction that drop entries violate this silently. The suite cannot
//     see this either; waltest.CheckRetention can.
//
// Payloads are opaque bytes: no Temporal types here or in implementations.
//
// [ADR 0002]: ../docs/adr/0002-wal-contract-is-backend-independent.md
// [chapter 04]: ../docs/handbook/04-contracts.md
// [CONTEXT.md]: ../CONTEXT.md
package wal

import (
	"context"
	"errors"
)

// ShardID identifies a Temporal history shard. Each shard has its own log.
type ShardID uint32

// Seqno is an entry's position in its shard's log, assigned by the writer.
type Seqno uint64

// Epoch is the shard-ownership token every append carries: Temporal's rangeID
// (I11), which can also grow without an ownership change. Zero is invalid
// ([ErrZeroEpoch]). Each acquire must use a strictly greater epoch: fencing
// cannot separate two writers holding the same one.
type Epoch uint64

// FirstSeqno is the seqno of a shard's first entry. Lower seqnos are reserved
// for backend bookkeeping.
const FirstSeqno Seqno = 1

// Entry is one record of a shard's log.
type Entry struct {
	Seqno Seqno
	// Epoch is the writer's epoch at append time; non-decreasing along the log.
	Epoch Epoch
	// Payload is never nil in a returned entry. It belongs to the reader: it
	// aliases neither the log's state nor another entry, so the caller may keep
	// it and decode into it in place.
	Payload []byte
}

// Errors a caller is expected to handle; match with [errors.Is], since
// implementations wrap them. Any other error is a bug or an infrastructure
// failure, which the caller can only report.
var (
	// ErrFenced means another epoch fenced the log, or the caller never fenced
	// at its own. The caller must stop writing.
	ErrFenced = errors.New("wal: shard is not fenced at this epoch")

	// ErrAlreadyWritten means the seqno is taken. After an ambiguous failure,
	// a retry that gets this knows the first attempt landed: it is the retry's
	// success signal. [ErrFenced] outranks it, so a writer whose epoch changed
	// in between must retry at its current epoch or read the log to find out.
	ErrAlreadyWritten = errors.New("wal: seqno already written")

	// ErrGap means the entry below seqno is missing (guarantee 4), e.g. a
	// pipelined append arrived out of order; retry once the predecessor lands.
	ErrGap = errors.New("wal: predecessor seqno is missing")

	// ErrZeroEpoch is returned for epoch 0, which means "nobody owns this", so
	// a caller that forgot to set an epoch is not told its shard was lost.
	ErrZeroEpoch = errors.New("wal: epoch 0 is not a valid epoch")
)

// Log is the WAL contract: one append-only, fenced, gap-free sequence of
// entries per shard, implemented by a WAL backend. Methods are safe for
// concurrent use, but each shard must have one writer.
//
// A call whose context is already done changes nothing. A cancel in flight may
// leave an [Log.Append] durable, so treat it as ambiguous: retry and read
// [ErrAlreadyWritten] as the ack, or read the log. Context errors match
// [context.Canceled] or [context.DeadlineExceeded] via [errors.Is].
//
// Invalid arguments are refused before the context and change nothing: epoch 0
// ([ErrZeroEpoch]); with ordinary errors, an append below [FirstSeqno] or with
// a nil payload, and a read limit ≤ 0. Never reinterpret them (limit 0 as an
// empty read gives the caller an endless loop). Only a read from below
// [FirstSeqno] is clamped.
//
// [CheckFence], [CheckAppend], [CheckRead], [CheckTrim], [FenceRefusal],
// [AppendRefusal] and [RefuseAtNext] implement these rules and the error
// precedence; wal/memwal is the worked example.
type Log interface {
	// Fence claims the shard's log for epoch, atomically cutting off appends
	// of every lower epoch so a zombie ex-owner cannot append after it (I4).
	// Appends are refused until the log is fenced at their epoch. Idempotent
	// per epoch, so a restart can replay the acquire; a higher epoch is also
	// how an owner renews (I11); a lower one fails with [ErrFenced]. Entries
	// stay and the new owner continues at the next seqno. A failed Fence
	// changes nothing.
	Fence(ctx context.Context, shard ShardID, epoch Epoch) error

	// Append writes payload as the entry at seqno, under epoch. seqno must be
	// at least [FirstSeqno]; the payload may be empty but not nil. One entry
	// per call, no batch: a partial batch would have no honest error
	// ([ADR 0010]). The backend must not retain or read payload after Append
	// returns, so the caller may reuse the buffer.
	//
	// nil means every entry ≤ seqno is durable (guarantee 3), so commitSeqno
	// becomes seqno. Errors, none of which write anything:
	//   - [ErrFenced] when the log belongs to another epoch,
	//   - [ErrAlreadyWritten] when the seqno is taken,
	//   - [ErrGap] when the entry below seqno is missing.
	// [ErrFenced] wins when several apply: [ErrAlreadyWritten] is an ack, and
	// a zombie must not take the new owner's entry as its own.
	//
	// [ADR 0010]: ../docs/adr/0010-the-log-appends-one-entry-at-a-time.md
	Append(ctx context.Context, shard ShardID, epoch Epoch, seqno Seqno, payload []byte) error

	// ReadFrom returns up to limit (> 0) entries with seqno ≥ from, in seqno
	// order; from below [FirstSeqno] reads from [FirstSeqno]. A short read
	// means the log ends there. Replay reads from just above appliedSeqno.
	// A read after a successful [Log.Fence] must see every entry held at the
	// fence, or the new owner would append over existing entries.
	ReadFrom(ctx context.Context, shard ShardID, from Seqno, limit int) ([]Entry, error)

	// Trim deletes the shard's entries at or below upTo; the apply cycle calls
	// it for entries already in the cold store, to keep reads cheap.
	//
	// Caller's obligation: upTo must never exceed the seqno the cold store has
	// committed. Above it the log is the only copy, and Trim cannot be undone.
	// No backend can check this: the log knows no cold store, and Trim takes
	// no epoch. cycle.Cycle keeps it by passing tailstate.Tail.Applied, which
	// moves only after a committed drain.
	//
	// Trimming absent entries is not an error. Ownership is unchanged and the
	// log stays appendable at the next seqno; a backend may keep entries it
	// needs for that (e.g. the last one, to check gap-freedom). Trimmed seqnos
	// stay spent: an append at one is refused with [ErrAlreadyWritten] or
	// [ErrGap] (backend's choice), since reuse would put a hole in the log.
	Trim(ctx context.Context, shard ShardID, upTo Seqno) error

	// Close releases what the backend holds (a connection, a lease, a
	// keepalive goroutine); without it a backend can keep owning shards it no
	// longer writes. Call it once, after the last append and any drain.
	// Entries stay, and ownership lasts until it expires or a successor fences.
	Close()
}

// PressureLevel is how urgently a backend wants storage back. Each level
// includes what the lower ones ask.
type PressureLevel int

const (
	// PressureNone means nothing reported; not a health promise.
	PressureNone PressureLevel = iota
	// PressureDrain asks the layer to drain and trim now, off-cadence.
	PressureDrain
	// PressureStop also asks it to stop appending until the level drops.
	// Acked entries stay acked; new writes are refused.
	PressureStop
)

// PressureSource is optionally implemented by a backend whose storage can run
// low while appends still succeed. Such an append is durable and must not be
// failed (that would deny an entry the log holds); the warning goes here.
// Pressure is a level, not an event: the backend keeps it current and lowers
// it itself. The layer polls it around every write and on its age tick, so it
// must be cheap and safe for concurrent use.
type PressureSource interface {
	Pressure(shard ShardID) PressureLevel
}
