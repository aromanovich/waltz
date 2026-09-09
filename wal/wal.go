// Package wal is the write-ahead log contract the layer in front of a Temporal
// history shard's cold store is built on.
//
// Everything above it (fold, apply, overlay, replay) depends on these five
// guarantees and on nothing else, so the log underneath can be replaced. See
// [ADR 0002] and [chapter 04]; the vocabulary is [CONTEXT.md].
//
//  1. Total order per shard: the writer, single by virtue of epoch fencing,
//     assigns seqnos itself.
//  2. [Log.Fence] atomically cuts off appends of all lower epochs.
//  3. Cumulative ack: a successful [Log.Append] up to seqno n means every entry
//     ≤ n is durable, so "confirmed ⟺ seqno ≤ commitSeqno" is inherited rather
//     than implemented.
//  4. Gap-freedom: an append never skips a seqno, so a shard's log is one
//     unbroken run. [Log.Trim] moves its lower end and [Log.Append] its upper
//     end; nothing puts a hole in the middle, so replay needs no hole tracking.
//  5. Readback: [Log.ReadFrom] returns every entry a completed append acked and
//     no trim has removed, in seqno order. [Log.Trim] moves the log's lower end
//     and nothing else, so what it leaves stays readable from that new lower
//     end, and the shard's ownership and its next seqno survive it.
//
// A WAL entry's payload is opaque bytes here: no Temporal types in this package
// or in its implementations.
//
// [ADR 0002]: ../docs/adr/0002-wal-contract-is-backend-independent.md
// [ADR 0010]: ../docs/adr/0010-the-log-appends-one-entry-at-a-time.md
// [chapter 04]: ../docs/handbook/04-contracts.md
// [CONTEXT.md]: ../CONTEXT.md
package wal

import (
	"context"
	"errors"
)

// ShardID identifies a Temporal history shard. Every shard has its own log,
// independent of every other shard's.
type ShardID uint32

// Seqno is the position of an entry in one shard's log: a per-shard LSN the
// writer assigns itself. Total order within a shard, no gaps.
type Seqno uint64

// Epoch is the shard-ownership token every append carries, identical to
// Temporal's rangeID (invariant I11). It may grow without an ownership change,
// because the server renews rangeID whenever a shard exhausts its ID range.
//
// Zero is not a valid epoch; see [ErrZeroEpoch].
//
// Whoever hands epochs out owes the log a strictly greater epoch per acquire:
// fencing cannot separate two writers holding the same epoch, and they race for
// seqnos and lose.
type Epoch uint64

// FirstSeqno is the seqno of a shard's first entry. Seqnos below it are not
// entries: they are reserved for whatever bookkeeping a backend needs.
const FirstSeqno Seqno = 1

// Entry is one record of a shard's log.
type Entry struct {
	// Seqno is the entry's position in its shard's log.
	Seqno Seqno
	// Epoch is the epoch the writer held when it appended the entry. Epochs
	// are non-decreasing along a shard's log.
	Epoch Epoch
	// Payload is opaque to this layer, and is never nil for an entry that the
	// log returns.
	//
	// The bytes are the reader's to keep: a payload the log hands out aliases
	// neither the log's own state nor another entry of the same read, so a
	// caller may hold it past the next call and decode into it in place.
	Payload []byte
}

// Errors a caller is expected to handle. Anything else comes back as an
// ordinary error and means a programming mistake or an infrastructure failure,
// both of which the caller can only report.
//
// Match with [errors.Is]; implementations wrap these with context.
var (
	// ErrFenced means the log is not the caller's to write: some other epoch
	// has fenced it, or the caller never fenced it at its own epoch. Shard
	// ownership is gone (or was never taken) and the caller must stop writing.
	ErrFenced = errors.New("wal: shard is not fenced at this epoch")

	// ErrAlreadyWritten means the seqno the append asked for is taken. After an
	// append that failed ambiguously it is the answer to "did it land?": it
	// did, so retrying an append is safe and this error is the retry's success
	// signal.
	//
	// It is that without qualification, because one entry is what an append
	// writes: the seqno is taken or it is not, and there is no half of it for
	// the answer to be about. [ErrFenced] outranks it, so a writer whose epoch
	// grew across the ambiguity must replay under the epoch it holds now, or
	// read the log, to learn whether the first attempt landed.
	ErrAlreadyWritten = errors.New("wal: seqno already written")

	// ErrGap means the append would leave a hole: the entry below it is
	// missing (guarantee 4). It is the expected outcome of a pipelined append
	// that reached the backend out of order; retry once the predecessor lands.
	ErrGap = errors.New("wal: predecessor seqno is missing")

	// ErrZeroEpoch is what a caller that forgot to set an epoch gets, rather
	// than an [ErrFenced] that reads like a lost shard. Epoch 0 is the "nobody
	// owns this" reading of an absent fence, so nothing can be claimed with it.
	ErrZeroEpoch = errors.New("wal: epoch 0 is not a valid epoch")
)

// Log is the WAL contract: one append-only, fenced, gap-free sequence of
// entries per shard. Implementations are called WAL backends.
//
// Every method is safe for concurrent use, which is not a licence for two
// writers: concurrent appends to one shard race for seqnos and lose.
//
// Every method but [Log.Close] takes a context, and all four owe it the same. A
// context already cancelled when the call begins is observed before the log
// changes, so such a call leaves it exactly as it was. Cancellation in flight is
// the case the contract does not resolve: an [Log.Append] cut off between the
// request and its ack may be durable, which is why a cancelled append counts as
// an attempt like any other — a caller that must know replays the same entry and
// reads [ErrAlreadyWritten] as the ack, or reads the log. An error a method returns
// because of its context satisfies [errors.Is] against [context.Canceled] or
// [context.DeadlineExceeded], whatever the backend wraps it in. Arguments the
// contract does not admit outrank the context: a call that is both cancelled
// and malformed reports the argument.
//
// Those arguments are refused and refusing changes nothing: a zero epoch, with
// [ErrZeroEpoch]; and, with ordinary errors, an append below [FirstSeqno] or
// with a nil payload, and a read whose limit is not positive. A backend that
// interprets one instead — answering a limit of zero with no entries and no
// error — hands its caller a loop that never ends or an ack for an entry the
// log does not hold, and both look like the log working. The single out-of-range
// argument that is clamped rather than refused is a read from below
// [FirstSeqno], which is where a caller reading the whole log starts.
//
// None of it has to be re-derived per backend. [CheckFence], [CheckAppend],
// [CheckRead] and [CheckTrim] hold the argument rules, and [FenceRefusal],
// [AppendRefusal] and [RefuseAtNext] turn what a backend found into the error
// this contract names, in the precedence it names it in. wal/memwal is the
// worked example.
type Log interface {
	// Fence claims the shard's log for epoch, atomically cutting off every
	// append of a lower epoch, so a zombie ex-owner cannot slip an append past
	// a completed Fence (invariant I4).
	//
	// It is idempotent per epoch, so a process restart without a change of
	// ownership can replay the same acquire path. Fencing at a higher epoch is
	// how the same owner renews (epoch := rangeID, I11); fencing at a lower one
	// fails with [ErrFenced].
	//
	// A fence changes ownership and nothing else: the entries stay, and the new
	// owner continues the log at the next seqno rather than starting one. A
	// fence that fails changes nothing, ownership included.
	//
	// Appends are refused until the log is fenced at the appending epoch.
	Fence(ctx context.Context, shard ShardID, epoch Epoch) error

	// Append writes payload as the entry at seqno, under epoch. seqno must be
	// at least [FirstSeqno]; the payload may be empty but not nil.
	//
	// One entry is the unit, and there is no batch. An append that carried
	// several entries would have to say what it left behind when only some of
	// them landed, and the three refusals below cannot: each of them says the
	// write is whole one way or the other. Backends whose append is one
	// transaction, one statement or one replicated command could carry a batch
	// and are not asked to: a log with no atomic multi-record append cannot,
	// and a contract only some implementations can keep is not one
	// ([ADR 0010]).
	//
	// The payload stays the caller's: no backend retains the slice or reads it
	// after Append returns, whatever it returns, so an encoder's scratch buffer
	// may be reused as soon as the call does.
	//
	// Returning nil means every entry up to and including seqno is durable
	// (cumulative ack, guarantee 3), so the caller's commitSeqno becomes seqno.
	//
	// Errors:
	//   - [ErrFenced] when the log belongs to another epoch,
	//   - [ErrAlreadyWritten] when the seqno is taken,
	//   - [ErrGap] when the entry below seqno is missing.
	// None of the three writes anything.
	//
	// Where more than one applies, [ErrFenced] wins: [ErrAlreadyWritten] is an
	// ack, and a zombie would take the word of the writer that took the shard
	// from it as its own commitSeqno.
	Append(ctx context.Context, shard ShardID, epoch Epoch, seqno Seqno, payload []byte) error

	// ReadFrom returns up to limit entries of the shard's log with seqno at or
	// above from, in seqno order. Replay after a shard acquire rebuilds the
	// tail with it, starting just above appliedSeqno. limit must be positive.
	//
	// A from below [FirstSeqno] reads from [FirstSeqno]. Fewer than limit
	// entries means the log ends there, so a caller reading the whole log loops
	// until a short read.
	//
	// A read that follows a successful [Log.Fence] sees every entry the log
	// held when the fence took it; a backend that handed the new owner a
	// shorter log would have it append over entries that are already there.
	ReadFrom(ctx context.Context, shard ShardID, from Seqno, limit int) ([]Entry, error)

	// Trim deletes the shard's entries at or below upTo. The apply cycle calls
	// it lazily for entries already folded into the cold store, because a small
	// log is what keeps a backend's reads cheap.
	//
	// Trimming entries that are not there is not an error: Trim states where
	// the log should start, and repeating it is harmless.
	//
	// Whatever upTo says, the log stays appendable at the next seqno and
	// ownership stays put. A backend may keep entries it needs to promise that
	// — one that checks an append for gap-freedom against the stored entry
	// below it has to keep the last one — so a trim past the tail may leave the
	// tail behind.
	//
	// The seqnos it removed stay spent: an append at one is refused and writes
	// nothing, with [ErrAlreadyWritten] or [ErrGap] as the backend keeps its
	// position — the contract picks neither, since one that derives the answer
	// from its rows has deleted them. A backend handing a trimmed seqno out
	// again would put a hole in the log and ack a commitSeqno below entries it
	// still holds.
	Trim(ctx context.Context, shard ShardID, upTo Seqno) error

	// Close releases what the backend holds around the log: a connection, a
	// lease, the goroutine some backends keep ownership alive from. Call it
	// once, after the last append and after any drain — the entries stay, and
	// whoever fenced a shard owns it until that ownership expires or a
	// successor takes it.
	//
	// Most backends hold nothing and do nothing here. It is on the contract
	// rather than reached for with a type assertion so that a composition
	// cannot hold a backend it never learned to release: a backend that keeps
	// its claim alive from a goroutine of its own — a lease renewal, a
	// keepalive on the transaction its appends run under — leaves a process
	// that never closes it owning shards it has stopped writing to.
	Close()
}
