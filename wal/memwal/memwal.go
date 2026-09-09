// Package memwal is the [wal] contract ([ADR 0002]) in process memory, and the
// only log shipped here. It is a backend and not a test double, and passing the
// conformance suite in [waltest] is what says so.
//
// A map of shards under one mutex, and per shard: the epoch that owns the log,
// the entries it still holds, and the seqno the next append must start at. That
// last one is the design. A backend that keeps its entries as rows derives an
// append's verdict from the rows around it and so has to keep the log's last
// entry through a trim, plus a marker for the one seqno no row can answer for;
// here the tail is state, so a trim may take everything and the log still knows
// where its next entry goes and that the seqnos below it are spent. The two
// shapes therefore differ below a trimmed prefix, where appending is
// [wal.ErrAlreadyWritten] here and [wal.ErrGap] there. Both refuse and write
// nothing — which is the contract's and asserted by the suite — while the
// contract picks neither answer, and no correct caller gets there.
//
// Payloads are copied in and out: this is the only backend that could hand out
// the caller's own memory, where a caller reusing an append buffer would
// silently rewrite the log.
//
// [ADR 0002]: ../../docs/adr/0002-wal-contract-is-backend-independent.md
// [waltest]: ../waltest
package memwal

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/aromanovich/waltz/wal"
)

// Backend holds every shard's log in this process's memory. Each one is a log
// of its own: two Backends share nothing, and a shard exists in exactly the one
// it was fenced in.
type Backend struct {
	// One mutex for every shard, not one per shard: nothing here is slow
	// enough for the lock to be visible to anything above it.
	mu     sync.Mutex
	shards map[wal.ShardID]*shardLog
}

var _ wal.Log = (*Backend)(nil)

// Close releases nothing. The log is this value, and a caller that drops it has
// released it.
func (b *Backend) Close() {}

// New returns an empty Backend: no shards, and so no log that is anybody's.
func New() *Backend {
	return &Backend{shards: make(map[wal.ShardID]*shardLog)}
}

// shardLog is one shard's log. Held by pointer so that a shard fetched from the
// map is the shard, and not a copy of it that an append would write into.
type shardLog struct {
	// epoch is who owns the log. Zero means nobody does.
	epoch wal.Epoch
	// entries are the entries the log still holds: ascending, contiguous, and
	// starting wherever the last trim left them.
	entries []wal.Entry
	// next is the seqno the next append must start at. It survives a trim that
	// takes every entry, and only ever goes up.
	next wal.Seqno
}

func (b *Backend) Fence(ctx context.Context, shard wal.ShardID, epoch wal.Epoch) error {
	if err := wal.CheckFence(shard, epoch); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("fencing shard %d at epoch %d: %w", shard, epoch, err)
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	log := b.logFor(shard)

	if err := wal.FenceRefusal(shard, log.epoch, epoch); err != nil {
		return err
	}
	log.epoch = epoch
	return nil
}

func (b *Backend) Append(
	ctx context.Context, shard wal.ShardID, epoch wal.Epoch, seqno wal.Seqno, payload []byte,
) error {
	if err := wal.CheckAppend(shard, epoch, seqno, payload); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("appending seqno %d to shard %d: %w", seqno, shard, err)
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	log := b.logFor(shard)

	// next is the whole of what this backend knows, and the contract derives
	// both answers from it.
	if err := wal.RefuseAtNext(shard, epoch, seqno, log.epoch, log.next); err != nil {
		return err
	}

	log.entries = append(log.entries, wal.Entry{
		Seqno:   seqno,
		Epoch:   epoch,
		Payload: clonePayload(payload),
	})
	log.next = seqno + 1
	return nil
}

func (b *Backend) ReadFrom(
	ctx context.Context, shard wal.ShardID, from wal.Seqno, limit int,
) ([]wal.Entry, error) {
	from, err := wal.CheckRead(shard, from, limit)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("reading shard %d from %d: %w", shard, from, err)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	log := b.shards[shard]
	if log == nil || len(log.entries) == 0 {
		return nil, nil
	}
	// The entries are contiguous, so the window start is arithmetic rather than
	// a search. from is an arbitrary uint64, so the offset is compared before
	// it is narrowed to an index.
	base := log.entries[0].Seqno
	count := wal.Seqno(len(log.entries))
	offset := wal.Seqno(0)
	if from > base {
		offset = from - base
	}
	if offset >= count {
		return nil, nil
	}
	window := log.entries[offset:min(offset+wal.Seqno(limit), count)]

	entries := make([]wal.Entry, 0, len(window))
	for _, entry := range window {
		entry.Payload = clonePayload(entry.Payload)
		entries = append(entries, entry)
	}
	return entries, nil
}

func (b *Backend) Trim(ctx context.Context, shard wal.ShardID, upTo wal.Seqno) error {
	if !wal.CheckTrim(upTo) {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("trimming shard %d up to %d: %w", shard, upTo, err)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	log := b.shards[shard]
	if log == nil || len(log.entries) == 0 {
		// Trimming entries that are not there is not an error.
		return nil
	}
	base := log.entries[0].Seqno
	if upTo < base {
		return nil
	}
	drop := upTo - base + 1
	if drop >= wal.Seqno(len(log.entries)) {
		log.entries = nil
		return nil
	}
	// Copied rather than re-sliced: the dropped entries would stay reachable
	// through the backing array, payloads and all, and a continuously trimmed
	// log would never give a byte back. The tail's memory budget is measured
	// against this backend.
	log.entries = slices.Clone(log.entries[drop:])
	return nil
}

// logFor returns the shard's log, creating an empty unowned one if the shard is
// new — an append that is then refused leaves one behind. A read or a trim of a
// shard nobody fenced answers "nothing" rather than bringing one into being.
//
// Callers hold b.mu.
func (b *Backend) logFor(shard wal.ShardID) *shardLog {
	log := b.shards[shard]
	if log == nil {
		log = &shardLog{next: wal.FirstSeqno}
		b.shards[shard] = log
	}
	return log
}

// clonePayload gives the log a copy of the caller's bytes, or the caller a copy
// of the log's. An empty payload stays empty rather than becoming nil: the
// contract has an entry's payload never nil.
func clonePayload(payload []byte) []byte {
	clone := make([]byte, len(payload))
	copy(clone, payload)
	return clone
}
