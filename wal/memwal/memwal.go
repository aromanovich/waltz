// Package memwal implements the [wal] contract ([ADR 0002]) in process memory.
// It is the only log shipped here, and a real backend, not a test double: it
// passes the conformance suite in [waltest].
//
// Each shard stores its owning epoch, the entries it still holds, and next,
// the seqno the next append must use. Because next is kept explicitly, a trim
// may remove every entry and the log still knows where to continue. A
// row-based backend instead infers this from neighbouring rows, so the two
// answer an append below a trimmed prefix differently: [wal.ErrAlreadyWritten]
// here, [wal.ErrGap] there. Both refuse and write nothing, which is all the
// contract requires.
//
// Payloads are copied in and out, so a caller reusing an append buffer cannot
// rewrite the log.
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

// Backend holds every shard's log in this process's memory. Two Backends
// share nothing.
type Backend struct {
	// One mutex for all shards: nothing here is slow enough to need more.
	mu     sync.Mutex
	shards map[wal.ShardID]*shardLog
}

var _ wal.Log = (*Backend)(nil)

// Close releases nothing; dropping the Backend releases the log.
func (b *Backend) Close() {}

// New returns an empty Backend.
func New() *Backend {
	return &Backend{shards: make(map[wal.ShardID]*shardLog)}
}

// shardLog is one shard's log, stored by pointer so appends modify the map's
// copy.
type shardLog struct {
	// epoch owns the log; zero means unowned.
	epoch wal.Epoch
	// entries are ascending and contiguous, starting after the last trim.
	entries []wal.Entry
	// next is the seqno the next append must use. It survives a trim of every
	// entry and never decreases.
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
	// Entries are contiguous, so the start is computed, not searched. The
	// offset is bounds-checked before use as an index, since from is any uint64.
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
	// Copy rather than re-slice, or the backing array keeps dropped entries
	// and their payloads alive and trimming never frees memory.
	log.entries = slices.Clone(log.entries[drop:])
	return nil
}

// logFor returns the shard's log, creating an empty unowned one if needed (a
// refused append leaves it behind). Reads and trims do not call it, so they
// create nothing. Callers hold b.mu.
func (b *Backend) logFor(shard wal.ShardID) *shardLog {
	log := b.shards[shard]
	if log == nil {
		log = &shardLog{next: wal.FirstSeqno}
		b.shards[shard] = log
	}
	return log
}

// clonePayload copies a payload. An empty payload stays non-nil, as the
// contract requires.
func clonePayload(payload []byte) []byte {
	clone := make([]byte, len(payload))
	copy(clone, payload)
	return clone
}
