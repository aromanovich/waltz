package waltest

// Only [wal.Log.Trim] may remove acked entries. A retention window, a table
// TTL or a compaction that drops old records breaks that silently, and the
// suite runs too fast to notice. The lost entries are acked data the cold
// store does not yet hold.
//
// [CheckRetention] is the check a deployment runs for this, in wall-clock
// time; [Expiring] is the backend it is proved against.

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/aromanovich/waltz/wal"
)

// retentionEntries is how many entries the check writes. More than one, so a
// policy that keeps the last record (as many engines do) is still caught.
const retentionEntries = 3

// CheckRetention appends a short run to shard, waits out window, and requires
// every entry still present in order with the same payloads, and the log
// still appendable above them. It returns an error rather than taking a
// *testing.T so a deployment can run it from any harness.
//
// Nothing else may write shard or fence it at epoch. The check fences twice
// at epoch (idempotent), so ownership expiring on the backend's own clock,
// which [wal.Log.Close] allows, does not fail it.
//
// A pass only says entries outlived window. Run it against a deliberately
// shortened policy (say, a two-minute TTL on a staging table, window two
// minutes) to learn whether expiry exists at all.
func CheckRetention(
	ctx context.Context, log wal.Log, shard wal.ShardID, epoch wal.Epoch, window time.Duration,
) error {
	if window <= 0 {
		return fmt.Errorf("waltest: a retention window of %s is not one to wait out", window)
	}
	if err := log.Fence(ctx, shard, epoch); err != nil {
		return fmt.Errorf("waltest: fencing shard %d at epoch %d: %w", shard, epoch, err)
	}

	// Payloads name the check, since they land in a real deployment's log.
	want := make([]wal.Entry, 0, retentionEntries)
	for i := range retentionEntries {
		seqno := wal.FirstSeqno + wal.Seqno(i)
		payload := fmt.Appendf(nil, "retention check, seqno %d", seqno)
		if err := log.Append(ctx, shard, epoch, seqno, payload); err != nil {
			return fmt.Errorf("waltest: appending seqno %d: %w", seqno, err)
		}
		want = append(want, wal.Entry{Seqno: seqno, Epoch: epoch, Payload: payload})
	}
	// Read back first, so a later failure is about time, not the appends.
	if err := requireRun(ctx, log, shard, want, "before the wait", retentionShortfall); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return fmt.Errorf("waltest: waiting out %s: %w", window, ctx.Err())
	case <-time.After(window):
	}

	// Idempotent; restores ownership only if it lapsed.
	if err := log.Fence(ctx, shard, epoch); err != nil {
		return fmt.Errorf("waltest: re-fencing shard %d at epoch %d after %s: %w", shard, epoch, window, err)
	}
	if err := requireRun(ctx, log, shard, want, fmt.Sprintf("after %s", window), retentionShortfall); err != nil {
		return err
	}

	// The next seqno must survive too, or a reset log would take this append.
	next := wal.FirstSeqno + retentionEntries
	if err := log.Append(ctx, shard, epoch, next, []byte("retention check, after the wait")); err != nil {
		return fmt.Errorf("waltest: shard %d no longer appends at seqno %d after %s, so what the log "+
			"continues at did not survive the wait: %w", shard, next, window, err)
	}
	return nil
}

// retentionShortfall explains entries missing after the wait: time-based loss.
const retentionShortfall = "what a completed append acked stays readable until a trim takes it, and " +
	"nothing here trimmed. A retention window, a TTL on the log's table or a compaction that drops " +
	"old records each break that, and each takes acked data the cold store does not hold"

// requireRun reads the shard's whole log and compares it with want. shortfall
// is the caller's diagnosis for missing entries.
func requireRun(
	ctx context.Context, log wal.Log, shard wal.ShardID, want []wal.Entry, when, shortfall string,
) error {
	got, err := readAll(ctx, log, shard, len(want)+1)
	if err != nil {
		return fmt.Errorf("waltest: reading shard %d back %s: %w", shard, when, err)
	}
	if len(got) < len(want) {
		return fmt.Errorf("waltest: shard %d holds %d of its %d entries %s: %s",
			shard, len(got), len(want), when, shortfall)
	}
	for i, w := range want {
		switch g := got[i]; {
		case g.Seqno != w.Seqno:
			return fmt.Errorf("waltest: shard %d's entry %d is at seqno %d and was appended at %d %s",
				shard, i, g.Seqno, w.Seqno, when)
		case !bytes.Equal(g.Payload, w.Payload):
			return fmt.Errorf("waltest: shard %d's entry at seqno %d came back holding other bytes %s",
				shard, w.Seqno, when)
		}
	}
	return nil
}

// Expiring is a [wal.Log] whose entries silently stop being readable once
// older than after, as under a retention window, TTL or compaction.
//
// [CheckRetention] is proved against it. Like [Unfenced] and [Truncating], it
// breaks the contract on purpose and has no other use.
//
// Age is measured from the append on the process clock, so what expires is
// always a prefix.
func Expiring(log wal.Log, after time.Duration) wal.Log {
	return &expiring{log: log, after: after, born: map[bornKey]time.Time{}}
}

type expiring struct {
	log   wal.Log
	after time.Duration

	mu   sync.Mutex
	born map[bornKey]time.Time
}

type bornKey struct {
	shard wal.ShardID
	seqno wal.Seqno
}

var _ wal.Log = (*expiring)(nil)

func (e *expiring) Fence(ctx context.Context, shard wal.ShardID, epoch wal.Epoch) error {
	return e.log.Fence(ctx, shard, epoch)
}

func (e *expiring) Append(
	ctx context.Context, shard wal.ShardID, epoch wal.Epoch, seqno wal.Seqno, payload []byte,
) error {
	if err := e.log.Append(ctx, shard, epoch, seqno, payload); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.born[bornKey{shard, seqno}] = time.Now()
	return nil
}

func (e *expiring) ReadFrom(
	ctx context.Context, shard wal.ShardID, from wal.Seqno, limit int,
) ([]wal.Entry, error) {
	entries, err := e.log.ReadFrom(ctx, shard, from, limit)
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.DeleteFunc(entries, func(entry wal.Entry) bool {
		born, ok := e.born[bornKey{shard, entry.Seqno}]
		return ok && time.Since(born) >= e.after
	}), nil
}

// Trim leaves born alone: trimmed seqnos are never read again.
func (e *expiring) Trim(ctx context.Context, shard wal.ShardID, upTo wal.Seqno) error {
	return e.log.Trim(ctx, shard, upTo)
}

func (e *expiring) Close() { e.log.Close() }
