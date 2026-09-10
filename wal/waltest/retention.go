package waltest

// The one obligation [RunContractSuite] cannot express, because what it is
// about is time.
//
// [wal.Log.Trim] is the only removal the contract excuses: what a completed
// append acked stays readable until a trim takes it, and a retention window, a
// TTL on the table, or a compaction that drops old records each break that —
// each silently. The suite runs in milliseconds and cannot age an entry, so a
// backend whose storage expires rows passes all nineteen cases and loses the
// first tail that outlives its policy. That tail is acked data with no second
// copy: the cold store does not hold it, which is the whole reason it is in the
// log.
//
// So it is a check the deployment runs rather than a case in the suite, and it
// takes wall-clock time to run. [CheckRetention] is that check; [Expiring] is
// the backend it is proved against.

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
// policy that keeps the last record — which several storage engines do, and
// which the contract itself lets a backend do past a trim — is still caught by
// the ones below it.
const retentionEntries = 3

// CheckRetention appends a short run to shard, waits out window, and requires
// every entry to still be there: same seqnos, same payloads, same order, and the
// log still appendable above them. It is [RunContractSuite]'s missing case, and
// it is a function returning an error rather than a case in the suite for two
// reasons — it costs window in wall-clock time, and a deployment runs it from
// whatever harness it has rather than only from `go test`.
//
// shard must be one nothing else writes, and epoch one nothing else has fenced
// it at; the check fences twice at that same epoch, which the contract makes
// idempotent, so a backend whose ownership expires on its own clock is not
// failed for it. That is deliberate: [wal.Log.Close] admits ownership expiring,
// and what this is about is the entries.
//
// **What it establishes and what it does not.** A pass says the entries outlived
// window on this deployment's storage. It does not say the backend has no
// retention — a policy longer than window is a policy this run did not reach.
// So run it against a deliberately shortened policy: set the log's table to
// expire in two minutes on a staging cluster and pass two minutes. What is being
// looked for is whether expiry exists as a mechanism at all, and its length is
// the thing least worth trusting — a policy nobody applied to this table today
// is one somebody applies to it next quarter.
func CheckRetention(
	ctx context.Context, log wal.Log, shard wal.ShardID, epoch wal.Epoch, window time.Duration,
) error {
	if window <= 0 {
		return fmt.Errorf("waltest: a retention window of %s is not one to wait out", window)
	}
	if err := log.Fence(ctx, shard, epoch); err != nil {
		return fmt.Errorf("waltest: fencing shard %d at epoch %d: %w", shard, epoch, err)
	}

	// The payloads say what wrote them rather than reusing the suite's
	// payloadFor: these are rows somebody reads out of a real deployment's log
	// table while wondering what put them there, which the suite's never are.
	want := make([]wal.Entry, 0, retentionEntries)
	for i := range retentionEntries {
		seqno := wal.FirstSeqno + wal.Seqno(i)
		payload := fmt.Appendf(nil, "retention check, seqno %d", seqno)
		if err := log.Append(ctx, shard, epoch, seqno, payload); err != nil {
			return fmt.Errorf("waltest: appending seqno %d: %w", seqno, err)
		}
		want = append(want, wal.Entry{Seqno: seqno, Epoch: epoch, Payload: payload})
	}
	// Before the wait, so that a failure after it is about time and not about
	// an append that never landed.
	if err := requireRun(ctx, log, shard, want, "before the wait"); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return fmt.Errorf("waltest: waiting out %s: %w", window, ctx.Err())
	case <-time.After(window):
	}

	// Idempotent at the same epoch, so this only puts back an ownership the
	// backend may have let lapse on its own clock.
	if err := log.Fence(ctx, shard, epoch); err != nil {
		return fmt.Errorf("waltest: re-fencing shard %d at epoch %d after %s: %w", shard, epoch, window, err)
	}
	if err := requireRun(ctx, log, shard, want, fmt.Sprintf("after %s", window)); err != nil {
		return err
	}

	// The log's upper end survived too: a backend that dropped the run and reset
	// where it continues would take this seqno as a fresh one.
	next := wal.FirstSeqno + retentionEntries
	if err := log.Append(ctx, shard, epoch, next, []byte("retention check, after the wait")); err != nil {
		return fmt.Errorf("waltest: shard %d no longer appends at seqno %d after %s, so what the log "+
			"continues at did not survive the wait: %w", shard, next, window, err)
	}
	return nil
}

// requireRun reads the shard's whole log and holds it against want.
func requireRun(ctx context.Context, log wal.Log, shard wal.ShardID, want []wal.Entry, when string) error {
	got, err := readAll(ctx, log, shard, len(want)+1)
	if err != nil {
		return fmt.Errorf("waltest: reading shard %d back %s: %w", shard, when, err)
	}
	if len(got) < len(want) {
		return fmt.Errorf("waltest: shard %d holds %d of its %d entries %s: what a completed append "+
			"acked stays readable until a trim takes it, and nothing here trimmed. A retention "+
			"window, a TTL on the log's table or a compaction that drops old records each break "+
			"that, and each takes acked data the cold store does not hold",
			shard, len(got), len(want), when)
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

// Expiring is a [wal.Log] whose entries stop being readable once they are older
// than after, which is the shape a retention window, a TTL on a table and a
// compaction that drops old records all have from above: the log answers reads
// with less than it acked, and says nothing about it.
//
// It is what [CheckRetention] is proved against, and it is the one decorator
// here that is *not* a log a backend may be — [Faulty] refuses calls, which is
// something a correct backend does, while this one breaks the readback
// guarantee. A caller has no other use for it.
//
// Ages are measured from the append with the process's own clock, and the whole
// of what expires is a prefix, since a log is appended in order.
func Expiring(log wal.Log, after time.Duration) wal.Log {
	return &expiring{log: log, after: after, born: map[bornKey]time.Time{}}
}

type expiring struct {
	log   wal.Log
	after time.Duration

	mu sync.Mutex
	// born is keyed by both halves at once: nothing here walks one shard's
	// entries, so a map per shard would buy a nil check and nothing else.
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

// Trim keeps no bookkeeping of its own: a seqno a trim removed stays spent, so
// its birth time is never consulted again whether or not it is still here.
func (e *expiring) Trim(ctx context.Context, shard wal.ShardID, upTo wal.Seqno) error {
	return e.log.Trim(ctx, shard, upTo)
}

func (e *expiring) Close() { e.log.Close() }
