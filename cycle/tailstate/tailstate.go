// Package tailstate is the only place the tail is counted: unsettled acked
// entries and their bytes (what I10 bounds), and the cold store's watermark lag.
//
// [Tail] is owned by the cycle's loop goroutine; [Mirror] carries the same
// counts and the stall seqno to other goroutines. Every [Tail] mutator ends by
// publishing to the mirror and the metric, and being a separate package makes
// a counter write outside a mutator a compile error. It must not name wal.Log
// or wal.Entry.
package tailstate

import (
	"sync/atomic"

	"github.com/aromanovich/waltz/cycle/window"
	"github.com/aromanovich/waltz/wal"
	"github.com/aromanovich/waltz/walmetrics"
)

// Tail is what a cycle has acked and not yet settled, in entries and bytes,
// plus the positions they are measured between.
type Tail struct {
	// commit is the last acked seqno; applied is the cold store's watermark,
	// moved only by a committed drain.
	commit  wal.Seqno
	applied wal.Seqno

	// resolved is applied plus anything above it a [KeepWatermark] settle
	// released. I10 does not count those entries, but applied must not move over
	// them: a trim goes to applied, and would strand a recovering owner.
	resolved wal.Seqno

	// bytes is I10's byte counter: every acked payload not yet settled. It is not
	// the window's bytes: the window empties when a drain starts, so an
	// unreadable outcome leaves its entries counted here.
	bytes int

	// stalled is the seqno of a drain whose outcome could not be read (zero for
	// none), with the bytes it acked and the error it got. While it stands no
	// position moves: a later commit would write the watermark over the
	// unresolved entries and a trim would delete them. All three end together,
	// at [Tail.Resolve] or [Tail.Floor]. The cause is carried, never interpreted.
	stalled      wal.Seqno
	stalledBytes int
	stalledBy    error

	// mirror must not be nil; a Tail built without [New] panics on its first move.
	mirror *Mirror
}

// New returns a tail that publishes to m.
func New(m *Mirror) Tail { return Tail{mirror: m} }

// Commit and Applied are the last acked seqno and the cold store's watermark.
func (t *Tail) Commit() wal.Seqno  { return t.commit }
func (t *Tail) Applied() wal.Seqno { return t.applied }

// Bytes is I10's byte counter.
func (t *Tail) Bytes() int { return t.bytes }

// Entries is the tail's size: entries between resolved and commit, acked with
// their fate open. It is not commit − applied; the two differ once a
// [KeepWatermark] settle has run.
func (t *Tail) Entries() int { return int(t.commit - t.resolved) }

// Empty reports whether the tail holds anything, on the loop;
// [Mirror.Empty] is for callers with no loop left to ask.
func (t *Tail) Empty() bool { return t.commit == t.resolved }

// Size is the pair I10 bounds.
func (t *Tail) Size() (entries, bytes int64) { return int64(t.Entries()), int64(t.bytes) }

// Floor resets every number to the cold store's watermark mark, clearing the
// stall. It is safe to run again: a failed replay is retried from a re-read
// watermark and re-acks the same entries, so bytes left behind would be counted
// twice and released once.
func (t *Tail) Floor(mark wal.Seqno) {
	t.applied, t.resolved, t.commit = mark, mark, mark
	t.bytes = 0
	t.clearStall()
	t.publish()
}

// Ack adds an entry already durable at seqno. size is the payload's encoded
// length, the unit both the window's byte trigger and I10 count.
func (t *Tail) Ack(seqno wal.Seqno, size int) {
	t.commit = seqno
	t.bytes += size
	t.publish()
}

// WatermarkMove says whether a settle moves applied. Neither is a default, so
// the caller states which.
type WatermarkMove int

const (
	// KeepWatermark settles entries no transaction wrote: a drain whose batch
	// was empty, or an entry decided without one. They leave the tail but
	// applied stays, since a trim past what was written strands a recovering
	// owner.
	KeepWatermark WatermarkMove = iota
	// MoveWatermark settles a window whose transaction committed, the only
	// outcome that puts rows in the cold store.
	MoveWatermark
)

// Settle removes a window from the tail: entries up to seqno are settled and
// its bytes released. It runs once per resolving outcome: a committed drain, an
// empty batch, an answered condition failure, a dropped provisional entry.
// held comes from [window.Window.Take] and is consumed, so the tail releases
// only bytes a window handed over, and never twice.
//
// A stalled tail settles nothing, not even these bytes, so entries and bytes
// never disagree. The caller checks [Tail.Stalled] first.
func (t *Tail) Settle(seqno wal.Seqno, held *window.Taken, mark WatermarkMove) {
	if t.stalled != 0 {
		return
	}
	t.resolved = seqno
	t.bytes -= held.Release()
	if mark == MoveWatermark {
		t.applied = seqno
	}
	t.publish()
}

// Stall records a drain whose transaction outcome could not be read: its
// entries are acked and whether the cold store holds them is unknown. held is
// consumed but its bytes stay counted until [Tail.Resolve] or [Tail.Floor].
// cause is what the drain was told, kept for whoever attributes the halt.
//
// Only the cold store's watermark can end it, so drains must re-read it while
// [Tail.Stalled] reports one. The tail is never empty during a stall. There is
// at most one: a caller may not drain until the standing stall is resolved.
func (t *Tail) Stall(seqno wal.Seqno, held *window.Taken, cause error) {
	t.stalled, t.stalledBytes, t.stalledBy = seqno, held.Release(), cause
	t.publish()
}

// Unresolved is a drain whose outcome could not be read: its seqno and the
// error it got, kept as one value so they cannot be mismatched.
type Unresolved struct {
	Seqno wal.Seqno
	Cause error
}

// Stalled returns the stalled drain, and whether there is one. A stall always
// has a non-zero seqno.
func (t *Tail) Stalled() (Unresolved, bool) {
	return Unresolved{Seqno: t.stalled, Cause: t.stalledBy}, t.stalled != 0
}

// Resolve ends a stall whose drain committed after all: its entries settle, its
// bytes are released and applied moves to its seqno. Call it only when the
// watermark equals the stalled seqno ([cycle.Cycle.resolve] decides); one above
// is another owner's and must halt instead, or a trim deletes entries this
// shard never committed. The zero check stays although
// [cycle.Cycle.resolveStalled] checks first: without it, applied and resolved
// would drop to zero and I10 would refuse every write.
func (t *Tail) Resolve() {
	if t.stalled == 0 {
		return
	}
	t.resolved, t.applied = t.stalled, t.stalled
	t.bytes -= t.stalledBytes
	t.clearStall()
	t.publish()
}

// clearStall clears the three stall fields together.
func (t *Tail) clearStall() { t.stalled, t.stalledBytes, t.stalledBy = 0, 0, nil }

// publish ends every mutator, updating the mirror and the metric.
func (t *Tail) publish() {
	t.mirror.store(t.Entries(), t.bytes, int(t.commit-t.applied), t.stalled)
}

// Mirror is the tail as seen by goroutines other than the loop, read without
// asking the loop. It also holds the metrics emitter.
type Mirror struct {
	entries atomic.Int64
	bytes   atomic.Int64
	// stalled is the [Tail]'s stall seqno, zero for none, so a writer is refused
	// before it queues behind a stuck applier.
	stalled atomic.Uint64
	metrics *walmetrics.Emitter
}

// NewMirror returns a mirror that emits to metrics. It holds atomics and must
// not be copied.
func NewMirror(metrics *walmetrics.Emitter) *Mirror { return &Mirror{metrics: metrics} }

// store is the mirror's only writer; only [Tail.publish] calls it.
func (m *Mirror) store(entries, bytes, unapplied int, stalled wal.Seqno) {
	m.entries.Store(int64(entries))
	m.bytes.Store(int64(bytes))
	m.stalled.Store(uint64(stalled))
	m.metrics.Tail(entries, bytes, unapplied)
}

// Size is the I10 reading taken before a write is queued, so a shard with a
// stuck applier refuses writers rather than parking them.
func (m *Mirror) Size() (entries, bytes int64) { return m.entries.Load(), m.bytes.Load() }

// StalledAt is [Tail.Stalled]'s seqno for the same pre-queue reader, zero for
// none. It omits the cause, which belongs to the halt the loop may reach.
func (m *Mirror) StalledAt() wal.Seqno { return wal.Seqno(m.stalled.Load()) }

// Empty is [Tail.Empty] for a cycle with no loop left. It may be stale by a
// successor cycle's writes, so only the two mutable-state reads and the
// history branch page may use it.
func (m *Mirror) Empty() bool { return m.entries.Load() == 0 }
