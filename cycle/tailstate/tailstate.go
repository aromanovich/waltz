// Package tailstate is the tail's arithmetic, in one place: everything I10
// bounds — unsettled acked entries, the bytes they hold, how far behind the
// cold store's watermark is — is counted here and nowhere else.
//
// The two halves span two goroutines. [Tail] is loop-owned, like the rest of
// the cycle's state; [Mirror] is those counts and the stall's seqno for
// goroutines that are not the loop: the bound a write is refused on before it
// queues, and the read a cycle whose loop is gone still has to answer. Every
// mutator lives on [Tail] and ends in publish, so moving the tail is publishing
// it, metric included. It is a package so that writing a counter outside a
// mutator does not compile.
//
// It counts seqnos and may not name wal.Log or wal.Entry: the log those seqnos
// index is what a counter grows reach into first.
package tailstate

import (
	"sync/atomic"

	"github.com/aromanovich/waltz/cycle/window"
	"github.com/aromanovich/waltz/wal"
	"github.com/aromanovich/waltz/walmetrics"
)

// Tail is what a cycle has acked and not yet settled, in the two units I10
// bounds plus the two positions those units are measured between.
type Tail struct {
	// commit is the last acked seqno and applied is the last one a committed
	// drain moved the cold store's watermark to.
	commit  wal.Seqno
	applied wal.Seqno

	// resolved is the highest seqno whose fate is settled: applied, plus
	// anything above it a [KeepWatermark] settle released. Those entries are
	// acked and dead, so counting them would have I10 bound memory nobody
	// holds, while moving applied with them would strand a recovering owner
	// (a trim goes to applied).
	resolved wal.Seqno

	// bytes is I10's byte counter: every acked payload whose fate is unsettled,
	// which is not the window's bytes. A drain empties the window before the
	// transaction is sent, so an unreadable outcome leaves entries here.
	bytes int

	// stalled is the seqno of a drain whose outcome could not be read, zero
	// where there is none; stalledBytes is what that drain acked and stalledBy
	// what it was told. It is a floor: while it stands no position moves, so
	// nothing this cycle does afterwards can claim those entries are anywhere.
	// A commit over them would — the cold store's watermark is written by
	// whichever drain commits rather than compared, so the store would claim the
	// unresolved entries too, and a trim goes to applied.
	//
	// The bytes are held here rather than by the window token that acked them:
	// that token is consumed at the stall, since the settle it was waiting for
	// is the one that will never come. The cause rides with them because all
	// three end together — at [Tail.Resolve], or at the floor a successor plants
	// — and a cause outliving its stall is an attribution for the wrong drain.
	// It is carried, never read: this package does not interpret errors.
	stalled      wal.Seqno
	stalledBytes int
	stalledBy    error

	// mirror is the half other goroutines read, and never nil: a tail that
	// publishes nowhere is the divergence this type prevents, so one built
	// without it panics on its first move. [New] is the way to one.
	mirror *Mirror
}

// New plants a tail on the mirror it publishes to.
func New(m *Mirror) Tail { return Tail{mirror: m} }

// Commit and Applied are the last acked seqno and the cold store's watermark.
// Readers only: moving either goes through a mutator that says which move it is.
func (t *Tail) Commit() wal.Seqno  { return t.commit }
func (t *Tail) Applied() wal.Seqno { return t.applied }

// Bytes is I10's byte counter as it stands; the bound reads it via [Tail.Size].
func (t *Tail) Bytes() int { return t.bytes }

// Entries is the tail's size, and the one spelling of it: an entry between
// resolved and commit is acked with its fate open. Not commit − applied, which
// is the same number only until a [KeepWatermark] settle parts the two.
func (t *Tail) Entries() int { return int(t.commit - t.resolved) }

// Empty asks the loop's own state whether the tail holds anything;
// [Mirror.Empty] answers it for callers with no loop left to ask.
func (t *Tail) Empty() bool { return t.commit == t.resolved }

// Size is the pair I10's bound is stated over, in the units it bounds them in.
func (t *Tail) Size() (entries, bytes int64) { return int64(t.Entries()), int64(t.bytes) }

// Floor plants the tail at the watermark the cold store holds: nothing at or
// below it is this cycle's to account for, and nothing above it has been acked
// by an attempt this tail still counts.
//
// Every number, which is what makes it safe to run again. A start that replays
// and fails is retried from the watermark it re-reads, so the entries an
// abandoned attempt acked are entries the successor will ack again — bytes left
// behind here would be counted twice and released once, and I10 bounds memory
// nobody holds. The stall goes with them: the watermark this plants is the
// answer an attempt's unresolved drain was waiting for.
func (t *Tail) Floor(mark wal.Seqno) {
	t.applied, t.resolved, t.commit = mark, mark, mark
	t.bytes = 0
	t.clearStall()
	t.publish()
}

// Ack takes an entry already durable at seqno into the tail. Its size is the
// payload's encoded length, which is what both the watermark and I10 count.
func (t *Tail) Ack(seqno wal.Seqno, size int) {
	t.commit = seqno
	t.bytes += size
	t.publish()
}

// WatermarkMove is the whole of the difference between the three settles: both
// readings are right and neither is a default, so the caller states which.
type WatermarkMove int

const (
	// KeepWatermark settles entries no transaction wrote: a drain whose batch
	// carried none, and an entry whose fate was decided without one. The class
	// rather than the callers, because a settle added to it and left out here
	// is a tail counting entries nobody will ever release. They leave the tail,
	// but applied stays put — a trim past what was written strands a recovering
	// owner.
	KeepWatermark WatermarkMove = iota
	// MoveWatermark settles a window whose transaction committed, the only
	// outcome that puts rows in the cold store and so the only one that may
	// move what a trim and a replay read.
	MoveWatermark
)

// Settle takes a window out of the tail: its entries up to seqno are accounted
// for and its bytes no longer held. Called once per resolving outcome — a
// committed drain, an answered condition failure, a dropped provisional entry.
//
// held is what [window.Window.Take] produced and is consumed here, so the bytes
// the tail releases are always bytes a window handed over, and never the same
// ones twice. The two counts stay two numbers — the window empties when a drain
// starts and the tail when its transaction resolves — and this is the one edge
// between them.
//
// A stalled tail settles nothing at all, this window's bytes included: half a
// settle would part the two counts, and a tail whose entries and bytes disagree
// is the bound reading a number no window will ever hand back. The caller asks
// [Tail.Stalled] first; a take nobody settles is already legal.
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

// Stall records a drain whose transaction outcome could not be read: its window
// is gone, its entries are acked, and whether the cold store holds them is
// unknown. held is consumed rather than released — those bytes stay counted,
// because the entries are still this cycle's to account for, and they leave
// with [Tail.Resolve] or with the floor a successor plants. cause is what that
// drain was told, carried for whoever eventually attributes the halt.
//
// The cold store's own watermark is the only witness, so the way out is to ask
// it again ([Tail.Stalled] is how a drain knows it must). A tail is never empty
// while one stands: the stalled drain acked the entries it is stalled at.
//
// One at a time: a caller that has not resolved the standing stall may not
// drain, so it cannot reach a second.
func (t *Tail) Stall(seqno wal.Seqno, held *window.Taken, cause error) {
	t.stalled, t.stalledBytes, t.stalledBy = seqno, held.Release(), cause
	t.publish()
}

// Unresolved is a drain whose transaction outcome could not be read: where it
// stopped, and what it was told. One value, because the two are read together
// by whoever ends it and a mismatched pair attributes the wrong drain.
type Unresolved struct {
	Seqno wal.Seqno
	Cause error
}

// Stalled is the drain the tail is stalled at, and whether there is one. The
// zero value is not one: a stalled tail always names a seqno.
func (t *Tail) Stalled() (Unresolved, bool) {
	return Unresolved{Seqno: t.stalled, Cause: t.stalledBy}, t.stalled != 0
}

// Resolve ends a stall, and only a watermark at exactly the stalled seqno
// entitles a caller to it ([cycle.Cycle.resolve] is where that is decided): the
// drain committed after all, so its entries are settled, its bytes are no longer
// held, and its seqno is in the cold store and therefore where a trim may go.
// One above it is another owner's and ends the stall in a halt instead, which is
// why this may not be reached on it — applied would move to a seqno this shard's
// own drains never committed, and the trim goes to applied.
func (t *Tail) Resolve() {
	if t.stalled == 0 {
		return
	}
	t.resolved, t.applied = t.stalled, t.stalled
	t.bytes -= t.stalledBytes
	t.clearStall()
	t.publish()
}

// clearStall is the one way out of one, so the three fields cannot part company.
func (t *Tail) clearStall() { t.stalled, t.stalledBytes, t.stalledBy = 0, 0, nil }

// publish ends every mutator above, so neither the mirror nor the metric is a
// step a call site can leave out.
func (t *Tail) publish() {
	t.mirror.store(t.Entries(), t.bytes, int(t.commit-t.applied), t.stalled)
}

// Mirror is the tail as goroutines other than the loop see it: what a write is
// refused on, read without asking the loop anything. It holds the emitter, so
// the tail is measured exactly where it moves.
type Mirror struct {
	entries atomic.Int64
	bytes   atomic.Int64
	// stalled is the seqno of [Tail]'s stall, zero for none. Here for the same
	// reason the counts are: a shard whose applier cannot say what it did must
	// refuse a writer before that writer is queued behind it.
	stalled atomic.Uint64
	metrics *walmetrics.Emitter
}

// NewMirror is where the emitter arrives, once. Handed out as a pointer,
// because a mirror holds atomics and must never be copied.
func NewMirror(metrics *walmetrics.Emitter) *Mirror { return &Mirror{metrics: metrics} }

// store is the mirror's only writer, and a tail's publish is its only caller.
func (m *Mirror) store(entries, bytes, unapplied int, stalled wal.Seqno) {
	m.entries.Store(int64(entries))
	m.bytes.Store(int64(bytes))
	m.stalled.Store(uint64(stalled))
	m.metrics.Tail(entries, bytes, unapplied)
}

// Size is the bound's reading, taken before a write is queued: a shard whose
// applier is stuck must refuse its writers rather than park them behind it.
func (m *Mirror) Size() (entries, bytes int64) { return m.entries.Load(), m.bytes.Load() }

// StalledAt is [Tail.Stalled]'s seqno for that same reader, and without the cause:
// a refusal before the queue says the shard cannot take the write, where the
// attribution belongs to the halt the loop may still reach.
// Zero is "not stalled", as on [Tail.Stalled]'s own seqno.
func (m *Mirror) StalledAt() wal.Seqno { return wal.Seqno(m.stalled.Load()) }

// Empty is [Tail.Empty] for the reader with no loop left to ask. It can be
// stale by the writes a successor cycle took, so only the two mutable-state
// reads may use it.
func (m *Mirror) Empty() bool { return m.entries.Load() == 0 }
