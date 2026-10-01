// Package trim is the lazy deletion of WAL entries the cold store already
// holds: the cadence that decides when, the one trim in flight, and the two
// counters that tell a trim that fired from a trim that reached the log.
//
// It runs beside the apply cycle rather than in it, which is the whole of why
// it is a package. A stuck log may not stop a shard from acking and applying,
// so the trim is detached — and a `go` statement in a function holding the
// loop's state is the one ownership break nothing can see into. Here there is
// no such state to hold: a [Trimmer] is handed a watermark by value, and the
// cycle's rule is the compiler's.
//
// Trimming is part of the latency budget rather than hygiene: a backend's
// reads get dearer as its log gets longer ([wal.Log.Trim]), so a drain is what
// fires one, rather than a sweeper on a clock of its own. A failed trim is
// logged and halts nothing: a cadenced one is retried at the next cadence, a
// forced one when its caller forces again.
package trim

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"

	"github.com/aromanovich/waltz/wal"
	"github.com/aromanovich/waltz/walmetrics"
)

// budget is how long one trim may take before it is abandoned. It is not a
// deadline the caller waits on: whoever asked has been answered a drain ago.
const budget = time.Minute

// Cadence is the two numbers a trim fires on, whichever trips first: drains
// since the last trim, and time since it. It is passed at the decision rather
// than held, so both may move under a shard this node is already holding.
type Cadence struct {
	Every int
	After time.Duration
}

// Trimmer keeps one shard's log short.
//
// [Trimmer.Drained] and [Trimmer.Force] are the cycle's loop and no other
// goroutine, which is what lets the cadence be plain fields. [Trimmer.Counters]
// is the loop's too, plus one read by whoever retires the cycle — taken after
// that caller's [Trimmer.Wait] and once the loop is gone, which is what makes
// reading the plain fired field there safe.
type Trimmer struct {
	shard  wal.ShardID
	log    wal.Log
	clock  clock.TimeSource
	emit   *walmetrics.Emitter
	logger log.Logger

	// The cadence, owned by the caller's loop: drains since the last trim, when
	// it was, and how many have fired.
	sinceTrim int
	lastAt    time.Time
	fired     int

	// running and mu guard the one detached trim and the one follow-up a Force
	// may queue behind it; committed is written by the trim's goroutine, since
	// only it knows the outcome.
	running  sync.WaitGroup
	mu       sync.Mutex
	inFlight bool
	// flightUpTo and doneUpTo are where the running trim is going and where a
	// committed one has been; pending (0 is none) is the queued follow-up,
	// coalesced to the highest watermark asked for. Together they are how
	// [Trimmer.start] refuses a request no trim needs.
	flightUpTo wal.Seqno
	doneUpTo   wal.Seqno
	pending    wal.Seqno
	committed  atomic.Int64
}

// New returns a Trimmer for one shard. now is where its clock starts, so the
// time half of the cadence is measured from the cycle's birth rather than from
// the zero time, which would fire on the first drain.
func New(shard wal.ShardID, log wal.Log, clock clock.TimeSource, emit *walmetrics.Emitter, logger log.Logger, now time.Time) *Trimmer {
	return &Trimmer{shard: shard, log: log, clock: clock, emit: emit, logger: logger, lastAt: now}
}

// Drained tells the Trimmer that a drain committed and left the watermark at
// applied. It starts a trim when the cadence says so — after the commit, off
// the critical path, up to the committed watermark with no safety lag, since
// recovery replays only what lies above the watermark.
//
// A cadence that comes due while a trim is in flight is skipped rather than
// queued: the next one takes a watermark that has moved further, and two trims
// of one log are the same trim twice.
func (t *Trimmer) Drained(applied wal.Seqno, cadence Cadence) {
	t.sinceTrim++
	now := t.clock.Now()
	if t.sinceTrim < cadence.Every && now.Sub(t.lastAt) < cadence.After {
		return
	}
	if !t.start(applied, false) {
		// In flight — skipped, and the cadence keeps accruing — or the
		// watermark is already covered and there is nothing to give back.
		return
	}
	t.sinceTrim = 0
	t.lastAt = now
	t.fired++
}

// Force starts a trim at once, outside the cadence: a backend that reported
// storage pressure is owed the applied entries' space now, not a cadence from
// now. A trim already in flight takes one follow-up rather than losing the
// request, coalesced to the highest watermark asked for; one that a trim has
// already reached, or is reaching, schedules nothing, so a level standing
// across ticks re-trims an unchanged watermark exactly never. Only a request
// that scheduled an attempt counts as a fired one; the cadence resets either
// way, because a request the trimmer refused is one a trim already covers, and
// the next cadenced trim is owed no sooner for it.
//
// A failed forced trim is not retried from here: the pressure that asked for it
// is a level, and whoever polls it forces again while it stands.
func (t *Trimmer) Force(applied wal.Seqno) {
	if !wal.CheckTrim(applied) {
		// Nothing has ever been applied, so there is no space to give back.
		return
	}
	t.sinceTrim = 0
	t.lastAt = t.clock.Now()
	if t.start(applied, true) {
		t.fired++
	}
}

// Wait blocks until no trim is in flight. Whoever retires a cycle calls it, so
// that a backend outlives the last trim it was asked for.
func (t *Trimmer) Wait() { t.running.Wait() }

// Counters is what this shard's trims have done: trims that fired, cadenced or
// forced, and the subset that reached the log. Two numbers rather than one,
// because a run whose every trim failed would otherwise read like one whose
// cadence never fired — and a reading taken while a trim is in flight, or
// queued behind one, leaves fired above committed by those, which is the
// honest answer rather than a race.
func (t *Trimmer) Counters() (fired, committed int) {
	return t.fired, int(t.committed.Load())
}

// start runs one trim beside the caller's loop, and reports whether it
// scheduled an attempt. queue is what a trim already in flight does with the
// request: a cadence is skipped — the next one takes a watermark that has
// moved further — where a forced request queues one follow-up, coalesced to
// the highest watermark asked for. Either way, a request no trim needs — at or
// below where one is going, is queued to go, or has already been — schedules
// nothing: a failed trim leaves doneUpTo where it was, so the retry still
// gets through, while a repeat of a committed one would give the backend
// nothing back. It takes the watermark as a value and holds nothing of the
// caller's: what the goroutine touches is this Trimmer's own guarded fields.
func (t *Trimmer) start(upTo wal.Seqno, queue bool) bool {
	t.mu.Lock()
	if t.inFlight {
		if !queue || upTo <= max(t.flightUpTo, t.pending) {
			t.mu.Unlock()
			return false
		}
		scheduled := t.pending == 0
		t.pending = upTo
		t.mu.Unlock()
		return scheduled
	}
	if upTo <= t.doneUpTo {
		t.mu.Unlock()
		return false
	}
	t.inFlight = true
	t.flightUpTo = upTo
	t.running.Add(1)
	t.mu.Unlock()
	go func() {
		// Order: inFlight is cleared before Done, so a Wait that returns leaves
		// the next cadence free to fire rather than skipping itself. Wait also
		// covers a queued follow-up, because the goroutine runs it before Done.
		defer t.running.Done()
		for {
			t.emit.Trim(walmetrics.TrimStarted)
			ctx, cancel := context.WithTimeout(context.Background(), budget)
			err := t.log.Trim(ctx, t.shard, upTo)
			cancel()
			if err != nil {
				// Both outcomes are counted, because "trims are failing" is a
				// ratio.
				t.emit.Trim(walmetrics.TrimFailed)
				t.logger.Warn("apply cycle: trim failed, retrying at the next cadence",
					tag.ShardID(int32(t.shard)), tag.Error(err))
			} else {
				t.committed.Add(1)
			}
			t.mu.Lock()
			if err == nil {
				t.doneUpTo = upTo
			}
			if t.pending != 0 {
				upTo, t.flightUpTo, t.pending = t.pending, t.pending, 0
				t.mu.Unlock()
				continue
			}
			t.inFlight = false
			t.mu.Unlock()
			return
		}
	}()
	return true
}
