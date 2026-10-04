// Package trim deletes WAL entries the cold store already holds: it decides
// when to trim, runs at most one trim at a time, and counts trims fired and
// trims that reached the log.
//
// Trims run beside the apply cycle, not in it, so a stuck log cannot stop a
// shard from acking and applying. It is a separate package so the goroutine
// it starts cannot reach the cycle's loop state: a [Trimmer] gets the
// watermark by value.
//
// A drain triggers trims, because a backend's reads get slower as its log
// grows ([wal.Log.Trim]). A failed trim is logged and halts nothing: a
// cadenced one is retried at the next cadence, a forced one when its caller
// forces again.
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

// budget is how long one trim may run before it is abandoned. No caller
// waits on it.
const budget = time.Minute

// Cadence says when a trim fires: after Every drains or After time since the
// last trim, whichever comes first. It is passed per call, not stored, so the
// policy can change while the shard is held.
type Cadence struct {
	Every int
	After time.Duration
}

// Trimmer keeps one shard's log short.
//
// Only the cycle's loop calls [Trimmer.Drained] and [Trimmer.Force], so the
// cadence fields need no lock. [Trimmer.Counters] is called from the loop, or
// by whoever retires the cycle after [Trimmer.Wait] once the loop has exited.
type Trimmer struct {
	shard  wal.ShardID
	log    wal.Log
	clock  clock.TimeSource
	emit   *walmetrics.Emitter
	logger log.Logger

	// Owned by the caller's loop.
	sinceTrim int
	lastAt    time.Time
	fired     int

	// running and mu guard the one running trim and the one follow-up a Force
	// may queue behind it. committed is written by the trim goroutine.
	running  sync.WaitGroup
	mu       sync.Mutex
	inFlight bool
	// flightUpTo is the running trim's target, doneUpTo the last committed
	// one. pending (0 = none) is the queued follow-up, raised to the highest
	// watermark asked for. [Trimmer.start] uses them to skip needless trims.
	flightUpTo wal.Seqno
	doneUpTo   wal.Seqno
	pending    wal.Seqno
	committed  atomic.Int64
}

// New returns a Trimmer for one shard. Cadence.After is measured from now;
// starting from the zero time would fire on the first drain.
func New(shard wal.ShardID, log wal.Log, clock clock.TimeSource, emit *walmetrics.Emitter, logger log.Logger, now time.Time) *Trimmer {
	return &Trimmer{shard: shard, log: log, clock: clock, emit: emit, logger: logger, lastAt: now}
}

// Drained tells the Trimmer a drain committed and left the watermark at
// applied. When the cadence is due it starts a trim up to applied, with no
// safety lag: recovery replays only entries above the watermark.
//
// If a trim is already running, a due cadence is skipped, not queued; the
// next one will trim to a later watermark.
func (t *Trimmer) Drained(applied wal.Seqno, cadence Cadence) {
	t.sinceTrim++
	now := t.clock.Now()
	if t.sinceTrim < cadence.Every && now.Sub(t.lastAt) < cadence.After {
		return
	}
	if !t.start(applied, false) {
		// Skipped or already covered; the cadence keeps accruing.
		return
	}
	t.sinceTrim = 0
	t.lastAt = now
	t.fired++
}

// Force starts a trim now, outside the cadence, for a backend reporting
// storage pressure. If a trim is running, one follow-up is queued, raised to
// the highest watermark asked for. A watermark already trimmed or being
// trimmed schedules nothing, so repeated pressure on an unchanged watermark
// costs no trims. Only a scheduled attempt counts as fired; the cadence
// resets either way, since a refused request is already covered.
//
// A failed forced trim is not retried here: the caller polls the pressure and
// forces again while it stands.
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

// Wait blocks until no trim is running. Whoever retires a cycle calls it, so
// the backend outlives the last trim.
func (t *Trimmer) Wait() { t.running.Wait() }

// Counters returns trims fired (cadenced or forced) and those that committed.
// Two numbers, so that all trims failing does not look like none firing.
// While a trim is running or queued, fired exceeds committed by those.
func (t *Trimmer) Counters() (fired, committed int) {
	return t.fired, int(t.committed.Load())
}

// start runs one trim in a goroutine and reports whether it scheduled an
// attempt. If a trim is running, queue=false skips the request and
// queue=true queues one follow-up at the highest watermark asked for. A
// request at or below a running, queued or committed target schedules
// nothing. A failed trim leaves doneUpTo unchanged, so a retry still runs.
// The goroutine touches only this Trimmer's guarded fields.
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
		// inFlight is cleared before Done, so after Wait returns the next
		// cadence can fire. Wait also covers a queued follow-up.
		defer t.running.Done()
		for {
			t.emit.Trim(walmetrics.TrimStarted)
			ctx, cancel := context.WithTimeout(context.Background(), budget)
			err := t.log.Trim(ctx, t.shard, upTo)
			cancel()
			if err != nil {
				// Count failures too: the failure rate is a ratio.
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
