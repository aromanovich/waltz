package trim_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"

	"github.com/aromanovich/waltz/cycle/trim"
	"github.com/aromanovich/waltz/wal"
	"github.com/aromanovich/waltz/wal/memwal"
	"github.com/aromanovich/waltz/wal/waltest"
	"github.com/aromanovich/waltz/walmetrics"
)

const shard = wal.ShardID(9)

// holding is a fault that keeps a trim inside the backend until the returned
// channel is closed, which is the state the cadence has to skip rather than
// queue.
func holding() (waltest.Fault, chan struct{}) {
	hold := make(chan struct{})
	return func(int) error { <-hold; return nil }, hold
}

// start returns a trimmer over a log that keeps the contract and can be made to
// fail, and the clock it reads, at a time far enough from the zero time that the
// age half of a cadence is not already due.
func start(t *testing.T) (*trim.Trimmer, *waltest.Faulty, *clock.EventTimeSource) {
	t.Helper()
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	ts := clock.NewEventTimeSource()
	ts.Update(now)
	backend := waltest.NewFaulty(memwal.New())
	return trim.New(shard, backend, ts, walmetrics.New(nil), log.NewNoopLogger(), now), backend, ts
}

func TestTheCadenceCountsDrains(t *testing.T) {
	trimmer, backend, _ := start(t)
	every := trim.Cadence{Every: 3, After: time.Hour}

	trimmer.Drained(1, every)
	trimmer.Drained(2, every)
	trimmer.Wait()
	require.Empty(t, backend.Trims(), "two drains do not reach a cadence of three")

	trimmer.Drained(3, every)
	trimmer.Wait()
	require.Equal(t, []wal.Seqno{3}, backend.Trims(),
		"the trim goes to the watermark of the drain that reached the cadence, with no lag")
}

func TestTheCadenceAlsoCountsTime(t *testing.T) {
	trimmer, backend, ts := start(t)
	rarely := trim.Cadence{Every: 1 << 20, After: time.Minute}

	trimmer.Drained(1, rarely)
	trimmer.Wait()
	require.Empty(t, backend.Trims(), "a shard that drains rarely has not reached the count")

	ts.Update(ts.Now().Add(2 * time.Minute))
	trimmer.Drained(2, rarely)
	trimmer.Wait()
	require.Equal(t, []wal.Seqno{2}, backend.Trims(), "drains or seconds, whichever trips first")
}

func TestACadenceDueWhileATrimIsInFlightIsSkipped(t *testing.T) {
	trimmer, backend, _ := start(t)
	fault, hold := holding()
	backend.OnTrim(fault)
	always := trim.Cadence{Every: 1, After: time.Hour}

	trimmer.Drained(1, always)
	trimmer.Drained(2, always)
	trimmer.Drained(3, always)
	close(hold)
	trimmer.Wait()

	require.Equal(t, []wal.Seqno{1}, backend.Trims(),
		"the drains behind the one in flight are skipped rather than queued")

	// And the next one takes the watermark that has moved furthest since.
	trimmer.Drained(4, always)
	trimmer.Wait()
	require.Equal(t, []wal.Seqno{1, 4}, backend.Trims())
}

func TestAFailedTrimIsRetriedAtTheNextCadence(t *testing.T) {
	trimmer, backend, _ := start(t)
	backend.OnTrim(waltest.Always(errors.New("the WAL table is busy")))
	always := trim.Cadence{Every: 1, After: time.Hour}

	trimmer.Drained(1, always)
	trimmer.Wait()
	require.Equal(t, []wal.Seqno{1}, backend.Trims(), "the first cadence trimmed, and it failed")

	backend.OnTrim(nil)
	trimmer.Drained(2, always)
	trimmer.Wait()
	require.Equal(t, []wal.Seqno{1, 2}, backend.Trims(), "the next cadence tries again")
}

func TestTheTwoCountersTellAFiredCadenceFromACommittedTrim(t *testing.T) {
	trimmer, backend, _ := start(t)
	backend.OnTrim(waltest.Always(errors.New("the WAL table is busy")))
	always := trim.Cadence{Every: 1, After: time.Hour}

	trimmer.Drained(1, always)
	trimmer.Wait()
	fired, committed := trimmer.Counters()
	require.Equal(t, 1, fired, "the cadence fired")
	require.Zero(t, committed, "and nothing reached the log")

	backend.OnTrim(nil)
	trimmer.Drained(2, always)
	trimmer.Wait()
	fired, committed = trimmer.Counters()
	require.Equal(t, 2, fired, "the next cadence tried again")
	require.Equal(t, 1, committed, "and only the one that committed is counted")
}

func TestForceTrimsOutsideTheCadence(t *testing.T) {
	trimmer, backend, _ := start(t)
	rarely := trim.Cadence{Every: 1 << 20, After: time.Hour}

	trimmer.Drained(1, rarely)
	trimmer.Wait()
	require.Empty(t, backend.Trims(), "the cadence alone would not have fired")

	trimmer.Force(2)
	trimmer.Wait()
	require.Equal(t, []wal.Seqno{2}, backend.Trims(), "a forced trim consults no cadence")
	fired, committed := trimmer.Counters()
	require.Equal(t, 1, fired)
	require.Equal(t, 1, committed)
}

func TestForceIsTheCadencesLastTrim(t *testing.T) {
	trimmer, backend, _ := start(t)
	every2 := trim.Cadence{Every: 2, After: time.Hour}

	trimmer.Force(1)
	trimmer.Wait()
	trimmer.Drained(2, every2)
	trimmer.Wait()
	require.Equal(t, []wal.Seqno{1}, backend.Trims(),
		"one drain since the forced trim does not reach a cadence of two")

	trimmer.Drained(3, every2)
	trimmer.Wait()
	require.Equal(t, []wal.Seqno{1, 3}, backend.Trims())
}

func TestAForceDuringATrimQueuesOneFollowUpAtTheHighestWatermark(t *testing.T) {
	trimmer, backend, _ := start(t)
	fault, hold := holding()
	backend.OnTrim(fault)

	trimmer.Force(1)
	trimmer.Force(5)
	trimmer.Force(9)
	close(hold)
	trimmer.Wait()

	require.Equal(t, []wal.Seqno{1, 9}, backend.Trims(),
		"the two requests behind the one in flight are one follow-up, at the highest watermark")
	fired, committed := trimmer.Counters()
	require.Equal(t, 2, fired, "a request coalesced into the queued follow-up is not a third attempt")
	require.Equal(t, 2, committed)
}

func TestAFailedTrimDoesNotLoseTheForcedFollowUp(t *testing.T) {
	trimmer, backend, _ := start(t)
	hold := make(chan struct{})
	backend.OnTrim(func(call int) error {
		if call == 1 {
			<-hold
			return errors.New("the WAL table is busy")
		}
		return nil
	})

	trimmer.Force(3)
	trimmer.Force(7)
	close(hold)
	trimmer.Wait()

	require.Equal(t, []wal.Seqno{3, 7}, backend.Trims(),
		"the follow-up runs whatever became of the trim it queued behind")
	fired, committed := trimmer.Counters()
	require.Equal(t, 2, fired)
	require.Equal(t, 1, committed, "the failed attempt is fired and not committed")
}

func TestWaitCoversTheQueuedFollowUp(t *testing.T) {
	trimmer, backend, _ := start(t)
	first, second := make(chan struct{}), make(chan struct{})
	backend.OnTrim(func(call int) error {
		if call == 1 {
			<-first
		} else {
			<-second
		}
		return nil
	})

	trimmer.Force(1)
	trimmer.Force(2)

	done := make(chan struct{})
	go func() {
		trimmer.Wait()
		close(done)
	}()
	close(first)
	select {
	case <-done:
		t.Fatal("Wait returned while the queued follow-up was still inside the backend")
	case <-time.After(20 * time.Millisecond):
	}
	close(second)
	<-done
	require.Equal(t, []wal.Seqno{1, 2}, backend.Trims())
}

func TestAForceATrimAlreadyCoveredSchedulesNothing(t *testing.T) {
	trimmer, backend, _ := start(t)

	trimmer.Force(5)
	trimmer.Wait()
	trimmer.Force(5)
	trimmer.Force(4)
	trimmer.Wait()

	require.Equal(t, []wal.Seqno{5}, backend.Trims(),
		"a watermark a trim has already reached has nothing left to give back")
	fired, committed := trimmer.Counters()
	require.Equal(t, 1, fired)
	require.Equal(t, 1, committed)
}

func TestAForceAtTheWatermarkInFlightQueuesNothing(t *testing.T) {
	trimmer, backend, _ := start(t)
	fault, hold := holding()
	backend.OnTrim(fault)

	trimmer.Force(5)
	trimmer.Force(5)
	close(hold)
	trimmer.Wait()

	require.Equal(t, []wal.Seqno{5}, backend.Trims(),
		"the trim in flight is already going where the second request asks")
	fired, _ := trimmer.Counters()
	require.Equal(t, 1, fired)
}

func TestAFailedForcedTrimIsNotCovered(t *testing.T) {
	trimmer, backend, _ := start(t)
	backend.OnTrim(waltest.Once(errors.New("the WAL volume is out of space")))

	trimmer.Force(5)
	trimmer.Wait()
	trimmer.Force(5)
	trimmer.Wait()

	require.Equal(t, []wal.Seqno{5, 5}, backend.Trims(),
		"only a committed trim covers its watermark: the retry gets through")
	fired, committed := trimmer.Counters()
	require.Equal(t, 2, fired)
	require.Equal(t, 1, committed)
}

func TestAForceBeforeAnythingAppliedTrimsNothing(t *testing.T) {
	trimmer, backend, _ := start(t)

	trimmer.Force(wal.FirstSeqno - 1)
	trimmer.Wait()

	require.Empty(t, backend.Trims(), "no watermark means no space to give back")
	fired, _ := trimmer.Counters()
	require.Zero(t, fired)
}

func TestWaitReturnsOnlyWhenTheTrimIsDone(t *testing.T) {
	trimmer, backend, _ := start(t)
	fault, hold := holding()
	backend.OnTrim(fault)

	trimmer.Drained(1, trim.Cadence{Every: 1, After: time.Hour})

	done := make(chan struct{})
	go func() {
		trimmer.Wait()
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("Wait returned while a trim was still inside the backend")
	case <-time.After(20 * time.Millisecond):
	}

	close(hold)
	<-done
	require.Equal(t, []wal.Seqno{1}, backend.Trims())
}
