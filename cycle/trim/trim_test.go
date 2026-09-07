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
