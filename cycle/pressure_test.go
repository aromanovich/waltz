package cycle

// The cycle over a backend reporting storage pressure ([wal.PressureSource]).
// What these tests hold above all is that the append that carried the signal
// stays the durable ack it was.

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	p "go.temporal.io/server/common/persistence"

	"github.com/aromanovich/waltz/wal"
	"github.com/aromanovich/waltz/wal/waltest"
	"github.com/aromanovich/waltz/walmetrics"
)

// pressured is the test log grown the optional face. The level is the test's
// to set from any goroutine, as a backend's own observations would.
type pressured struct {
	*waltest.Faulty
	level atomic.Int32
}

func (pl *pressured) Pressure(wal.ShardID) wal.PressureLevel {
	return wal.PressureLevel(pl.level.Load())
}

func (pl *pressured) report(l wal.PressureLevel) { pl.level.Store(int32(l)) }

// pressureEnv is newEnv over a log that reports pressure, with every trigger
// and the trim cadence out of reach: the only drain and the only trim that can
// happen in these tests are the ones pressure asks for, unless the shape says
// otherwise.
func pressureEnv(t *testing.T, shape func(*Config)) (*env, *pressured) {
	t.Helper()
	pl := &pressured{Faulty: newLog()}
	e := newEnvWith(t, pl, pl.Faulty, func(c *Config) {
		neverDrains(c)
		c.TrimEvery, c.TrimAfter = 1<<30, time.Hour
		if shape != nil {
			shape(c)
		}
	})
	return e, pl
}

// TestPressureDrainsTheWindowAndForcesTheTrim: the drain level empties a
// window no trigger would have, the drain says what asked for it, and the
// committed drain's trim consults no cadence.
func TestPressureDrainsTheWindowAndForcesTheTrim(t *testing.T) {
	e, pl := pressureEnv(t, nil)
	ns, wf, run := ids()

	require.NoError(t, e.add(t, mkCreate(ns, wf, run)))
	require.Empty(t, e.apply.drains, "without pressure this window drains on no trigger")

	pl.report(wal.PressureDrain)
	require.NoError(t, e.add(t, mkUpdate(ns, wf, run, 2)),
		"the append that carried the signal is an ack like any other")
	require.Len(t, e.apply.drains, 1)
	require.Equal(t, []string{walmetrics.TriggerStoragePressure}, e.tagged("wal_drains", "trigger"))

	e.c.trimmer.Wait()
	require.Equal(t, []wal.Seqno{2}, e.log.Trims(),
		"the trim went to the new applied watermark, past the cadence")
}

// TestStopRefusesTheNextWriteUntilTheBackendLowersIt: nothing is appended
// while the stop level stands, the refusal is the concrete type the shard's
// write path reads as definitely-not-committed, and the first write after the
// backend lowers the level goes through with nothing to reset.
func TestStopRefusesTheNextWriteUntilTheBackendLowersIt(t *testing.T) {
	e, pl := pressureEnv(t, nil)
	ns, wf, run := ids()
	require.NoError(t, e.add(t, mkCreate(ns, wf, run)))

	pl.report(wal.PressureStop)
	err := e.add(t, mkUpdate(ns, wf, run, 2))
	requireRefusal(t, err)
	require.False(t, p.OperationPossiblySucceeded(err))
	require.Len(t, e.entries(t), 1, "the refused write reached no log")
	require.Equal(t, []string{walmetrics.LimitStoragePressure},
		e.tagged("wal_backpressure_refusals", "limit"))

	pl.report(wal.PressureNone)
	require.NoError(t, e.add(t, mkUpdate(ns, wf, run, 2)))
	require.Len(t, e.entries(t), 2)
}

// TestSyncModeKeepsItsAnswerUnderPressure: sync mode's drain is the one that
// would have run anyway, so the trigger stays sync; what pressure adds there
// is only the trim behind it.
func TestSyncModeKeepsItsAnswerUnderPressure(t *testing.T) {
	e, pl := pressureEnv(t, func(c *Config) { c.Sync = true })
	ns, wf, run := ids()

	pl.report(wal.PressureDrain)
	require.NoError(t, e.add(t, mkCreate(ns, wf, run)))
	require.Equal(t, []string{walmetrics.TriggerSync}, e.tagged("wal_drains", "trigger"),
		"pressure names no drain sync mode did not already run")
	e.c.trimmer.Wait()
	require.Equal(t, []wal.Seqno{1}, e.log.Trims())
}

// TestSyncModeAnswersItsConditionFailureUnderPressure: the one outcome sync
// mode reads differently is still read that way — the caller gets the store's
// own error, the shard stays running, and a drain that committed nothing
// forces no trim.
func TestSyncModeAnswersItsConditionFailureUnderPressure(t *testing.T) {
	e, pl := pressureEnv(t, func(c *Config) { c.Sync = true })
	e.apply.errs = []error{&p.WorkflowConditionFailedError{Msg: "stale"}}
	ns, wf, run := ids()

	pl.report(wal.PressureDrain)
	err := e.add(t, mkCreate(ns, wf, run))
	_, ok := err.(*p.WorkflowConditionFailedError) //nolint:errorlint // the concrete type is the assertion
	require.True(t, ok, "got %T: %v", err, err)
	require.Equal(t, StateRunning, e.c.State())

	e.c.trimmer.Wait()
	require.Empty(t, e.log.Trims(), "nothing was applied, so there is nothing to give back")
}

// TestAnAcquireUnderPressureReclaimsThePreviousOwnersEntries: the first trim
// goes at the watermark start just read, before the first new append asks the
// backend for more space.
func TestAnAcquireUnderPressureReclaimsThePreviousOwnersEntries(t *testing.T) {
	e, pl := pressureEnv(t, nil)
	e.inherit(t, 41)
	e.mark.answers = []wmAnswer{{seqno: 41, found: true}}
	pl.report(wal.PressureDrain)
	ns, wf, run := ids()

	require.NoError(t, e.add(t, mkCreate(ns, wf, run)))
	e.c.trimmer.Wait()
	require.Equal(t, []wal.Seqno{41, 42}, e.log.Trims(),
		"the inherited entries went at the watermark start read, the new append's behind its own drain")
}

// TestStandingPressureRetriesAFailedForcedTrimAtTheAgeTick: under the stop
// level the writers are refused, so no write brings a drain or a trim with it
// — the tick is the retry, on the same arm that re-asks a stalled drain.
func TestStandingPressureRetriesAFailedForcedTrimAtTheAgeTick(t *testing.T) {
	e, pl := pressureEnv(t, nil)
	ns, wf, run := ids()

	pl.report(wal.PressureDrain)
	e.log.OnTrim(waltest.Once(errors.New("the WAL volume is out of space")))
	require.NoError(t, e.add(t, mkCreate(ns, wf, run)))
	e.c.trimmer.Wait()
	require.Equal(t, []wal.Seqno{1}, e.log.Trims(), "the forced trim went, and failed")

	pl.report(wal.PressureStop)
	e.advance(t, e.cfg.Age+time.Second)
	e.c.trimmer.Wait()
	require.Equal(t, []wal.Seqno{1, 1}, e.log.Trims(), "the tick asked again at the same watermark")
}

// TestPressureDrainsAWindowTheAgeWouldNotYet: a window pressure found already
// sitting there — the level rose between writes — is the tick's to drain
// without waiting out the age, and the drain is attributed to what actually
// fired it.
func TestPressureDrainsAWindowTheAgeWouldNotYet(t *testing.T) {
	e, pl := pressureEnv(t, nil)
	ns, wf, run := ids()
	// Land the write just before the tick, so the window is far younger than
	// the age when the timer fires.
	e.advance(t, e.cfg.Age-time.Minute)
	require.NoError(t, e.add(t, mkCreate(ns, wf, run)))

	pl.report(wal.PressureDrain)
	e.advance(t, time.Minute)
	require.Len(t, e.apply.drains, 1)
	require.Equal(t, []string{walmetrics.TriggerStoragePressure}, e.tagged("wal_drains", "trigger"))
	e.c.trimmer.Wait()
	require.Equal(t, []wal.Seqno{1}, e.log.Trims())
}

// TestAnUnstartedCycleUnderPressureTrimsNothing: before the first request the
// watermark has never been read, so there is no position a trim could safely
// go to, whatever the backend reports.
func TestAnUnstartedCycleUnderPressureTrimsNothing(t *testing.T) {
	e, pl := pressureEnv(t, nil)

	pl.report(wal.PressureStop)
	e.advance(t, e.cfg.Age+time.Second)
	e.c.trimmer.Wait()
	require.Empty(t, e.log.Trims())
}
