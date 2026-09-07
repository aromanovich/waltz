package cycle

// §M's numbers, over cycle_test.go's fakes: every emission point is on a path
// those fakes already drive — a drain's outcome, a refused append, a tail that
// moved. What each test pins is the *reading* a dashboard takes off a series,
// not that the call was made.

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/metrics/metricstest"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"

	"github.com/aromanovich/waltz/verify/coldtasks"
	"github.com/aromanovich/waltz/wal/waltest"
	"github.com/aromanovich/waltz/walmetrics"
)

// TestEveryDrainNamesWhatTriggeredIt: the drain counter without its trigger tag
// is a number nobody can act on. The five values are five different diagnoses —
// size is the design working, age is a shard nobody is pushing on, refusal is
// fold's drain-and-retry, sync is intercept mode, explicit is a shutdown.
func TestEveryDrainNamesWhatTriggeredIt(t *testing.T) {
	ns := uuid.NewString()

	t.Run("the size watermark, in mutations", func(t *testing.T) {
		e := newEnv(t, func(c *Config) { c.Mutations = 2 })
		require.NoError(t, e.add(t, mkCreate(ns, "a", "r1")))
		require.NoError(t, e.add(t, mkCreate(ns, "b", "r2")))
		require.Equal(t, []string{walmetrics.TriggerMutations}, e.tagged("wal_drains", walmetrics.TagTrigger))
	})

	t.Run("the size watermark, in bytes", func(t *testing.T) {
		e := newEnv(t, func(c *Config) { c.Mutations = 1000; c.Bytes = 1 })
		require.NoError(t, e.add(t, mkCreate(ns, "a", "r1")))
		require.Equal(t, []string{walmetrics.TriggerBytes}, e.tagged("wal_drains", walmetrics.TagTrigger))
	})

	t.Run("the age watermark", func(t *testing.T) {
		e := newEnv(t, func(c *Config) { c.Mutations = 1000; c.Bytes = 1 << 30 })
		require.NoError(t, e.add(t, mkCreate(ns, "a", "r1")))
		e.advance(t, 2*e.cfg.Age)
		require.Equal(t, []string{walmetrics.TriggerAge}, e.tagged("wal_drains", walmetrics.TagTrigger))
	})

	t.Run("the accumulator refusing a mutation", func(t *testing.T) {
		e := newEnv(t, func(c *Config) { c.Mutations = 1000; c.Bytes = 1 << 30 })
		require.NoError(t, e.add(t, mkCreate(ns, "w", "r1")))
		// A continue-as-new out of a run whose window state is a snapshot: the
		// fold refuses, which force-drains and retries. Version 2 and not 1 —
		// the create wrote 1, so an update asserting base 0 would be answered
		// by the condition authority rather than folded in.
		require.NoError(t, e.add(t, mkContinueAsNew(ns, "w", "r1", "r2", 2)))
		require.Equal(t, []string{walmetrics.TriggerRefusal}, e.tagged("wal_drains", walmetrics.TagTrigger))
	})

	t.Run("sync mode, and a shutdown", func(t *testing.T) {
		e := newEnv(t, func(c *Config) { c.Sync = true })
		require.NoError(t, e.add(t, mkCreate(ns, "a", "r1")))
		require.NoError(t, e.c.drainNow(t.Context()))
		// The explicit drain of an empty window is a no-op and emits nothing.
		require.Equal(t, []string{walmetrics.TriggerSync}, e.tagged("wal_drains", walmetrics.TagTrigger))

		require.NoError(t, e.add(t, mkCreate(ns, "b", "r2")))
		require.Equal(t, []string{walmetrics.TriggerSync, walmetrics.TriggerSync},
			e.tagged("wal_drains", walmetrics.TagTrigger))
	})
}

// TestTheCollapseGoesOutAsTwoCountersAndNotAsARatio: the collapse goes out as
// mutations and workflows, and the division is the dashboard's. A pre-divided
// ratio would carry no record of what it was measured over and would be
// averaged over whatever scrape interval the operator configured.
func TestTheCollapseGoesOutAsTwoCountersAndNotAsARatio(t *testing.T) {
	ns := uuid.NewString()
	e := newEnv(t, func(c *Config) { c.Mutations = 3 })

	// Three mutations on one workflow, collapsed into one write. Versions 2 and
	// 3 because the create wrote 1: an update asserting a version the window
	// did not write is answered rather than folded in.
	require.NoError(t, e.add(t, mkCreate(ns, "w", "r1")))
	require.NoError(t, e.add(t, mkUpdate(ns, "w", "r1", 2)))
	require.NoError(t, e.add(t, mkUpdate(ns, "w", "r1", 3)))

	require.Equal(t, []any{int64(3)}, values(e.recorded("wal_drained_mutations")))
	require.Equal(t, []any{int64(1)}, values(e.recorded("wal_drained_workflows")))

	for name := range e.capture.Snapshot() {
		require.NotContains(t, name, "ratio",
			"a pre-divided collapse ratio is the bare number the rule forbids (§M, #12, #19)")
	}
}

// TestTheTailIsRecordedInBothUnitsWhereverItMoves. The tail goes out in bytes,
// entries and age, and the three are not interchangeable: entries alone cannot
// bound memory, bytes alone bound no replay, and age is what the idle watermark
// fires on.
//
// It also pins that the tail and commitSeqno − appliedSeqno go out as two
// series wherever the tail moves, here carrying the same number. They part
// company at a settle that keeps the watermark, which is why neither stands in
// for the other.
func TestTheTailIsRecordedInBothUnitsWhereverItMoves(t *testing.T) {
	ns := uuid.NewString()
	e := newEnv(t, func(c *Config) { c.Mutations = 1000; c.Bytes = 1 << 30 })

	require.NoError(t, e.add(t, mkCreate(ns, "a", "r1")))
	require.NoError(t, e.add(t, mkCreate(ns, "b", "r2")))

	// One recording per append, plus the one the first Add's watermark read
	// made before anything was in the tail.
	require.Equal(t, []any{int64(0), int64(1), int64(2)}, values(e.recorded("wal_tail_entries")))
	require.Equal(t, []any{int64(0), int64(1), int64(2)}, values(e.recorded("wal_unapplied_entries")))

	bytes := values(e.recorded("wal_tail_bytes"))
	require.Len(t, bytes, 3)
	require.Equal(t, int64(0), bytes[0])
	require.Greater(t, bytes[1], int64(0), "an acked mutation weighs something")
	require.Greater(t, bytes[2], bytes[1], "and two weigh more than one")

	require.NoError(t, e.c.drainNow(t.Context()))
	require.Equal(t, int64(0), last(values(e.recorded("wal_tail_entries"))), "a committed drain empties the tail")
	require.NotEmpty(t, e.recorded("wal_window_age"), "the third unit §M asks for")
}

// TestBackpressureNamesTheUnitThatBound: a write is refused before the append
// for three reasons, and which one is the diagnosis — entries means a stalled
// applier, bytes one workflow near the server's own blob limits, unresolved an
// applier that cannot say whether its last drain committed.
func TestBackpressureNamesTheUnitThatBound(t *testing.T) {
	ns := uuid.NewString()

	t.Run("entries", func(t *testing.T) {
		e := newEnv(t, func(c *Config) {
			c.Mutations, c.Bytes = 1000, 1<<30
			c.HardMaxEntries, c.HardMaxBytes = 1, 1<<30
			c.MaxShards, c.TailBudgetBytes = 1, 1<<30
		})
		require.NoError(t, e.add(t, mkCreate(ns, "a", "r1")))
		require.Error(t, e.add(t, mkCreate(ns, "b", "r2")))
		require.Equal(t, []string{walmetrics.LimitEntries}, e.tagged("wal_backpressure_refusals", walmetrics.TagLimit))
	})

	t.Run("bytes", func(t *testing.T) {
		e := newEnv(t, func(c *Config) {
			c.Mutations, c.Bytes = 1000, 1<<30
			c.HardMaxEntries, c.HardMaxBytes = 1000, 1
			c.MaxShards, c.TailBudgetBytes = 1, 1<<30
		})
		require.NoError(t, e.add(t, mkCreate(ns, "a", "r1")))
		require.Error(t, e.add(t, mkCreate(ns, "b", "r2")))
		require.Equal(t, []string{walmetrics.LimitBytes}, e.tagged("wal_backpressure_refusals", walmetrics.TagLimit))
	})

	t.Run("an unresolved drain", func(t *testing.T) {
		e := unresolvedEnv(t, wmAnswer{err: errUnreachable})
		require.Error(t, e.add(t, mkCreate(ids())))
		require.Equal(t, []string{walmetrics.LimitUnresolved}, e.tagged("wal_backpressure_refusals", walmetrics.TagLimit))
	})

	// The rule is asked twice — off the mirror before the request is queued, and
	// again inside the loop — but the counter counts writes, so only the check
	// that answers may emit.
	t.Run("once per refused write, not once per check", func(t *testing.T) {
		e := newEnv(t, func(c *Config) {
			c.Mutations, c.Bytes = 1000, 1<<30
			c.HardMaxEntries, c.HardMaxBytes = 1, 1<<30
			c.MaxShards, c.TailBudgetBytes = 1, 1<<30
		})
		require.NoError(t, e.add(t, mkCreate(ns, "a", "r1")))
		require.Error(t, e.add(t, mkCreate(ns, "b", "r2")))
		require.Error(t, e.add(t, mkCreate(ns, "c", "r3")))
		require.Len(t, e.recorded("wal_backpressure_refusals"), 2)
	})
}

// TestAHaltIsCountedByItsClass. The two halt states are opposites: halted-lost
// is fencing working and the next owner takes over, halted-invariant is a
// divergence this process owns and nobody can pick up. A dashboard adding them
// together would page for the first and lose the second in the noise.
func TestAHaltIsCountedByItsClass(t *testing.T) {
	ns := uuid.NewString()

	t.Run("lost", func(t *testing.T) {
		e := newEnv(t, nil)
		e.apply.errs = []error{&p.ShardOwnershipLostError{ShardID: int32(testShard), Msg: "fenced"}}
		require.NoError(t, e.add(t, mkCreate(ns, "a", "r1")))
		require.Error(t, e.c.drainNow(t.Context()))
		require.Equal(t, []string{StateHaltedLost.String()}, e.tagged("wal_halts", walmetrics.TagState))
	})

	t.Run("invariant", func(t *testing.T) {
		e := newEnv(t, nil)
		e.apply.errs = []error{&p.WorkflowConditionFailedError{Msg: "stale"}}
		require.NoError(t, e.add(t, mkCreate(ns, "a", "r1")))
		require.Error(t, e.c.drainNow(t.Context()))
		require.Equal(t, []string{StateHaltedInvariant.String()}, e.tagged("wal_halts", walmetrics.TagState))
	})

	// The state is terminal, so the counter must not tick again for every later
	// call the halt refuses.
	t.Run("once", func(t *testing.T) {
		e := newEnv(t, nil)
		e.apply.errs = []error{&p.ShardOwnershipLostError{ShardID: int32(testShard), Msg: "fenced"}}
		require.NoError(t, e.add(t, mkCreate(ns, "a", "r1")))
		require.Error(t, e.c.drainNow(t.Context()))
		require.Error(t, e.add(t, mkCreate(ns, "b", "r2")))
		require.Error(t, e.c.drainNow(t.Context()))
		require.Len(t, e.recorded("wal_halts"), 1)
	})
}

// TestASyncConditionFailureIsCountedAndIsNotADrain: a condition failure sync
// mode answers is expected traffic — a start racing a start — so it must not
// look like a halt, and it did not commit, so it must not look like a drain.
func TestASyncConditionFailureIsCountedAndIsNotADrain(t *testing.T) {
	ns := uuid.NewString()
	e := newEnv(t, func(c *Config) { c.Sync = true })
	e.apply.errs = []error{&p.WorkflowConditionFailedError{Msg: "stale"}}

	require.Error(t, e.add(t, mkCreate(ns, "a", "r1")))

	require.Len(t, e.recorded("wal_answered_condition_failures"), 1)
	require.Empty(t, e.recorded("wal_drains"), "an answered condition failure is not a committed drain")
	require.Empty(t, e.recorded("wal_halts"), "and it is not a halt")

	// The one moment the two tail series part company, and therefore the only
	// place a test can tell them apart: the entry is settled, so the tail I10
	// bounds stops counting it, while the watermark stays where the cold store
	// put it because trim goes there.
	require.Equal(t, int64(0), last(values(e.recorded("wal_tail_entries"))),
		"a settled entry is not in the tail the bound reads")
	require.Equal(t, int64(1), last(values(e.recorded("wal_unapplied_entries"))),
		"and it is still ahead of the watermark, which is what trim may not pass")
}

// TestTrimsAreCountedByOutcome. A failed trim halts nothing and is retried at
// the next cadence, so this counter is the only way it is visible from outside
// the process.
func TestTrimsAreCountedByOutcome(t *testing.T) {
	ns := uuid.NewString()
	e := newEnv(t, func(c *Config) { c.Sync = true; c.TrimEvery = 1; c.TrimAfter = time.Nanosecond })
	e.log.OnTrim(waltest.Always(errors.New("the log is unreachable")))

	require.NoError(t, e.add(t, mkCreate(ns, "a", "r1")))
	e.c.Retire() // waits for the trim goroutine

	require.Equal(t, []string{walmetrics.TrimStarted, walmetrics.TrimFailed}, e.tagged("wal_trims", walmetrics.TagOutcome))
}

func values(recs []*metricstest.CapturedRecording) []any {
	out := make([]any, 0, len(recs))
	for _, r := range recs {
		out = append(out, r.Value)
	}
	return out
}

func last[T any](s []T) T {
	var zero T
	if len(s) == 0 {
		return zero
	}
	return s[len(s)-1]
}

// TestADedupCollisionIsCountedAndNothingElseIs: a collision counts the dedup
// safety net's usage rather than an error — the two sources are disjoint by
// construction — so a page that merged cleanly must emit nothing here. Only the
// shard's own goroutine can know what a page found in both sources; the wrapper
// counts pages routed.
func TestADedupCollisionIsCountedAndNothingElseIs(t *testing.T) {
	ns := uuid.NewString()
	e := newEnv(t, func(c *Config) { c.Mutations = 1 << 20 })
	cold := coldtasks.New()
	cold.Hold(tasks.CategoryTransfer, immediate(10), immediate(30))

	e.coldWorkflow("wf", "run", 1)
	require.NoError(t, e.add(t, mkTasks(ns, "wf", "run", 2, map[tasks.Category][]p.InternalHistoryTask{
		tasks.CategoryTransfer: {immediate(20)},
	})))
	minKey, maxKey := immediateRange()

	// A clean merge: three keys, two sources, nothing shared.
	paginate(t, e.c, cold.Read, taskReq(tasks.CategoryTransfer, minKey, maxKey, 100))
	require.Empty(t, e.recorded("wal_merged_task_collisions"),
		"a page whose two sources were disjoint counted a collision")

	// The key the window and the cold store both hold.
	cold.Hold(tasks.CategoryTransfer, immediate(20))
	paginate(t, e.c, cold.Read, taskReq(tasks.CategoryTransfer, minKey, maxKey, 100))
	require.Len(t, e.recorded("wal_merged_task_collisions"), 1)
	require.Equal(t, 1, e.c.Stats().TaskCollisions)
}
