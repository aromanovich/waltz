package cycle

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"

	"github.com/aromanovich/waltz/internal/verify/basetest"
)

// TestAWatermarkThatMovesReachesARunningCycle is why [Policy] is a function:
// the window changes under a shard this node is already holding. The last two
// assertions are the half easy to lose — a re-acquire is a fence, a replay and
// a transaction per shard, so a drain that arrived after one would prove the
// number reached the cycle and none of what it cost.
func TestAWatermarkThatMovesReachesARunningCycle(t *testing.T) {
	ctx := context.Background()

	var window atomic.Int64
	window.Store(100)

	static := Defaults()
	static.Sync = false
	// A clock that never advances, so the age watermark cannot be what drains
	// this window.
	static.timeSource = clock.NewEventTimeSource()

	store := basetest.New()
	ap := &fakeApplier{store: store}
	m, err := NewManager(testDeps(newLog(), ap), Live(static, Moving{Mutations: func() int { return int(window.Load()) }}))
	require.NoError(t, err)
	t.Cleanup(func() { m.Close(ctx) })

	require.NoError(t, m.ShardAcquired(ctx, testShard, 7))
	c := m.Shard(testShard)

	ns, wf, run := ids()
	require.NoError(t, c.write(ctx, mkCreate(ns, wf, run), store.Rows()))
	require.NoError(t, c.write(ctx, mkUpdate(ns, wf, run, 2), store.Rows()))
	require.Empty(t, ap.drains, "two mutations is nowhere near a window of a hundred")

	// The change a deployment makes: one number in the dynamic config.
	window.Store(1)

	require.NoError(t, c.write(ctx, mkUpdate(ns, wf, run, 3), store.Rows()))
	require.Len(t, ap.drains, 1,
		"the write drained at the window in force, not at the one the cycle was created with")

	require.Same(t, c, m.Shard(testShard), "the same cycle answered: nothing was re-acquired")
	require.Equal(t, 1, m.Totals().Epochs, "and no second epoch was ever created")
}

// TestFixedIsTheCompletePolicy: every [Policy] call must answer a filled
// [Config], and only [Fixed] and [Live] can fill one — a source built by hand
// outside this package would hand a cycle a nil clock, four zero bounds and an
// age its loop would spin on.
func TestFixedIsTheCompletePolicy(t *testing.T) {
	c := Fixed(Config{Mutations: 4})()
	require.NotNil(t, c.timeSource, "a cycle with no clock is a cycle that panics on its first tick")
	require.Equal(t, Defaults().Age, c.Age, "and one with no age arms its timer at zero, forever")
	require.Equal(t, Defaults().HardMaxEntries, c.HardMaxEntries)
	require.Equal(t, Defaults().HardMaxBytes, c.HardMaxBytes)
	require.Equal(t, Defaults().MaxShards, c.MaxShards)
	require.Equal(t, Defaults().TailBudgetBytes, c.TailBudgetBytes)
	require.Equal(t, 4, c.Mutations, "and what the caller did say is not defaulted over")

	// The four whose zero is a reading rather than a hole, so filling them would
	// take a configuration away instead of completing one.
	zeroed := Fixed(Config{})()
	require.Zero(t, zeroed.Mutations, "a size watermark of zero drains every write")
	require.Zero(t, zeroed.Bytes)
	require.Zero(t, zeroed.TrimEvery, "a trim cadence of zero trims at every drain")
	require.Zero(t, zeroed.TrimAfter)
}

// TestTheAnswerIsFilledAndNotJustTheStaticHalf: [Live]'s getters read keys an
// operator sets while the node runs, so a zero arriving from one of them is not
// a literal somebody wrote — it is a config edit, and for the age it is every
// held shard's goroutine at a whole CPU with nothing to restart. Filling the
// static half at construction cannot reach it.
func TestTheAnswerIsFilledAndNotJustTheStaticHalf(t *testing.T) {
	live := Live(Defaults(), Moving{
		Age:       func() time.Duration { return 0 },
		Mutations: func() int { return 0 },
		TrimEvery: func() int { return 0 },
	})

	require.Equal(t, Defaults().Age, live().Age,
		"the age has no reading at zero, so the source cannot answer one")
	require.Zero(t, live().Mutations,
		"the size watermark does, so the source can: drain every write")
	require.Zero(t, live().TrimEvery, "and so does the trim cadence: trim at every drain")

	// A source that moves is still read at every call, not frozen by the fill.
	var age atomic.Int64
	moving := Live(Defaults(), Moving{Age: func() time.Duration { return time.Duration(age.Load()) }})
	require.Equal(t, Defaults().Age, moving().Age)
	age.Store(int64(90 * time.Second))
	require.Equal(t, 90*time.Second, moving().Age)
}

// TestLiveLeavesWhatItIsNotGiven: a [Moving] getter nobody supplied falls back
// to the static value and never to the zero of its type, which would be a node
// draining on every write.
func TestLiveLeavesWhatItIsNotGiven(t *testing.T) {
	static := Defaults()
	static.Mutations, static.TrimAfter = 64, time.Minute

	c := Live(static, Moving{TrimEvery: func() int { return 9 }})()
	require.Equal(t, 9, c.TrimEvery, "what the source carries comes from the source")
	require.Equal(t, 64, c.Mutations, "and what it does not is the policy the node was built with")
	require.Equal(t, time.Minute, c.TrimAfter)
	require.Equal(t, Defaults().Bytes, c.Bytes)
}
