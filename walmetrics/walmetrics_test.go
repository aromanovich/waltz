package walmetrics_test

// The emitter's own contract, which nothing judged before: every other test of
// these numbers drives a component and reads what came out, so it sees the
// combinations that component happens to produce. The two pairs below exist to
// be read together, and each is a pair rather than a ratio for one reason —
// "everything was dropped" has to stay distinguishable from "there was nothing
// to drop", which a pre-divided share cannot say. That is a claim about which
// of the four combinations emit, and a run that never produces the lopsided
// ones cannot make it.

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/metrics/metricstest"

	"github.com/aromanovich/waltz/walmetrics"
)

// recorded is the values one series took, in the order they were recorded.
func recorded(t *testing.T, c *metricstest.Capture, name string) []int64 {
	t.Helper()
	var out []int64
	for _, r := range c.Snapshot()[name] {
		v, ok := r.Value.(int64)
		require.Truef(t, ok, "%s recorded a %T, not an int64", name, r.Value)
		out = append(out, v)
	}
	return out
}

func capture(t *testing.T) (*walmetrics.Emitter, *metricstest.Capture) {
	t.Helper()
	h := metricstest.NewCaptureHandler()
	c := h.StartCapture()
	t.Cleanup(func() { h.StopCapture(c) })
	return walmetrics.New(h), c
}

// TestEachHalfOfAPairIsEmittedOnItsOwnCount is the four combinations of both
// pairs. The interesting rows are the lopsided ones: a drain whose range
// deletes took every task it held and wrote none reports a total drop, and a
// replay that dropped every entry it read reports a total drop too. Gating
// either count on the other's makes exactly those two report nothing, which
// reads as a shard that dropped nothing at all.
func TestEachHalfOfAPairIsEmittedOnItsOwnCount(t *testing.T) {
	type pair struct {
		name          string
		emit          func(*walmetrics.Emitter, int, int)
		first, second string
	}
	pairs := []pair{{
		name:   "I7's task accounting",
		emit:   func(e *walmetrics.Emitter, dropped, written int) { e.Tasks("transfer", dropped, written) },
		first:  "wal_dropped_tasks",
		second: "wal_written_tasks",
	}, {
		name:   "a replay's entries",
		emit:   func(e *walmetrics.Emitter, entries, dropped int) { e.Replayed(entries, dropped) },
		first:  "wal_replayed_entries",
		second: "wal_replay_dropped_entries",
	}}

	cases := []struct{ first, second int }{{3, 2}, {3, 0}, {0, 2}, {0, 0}}

	for _, p := range pairs {
		t.Run(p.name, func(t *testing.T) {
			for _, c := range cases {
				t.Run("", func(t *testing.T) {
					e, snap := capture(t)
					p.emit(e, c.first, c.second)

					want := func(n int) []int64 {
						if n == 0 {
							return nil
						}
						return []int64{int64(n)}
					}
					require.Equal(t, want(c.first), recorded(t, snap, p.first),
						"%s must carry %d whatever %s is: a count gated on its partner's "+
							"reports nothing for the shard that dropped everything",
						p.first, c.first, p.second)
					require.Equal(t, want(c.second), recorded(t, snap, p.second),
						"%s must carry %d whatever %s is", p.second, c.second, p.first)
				})
			}
		})
	}
}

// TestACollisionCountOfZeroIsNotARecording is the third counter with a gate on
// it, and its gate says the opposite thing: the two sources a merged page draws
// from are disjoint by construction, so a non-zero value means something is
// wrong and a zero is every healthy page. Recording those zeroes would put a
// series on every scrape whose only reading is "no news".
func TestACollisionCountOfZeroIsNotARecording(t *testing.T) {
	e, snap := capture(t)

	e.TaskCollisions(0)
	require.Empty(t, recorded(t, snap, "wal_merged_task_collisions"),
		"a page with no collisions is every page: recording it makes the series unreadable")

	e.TaskCollisions(2)
	require.Equal(t, []int64{2}, recorded(t, snap, "wal_merged_task_collisions"))
}

// TestTheHandoverKeepsTheFirstHandler is [walmetrics.Emitter.Use]'s whole rule.
// The server's handler arrives at the first data store factory the binary
// builds, and a binary running several services builds one per service — so an
// emitter that took the latest would send the layer's numbers wherever the last
// service's handler points, with both halves of the layer still recording
// through this one value.
func TestTheHandoverKeepsTheFirstHandler(t *testing.T) {
	first := metricstest.NewCaptureHandler()
	firstCapture := first.StartCapture()
	t.Cleanup(func() { first.StopCapture(firstCapture) })

	second := metricstest.NewCaptureHandler()
	secondCapture := second.StartCapture()
	t.Cleanup(func() { second.StopCapture(secondCapture) })

	e := walmetrics.New(nil)
	e.Use(first)
	e.Use(second)

	e.AnsweredConditionFailure()
	require.Equal(t, []int64{1}, recorded(t, firstCapture, "wal_answered_condition_failures"),
		"the handler that arrived first is the one the layer's numbers go to")
	require.Empty(t, recorded(t, secondCapture, "wal_answered_condition_failures"))
}

// TestANilHandlerIsTheNoopAndNotAPanic covers the value a component built
// before the server exists holds, and keeps for good if no handler ever
// arrives: recording through it has to be a no-op rather than a crash on a
// path that only runs in that configuration.
func TestANilHandlerIsTheNoopAndNotAPanic(t *testing.T) {
	e := walmetrics.New(nil)
	require.NotPanics(t, func() {
		e.InterceptedWrite("CreateWorkflowExecution")
		e.Tasks("transfer", 1, 1)
		e.Tail(1, 2, 3)
		e.Halt("halted-lost")
	})

	h := metricstest.NewCaptureHandler()
	c := h.StartCapture()
	t.Cleanup(func() { h.StopCapture(c) })
	e.Use(h)
	e.Halt("halted-lost")
	require.Equal(t, []int64{1}, recorded(t, c, "wal_halts"),
		"a nil handler at construction must not stop the real one from arriving")
}
