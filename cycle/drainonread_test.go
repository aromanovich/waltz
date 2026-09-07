package cycle

// [Config.DrainOnRead] from the cycle's side: an attribution instrument, not a
// mode this repository ships. The arm has to be correct and not merging, in
// that order, so assertions check the answer before the counter.
//
// The file is separate so that it can be deleted with the field it drives.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"

	"github.com/aromanovich/waltz/fold"
	"github.com/aromanovich/waltz/internal/verify/coldtasks"
	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/walmetrics"
)

// TestDrainOnReadIsNotTheMeasuredPolicy: the arm is off in [Defaults], which
// every shipped path builds its config from.
func TestDrainOnReadIsNotTheMeasuredPolicy(t *testing.T) {
	require.False(t, Defaults().DrainOnRead,
		"the measured policy is the collapse; draining on read is what gives it away")
}

// TestUnderDrainOnReadAHeldRunIsAnsweredByTheColdStore: the create that
// [TestAReadIsAnsweredFromTheWindow] answers out of the window is answered
// instead by the store the read's own drain has just written to. Both halves
// are asserted — the base was asked, and what it said is what the drain put
// there.
func TestUnderDrainOnReadAHeldRunIsAnsweredByTheColdStore(t *testing.T) {
	e := newEnv(t, func(c *Config) { c.DrainOnRead = true })
	ns, wf, run := ids()
	base := &coldStore{info: "not yet applied"}
	e.apply.committed = func([]*fold.Emitted, fold.TaskWork) { base.set("what the drain applied") }

	require.NoError(t, e.add(t, mkCreate(ns, wf, run)))
	require.Empty(t, e.apply.drains, "a write still batches: the arm takes out the reads and nothing else")

	resp, err := e.c.getWorkflowExecution(context.Background(), getExec(ns, wf, run), base.read)
	require.NoError(t, err)
	require.Equal(t, 1, base.asked(), "the read went to the cold store rather than merging over the window")
	require.Equal(t, "what the drain applied", string(resp.State.ExecutionInfo.Data),
		"the store answered with what the read's own drain committed")
	require.Len(t, e.apply.drains, 1, "the read drained the window it would have merged")
	require.Equal(t, []string{walmetrics.TriggerRead}, e.tagged("wal_drains", "trigger"))

	stats := e.c.Stats()
	require.Equal(t, 1, stats.Reads)
	require.Equal(t, 1, stats.ReadsHeld,
		"the window held this run when the read arrived, which is the fact the counter is for: "+
			"counting after the drain would report an empty window in the one mode that empties it")
}

// TestUnderDrainOnReadATaskPageCarriesNothingOutOfTheWindow is the same claim
// for the task read: the page still holds both rows, and neither came out of a
// window.
func TestUnderDrainOnReadATaskPageCarriesNothingOutOfTheWindow(t *testing.T) {
	e := newEnv(t, func(c *Config) {
		c.DrainOnRead = true
		c.Mutations = 1 << 20 // nothing drains by size: the read is the trigger under test
	})
	cold := coldtasks.New()
	cold.Hold(tasks.CategoryTransfer, immediate(10))
	e.apply.committed = func(_ []*fold.Emitted, work fold.TaskWork) { cold.Commit(work.Insert) }

	require.NoError(t, e.add(t, mutation.Mutation{AddTasks: &p.InternalAddHistoryTasksRequest{
		ShardID:     int32(testShard),
		NamespaceID: "ns",
		WorkflowID:  "wf",
		Tasks:       map[tasks.Category][]p.InternalHistoryTask{tasks.CategoryTransfer: {immediate(20)}},
	}}))

	minKey, maxKey := immediateRange()
	pages := paginate(t, e.c, cold.Read, taskReq(tasks.CategoryTransfer, minKey, maxKey, 100))
	require.Equal(t, []int64{10, 20}, taskIDs(pages),
		"the window's task is still in the range that would have fired it — through the store, not through the merge")

	stats := e.c.Stats()
	require.Equal(t, 1, stats.TaskReads)
	require.Zero(t, stats.TaskReadsMerged, "no page carried a row out of the window: that is the arm")
	require.Len(t, e.apply.drains, 1)
}
