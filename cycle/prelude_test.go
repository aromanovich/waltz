package cycle

// Where each step of [Cycle.prelude] sits relative to the others. The steps
// themselves are tested through the reads that run them; what is here is the
// order, which every read shares and none of them states.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"

	"github.com/aromanovich/waltz/verify/coldtasks"
	"github.com/aromanovich/waltz/wal"
	"github.com/aromanovich/waltz/wal/waltest"
)

// TestAReadTheHaltRulePassesThroughIsStillCounted: the count sits between the
// gate and the halt rule, so passing a read to the cold store does not hide it
// from the counters. A witness reads Reads as "reads this shard answered", and
// a mode whose reads are all passed through would otherwise report none.
func TestAReadTheHaltRulePassesThroughIsStillCounted(t *testing.T) {
	e := newEnv(t, func(c *Config) { c.Mutations = 1 })
	ns, wf, run := ids()
	require.NoError(t, e.add(t, mkCreate(ns, wf, run)))
	e.log.OnAppend(waltest.Once(wal.ErrFenced))
	require.Error(t, e.add(t, mkUpdate(ns, wf, run, 2)))
	require.Equal(t, StateHaltedLost, e.c.State())

	base := &coldStore{info: "cold"}
	_, err := e.c.getWorkflowExecution(context.Background(), getExec(ns, wf, run), base.read)
	require.NoError(t, err)
	require.Equal(t, 1, base.asked(), "the halt rule passed it through")
	require.Equal(t, 1, e.c.Stats().Reads)
}

// TestAReadWhoseGateFailsIsNotCounted is the other side of that position: the
// gate runs first, and a read it fails never took a view, so counting it would
// count a read of a window nobody looked at.
func TestAReadWhoseGateFailsIsNotCounted(t *testing.T) {
	ctx := context.Background()
	log := newLog()

	first := takeShard(t, log, 7, nil)
	ns, wf, run := ids()
	require.NoError(t, first.c.write(ctx, mkCreate(ns, wf, run), coldRows()))
	first.c.Retire()

	log.OnRead(waltest.Once(errUnreachable))
	c := zombie(t, log, 8)

	_, err := c.getWorkflowExecution(ctx, getExec(ns, wf, run),
		func(context.Context) (*p.InternalGetWorkflowExecutionResponse, error) {
			return &p.InternalGetWorkflowExecutionResponse{}, nil
		})
	require.Error(t, err)
	require.Equal(t, StateRunning, c.State(), "the gate failed without halting")
	require.Zero(t, c.Stats().Reads)
}

// TestATaskPageIsCountedEvenWhenItsGateFails is the asymmetry the task read
// keeps deliberately: TaskReads counts pages routed, so it is raised before the
// gate rather than through [Cycle.prelude]'s takeView. Moving it inside would make
// a page this shard was asked for and could not serve invisible.
func TestATaskPageIsCountedEvenWhenItsGateFails(t *testing.T) {
	ctx := context.Background()
	log := newLog()

	first := takeShard(t, log, 7, nil)
	ns, wf, run := ids()
	require.NoError(t, first.c.write(ctx, mkCreate(ns, wf, run), coldRows()))
	first.c.Retire()

	log.OnRead(waltest.Once(errUnreachable))
	c := zombie(t, log, 8)

	minKey, maxKey := immediateRange()
	_, err := c.getHistoryTasks(ctx, taskReq(tasks.CategoryTransfer, minKey, maxKey, 100), coldtasks.New().Read)
	require.Error(t, err)
	require.Equal(t, StateRunning, c.State())
	require.Equal(t, 1, c.Stats().TaskReads)
	require.Zero(t, c.Stats().Reads, "and it is not one of the two that hold a view")
}
