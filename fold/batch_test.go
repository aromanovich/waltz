package fold_test

// What a drained batch is true about whatever the window held. Apply drives
// these rather than checking them — a batch is only ever built here — so they
// are claims about Drain and this is where they are judged.

import (
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	p "go.temporal.io/server/common/persistence"

	"github.com/aromanovich/waltz/fold"
	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/wal"
)

// inWorkflow moves an update to another workflow of the same namespace, so a
// window can hold several and the accumulator's map has an order to lose.
func inWorkflow(id string) func(*p.InternalWorkflowMutation) {
	return func(m *p.InternalWorkflowMutation) { m.WorkflowID = id }
}

// The order is apply's: a merged request writes its folded tail state, so a
// sequentially interleaved request must land before that tail. The map the
// window keeps its workflows in has no order at all, which is what makes this
// a property of the drain rather than of the input.
func TestTheDrainEmitsItsRequestsInTailSeqnoOrder(t *testing.T) {
	a := fold.New(shard)
	// Eight workflows, each updated twice, interleaved: whichever way the map
	// ranges, the tails are 9..16 and the heads 1..8.
	var ms []mutation.Mutation
	for round := range 2 {
		for i := range 8 {
			ms = append(ms, mkUpdate(runX, int64(round+2), inWorkflow(fmt.Sprintf("wf-%d", i))))
		}
	}
	add(t, a, ms...)

	b := a.Drain()
	require.Equal(t, 8, b.Len())

	var tails []wal.Seqno
	firsts := 0
	for e := range b.Each() {
		tails = append(tails, e.TailSeqno)
		require.Less(t, e.HeadSeqno, e.TailSeqno)
		if e.FirstOfWorkflow() {
			firsts++
		}
	}
	require.True(t, slices.IsSorted(tails), "the drain emitted %v", tails)
	require.Equal(t, 8, firsts, "eight records, each named first by exactly one request")
}

// The watermark is what the drain's transaction acks, so it may not sit below
// anything that transaction applies — in either half of the batch. The task
// half is the one outside the requests' ordering: it belongs to no workflow,
// and a watermark taken from the requests alone would leave applied task rows
// above the position a replay resumes from.
func TestTheWatermarkCoversBothHalvesOfTheBatch(t *testing.T) {
	t.Run("the requests", func(t *testing.T) {
		a := fold.New(shard)
		add(t, a, mkUpdate(runX, 2), mkUpdate(runX, 3, inWorkflow("wf-2")))

		b := a.Drain()
		for e := range b.Each() {
			require.LessOrEqual(t, e.TailSeqno, b.Watermark())
		}
		require.EqualValues(t, 2, b.Watermark(), "the last seqno the window folded")
	})

	t.Run("the task work behind them", func(t *testing.T) {
		a := fold.New(shard)
		add(t, a, mkUpdate(runX, 2), mkAddTasks(keyed(1, "a")))

		b := a.Drain()
		work := b.Tasks()
		require.EqualValues(t, 2, work.TailSeqno)
		require.EqualValues(t, 2, b.Watermark(), "the task mutation is the window's last entry")
	})

	t.Run("the task work alone", func(t *testing.T) {
		a := fold.New(shard)
		add(t, a, mkAddTasks(keyed(1, "a")))

		b := a.Drain()
		require.Zero(t, b.Len())
		require.EqualValues(t, 1, b.Watermark())
	})
}

// Only a tombstone carries orphaned tasks: they are the task slot of the
// request its collapse dropped, and the Delete that collapsed it has no slot of
// its own. Apply writes them at the tombstone's arm and nowhere else.
func TestOnlyATombstoneCarriesOrphanedTasks(t *testing.T) {
	a := fold.New(shard)
	add(t, a,
		mkUpdate(runX, 2, withTask("t-upd")),
		mkDelete(runX),
		mkUpdate(runY, 2, withTask("t-other")),
	)

	orphaned := 0
	for e := range a.Drain().Each() {
		if len(e.OrphanedTasks()) == 0 {
			continue
		}
		orphaned++
		require.Equal(t, mutation.KindDelete, e.Request.Kind())
		require.Equal(t, []string{"t-upd"}, taskNames(e.OrphanedTasks()))
	}
	require.Equal(t, 1, orphaned, "the one collapse in the window")
}

// A window folds one shard's mutations and the batch says which, so apply pairs
// the drain with the shard it was asked to write rather than with the shard the
// requests happen to name.
func TestTheBatchNamesTheShardItWasFoldedFor(t *testing.T) {
	a := fold.New(shard)
	add(t, a, mkUpdate(runX, 2))
	require.Equal(t, shard, a.Drain().Shard())

	foreign := mkUpdate(runX, 2)
	foreign.Update.ShardID = int32(shard) + 1
	require.Error(t, fold.New(shard).Add(1, foreign),
		"a mutation of another shard may not enter this window at all")
}
