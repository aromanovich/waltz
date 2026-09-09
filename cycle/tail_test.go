package cycle

// The tail's two halves, and the claim that they cannot drift apart. The loop
// owns the tailstate.Tail; [Cycle.write] and [Cycle.stoppedRead] read the
// tailstate.Mirror from other goroutines, so a mirror left stale refuses
// writers the tail has room for, or admits ones it has not.
//
// This file drives the sites that exist. The other half — a new site writing
// the counters around the mutators — is a type error since they moved into
// tailstate, and is no longer tested here.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	p "go.temporal.io/server/common/persistence"

	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/wal/memwal"
)

// requireMirrored asserts that the loop's tail and the mirror hold the same two
// numbers. [Cycle.Stats] is answered by the loop, so it is the source.
func requireMirrored(t *testing.T, c *Cycle, after string) {
	t.Helper()
	s := c.Stats()
	entries, bytes := c.mirror.Size()
	require.EqualValues(t, s.TailEntries, entries,
		"after %s the mirror holds %d entries and the loop holds %d: a bound read off "+
			"the stale one refuses writers the tail has room for, or admits ones it has not", after, entries, s.TailEntries)
	require.EqualValues(t, s.TailBytes, bytes, "after %s the mirror's byte count is stale", after)
	require.Equal(t, s.TailEntries == 0, c.mirror.Empty(),
		"after %s the two spellings of \"is the tail empty\" disagree, which is what "+
			"decides whether a retired cycle's read may be answered from the cold store", after)
}

// TestTheMirrorFollowsEveryTailMove drives the tail moves a write path makes and
// asserts the mirror moved with each: the floor a watermark puts under it, an
// append, and three of the four settles — the committed drain (which also moves
// the watermark), sync mode's answered condition failure, and replay's dropped
// provisional entry. The fourth, a window that acked and folded to nothing, is
// the test below; a stall and its resolve move the tail too, and no test here
// reads the mirror they publish.
func TestTheMirrorFollowsEveryTailMove(t *testing.T) {
	ctx := context.Background()

	t.Run("the floor, an append and a committed drain", func(t *testing.T) {
		e := newEnv(t, nil)
		// A watermark a previous owner left, so the floor is not zero and a
		// mirror that never moved cannot pass by luck.
		e.inherit(t, 5)
		e.mark.answers = []wmAnswer{{seqno: 5, found: true}}
		ns, wf, run := ids()

		require.NoError(t, e.add(t, mkCreate(ns, wf, run)))
		require.Equal(t, 1, e.c.Stats().TailEntries, "the fixture did not leave the entry in the tail")
		requireMirrored(t, e.c, "an append over an inherited watermark")

		require.NoError(t, e.c.drainNow(ctx))
		require.Zero(t, e.c.Stats().TailEntries, "a committed drain empties the tail")
		requireMirrored(t, e.c, "a committed drain")
	})

	t.Run("sync mode's answered condition failure", func(t *testing.T) {
		e := newEnv(t, func(c *Config) { c.Sync = true })
		e.apply.errs = []error{&p.WorkflowConditionFailedError{Msg: "stale"}}
		ns, wf, run := ids()

		require.Error(t, e.add(t, mkCreate(ns, wf, run)))
		s := e.c.Stats()
		require.EqualValues(t, 1, s.CommitSeqno, "the entry was acked")
		require.EqualValues(t, 0, s.AppliedSeqno, "and nothing committed, so no watermark moved")
		requireMirrored(t, e.c, "a settled condition failure")
	})

	t.Run("a replayed provisional entry dropped", func(t *testing.T) {
		log := memwal.New()

		// Sync mode acks before the condition is verified, the drain answers
		// the caller "no", and the entry stays in the log for the next owner to
		// drop.
		first := takeShard(t, log, 7, func(c *Config) { c.Sync = true })
		first.ap.errs = []error{&p.WorkflowConditionFailedError{Msg: "stale"}}
		ns, wf, run := ids()
		require.Error(t, first.c.write(ctx, mkCreate(ns, wf, run), coldRows()))
		first.c.Retire()

		second := takeShard(t, log, 8, nil)
		second.ap.errs = []error{&p.WorkflowConditionFailedError{Msg: "stale"}}
		_, err := second.c.getCurrentExecution(ctx,
			&p.GetCurrentExecutionRequest{ShardID: int32(testShard), NamespaceID: ns, WorkflowID: wf},
			func(context.Context) (*p.InternalGetCurrentExecutionResponse, error) {
				return nil, serviceerror.NewNotFound("no current execution")
			})
		require.Error(t, err)
		require.Equal(t, 1, second.c.Stats().Dropped, "the fixture did not produce the drop it is here for")
		requireMirrored(t, second.c, "a dropped provisional entry")
	})

	t.Run("a retired cycle answers off the mirror the loop left", func(t *testing.T) {
		// [Cycle.stoppedRead] has nothing else to consult: its answer is the
		// last thing the loop published, which is sound only because publishing
		// is not a step a tail move can skip.
		e := newTailEnv(t, nil)
		ns, wf, run := ids()
		e.coldWorkflow(wf, run, 0)
		require.NoError(t, e.add(mkUpdate(ns, wf, run, 1)))
		requireMirrored(t, e.c, "an append")

		e.c.Retire()
		require.False(t, e.c.mirror.Empty(),
			"the retired cycle held an unapplied tail, and its read must not be passed through")
		_, refusal := e.c.stoppedRead(mutableStateRead, nil)
		require.Error(t, refusal, "a non-empty tail on a retired cycle is a refusal")
	})
}

// TestADrainThatFoldsToNothingStillSettlesWhatItAcked: an AddHistoryTasks with
// no rows in it is acked and folds to nothing
// (fold.TestAnAddHistoryTasksWithNoRowsIsTheOneMutationThatFoldsToNothing), so
// its drain carries no transaction — and its bytes must still leave I10's
// counter, or a shard that saw enough of them refuses every write over a tail
// nobody holds. Sync mode is what makes the window exactly this one entry.
func TestADrainThatFoldsToNothingStillSettlesWhatItAcked(t *testing.T) {
	e := newEnv(t, func(c *Config) { c.Sync = true })
	require.NoError(t, e.add(t, mutation.Mutation{AddTasks: &p.InternalAddHistoryTasksRequest{
		ShardID: int32(testShard), NamespaceID: "ns", WorkflowID: "wf",
	}}))

	s := e.c.Stats()
	require.EqualValues(t, 1, s.CommitSeqno, "the entry was acked")
	require.Zero(t, s.TailBytes, "the window's bytes are still charged against I10")
	require.Zero(t, s.TailEntries, "and the entry is still counted unsettled")
	require.EqualValues(t, 0, s.AppliedSeqno,
		"no transaction ran, so the cold store's watermark may not move")
	requireMirrored(t, e.c, "a drain whose batch was empty")
}

// TestADrainOfAnEmptyWindowSettlesNothing is the other direction of the same
// early return, and the more dangerous one: drainNow at shutdown, a read under
// DrainOnRead and replay's terminal drain all reach it with a window that folded
// nothing, which is what fold.Batch.Settles answers false for. Settling such a
// drain anyway would put resolved under the whole log and report every entry
// ever acked as unsettled, which is I10 refusing every write on the shard over
// memory nobody holds.
// Replay is where the pre-existing suite goes red without the guard
// (TestAProvisionalEntryWhoseConditionFailsIsDropped); drainNow is the cause
// this test can drive with nothing else left in the tail to mis-settle.
func TestADrainOfAnEmptyWindowSettlesNothing(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, nil)
	ns, wf, run := ids()

	require.NoError(t, e.add(t, mkCreate(ns, wf, run)))
	require.NoError(t, e.c.drainNow(ctx))
	before := e.c.Stats()
	require.EqualValues(t, 1, before.CommitSeqno, "the fixture left nothing in the log to mis-settle")
	require.EqualValues(t, 1, before.AppliedSeqno, "the committed drain moved the watermark")

	require.NoError(t, e.c.drainNow(ctx))

	s := e.c.Stats()
	require.Zero(t, s.TailEntries, "an empty window resolves nothing, so it may not move resolved")
	require.Zero(t, s.TailBytes, "nor the byte counter")
	require.Equal(t, before.CommitSeqno, s.CommitSeqno)
	require.Equal(t, before.AppliedSeqno, s.AppliedSeqno)
	requireMirrored(t, e.c, "a drain of an empty window")
}
