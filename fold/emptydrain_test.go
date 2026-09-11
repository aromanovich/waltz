package fold_test

// What a drain of an empty batch still owes the entries behind it. A batch is
// empty exactly where its window folded nothing, with one exception named
// below, and Batch.Settles is where that lives: an empty batch carries no
// transaction, so the entries it folded are settled off its answer or never,
// and their bytes are charged against I10 until they are.
//
// The kind axis is driven off mutation.KindCount rather than off the table's
// own rows, the kinds.go idiom: a ninth kind fails here by name instead of
// being judged by nobody.

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/aromanovich/waltz/fold"
	"github.com/aromanovich/waltz/mutation"
)

// foldedWindow is one window driven through a fresh accumulator, ending in the
// kind the row is about.
type foldedWindow struct {
	name string
	// window is the whole prefix, not the mutation alone: three of the kinds
	// fold into a window rather than emit on their own.
	window []mutation.Mutation
}

// kind is what the window ends in, which is the kind the row is about.
func (w foldedWindow) kind() mutation.Kind { return w.window[len(w.window)-1].Kind() }

// foldedWindows covers every kind, and covers twice each of the three whose
// folds have a shape a reader will suspect of emitting nothing — fold's three
// no-op paths, the ones an empty batch over a non-empty window would come from
// if it came from anywhere but the shape below.
var foldedWindows = []foldedWindow{
	{
		name:   "create",
		window: []mutation.Mutation{mkCreate(runX)},
	},
	{
		name:   "update",
		window: []mutation.Mutation{mkUpdate(runX, 2)},
	},
	{
		name:   "conflict-resolve",
		window: []mutation.Mutation{mkConflictResolve(runX, 2)},
	},
	{
		name:   "set",
		window: []mutation.Mutation{mkSet(runX, 2)},
	},
	{
		name:   "delete",
		window: []mutation.Mutation{mkDelete(runX)},
	},
	{
		// The idempotent no-op: deleting an absent row succeeds sequentially
		// too, so the second delete returns having appended nothing — the
		// no-op that looks most like a window folding to nothing, and the
		// first delete is why it is not.
		name:   "delete/of a run the window already tombstoned",
		window: []mutation.Mutation{mkDelete(runX), mkDelete(runX)},
	},
	{
		name:   "delete-current",
		window: []mutation.Mutation{mkCreate(runX), mkDeleteCurrent(runX)},
	},
	{
		// The guarded no-op: the run named is not the one the window wrote the
		// current row for, so this appends no pending request of its own.
		name:   "delete-current/naming another run",
		window: []mutation.Mutation{mkCreate(runX), mkDeleteCurrent(runY)},
	},
	{
		name:   "add-tasks",
		window: []mutation.Mutation{mkAddTasks(keyed(1, "a"))},
	},
	{
		// Alone, so there is nothing in the window for the range to sweep: the
		// range itself is the work, and it stays in TaskWork.Delete until a
		// drain applies it.
		name:   "range-complete-tasks",
		window: []mutation.Mutation{mkRangeComplete(0, 10)},
	},
	{
		// And with every row it could sweep swept, which is the shape that
		// looks like it folds to nothing and does not.
		name:   "range-complete-tasks/sweeping the whole window",
		window: []mutation.Mutation{mkAddTasks(keyed(1, "a")), mkRangeComplete(0, 10)},
	},
}

// TestAWindowThatFoldedAnythingSettlesWhatItAcked is the invariant in the
// direction that costs bytes: a window whose entries are acked and which
// settles nothing leaves those bytes charged against I10 forever.
func TestAWindowThatFoldedAnythingSettlesWhatItAcked(t *testing.T) {
	for _, w := range foldedWindows {
		t.Run(w.name, func(t *testing.T) {
			a := fold.New(shard)
			add(t, a, w.window...)

			batch := a.Drain()
			seqno, ok := batch.Settles()
			require.True(t, ok, "this window folded %d mutation(s) and settles nothing: the "+
				"entries behind it are acked, and their bytes are charged against I10 until "+
				"somebody settles them", len(w.window))
			require.Positive(t, seqno, "the position settled is one of the window's own seqnos")
			require.LessOrEqual(t, int(seqno), len(w.window),
				"and never above its last, which would settle an entry this drain did not carry")
			require.False(t, batch.Empty(),
				"and it drains a transaction too — the one shape that does not is below")
		})
	}
}

// TestAWindowThatFoldedNothingSettlesNothing is the other direction, and the
// dangerous one: drainNow at shutdown, a read under DrainOnRead and replay's
// terminal drain all reach a drain with nothing in the window, and a position
// taken from such a batch is zero — which would report every entry ever acked
// as unsettled and turn I10 into a shard that refuses every write.
func TestAWindowThatFoldedNothingSettlesNothing(t *testing.T) {
	seqno, ok := fold.New(shard).Drain().Settles()
	require.False(t, ok, "an untouched window acked nothing, so it has no position to settle")
	require.Zero(t, seqno)
}

// TestEveryKindIsInTheFoldedWindowTable is what makes a ninth kind fail by name
// rather than pass unjudged, every other kind being held to the invariant above.
func TestEveryKindIsInTheFoldedWindowTable(t *testing.T) {
	covered := map[mutation.Kind]bool{}
	for _, w := range foldedWindows {
		covered[w.kind()] = true
	}
	for k := mutation.KindInvalid + 1; int(k) < mutation.KindCount; k++ {
		require.Truef(t, covered[k], "no window ends in %s: every other kind is held to "+
			"\"a window that folded anything settles what it acked\" and this one is not", k)
	}
}

// TestAnAddHistoryTasksWithNoRowsIsTheOneMutationThatFoldsToNothing is the
// exception, and it is an exception rather than a bug because nothing builds
// one: every shardContext.AddTasks call site in the server fills the map from
// at least one task, and this repo's own generator declines to build the shape
// at all (internal/verify/mutgen's emitAddTasks, which says why there).
//
// It is the whole reason Settles is not Empty answered backwards: this batch
// carries no transaction and still owes its entry a position, because the fold
// marked the task seqno the entry occupied.
func TestAnAddHistoryTasksWithNoRowsIsTheOneMutationThatFoldsToNothing(t *testing.T) {
	a := fold.New(shard)
	add(t, a, mkAddTasks())

	batch := a.Drain()
	require.True(t, batch.Empty(), "an AddHistoryTasks with no rows in it writes nothing")

	seqno, ok := batch.Settles()
	require.True(t, ok, "and the window still folded the entry")
	require.EqualValues(t, 1, seqno, "at the seqno the entry occupied")
}
