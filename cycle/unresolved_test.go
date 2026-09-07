package cycle

// A drain whose transaction outcome could not be read, and what the cycle may
// do while that stands. The window is gone and its entries are acked; whether
// the cold store holds them is a question only that store's own watermark
// answers, and until it does, nothing here may act as though it had.

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/service/history/tasks"

	"github.com/aromanovich/waltz/internal/verify/coldtasks"
	"github.com/aromanovich/waltz/wal"
)

// unresolvedEnv drives a cycle to the state this file is about: two writes, one
// drain whose Apply is ambiguous, and a watermark read that fails with it. The
// cycle is left running with an unresolved drain at seqno 2.
//
// next is what the store answers when it is asked again, after the two reads
// this takes: the start's floor read and the resolve that failed.
func unresolvedEnv(t *testing.T, next wmAnswer) *env {
	t.Helper()
	e := newEnv(t, func(c *Config) { c.Mutations = 2 })
	e.mark.answers = []wmAnswer{{}, {err: errUnreachable}, next}
	e.apply.errs = []error{errUnreachable}

	ns, wf, run := ids()
	e.coldWorkflow(wf, run, 0)
	require.NoError(t, e.add(t, mkUpdate(ns, wf, run, 1)))
	require.Error(t, e.add(t, mkUpdate(ns, wf, run, 2)), "the drain the second write trips has no outcome")

	require.Equal(t, StateRunning, e.c.State(), "a watermark that could not be read is not an answer, and not a halt")
	s := e.c.Stats()
	require.Equal(t, 2, s.TailEntries, "the entries are acked, unapplied, and this node's problem")
	require.EqualValues(t, 0, s.AppliedSeqno)
	return e
}

// TestNothingCommitsOverAnUnresolvedDrain is what the floor is for, and the
// reason is at the stalled field of [tailstate.Tail]: a transaction landing on
// top of an unresolved one has the cold store claim that one's entries too.
func TestNothingCommitsOverAnUnresolvedDrain(t *testing.T) {
	e := unresolvedEnv(t, wmAnswer{err: errUnreachable})

	// The store takes writes again — only its watermark is still unreadable.
	requireRefusal(t, e.add(t, mkCreate(ids())))

	require.Error(t, e.c.drainNow(context.Background()), "and a drain asked for outright re-asks and refuses too")
	require.Len(t, e.apply.drains, 1, "no second transaction ran")
	require.EqualValues(t, 0, e.c.Stats().AppliedSeqno, "so the watermark is where the cold store put it")
	require.Len(t, e.entries(t), 2, "the refused write consumed no seqno")
}

// TestAnAnsweredWatermarkResolvesTheTail: the drain had committed, and the
// answer is what settles it. Its bytes leave with it — they are released by the
// resolution rather than by a settle that never comes, which is the tail
// reporting debt no window will ever hand over.
func TestAnAnsweredWatermarkResolvesTheTail(t *testing.T) {
	e := unresolvedEnv(t, wmAnswer{seqno: 2, found: true})
	require.NotZero(t, e.c.Stats().TailBytes)

	require.NoError(t, e.c.drainNow(context.Background()))
	s := e.c.Stats()
	require.EqualValues(t, 2, s.AppliedSeqno, "the drain had committed after all")
	require.Zero(t, s.TailEntries)
	require.Zero(t, s.TailBytes, "and the bytes it held are not the cycle's for the rest of its life")

	require.NoError(t, e.add(t, mkCreate(ids())), "a resolved cycle takes writes again")
}

// TestTheAgeTickResolvesAStalledTail: a cycle in this state refuses its
// writers, so no write arrives to bring a drain with it, and the window a drain
// would otherwise be triggered by is empty. A shard whose recovery needed a
// write it was refusing would never recover at all.
func TestTheAgeTickResolvesAStalledTail(t *testing.T) {
	e := unresolvedEnv(t, wmAnswer{seqno: 2, found: true})
	require.Zero(t, e.c.Stats().Mutations, "the window is empty: there is nothing here to age out")

	e.advance(t, 2*e.cfg.Age)

	require.EqualValues(t, 2, e.c.Stats().AppliedSeqno, "the tick asked, and nothing else could have")
}

// TestAStalledCycleAnswersNoReadEither: the stalled drain emptied its window
// when it started, and whether the cold store took its rows is exactly what
// could not be read — so a merge over that store is not staleness but a write
// undone. A mutable-state read would hand back a row older than the version its
// own writer was acked, and a task page would come back short that drain's
// tasks to the one caller that completes the range it read.
//
// Both are refused with the refusal a write meets, because this state heals:
// what the caller is owed is "ask again" and not a failover.
func TestAStalledCycleAnswersNoReadEither(t *testing.T) {
	e := unresolvedEnv(t, wmAnswer{seqno: 2, found: true})
	cold := coldtasks.New()
	cold.Hold(tasks.CategoryTransfer, immediate(10))

	got := askAllThree(t, e.c, cold)
	requireRefusal(t, got.exec)
	requireRefusal(t, got.current)
	requireRefusal(t, got.tasks)
	require.Zero(t, cold.Calls, "and a refused page must not reach the store below either")
	require.Equal(t, StateRunning, e.c.State(), "a refused read is not a halt")

	// The age tick is what asks, a stalled cycle having refused every writer
	// that might have brought a drain with it.
	e.advance(t, 2*e.cfg.Age)
	require.EqualValues(t, 2, e.c.Stats().AppliedSeqno)

	again := askAllThree(t, e.c, cold)
	require.NoError(t, again.exec, "one readable watermark, and the shard answers again")
	require.NoError(t, again.current)
	require.NoError(t, again.tasks)
	require.NotZero(t, cold.Calls)
}

// TestAWatermarkBelowAnUnresolvedDrainHalts is the other answer, and the one
// the log is kept for: the drain provably did not commit, its window was
// drained and its requests driven, so re-driving them would build a transaction
// out of mutated state. The entries stay in the log for the next owner.
func TestAWatermarkBelowAnUnresolvedDrainHalts(t *testing.T) {
	e := unresolvedEnv(t, wmAnswer{seqno: 1, found: true})

	require.Error(t, e.c.drainNow(context.Background()))
	require.Equal(t, StateHaltedInvariant, e.c.State())
	require.Len(t, e.entries(t), 2, "the evidence is the log")

	// The ambiguity the drain was told rides into the halt: it is the only
	// record of why the outcome was unreadable in the first place.
	_, err := e.c.getWorkflowExecution(context.Background(), getExec(ids()), nil)
	require.ErrorIs(t, err, errUnreachable)
}

// TestAnEntryWhoseRecoveryDrainFailedIsStillFolded: the fold refuses, the drain
// that would make room for the mutation fails, and the entry is left acked and
// durable and in no window at all. Nothing would ever carry it — a later drain
// would move the watermark past its seqno, and the trim behind that watermark
// would take it out of the log: a mutation acked to its caller and applied
// nowhere.
func TestAnEntryWhoseRecoveryDrainFailedIsStillFolded(t *testing.T) {
	e := newEnv(t, nil)
	e.mark.answers = []wmAnswer{{}, {err: errUnreachable}, {seqno: 1, found: true}}
	e.apply.errs = []error{errUnreachable}
	ns, wf, run := ids()

	require.NoError(t, e.add(t, mkCreate(ns, wf, run)))
	// A continue-as-new out of a run whose window state is a snapshot: the fold
	// refuses it, which force-drains — and that drain comes back ambiguous.
	require.Error(t, e.add(t, mkContinueAsNew(ns, wf, run, uuid.NewString(), 2)))

	require.Len(t, e.entries(t), 2, "the refused mutation is acked: it was appended before it was folded")
	s := e.c.Stats()
	require.Equal(t, 1, s.Mutations, "and it heads the window the failed drain left empty")
	require.Equal(t, 2, s.TailEntries)

	// The first drain had committed, so the second one carries the entry the
	// first could not hold.
	require.NoError(t, e.c.drainNow(context.Background()))
	require.Equal(t, []wal.Seqno{1, 2}, e.apply.seqnos)
	s = e.c.Stats()
	require.EqualValues(t, 2, s.AppliedSeqno)
	require.Zero(t, s.TailBytes)
}
