package fold

// The condition authority against a stream, catching a rule that is subtly too
// strict. Every condition in a generated stream held on the sequential path the
// generator models, so a condition failure raised on it is a legal write the
// authority rejected.
//
// The stream is driven the way cycle drives it: encoded and decoded as the log
// delivers it, through a real accumulator under a drain policy, with the check
// consulted before the append and drain-and-retry around the refusals. In
// package fold rather than fold_test because the shares come off the check's
// unexported counters.
//
// No cold store, so a delegated assertion is counted and not verified.

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/service/history/tasks"

	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/verify/mutgen"
	"github.com/aromanovich/waltz/wal"
)

// The drain policy the shares are measured at: cycle.Defaults()' window
// restated rather than imported, because cycle imports fold.
const (
	corpusWindowMutations = 256
	corpusWindowBytes     = 256 << 10
	corpusMutations       = 20_000
	corpusSeed            = 20260801
)

// corpusRun is what one stream through the authority turned out to be.
type corpusRun struct {
	window int

	mutations int
	windows   int
	// foldRefusals are fold's own ErrRefused from Add; checkRefusals are the
	// authority's, on a discarded assertion the window does not determine.
	foldRefusals  int
	checkRefusals int

	// The assertion counters, summed over the stream.
	asserted  int
	recorded  int
	evaluated int
	// delegatingMutations counts mutations that hand at least one assertion on,
	// each of which pays a cold-store read on the write path.
	delegatingMutations int
	delegatedRuns       int
	delegatedCurrents   int

	// answered counts condition failures the window raised. Zero on a valid
	// stream; the stale-writer run below is where it is not.
	answered int
	// firstAnswer and firstAt name the mutation behind the first failure.
	firstAnswer error
	firstAt     int
}

func (r corpusRun) recordedShare() float64 {
	if r.asserted == 0 {
		return 0
	}
	return 100 * float64(r.recorded) / float64(r.asserted)
}

func (r corpusRun) evaluatedShare() float64 {
	if r.asserted == 0 {
		return 0
	}
	return 100 * float64(r.evaluated) / float64(r.asserted)
}

// driveCorpus runs one stream through the accumulator with the authority in
// cycle.add's position. resend, when set, sends every mutation a second time
// after it has folded — the stale writer in its simplest form.
func driveCorpus(t *testing.T, cfg mutgen.Config, n, window int, resend bool) corpusRun {
	t.Helper()

	g, err := mutgen.New(cfg)
	require.NoError(t, err)
	registry := tasks.NewDefaultTaskCategoryRegistry()
	a := New(wal.ShardID(cfg.ShardID))

	run := corpusRun{window: window, mutations: n}
	inWindow, bytesInWindow, next := 0, 0, wal.FirstSeqno
	drain := func() {
		batch := a.Drain()
		// The biconditional emptydrain_test.go states one kind at a time, here
		// over a stream that mixes them — including the refusal drains and the
		// trailing one over an empty window. cycle.drain settles an empty batch
		// off Stats.MutationsIn, and a window that acked entries and folded to
		// nothing leaves their bytes charged against I10 forever.
		require.Equal(t, batch.Stats().MutationsIn == 0, batch.Empty(),
			"a drain is empty exactly where its window folded nothing")
		run.windows++
		inWindow, bytesInWindow = 0, 0
	}

	// offer puts one mutation through the pair cycle.add puts it through:
	// CheckOrDrain, then the seqno, then AddOrDrain. What it cannot hold is the
	// order of those two, which is the cycle's — see check.go's header for why
	// the check is before the append; the cycle owns that ordering.
	offer := func(i int, m mutation.Mutation, payload []byte) {
		t.Helper()
		del, refusal, cov, err := a.checkOrDrain(m, func() error { drain(); return nil })
		require.NotErrorIs(t, err, ErrRefused,
			"a refusal must not survive a drain: an empty window discards nothing (mutation %d)", i)
		if refusal.Drained {
			run.checkRefusals++
		}
		run.asserted += cov.asserted
		run.recorded += cov.recorded
		run.evaluated += cov.evaluated
		if del.Any() {
			run.delegatingMutations++
			run.delegatedRuns += len(del.Runs)
			if del.Current != nil {
				run.delegatedCurrents++
			}
		}
		if err != nil {
			run.answered++
			if run.firstAnswer == nil {
				run.firstAnswer, run.firstAt = err, i
			}
			return // the caller was answered: nothing is acked, nothing folds
		}

		// The seqno is the log's, not the stream's: an answered mutation
		// consumes none, and a re-sent one is a second entry.
		seqno := next
		next++
		folded, err := a.AddOrDrain(seqno, m, func() error { drain(); return nil })
		require.NoError(t, err,
			"mutation %d of seed %d did not fold, and a stream the authority admitted must", i, cfg.Seed)
		if folded.Drained {
			run.foldRefusals++
		}
		inWindow++
		bytesInWindow += len(payload)
		if inWindow >= window || bytesInWindow >= corpusWindowBytes {
			drain()
		}
	}

	for i := range n {
		m, err := g.Next()
		require.NoError(t, err, "generating mutation %d of seed %d", i, cfg.Seed)
		payload, err := mutation.Encode(m)
		require.NoError(t, err, "encoding mutation %d of seed %d", i, cfg.Seed)
		// Decoded twice when resending: the accumulator merges requests in place
		// and owns what it is handed, so a duplicate must come off the bytes.
		decoded, err := mutation.Decode(payload, registry)
		require.NoError(t, err, "decoding mutation %d of seed %d", i, cfg.Seed)

		offer(i, decoded, payload)
		if resend {
			again, err := mutation.Decode(payload, registry)
			require.NoError(t, err)
			offer(i, again, payload)
		}
	}
	drain()
	return run
}

func corpusConfig() mutgen.Config {
	cfg := mutgen.Default()
	cfg.Seed = corpusSeed
	cfg.ShardID = 7
	// The locality cap: without it the workflow pool outgrows the window, two
	// touches of one workflow almost never land in the same drain, and the
	// shares measure the pool rather than the rule.
	cfg.Workflows = 32
	return cfg
}

// TestTheAuthorityRefusesNothingAValidStreamContains: every mutation the
// generator produces is one the sequential path acked, so any condition failure
// raised on it is a false refusal.
func TestTheAuthorityRefusesNothingAValidStreamContains(t *testing.T) {
	cfg := corpusConfig()
	run := driveCorpus(t, cfg, corpusMutations, corpusWindowMutations, false)

	t.Logf("\n=== %d mutations at Workflows=%d WorkflowReuse=%.2f, window %d\n"+
		"    %d windows (%d fold refusals, %d check refusals)\n"+
		"    %d assertions carried — %d recorded (%.1f%%), %d evaluated (%.1f%%)\n"+
		"    %d mutations delegate at least one assertion (%.1f%%): %d run rows, %d current rows",
		run.mutations, cfg.Workflows, cfg.WorkflowReuse, run.window,
		run.windows, run.foldRefusals, run.checkRefusals,
		run.asserted, run.recorded, run.recordedShare(), run.evaluated, run.evaluatedShare(),
		run.delegatingMutations, 100*float64(run.delegatingMutations)/float64(run.mutations),
		run.delegatedRuns, run.delegatedCurrents)

	require.Zero(t, run.answered,
		"the authority refused a legal write: mutation %d of seed %d — %v", run.firstAt, cfg.Seed, run.firstAnswer)
	require.NotZero(t, run.evaluated,
		"nothing was evaluated against the window at all: the run proves only that the check is inert")
}

// TestTheAuthoritysCoverageIsAFunctionOfTheWindow: a window of one records
// every assertion and evaluates nothing, and the share the window answers rises
// with the window. The complement is the residual — assertions standing on the
// pre-window row, each costing the write path a read (Delegated.Settle).
func TestTheAuthoritysCoverageIsAFunctionOfTheWindow(t *testing.T) {
	cfg := corpusConfig()
	n := corpusMutations / 4

	var shares []float64
	for _, window := range []int{1, 8, 64, 256} {
		run := driveCorpus(t, cfg, n, window, false)
		shares = append(shares, run.evaluatedShare())
		t.Logf("window %4d: %6d assertions — %5.1f%% recorded, %5.1f%% evaluated; "+
			"%5.1f%% of mutations delegate (%d run rows, %d current rows)",
			window, run.asserted, run.recordedShare(), run.evaluatedShare(),
			100*float64(run.delegatingMutations)/float64(run.mutations),
			run.delegatedRuns, run.delegatedCurrents)
		require.Zero(t, run.answered,
			"a false refusal at window %d: mutation %d — %v", window, run.firstAt, run.firstAnswer)
	}

	require.Zero(t, shares[0],
		"at a window of one the accumulator is empty at every offer: the check must be inert there")
	for i := 1; i < len(shares); i++ {
		require.Greater(t, shares[i], shares[i-1],
			"the share the window answers must rise with the window")
	}
}

// TestAStaleWriteInTheStreamIsCaught: the generator cannot produce a failing
// condition, so every mutation is sent twice — the stale writer in its simplest
// form, and the only way to exercise the answer at all.
//
// The misses are the two tombstone kinds, which carry no assertion: a delete of
// an absent row and a delete-current naming the wrong run are legal no-ops
// sequentially, and this layer may not be stricter than the store it replaces.
func TestAStaleWriteInTheStreamIsCaught(t *testing.T) {
	cfg := corpusConfig()
	n := corpusMutations / 10
	run := driveCorpus(t, cfg, n, corpusWindowMutations, true)

	t.Logf("%d mutations, each re-sent after it folded: %d answered, %d admitted "+
		"(%d assertions carried, %.1f%% evaluated by the window)",
		n, run.answered, 2*n-run.answered, run.asserted, run.evaluatedShare())

	require.NotZero(t, run.answered, "not one duplicate was answered: the authority is not looking")
	// The duplicates that carry an assertion the window determines are answered;
	// what is left is the tombstones and whatever the window had already drained
	// away by the time the copy arrived.
	require.Greater(t, run.answered, n/2,
		"most of a stream sent twice must be answered rather than folded in")
}
