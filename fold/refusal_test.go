package fold_test

// AddOrDrain's recovery, and the property that makes it terminate.

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/service/history/tasks"

	"github.com/aromanovich/waltz/fold"
	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/verify/mutgen"
	"github.com/aromanovich/waltz/wal"
)

// refusalCorpus is the stream this drives: check_corpus_test.go's seed, restated
// rather than shared because that file is in package fold and this one is not.
const (
	refusalCorpusSeed      = 20260801
	refusalCorpusMutations = 20_000
)

var errDrainFailed = errors.New("the drain's transaction did not commit")

// TestAnEmptyWindowRefusesNothing is what lets the recovery retry exactly once.
// Every refusal either door raises is a refusal beside something: Add cannot
// express a mutation as a merged request next to the ones in front of it, and
// Check cannot determine an assertion the window has already discarded. A drain
// removes everything either could be beside, so the retry after one cannot
// refuse again, and a second refusal is a violation rather than a case to keep
// draining at.
//
// A refusal turning on something a drain does not reset (the seqno floor and the
// task deletion bounds both survive one) would be a loop in every consumer
// instead of a failure here.
func TestAnEmptyWindowRefusesNothing(t *testing.T) {
	cfg := mutgen.Default()
	cfg.Seed = refusalCorpusSeed
	g, err := mutgen.New(cfg)
	require.NoError(t, err)
	registry := tasks.NewDefaultTaskCategoryRegistry()

	// Encoded and decoded on the way in: what a retry folds is what came back out
	// of the log, not what went into it.
	for i := range refusalCorpusMutations {
		m, err := g.Next()
		require.NoError(t, err, "generating mutation %d", i)
		payload, err := mutation.Encode(m)
		require.NoError(t, err, "encoding mutation %d", i)
		decoded, err := mutation.Decode(payload, registry)
		require.NoError(t, err, "decoding mutation %d", i)

		empty := fold.New(wal.ShardID(cfg.ShardID))
		_, err = empty.Check(decoded)
		require.NoError(t, err,
			"an empty window determines every assertion offered to it: mutation %d (%s)", i, m.Kind())

		empty = fold.New(wal.ShardID(cfg.ShardID))
		require.NotErrorIs(t, empty.Add(wal.FirstSeqno, decoded), fold.ErrRefused,
			"an empty window has nothing for a mutation to be unfoldable beside: mutation %d (%s)", i, m.Kind())
	}
}

// TestTheRecoveryDrainsOnceAndSaysWhatItDid pins what [fold.Refusal] reports:
// that a drain happened at all, which callers count as a drain cadence, and that
// an error is the drain's own rather than fold's. Neither is recoverable from
// the error alone.
func TestTheRecoveryDrainsOnceAndSaysWhatItDid(t *testing.T) {
	// A window holding a guarded delete-current has nothing for a create's
	// current-row assertion to stand on.
	refusing := func() (*fold.Accumulator, mutation.Mutation) {
		a := fold.New(shard)
		require.NoError(t, a.Add(1, mkDeleteCurrent(runX)))
		return a, mkCreate(runY)
	}

	t.Run("a window that takes the mutation drains nothing", func(t *testing.T) {
		a := fold.New(shard)
		drains := 0
		r, err := a.AddOrDrain(1, mkCreate(runX), func() error { drains++; return nil })
		require.NoError(t, err)
		require.Equal(t, fold.Refusal{}, r, "the zero value is the ordinary case")
		require.Zero(t, drains)
	})

	t.Run("a refusal drains once and folds at the head of the fresh window", func(t *testing.T) {
		a, refused := refusing()
		drains := 0
		r, err := a.AddOrDrain(2, refused, func() error { a.Drain(); drains++; return nil })
		require.NoError(t, err, "the retry folds at the head of a fresh window")
		require.Equal(t, fold.Refusal{Drained: true}, r)
		require.Equal(t, 1, drains, "exactly one drain, and it is the refusal's")

		out := reqs(a.Drain())
		require.Len(t, out, 1)
		require.Equal(t, mutation.KindCreate, out[0].Request.Kind(),
			"and what is in the fresh window is the mutation that was refused")
	})

	t.Run("the same procedure serves the condition authority", func(t *testing.T) {
		a, refused := refusing()
		drains := 0
		_, r, err := a.CheckOrDrain(refused, func() error { a.Drain(); drains++; return nil })
		require.NoError(t, err)
		require.Equal(t, fold.Refusal{Drained: true}, r)
		require.Equal(t, 1, drains)
	})

	t.Run("a drain that fails is reported as the drain's own error", func(t *testing.T) {
		a, refused := refusing()
		attempts := 0
		r, err := a.AddOrDrain(2, refused, func() error { attempts++; return errDrainFailed })
		require.ErrorIs(t, err, errDrainFailed,
			"returned as it came, so a caller can still type-switch on it")
		require.Equal(t, fold.Refusal{Drained: true, DrainFailed: true}, r,
			"a drain failure has already classified itself, and reclassifying it as a fold "+
				"violation is the bug this distinction exists to prevent")
		require.Equal(t, 1, attempts, "and nothing is retried after it")
	})

	t.Run("a refusal with no drain to recover with is the refusal itself", func(t *testing.T) {
		a, refused := refusing()
		_, err := a.AddOrDrain(2, refused, nil)
		require.ErrorIs(t, err, fold.ErrRefused)
		require.ErrorContains(t, err, "no drain to recover with")
	})
}
