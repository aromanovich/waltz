package acceptance

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/aromanovich/waltz/fold"
	"github.com/aromanovich/waltz/internal/verify/drive"
	"github.com/aromanovich/waltz/internal/verify/foldrun"
	"github.com/aromanovich/waltz/internal/verify/mutgen"
	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/wal"
)

// envAcceptanceMutations overrides the stream length. A full-volume run is
// 10^6 and is set through this variable; the default is a tenth of it, which
// keeps the test inside a normal run's budget while still being an order of
// magnitude past anything the cluster-bound runs can afford.
const envAcceptanceMutations = "WAL_ACCEPTANCE_MUTATIONS"

const defaultAcceptanceMutations = 100_000

// acceptanceWindow is how many mutations fold accumulates before a drain,
// standing in for a drain policy this suite does not model: big enough for
// chains (MaxChainLength 8 at Default()) to land inside one window and merge —
// measured: 256 leaves the fold ratio at 1.30 because a chain keeps straddling
// the boundary — small enough that the accumulator's memory stays a window and
// not a stream (a window is ~0.7 MB of encoded payload at Default()'s shapes).
const acceptanceWindow = 1024

// noClusterRun is what one stream through the path turned out to be. The
// counters exist so the assertions below can say "this run exercised X", not
// "a run of this size should have".
//
// The loop's own counts are [foldrun.Run]; what is added here is what only this
// run means — the generator's report, the encoded volume, and a tombstone count
// that is KindDelete alone, which is this suite's definition of the word.
type noClusterRun struct {
	foldrun.Run
	report     mutgen.Report
	tombstones int // KindDelete requests among the emitted
	bytes      int64
}

// driveNoCluster runs one stream end to end.
func driveNoCluster(t *testing.T, cfg mutgen.Config, n int) noClusterRun {
	t.Helper()

	s, err := drive.NewStream(cfg)
	require.NoError(t, err)

	var run noClusterRun
	d := foldrun.New(wal.ShardID(cfg.ShardID), acceptanceWindow, func(batch fold.Batch) error {
		for e := range batch.Each() {
			if e.Request.Kind() == mutation.KindDelete {
				run.tombstones++
			}
		}
		return nil
	})

	require.NoError(t, s.Drive(n, func(m drive.Delivery) error {
		run.bytes += int64(len(m.Payload))
		if err := d.Add(wal.Seqno(m.Index+1), m.Mutation); err != nil {
			// Anything left after the refusal's own recovery means the stream is
			// corrupt, which is a generator bug, not fold's: the seed reproduces
			// it alone.
			return fmt.Errorf("mutation %d (%s) of seed %d did not fold: %w",
				m.Index, m.Mutation.Kind(), cfg.Seed, err)
		}
		return nil
	}))
	require.NoError(t, d.Flush())
	run.Run = d.Run()
	run.report = s.Report()
	return run
}

// TestAcceptanceFoldNoCluster is the volume run, [defaultAcceptanceMutations]
// mutations unless [envAcceptanceMutations] says otherwise, at Default()'s
// knobs — WorkflowReuse 0.80, KeyReuse 0.50, both stated in the log because the
// ratio is a function of them and reads as a constant without them. A second
// run at WorkflowReuse 0 follows for exactly that reason: it must report ratio
// 1.00, and if it ever collapses, either the knob or the report is lying.
func TestAcceptanceFoldNoCluster(t *testing.T) {
	n := defaultAcceptanceMutations
	if s := os.Getenv(envAcceptanceMutations); s != "" {
		parsed, err := strconv.Atoi(s)
		require.NoError(t, err, "%s must be a number, got %q", envAcceptanceMutations, s)
		require.Positive(t, parsed, "%s must be positive", envAcceptanceMutations)
		n = parsed
	}

	cfg := mutgen.Default()
	cfg.Seed = 20260730
	cfg.ShardID = 1
	// The cap is the locality knob, and without it a windowed drain collapses
	// nothing: pick() reuses uniformly over the whole pool, so at Workflows 0
	// the pool grows past the window and two touches of one workflow almost
	// never land in the same drain (measured: fold ratio 1.2 at 20k mutations,
	// against the 5.06 the stream as a whole would allow). The cap has to sit
	// well under the *effective* window, not the configured one: at this reuse
	// about 1% of mutations are refused — a continue-as-new out of a snapshot
	// window — and each refusal cuts the window short, so drains average ~90
	// mutations and a 128-workflow hot set still left the ratio at 1.47. 32
	// concurrently-hot workflows is a shard whose hot set turns over faster
	// than it drains, which is the shape the fold path exists for.
	cfg.Workflows = 32
	run := driveNoCluster(t, cfg, n)

	t.Logf("\n=== no-cluster acceptance, %d mutations (%.1f MB encoded), window %d\n%s\n"+
		"fold: %d windows, %d merged requests out (%d tombstones), fold ratio %.2f at WorkflowReuse %.2f, %d refusals recovered",
		n, float64(run.bytes)/(1<<20), acceptanceWindow, run.report,
		run.Drains, run.Emitted, run.tombstones, run.CollapseRatio(), cfg.WorkflowReuse, run.Refusals)

	// Every mutation of the stream landed in exactly one window, through the
	// bytes and back. This is the line 10^6 exists for.
	require.Equal(t, n, run.FoldedIn, "every mutation must land in exactly one window")

	// The stream contained what it was configured to contain — asserted, not
	// assumed from n. A generator regression that quietly stopped producing a
	// shape would otherwise turn this into a volume test of creates.
	r := run.report
	require.True(t, r.Collapses(), "the stream does not collapse; it asks fold nothing: %s", r)
	require.NotZero(t, r.Updates, "no chain")
	require.NotZero(t, r.Sets, "no snapshot barrier of the Set shape")
	require.NotZero(t, r.ConflictResolves, "no snapshot barrier of the conflict-resolve shape")
	require.NotZero(t, r.ContinueAsNews, "no request carrying two runs")
	require.NotZero(t, r.BufferedBatches, "no buffered events")
	require.NotZero(t, r.BufferedClears, "nothing cleared its buffer")
	require.NotZero(t, r.Deletes, "no tombstone")
	require.NotZero(t, r.Recreations, "no workflow id reused by a second run")
	require.NotZero(t, r.SubDeletes, "no delete of a key that was there")
	require.Len(t, r.TasksByCat, 4, "history tasks must cover all four categories: %v", r.TasksByCat)

	// And fold did something with it: windows collapsed and the refusal loop —
	// the contract every consumer has to implement — actually ran.
	require.Greater(t, run.CollapseRatio(), 1.5,
		"the windows must collapse, or the volume proved nothing")
	require.NotZero(t, run.Refusals, "the stream never exercised fold's refusal path")
	require.NotZero(t, run.tombstones, "the stream's deletes never came out as tombstones")

	// The control at the other end of the dial. A corpus that never re-touches
	// a workflow reports ratio 1.00 — and must, or the headline number above
	// is not a measurement. A tenth of the volume is plenty: this run
	// exists to show the ratio moving, not to cover shapes.
	flat := cfg
	flat.WorkflowReuse = 0
	flat.Workflows = 0 // unbounded: reuse 0 with a capped key space would still collide
	control := driveNoCluster(t, flat, n/10)
	t.Logf("knob control: %d mutations at WorkflowReuse 0.00 — generator ratio %.2f, fold ratio %.2f",
		n/10, control.report.CollapseRatio, control.CollapseRatio())
	require.Equal(t, 1.0, control.report.CollapseRatio,
		"at WorkflowReuse 0 every mutation is a workflow of its own")
	require.False(t, control.report.Collapses(),
		"Collapses() must recognise the worthless corpus")
	require.Greater(t, run.report.CollapseRatio, control.report.CollapseRatio,
		"the ratio must be visibly a function of the knob")
}
