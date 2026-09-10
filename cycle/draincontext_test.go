package cycle

// What may cut a drain short, and what may not. Both claims are about a
// context: the read that resolves an unreadable outcome may not run on the
// context whose expiry produced it, and a drain no caller is waiting for may
// not be bounded by the deadline of the write it happens to run inside.

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"

	"github.com/aromanovich/waltz/baserow"
	"github.com/aromanovich/waltz/fold"
	"github.com/aromanovich/waltz/internal/verify/basetest"
	"github.com/aromanovich/waltz/wal"
	"github.com/aromanovich/waltz/wal/memwal"
)

// expiringApplier is a store whose round trip outlasts the deadline of the call
// it was reached from: it cancels that call, then answers on the context the
// drain actually brought it.
type expiringApplier struct {
	cancel  context.CancelFunc
	applied []wal.Seqno
	// answer, where set, is returned instead of the drain context's own error,
	// which is how a genuinely ambiguous store answer is staged.
	answer error
}

func (a *expiringApplier) Apply(ctx context.Context, _ wal.ShardID, _ wal.Epoch, batch fold.Batch) error {
	a.cancel()
	if a.answer != nil {
		return a.answer
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	a.applied = append(a.applied, batch.Watermark())
	return nil
}

// ctxEnv is one cycle over a store that expires its caller, plus the context
// that store cancels.
type ctxEnv struct {
	apply *expiringApplier
	mark  *fakeWatermark
	rows  *baserow.Rows
	c     *Cycle
	// writeCtx is the caller's, and the applier cancels it mid-drain. What a
	// write returns on it is therefore a race between the loop's answer and the
	// cancellation, so the claims below are made about the shard rather than
	// about that value.
	writeCtx context.Context
}

func newCtxEnv(t *testing.T, shape func(*Config)) *ctxEnv {
	t.Helper()
	writeCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	tick := clock.NewEventTimeSource()
	tick.Update(time.Unix(1700000000, 0))
	e := &ctxEnv{
		apply:    &expiringApplier{cancel: cancel},
		mark:     &fakeWatermark{},
		rows:     basetest.New().Rows(),
		writeCtx: writeCtx,
	}
	cfg := Defaults()
	if shape != nil {
		shape(&cfg)
	}
	cfg.timeSource = tick
	e.c = standUp(t, testEpoch, Deps{Log: memwal.New(), Writer: e.apply, Recoverer: e.mark}, cfg)
	return e
}

// TestAnAmbiguousDrainIsResolvedOffTheWritersDeadline: the watermark is the only
// witness to what an unreadable drain did, so reading it on the context that
// just expired is a question that cannot be answered in the one case it is
// asked. The shard would then stall on an outcome the store was ready to state.
func TestAnAmbiguousDrainIsResolvedOffTheWritersDeadline(t *testing.T) {
	t.Run("it had committed", func(t *testing.T) {
		e := newCtxEnv(t, func(c *Config) { c.Sync = true })
		e.apply.answer = context.DeadlineExceeded
		// The floor first — no drain has ever committed here — then the readback,
		// at the drain's own seqno.
		e.mark.answers = []wmAnswer{{}, {seqno: 1, found: true}}
		ns, wf, run := ids()

		_ = e.c.write(e.writeCtx, mkCreate(ns, wf, run), e.rows)

		st := e.c.Stats()
		require.Equal(t, StateRunning, st.State)
		require.EqualValues(t, 1, st.AppliedSeqno, "the watermark says it committed after all")
		require.Zero(t, st.TailEntries, "and so there is nothing left unsettled")

		e.apply.answer = nil
		ns, wf, run = ids()
		require.NoError(t, e.c.write(context.Background(), mkCreate(ns, wf, run), e.rows),
			"a resolved outcome leaves nothing for the next writer to be refused on")
	})

	t.Run("it had not", func(t *testing.T) {
		e := newCtxEnv(t, func(c *Config) { c.Sync = true })
		e.apply.answer = context.DeadlineExceeded
		ns, wf, run := ids()

		_ = e.c.write(e.writeCtx, mkCreate(ns, wf, run), e.rows)

		require.Equal(t, StateHaltedInvariant, e.c.State(),
			"the watermark answered, so this is a divergence and not a shard that cannot say what it did")
	})
}

// TestADrainNoCallerWaitsForOutlivesTheWriteItRunsInside: a size watermark trips
// inside some writer's call, and what the drain then carries is every earlier
// writer's acked mutation. Those writers have been told their writes succeeded
// and are gone; the one still on the line is waiting for its own append and not
// for this transaction, so its clock is not the one that may cut it short.
func TestADrainNoCallerWaitsForOutlivesTheWriteItRunsInside(t *testing.T) {
	e := newCtxEnv(t, func(c *Config) { c.Mutations = 2 })
	ns, first, firstRun := ids()
	_, second, secondRun := ids()

	require.NoError(t, e.c.write(context.Background(), mkCreate(ns, first, firstRun), e.rows))
	_ = e.c.write(e.writeCtx, mkCreate(ns, second, secondRun), e.rows)

	st := e.c.Stats()
	require.Equal(t, []wal.Seqno{2}, e.apply.applied, "the drain reached the store")
	require.Equal(t, StateRunning, st.State)
	require.EqualValues(t, 2, st.AppliedSeqno)
}
