package cycle

// Replay: what a new owner does with a tail it did not write. A read loop over
// `(appliedSeqno, tail]` into the accumulator, and four decisions:
//
//   - it is [Cycle.start] grown a body, which makes it the readiness gate: a
//     request arriving mid-replay parks in [ask] behind it. A read
//     triggers it as much as a write does, or a read on an inherited tail is
//     answered from a cold store the log is ahead of;
//   - it ends in a drain, so the window is empty when the first caller is
//     served. Left to the ordinary watermarks it would mix entries whose
//     callers are gone with a fresh caller's write, where a condition failure
//     is attributable to nobody and halts;
//   - a provisional entry is carried alone and its condition failure is a drop
//     (below);
//   - an entry above this cycle's epoch means we are the zombie, since epochs
//     are non-decreasing and a Fence cuts off every lower one. Halt lost,
//     checked here rather than left to the apply transaction's epoch CAS,
//     because the new owner's fence and its rangeID bump are not atomic and in
//     between a zombie's CAS still succeeds.
//
// # Why a provisional entry may be dropped
//
// An ack is normally the answer, which is what makes a condition failure at
// apply a divergence. Sync mode acks before the condition is verified, since its
// drain is what answers the caller, and such an entry stays in the log above the
// watermark: replaying it naively fails its assertion again and halts a healthy
// shard on its new owner. The two classes cannot be told apart after the fact,
// so the writer marks them at the append (Payload.provisional, set in
// [Cycle.add]); absent means verified.
//
// The drop is safe because a provisional entry replays to the outcome its own
// drain would have produced: the fence makes this layer the shard's only writer
// and replay applies the log in its own order, so the condition meets the state
// it met before.
//
// Replay does not re-run the condition authority, whose callers are all gone,
// and does not rebuild a window the previous owner drained, since an entry below
// the watermark is a row the cold store holds. It does not bound itself either:
// I10 bounds what a running cycle acks, and a tail that exceeds it must still be
// replayed or the shard is unrecoverable.

import (
	"context"
	"fmt"

	"go.temporal.io/server/common/log/tag"

	"github.com/aromanovich/waltz/cycle/tailstate"
	"github.com/aromanovich/waltz/cycle/window"
	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/wal"
)

// replay reads the tail the previous owner left and applies it. The floor has
// already been read: s.next is the watermark's successor.
//
// A failure leaves the cycle unstarted and the window empty, so the next
// request retries from the watermark. It does not halt — a log read that failed
// is not an answer.
func (c *Cycle) replay(ctx context.Context, s *state) error {
	if c.deps.Registry == nil {
		// A payload's task groups name their category by id, so a cycle with no
		// registry could not decode a tail even if it found one. See
		// [Deps.Registry].
		return fmt.Errorf("cycle: shard %d has no task category registry, so it cannot replay a tail", c.shard)
	}
	// A page is the window's own size, so at most one window's worth of encoded
	// entries is resident beyond the accumulator.
	page := c.policy().Mutations
	if page <= 0 {
		page = 1
	}
	for e, err := range wal.Entries(ctx, c.deps.Log, c.shard, s.next, page) {
		if err != nil {
			return fmt.Errorf("cycle: shard %d: reading the tail: %w", c.shard, err)
		}
		if err := c.replayEntry(ctx, s, e); err != nil {
			return err
		}
	}
	if s.counted().Replayed == 0 {
		return nil
	}
	c.deps.Logger.Info("apply cycle: replaying the tail a previous owner left",
		tag.ShardID(int32(c.shard)), tag.NewInt64("entries", int64(s.counted().Replayed)),
		tag.NewInt64("dropped", int64(s.counted().Dropped)), tag.NewInt64("through", int64(s.tail.Commit())))
	// Nothing is served until the tail has been applied.
	if err := c.drain(ctx, s, drainReplay); err != nil {
		return err
	}
	c.deps.Metrics.Replayed(s.counted().Replayed, s.counted().Dropped)
	return nil
}

// FencedAway is the cause a replay carries when it finds the log already at a
// higher epoch, and the words are load-bearing outside this package: at the
// store boundary every road to a fence answers the same ShardOwnershipLost, so
// an instrument that has to tell this road from an append fence has only the
// cause to read. Match on this constant rather than copying the string.
const FencedAway = "the shard has been fenced away"

// replayEntry folds one entry of the tail, drains around it when it is
// provisional, and lets the ordinary size watermarks cut the rest.
func (c *Cycle) replayEntry(ctx context.Context, s *state, e wal.Entry) error {
	if e.Epoch > c.epoch {
		err := fmt.Errorf("the log holds seqno %d at epoch %d, above this cycle's %d: %s",
			e.Seqno, e.Epoch, c.epoch, FencedAway)
		c.halt(s, StateHaltedLost, err)
		return c.halted(s)
	}
	if e.Seqno != s.next {
		// Guarantee 4 says seqnos are gap-free. If they are not, the log is not
		// what this layer's invariants are written against.
		err := fmt.Errorf("the log skips from seqno %d to %d", s.next, e.Seqno)
		c.halt(s, StateHaltedInvariant, err)
		return c.halted(s)
	}

	m, provisional, err := mutation.DecodeEntry(e.Payload, c.deps.Registry)
	if err != nil {
		// A newer codec, or a task category this node has no registration for.
		// Both are fatal to the replay on purpose, and the entry is acked, so
		// there is nothing to do but stop.
		c.halt(s, StateHaltedInvariant, fmt.Errorf("cycle: decoding seqno %d: %w", e.Seqno, err))
		return c.halted(s)
	}
	if got, want := m.ShardID(), int32(c.shard); got != want {
		c.halt(s, StateHaltedInvariant,
			fmt.Errorf("cycle: seqno %d belongs to shard %d, this cycle owns shard %d", e.Seqno, got, want))
		return c.halted(s)
	}

	// A provisional entry may legitimately fail its condition, and a drain fails
	// all-or-nothing, so it may share a batch with nothing: the window in front
	// of it is drained first, and it is drained alone after.
	if provisional {
		if err := c.drain(ctx, s, drainReplay); err != nil {
			return err
		}
	}
	if err := c.accept(ctx, s, m, len(e.Payload)); err != nil {
		return err
	}
	s.counted().Replayed++
	if provisional {
		return c.drain(ctx, s, drainReplayProvisional)
	}
	// The steady state's size watermarks, so a replayed transaction is the size
	// of an ordinary one: a tail at I10's bound applied whole would be a
	// transaction nothing has ever executed. The age watermark is not consulted,
	// since every entry here is already as old as the incident — which is why
	// the rule is asked for by name rather than off the whole policy.
	if s.window.Trips(c.policy().watermarks()) != window.NoTrip {
		return c.drain(ctx, s, drainReplay)
	}
	return nil
}

// dropProvisional settles a replayed provisional entry whose condition failed:
// its caller already has the answer, or saw the ambiguity. The entry is settled
// exactly as [Cycle.answerWriter] settles one, and replay carries on.
//
// This is not "a condition failure at replay is forgiven". An entry whose
// assertion was verified before its ack cannot legitimately fail, and one that
// does still halts.
func (c *Cycle) dropProvisional(s *state, seqno wal.Seqno, held *window.Taken, cause error) error {
	// No transaction wrote its rows, so the watermark may not move over it.
	s.tail.Settle(seqno, held, tailstate.KeepWatermark)
	s.counted().Dropped++
	c.deps.Logger.Info("apply cycle: a replayed provisional entry did not apply, and was not meant to",
		tag.ShardID(int32(c.shard)), tag.NewInt64("seqno", int64(seqno)), tag.Error(cause))
	return nil
}
