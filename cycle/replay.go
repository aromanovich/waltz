package cycle

// Replay applies the tail a previous owner left: it reads from the watermark's
// successor to the end of the log into the accumulator.
//
//   - It runs inside [Cycle.start], so it is the readiness gate: requests park
//     in [ask] until it finishes. Reads trigger it too, or a read on an
//     inherited tail would be answered from a cold store the log is ahead of.
//   - It ends in a drain, so the first caller meets an empty window. Otherwise
//     old entries would share a batch with a fresh write, and a condition
//     failure there could be attributed to nobody.
//   - An entry above this cycle's epoch means another owner fenced us: halt
//     lost. This is checked here, not left to the apply transaction's epoch
//     CAS, because a new owner's fence and rangeID bump are not atomic.
//   - It has no bound: a tail over I10 must still be replayed, or the shard is
//     unrecoverable. It does not re-run the condition authority (the callers
//     are gone) nor rebuild windows below the watermark.
//
// Provisional entries: sync mode acks before the condition is verified, so its
// entries are marked at the append (Payload.provisional, set in [Cycle.add]).
// Each replays alone and a condition failure drops it rather than halting. That
// is safe because the fence makes this layer the only writer and replay keeps
// log order, so the condition meets the same state as the first time. Any
// other entry's condition failure still halts.

import (
	"context"
	"fmt"

	"go.temporal.io/server/common/log/tag"

	"github.com/aromanovich/waltz/cycle/tailstate"
	"github.com/aromanovich/waltz/cycle/window"
	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/wal"
)

// replay applies the tail; s.next is already the watermark's successor. A
// failed log read does not halt: the cycle stays unstarted with an empty
// window, and the next request retries from the watermark.
func (c *Cycle) replay(ctx context.Context, s *state) error {
	if c.deps.Registry == nil {
		// Task groups name their category by id; see [Deps.Registry].
		return fmt.Errorf("%w (shard %d)", ErrNoRegistry, c.shard)
	}
	// One policy read for the whole replay, so the bounds cannot change midway.
	cfg := c.policy()
	// Pages are window-sized, bounding what is resident beside the accumulator.
	page := max(cfg.Mutations, 1)
	marks := cfg.watermarks()
	for e, err := range wal.Entries(ctx, c.deps.Log, c.shard, s.next, page) {
		if err != nil {
			return fmt.Errorf("cycle: shard %d: reading the tail: %w", c.shard, err)
		}
		if err := c.replayEntry(ctx, s, e, marks); err != nil {
			return err
		}
	}
	// The loop stops on a short page, which the contract calls the end of the
	// log. A backend that caps response size also answers short, and trusting
	// that would serve reads missing everything above the cut. So one more
	// one-entry read confirms the end.
	rest, err := c.deps.Log.ReadFrom(ctx, c.shard, s.next, 1)
	if err != nil {
		return fmt.Errorf("cycle: shard %d: confirming the tail ends below seqno %d: %w", c.shard, s.next, err)
	}
	if len(rest) > 0 {
		// A fencing successor may have appended since the loop's last read.
		if fenced := c.fencedAway(rest[0]); fenced != nil {
			c.halt(s, StateHaltedLost, fenced)
			return c.halted(s)
		}
		c.strand(s, rest[0], fmt.Errorf(
			"the tail read ended below seqno %d and the log still holds seqno %d",
			s.next, rest[0].Seqno))
		return c.halted(s)
	}

	if s.counted().Replayed == 0 {
		// Clean acquire. Skip the log line, which tells an operator the shard
		// changed hands with writes in flight and is worthless if printed always.
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

// FencedAway is the halt cause when a fence is discovered rather than returned
// by an append: replay finds a higher epoch in the log, or a drain finds the
// watermark moved past by another owner ([Cycle.resolve]). Every fence reaches
// the store boundary as the same ShardOwnershipLost, so callers telling these
// apart read the cause; match on this constant, not a copy of the string.
const FencedAway = "the shard has been fenced away"

// fencedAway returns an error if e was written above this cycle's epoch, which
// means another owner fenced this cycle away; nil otherwise. Both callers must
// use it so a failover is never mistaken for halted-invariant.
func (c *Cycle) fencedAway(e wal.Entry) error {
	if e.Epoch <= c.epoch {
		return nil
	}
	return fmt.Errorf("the log holds seqno %d at epoch %d, above this cycle's %d: %s",
		e.Seqno, e.Epoch, c.epoch, FencedAway)
}

// replayEntry folds one tail entry, draining around it if it is provisional.
func (c *Cycle) replayEntry(
	ctx context.Context, s *state, e wal.Entry, marks window.Watermarks,
) error {
	if fenced := c.fencedAway(e); fenced != nil {
		c.halt(s, StateHaltedLost, fenced)
		return c.halted(s)
	}
	if e.Seqno != s.next {
		// Log guarantee 4: seqnos are gap-free.
		err := fmt.Errorf("the log skips from seqno %d to %d", s.next, e.Seqno)
		c.strand(s, e, err)
		return c.halted(s)
	}

	m, provisional, err := mutation.DecodeEntry(e.Payload, c.deps.Registry)
	if err != nil {
		// A newer codec or an unregistered task category: fatal by design.
		c.strand(s, e, fmt.Errorf("decoding seqno %d: %w", e.Seqno, err))
		return c.halted(s)
	}
	if got, want := m.ShardID(), int32(c.shard); got != want {
		c.strand(s, e,
			fmt.Errorf("seqno %d belongs to shard %d, this cycle owns shard %d", e.Seqno, got, want))
		return c.halted(s)
	}

	// A provisional entry may fail its condition and a drain is all-or-nothing,
	// so it is drained alone: the window before it first, then itself.
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
	// Size triggers only, so replayed transactions are ordinary-sized. The age
	// trigger would fire on every entry, all as old as the incident.
	if s.window.Trips(marks) != window.NoTrip {
		return c.drain(ctx, s, drainReplay)
	}
	return nil
}

// strand halts invariant over an entry replay could not take, after adding the
// entry to the tail. Its callers (a seqno gap, an undecodable entry, an entry
// for another shard, an entry past the confirmed end) halt before
// [Cycle.accept], so without this the tail would stay empty, and [tailRoute]
// would pass mutable-state and history reads to a cold store missing this
// acked entry. Only non-emptiness matters; the count itself is meaningless.
//
// Open in DURABILITY.md: [Cycle.Close] re-runs start, which floors this tail
// away and restores it only if its replay reads the log again.
func (c *Cycle) strand(s *state, e wal.Entry, cause error) {
	s.tail.Ack(e.Seqno, len(e.Payload))
	c.halt(s, StateHaltedInvariant, cause)
}

// dropProvisional settles a replayed provisional entry whose condition failed,
// as [Cycle.answerWriter] does; its caller already got the answer. Replay
// continues. Non-provisional condition failures still halt.
func (c *Cycle) dropProvisional(s *state, seqno wal.Seqno, held *window.Taken, cause error) error {
	// No transaction wrote its rows, so the watermark must not move over it.
	s.tail.Settle(seqno, held, tailstate.KeepWatermark)
	s.counted().Dropped++
	c.deps.Logger.Info("apply cycle: a replayed provisional entry did not apply, and was not meant to",
		tag.ShardID(int32(c.shard)), tag.NewInt64("seqno", int64(seqno)), tag.Error(cause))
	return nil
}
