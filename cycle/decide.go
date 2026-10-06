package cycle

// The decision rules this package's outcomes turn on. Each is a pure function
// of the values it is handed, so decide_test.go can enumerate them without a
// cycle; a call site's only job is to hand over the right values.
//
// They cover: routing a read the layer cannot answer from both sources
// ([readRoute]), what a write meets before its append, the store boundary's
// error translation, what an append's error means, a drain's attribution and
// settlement, and what one age tick does.

import (
	"errors"
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	p "go.temporal.io/server/common/persistence"

	"github.com/aromanovich/waltz/apply"
	"github.com/aromanovich/waltz/wal"
	"github.com/aromanovich/waltz/walmetrics"
)

// reader is the kind of read being routed. The two kinds get different
// answers in several of the rules below.
type reader int

const (
	// mutableStateRead may come from callers that do not own the shard, so it
	// may be answered stale. The history-branch page routes as one: its reader
	// deletes nothing, so a page missing the newest nodes is only stale.
	mutableStateRead reader = iota
	// taskRead has one caller, the owning shard's queue processors. A short
	// page is never harmless: the reader completes the range and acks past
	// whatever was missing.
	taskRead
)

// readRoute is what becomes of a read, decided at five moments: no cycle for
// the shard, the cycle's goroutine is gone, the cycle is halted, its last
// drain's outcome is unreadable, or the answering cycle was superseded. Each
// rule below returns a route and its refusal error (nil when it answers).
type readRoute int

const (
	// merge: answer from the cycle's window over the cold store's rows.
	merge readRoute = iota
	// passThrough: the layer holds nothing for this read; the cold store's
	// answer is the whole answer.
	passThrough
	// refuseAsLost: ShardOwnershipLost, unwrapped, which the shard's read path
	// matches to re-acquire.
	refuseAsLost
	// refuseAsHalt: the halt's own error, unchanged; its cause is the only
	// record of what diverged.
	refuseAsHalt
	// refuseAsUnresolved: the last drain's outcome is unreadable, so what it
	// acked is in neither source for certain. Uses the write path's refusal
	// because this state heals: the caller should retry, not fail over.
	refuseAsUnresolved
	// retryOnSuccessor: the shard changed hands during the call; discard the
	// answer and re-issue on the replacing cycle. Only [supersededRoute]
	// returns it, for task reads.
	retryOnSuccessor
)

// noCycleRoute routes a read for a shard with no registered cycle (never
// acquired here, or released). A task read is refused; a mutable-state read
// passes through, since refusing it would break every role that legitimately
// reads a shard it does not own.
func noCycleRoute(who reader, shard wal.ShardID) (readRoute, error) {
	if who == taskRead {
		return refuseAsLost, lost(shard,
			"this node holds no apply cycle for it, so its tail cannot be merged into a task page")
	}
	return passThrough, nil
}

// loopRoute routes a read for a cycle whose loop is still running.
//
// Running: merge, unless the tail is stalled at a drain whose outcome could
// not be read (see [tailstate.Tail]). Then neither source is sure to hold what
// that drain acked: the window emptied when the drain started, and whether the
// store took its rows is unknown. A merge could return state older than an
// acked write, so both readers are refused with the write path's refusal,
// because one readable watermark heals it.
//
// Halted:
//
//   - task read: refused whatever the tail says. On halted-lost the missing
//     acks are another owner's. On halted-invariant even an empty tail is
//     refused: the page would carry the base store's page token, and if the
//     shard is re-acquired mid-pagination (a rangeID renewal does this without
//     an unload) the merging cycle refuses that token
//     (fold.ErrForeignPageToken), while finishing on the base alone would skip
//     acked window rows the reader then deletes. A task page is answered by a
//     running cycle or not at all. The refusal is ShardOwnershipLost when lost
//     and the unconverted halt otherwise, since converting a divergence this
//     process owns would hand it to the next owner as a normal failover;
//   - mutable-state read: [tailRoute].
//
// Halt outranks a stall: a halted, stalled cycle is refused as halted, since
// the halt's cause is the record of where the shard stopped for good.
//
// halt is built by the caller ([Cycle.halted]) and returned as is, identity
// included.
func loopRoute(
	st State, tailEmpty bool, stalled wal.Seqno, who reader, shard wal.ShardID, halt error,
) (readRoute, error) {
	if st == StateRunning {
		if stalled != 0 {
			return refuseAsUnresolved, unresolvedDrain(shard, stalled, "answers no read")
		}
		return merge, nil
	}
	if who == taskRead {
		if st == StateHaltedLost {
			return refuseAsLost, lost(shard,
				"its cycle is halted, so this node is no longer the owner whose tail a task page would merge")
		}
		return refuseAsHalt, halt
	}
	return tailRoute(st, tailEmpty, shard, "it is halted holding an unapplied tail", halt)
}

// stoppedRoute routes a read for a cycle whose goroutine is gone: superseded
// by a higher epoch, retired by name (Layer.RetireShard), or closed with the
// node. tailEmpty comes from the mirror, since there is no loop to ask
// ([Cycle.stoppedRead]).
//
// A task read is always refused: if a successor exists, its window holds the
// shard's newest rows, invisible here, and a page short of them would be acked
// past. [Manager.taskPage] retries it on the successor; a cycle retired by
// name has none, so the refusal stands. For a mutable-state read that gap is
// only staleness.
func stoppedRoute(st State, tailEmpty bool, who reader, shard wal.ShardID, halt error) (readRoute, error) {
	if who == taskRead {
		return refuseAsLost, lost(shard,
			"its cycle at this epoch has been retired, so the tail a task page would merge is another cycle's")
	}
	return tailRoute(st, tailEmpty, shard, "its cycle was retired holding an unapplied tail", halt)
}

// tailRoute is the mutable-state rule shared by the two above. An empty tail
// means everything this cycle acked is in the cold store, so it can answer.
// A non-empty tail means the store is missing entries, so the read is refused.
// lostWhy is the operator-facing message for the refusal.
//
// The rule trusts the tail, and one path breaks that: [Cycle.Close] on a cycle
// halted inside its replay floors the tail and, if its log read fails, retires
// it empty, so [stoppedRoute] passes a mutable-state read to a store missing
// those entries. This is an open entry in DURABILITY.md.
func tailRoute(st State, tailEmpty bool, shard wal.ShardID, lostWhy string, halt error) (readRoute, error) {
	if tailEmpty {
		return passThrough, nil
	}
	if st == StateHaltedLost {
		return refuseAsLost, lost(shard, lostWhy)
	}
	return refuseAsHalt, halt
}

// supersededRoute routes an answered task page by whether the cycle that built
// it is still the shard's. stillCurrent must be read after the answer, so it
// covers the whole call; retried means the one retry is already spent.
//
// A superseded page is discarded and re-issued on the successor: task reads
// are pure, and the successor replays its predecessor's tail first, so it
// merges a superset. Refusing instead would be a self-inflicted failover,
// since this node re-acquired and the successor is already registered. After
// one retry the shard is declared lost: two acquires within one page read is
// churn faster than a page can be built.
func supersededRoute(stillCurrent, retried bool, shard wal.ShardID) (readRoute, error) {
	switch {
	case stillCurrent:
		return merge, nil
	case !retried:
		return retryOnSuccessor, nil
	}
	return refuseAsLost, lost(shard,
		"its cycle was superseded twice while one task page was being built, so this node cannot say what its tail holds")
}

// tickAction is what one age tick does. The tick is the loop's only
// self-driven moment, so all work no request brings lands here.
type tickAction int

const (
	// tickNothing: not running, or nothing due.
	tickNothing tickAction = iota
	// tickDrainAge: drain an aged window, or a stalled tail, re-asking its
	// watermark on the tick's own context. A stalled cycle refuses writes and
	// reads, so the tick is its only healer and [Config.Age] is also the
	// stall's retry cadence.
	tickDrainAge
	// tickDrainPressure: the same drain (stall re-ask included), fired by
	// standing backend pressure and counted under pressure's name, since the
	// trim its commit forces is pressure's doing.
	tickDrainPressure
	// tickForceTrim: standing pressure with an empty window. No commit will
	// force a trim, so the tick asks the trimmer directly. This also retries a
	// failed forced trim while the stop level refuses the writers that would
	// otherwise bring a drain.
	tickForceTrim
)

// tickActionOf decides the tick. An unstarted cycle ignores pressure: its
// watermark was never read, so no trim position is safe. Pressure with work
// in the window drains rather than trims, because only a commit moves the
// watermark a trim goes to.
func tickActionOf(
	st State, started, stalled, aged bool, pressure wal.PressureLevel, windowEmpty bool,
) tickAction {
	if st != StateRunning {
		return tickNothing
	}
	urgent := started && pressure >= wal.PressureDrain
	switch {
	case stalled || aged || (urgent && !windowEmpty):
		if urgent {
			return tickDrainPressure
		}
		return tickDrainAge
	case urgent:
		return tickForceTrim
	}
	return tickNothing
}

// writeRefused is the refusal a write meets before its append, or nil if it
// may proceed. The second result is the metric's limit name; this rule emits
// nothing ([Cycle.writeRefused] does). Precedence names the cause least in
// this shard's power to clear: stall, then pressure, then size.
//
//   - stalled: the last drain's outcome is unknown, so nothing may be applied
//     over it (see [tailstate.Tail]) and more work would grow a tail that
//     cannot be discharged;
//   - pressure at [wal.PressureStop]: the backend is short of storage for
//     acked entries. Only the backend can lower it (this shard's drains and
//     trims are already forced), so naming a size would mislead the operator;
//   - I10: entries means the applier is behind; bytes means that, or a few
//     very large entries. Over both is named as bytes. The check reads the
//     tail as it stands, not including this mutation, so no mutation is
//     refused for its own size and the tail overshoots by at most one entry.
//
// Return the refusal unwrapped: ContextImpl.handleWriteErrorLocked switches on
// the concrete type, where *serviceerror.ResourceExhausted means "definitely
// not committed", and a %w-wrapped error falls to a background re-acquire.
// Cause and scope match the server's persistence rate limiter, so the history
// client retries it.
func writeRefused(
	entries, bytes int64, stalled wal.Seqno, pressure wal.PressureLevel, shard wal.ShardID, cfg Config,
) (*serviceerror.ResourceExhausted, string) {
	switch {
	case stalled != 0:
		return unresolvedDrain(shard, stalled, "takes no writes"), walmetrics.LimitUnresolved
	case pressure >= wal.PressureStop:
		return persistenceLimit(
			"shard %d's WAL backend reports storage pressure and takes no new appends until it clears",
			shard), walmetrics.LimitStoragePressure
	case entries >= int64(cfg.HardMaxEntries) || bytes >= int64(cfg.HardMaxBytes):
		limit := walmetrics.LimitEntries
		if bytes >= int64(cfg.HardMaxBytes) {
			limit = walmetrics.LimitBytes
		}
		return persistenceLimit(
			"shard %d's WAL tail is at its limit (%d/%d entries, %d/%d bytes): the apply cycle is behind",
			shard, entries, cfg.HardMaxEntries, bytes, cfg.HardMaxBytes), limit
	}
	return nil, ""
}

// persistenceLimit builds every refusal here, write and read alike. The shard
// matches on the type and this Cause/Scope pair, so it is spelled once.
func persistenceLimit(format string, args ...any) *serviceerror.ResourceExhausted {
	return &serviceerror.ResourceExhausted{
		Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_LIMIT,
		Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM,
		Message: fmt.Sprintf(format, args...),
	}
}

// unresolvedDrain is the refusal for an unreadable drain outcome, shared by
// the write and read paths. waits says what the caller's side withholds.
func unresolvedDrain(shard wal.ShardID, stalled wal.Seqno, waits string) *serviceerror.ResourceExhausted {
	return persistenceLimit(
		"shard %d's apply cycle cannot read the outcome of its drain at seqno %d, and %s until it can",
		shard, stalled, waits)
}

// storeError translates a write outcome at the store boundary. Errors that
// persistence.OperationPossiblySucceeded reads as definitely not committed
// pass through untouched (condition failures, fenced epochs, I10's refusal).
//
// Otherwise only [StateHaltedLost] maps, to ShardOwnershipLost. A
// halted-invariant cycle, an encode failure or an unknown outcome stay
// unrecognised and fall to the background re-acquire: converting them would
// hand a divergence this process owns to the next owner as a normal failover.
//
// st is the state after the write, so [Manager.Write] reads it then.
func storeError(st State, shard wal.ShardID, err error) error {
	if err == nil {
		return nil
	}
	if !p.OperationPossiblySucceeded(err) {
		return err
	}
	if st == StateHaltedLost {
		return lost(shard, err.Error())
	}
	return err
}

// appendOutcome is what a failed [wal.Log.Append] left in the log. Three are
// the contract's named refusals, each definite; the fourth is everything else.
type appendOutcome int

const (
	// appendUnknown: an error the contract does not name (a transport failure,
	// say), so whether the entry is in the log is unknown. No other mutation
	// may take the same seqno: the first attempt may still land, and a caller
	// would have been told both answers. The log is read back to settle it
	// ([Cycle.settleAppend]). It is the zero value so that an unset outcome
	// means unknown, never an assumed one.
	appendUnknown appendOutcome = iota
	// appendNothing: a named refusal that writes nothing, so the seqno is free
	// for the next mutation. Callers' switches have no arm for it: nothing
	// needs to happen.
	appendNothing
	// appendFenced: the shard has a new owner (I4).
	appendFenced
	// appendTaken: the seqno this cycle replayed past is occupied at this
	// cycle's own epoch. See [ErrTailNotEmpty].
	appendTaken
)

// appendOutcomeOf classifies an append's error. Anything unnamed is
// [appendUnknown], never "nothing written", or a seqno of unknown fate would
// be handed to the next mutation.
func appendOutcomeOf(err error) appendOutcome {
	switch {
	case errors.Is(err, wal.ErrFenced):
		return appendFenced
	case errors.Is(err, wal.ErrAlreadyWritten):
		return appendTaken
	case errors.Is(err, wal.ErrGap):
		return appendNothing
	}
	return appendUnknown
}

// attribution is whose answer a failed assertion at apply is, if anyone's.
type attribution int

const (
	// haltsShard is the default: the window's writers were all acked, so the
	// failure cannot be pinned on one of them and a retry fixes nothing.
	haltsShard attribution = iota
	// answersItsCaller: sync mode's window of one, whose writer is still
	// inside the [Cycle.write] that appended it ([Cycle.answerWriter]).
	answersItsCaller
	// dropsItsEntry: a replayed provisional entry, carried alone, whose caller
	// already has its answer ([Cycle.dropProvisional]).
	dropsItsEntry
)

// attribute decides attribution from both the cause and a window of exactly
// one mutation. With the cause alone, a second call site using that cause
// would hand another writer's failure to a caller; getting it wrong the other
// way halts where an answer was owed.
func attribute(cause drainCause, mutationsIn int) attribution {
	if mutationsIn != 1 {
		return haltsShard
	}
	switch cause.caller {
	case answersCaller:
		return answersItsCaller
	case dropsProvisional:
		return dropsItsEntry
	}
	return haltsShard
}

// settlement is what a drain's outcome means for the shard; [apply.Class] is
// only what the transaction did.
//
// Only the first two can put rows in the cold store, so only they may move the
// watermark. The rest leave it alone: a trim goes to the watermark, and moving
// it over rows that are not there strands a recovering owner.
type settlement int

const (
	// settlesForward: the rows are in the cold store; move the watermark over
	// them, count and emit the drain.
	settlesForward settlement = iota
	// asksTheWatermark: the outcome is unreadable, so read the watermark. At
	// the drain's seqno it committed and settles forward; below, it did not;
	// above, another owner wrote it. The last two halt, on different sides.
	asksTheWatermark
	// answersItsWriter: sync mode's window of one, whose writer is still inside
	// the call that appended it ([Cycle.answerWriter]).
	answersItsWriter
	// dropsTheEntry: a replayed provisional entry whose caller already has its
	// answer ([Cycle.dropProvisional]).
	dropsTheEntry
	// haltsInvariant: a divergence this process owns; never convert it to
	// ShardOwnershipLost.
	haltsInvariant
	// haltsLost: the fence working.
	haltsLost
)

// settlementOf decides the settlement. An unrecognised class halts on the
// invariant side: halting as lost would hand an unenumerated outcome to the
// next owner as a normal failover.
func settlementOf(class apply.Class, cause drainCause, mutationsIn int) settlement {
	switch class {
	case apply.ClassCommitted:
		return settlesForward
	case apply.ClassUnknownOutcome:
		return asksTheWatermark
	case apply.ClassShardLost:
		return haltsLost
	case apply.ClassInvariantViolated:
		switch attribute(cause, mutationsIn) {
		case answersItsCaller:
			return answersItsWriter
		case dropsItsEntry:
			return dropsTheEntry
		}
	}
	return haltsInvariant
}
