package cycle

// The predicates this package's outcomes turn on, each a function of the
// values it decides over and of nothing else, so decide_test.go can enumerate
// them without a cycle. Each has a method beside its call site that supplies
// the values.
//
// The first family is one rule in five moments — what becomes of a read the
// layer cannot answer out of both its sources ([readRoute]); then I10's
// refusal, the store boundary's translation, the drain's attribution and what a
// drain's outcome means.

import (
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	p "go.temporal.io/server/common/persistence"

	"github.com/aromanovich/waltz/apply"
	"github.com/aromanovich/waltz/wal"
	"github.com/aromanovich/waltz/walmetrics"
)

// reader is which of the two questions a routing rule is being asked, and it
// changes the answer at every moment below: the two are not owed the same one.
type reader int

const (
	// mutableStateRead has legitimate callers that do not own the shard, for
	// which a stale answer is what ADR 0003 already costs.
	mutableStateRead reader = iota
	// taskRead has exactly one caller — the owning shard's queue processors —
	// and no such thing as a harmlessly incomplete page: the reader completes
	// the range it asked for and acks past whatever was missing.
	taskRead
)

// readRoute is what becomes of a read the layer cannot answer out of both its
// sources, which is one rule observed at five moments: the registry holds no
// cycle for the shard, the cycle's goroutine is gone, the cycle is halted, the
// cycle cannot say what its last drain did, or the cycle that answered is no
// longer the shard's. Each is a function below returning one of these and the
// refusal it carries — nil on the routes that answer.
type readRoute int

const (
	// merge: the cycle asked is the shard's, so the read is answered from its
	// window over the cold store's rows, and a page it built is the shard's.
	merge readRoute = iota
	// passThrough: the layer holds nothing of its own for this read, so the
	// cold store's answer is the whole answer.
	passThrough
	// refuseAsLost: ShardOwnershipLost, unwrapped, which is what the shard's
	// read path matches to re-acquire.
	refuseAsLost
	// refuseAsHalt: the halt's own error, handed back exactly as it came — its
	// cause is the only record of what diverged.
	refuseAsHalt
	// refuseAsUnresolved: the cycle's last drain has no readable outcome, so
	// what it acked is in neither source with certainty. The write path's own
	// refusal, because this state heals: the caller is owed "ask again" and not
	// a failover.
	refuseAsUnresolved
	// retryOnSuccessor: the shard changed hands around this call, so the answer
	// is discarded and the read re-issued on the cycle that replaced it. Only
	// [supersededRoute] returns it, and only a task read reaches that rule.
	retryOnSuccessor
)

// noCycleRoute routes a read for a shard the registry holds no cycle for: one
// this node never acquired, or has released. The two readers part here, and
// the difference is deliberate — a mutable-state read that refused would break
// every role that legitimately reads a shard without owning it.
func noCycleRoute(who reader, shard wal.ShardID) (readRoute, error) {
	if who == taskRead {
		return refuseAsLost, lost(shard,
			"this node holds no apply cycle for it, so its tail cannot be merged into a task page")
	}
	return passThrough, nil
}

// loopRoute is the rule for a cycle whose loop is still there to ask, which is
// two questions: whether a running cycle can answer at all, and what a halted
// one answers with.
//
// A running cycle merges its window over the cold store, unless the tail is
// stalled at a drain whose outcome could not be read (see the stalled field of
// [tailstate.Tail]). Then neither source can be trusted to hold what that drain
// acked — the window emptied when it started, and whether the store took its
// rows is exactly what could not be read — so a merge would hand back state
// older than what was acked to its writer. That is [tailRoute]'s own reading,
// entries nobody can place meaning the store is incomplete and the layer cannot
// say by what, and both readers are refused on it. The refusal is the write
// path's rather than a halt's because this state heals: one readable watermark
// and the shard answers again.
//
// Halted, it turns on the tail, which a halt leaves alone, and under
// [StateHaltedLost] on which read is asking:
//
//   - lost + task read: refused whatever the tail says, since halted-lost means
//     another owner whose acks this cycle can neither see nor merge;
//   - lost + mutable-state read: the cold store on an empty tail (ADR 0003),
//     else ShardOwnershipLost;
//   - halted-invariant: the tail rule alone for both readers, and a non-empty
//     tail gets halt unconverted. Converting a divergence this process owns
//     would hand it to the next owner as an ordinary failover.
//
// A halted cycle stalled at a drain is refused as halted and not as unresolved,
// the two being reached in that order: the halt is where this shard stopped for
// good, and its cause is the record of it.
//
// halt is built by the caller ([Cycle.halted]), so [refuseAsHalt] returns the
// value it was given, identity included.
func loopRoute(
	st State, tailEmpty bool, stalled wal.Seqno, who reader, shard wal.ShardID, halt error,
) (readRoute, error) {
	if st == StateRunning {
		if stalled != 0 {
			return refuseAsUnresolved, persistenceLimit(
				"shard %d's apply cycle cannot read the outcome of its drain at seqno %d, and answers no read until it can",
				shard, stalled)
		}
		return merge, nil
	}
	if st == StateHaltedLost && who == taskRead {
		return refuseAsLost, lost(shard,
			"its cycle is halted, so this node is no longer the owner whose tail a task page would merge")
	}
	return tailRoute(st, tailEmpty, shard, "it is halted holding an unapplied tail", halt)
}

// stoppedRoute is the rule for a cycle whose goroutine is gone — retired by a
// higher epoch, or closed with the node. tailEmpty is the mirrored counter's
// answer, there being no loop left to ask ([Cycle.stoppedRead]).
//
// A task read is refused whatever that tail says, and this is where the two
// readers part hardest: a stopped cycle was superseded, so the shard's tail is
// now the fresh cycle's window, invisible from here. For a mutable-state read
// that is staleness; for a task page it is a page short exactly those rows,
// handed to the one caller that completes the range it read.
// [Manager.taskPage] answers that one on the successor instead.
func stoppedRoute(st State, tailEmpty bool, who reader, shard wal.ShardID, halt error) (readRoute, error) {
	if who == taskRead {
		return refuseAsLost, lost(shard,
			"its cycle at this epoch has been retired, so the tail a task page would merge is another cycle's")
	}
	return tailRoute(st, tailEmpty, shard, "its cycle was retired holding an unapplied tail", halt)
}

// tailRoute is what the two rules above share once the reader is settled: an
// empty tail means everything this cycle acked is in the cold store, so that
// store can answer; entries in it mean the layer knows the store is incomplete
// and cannot say by what. lostWhy is what the refusal tells an operator, and
// the two callers differ in it because the cycle is in different shapes.
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
// it is still the shard's. stillCurrent is read after the answer, so the window
// it covers is the whole call; retried says one rebuild has already been spent.
//
// A page from a superseded cycle is discarded and the read re-issued on the
// cycle that replaced it: a task read is pure, and the fresh cycle replays its
// predecessor's tail before answering, so what it merges is a superset. One
// retry, then the shard is declared lost, since two acquires inside one page
// read is churn faster than a page can be built.
//
// Refusing where this retries would be wrong: a retire means this node
// re-acquired, so the cycle that can answer is in the map already, and
// converting that into a re-acquire is a self-inflicted failover over a race
// one map lookup resolves.
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

// writeRefused is what a write meets before the append, nil where it may
// proceed. Two reasons in one rule, because the precedence between them is a
// decision rather than the order two calls happen to sit in: a stalled applier
// is refused as such even where the tail is also full, that being the one an
// operator can act on and the one waiting will not clear. The second result
// names it for the metric, so asking this rule a question counts nothing
// ([Cycle.writeRefused] emits).
//
//   - stalled: the cycle cannot say whether its last drain committed, so
//     nothing may be applied over it (see the stalled field of
//     [tailstate.Tail]) and a shard taking more work would grow a tail it has
//     no way to discharge. I10's rule at the moment the applier is not behind
//     but blind;
//   - I10 itself: entries means the applier is stalled, bytes a workflow near
//     the server's own blob limits, both over reads as bytes. It reads the tail
//     as it stands, never the tail this mutation would make, so no mutation is
//     refused for its own size and the tail overshoots by at most one entry.
//
// The refusal must reach the caller unwrapped: ContextImpl.handleWriteErrorLocked
// switches on the concrete type, where *serviceerror.ResourceExhausted means
// "definitely not committed", and one %w falls to the default arm — a
// background re-acquire. Cause and scope are the server's own persistence rate
// limiter's, so the retry stays inside the history client.
func writeRefused(
	entries, bytes int64, stalled wal.Seqno, shard wal.ShardID, cfg Config,
) (*serviceerror.ResourceExhausted, string) {
	switch {
	case stalled != 0:
		return persistenceLimit(
			"shard %d's apply cycle cannot read the outcome of its drain at seqno %d, and takes no writes until it can",
			shard, stalled), walmetrics.LimitUnresolved
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

// persistenceLimit is the shape every refusal here is — the two above and the
// read [loopRoute] refuses on the same unreadable drain — spelled once: copies
// of it agree only while somebody keeps them agreeing, and what the shard reads
// off them is the type and this pair.
func persistenceLimit(format string, args ...any) *serviceerror.ResourceExhausted {
	return &serviceerror.ResourceExhausted{
		Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_LIMIT,
		Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM,
		Message: fmt.Sprintf(format, args...),
	}
}

// storeError translates a write outcome at the store boundary. A value
// persistence.OperationPossiblySucceeded already recognises passes through
// untouched — apply converts at the transaction boundary, so condition
// failures, fenced epochs and I10's refusal are already in that list.
//
// Of the cycle's own states only [StateHaltedLost] maps, since a fence is what
// ShardOwnershipLost means. [StateHaltedInvariant], an encode failure and an
// unknown outcome stay unrecognised and fall to the background re-acquire:
// converting them would hand a divergence this process owns to the next owner
// as an ordinary failover.
//
// st is the state the write left the cycle in, so [Manager.Write] reads it
// after the write and not with it.
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

// attribution is whose answer a failed assertion at apply is, if anybody's.
// The three are exhaustive.
type attribution int

const (
	// haltsShard is the default: a window's writers have all been acked, so a
	// condition that did not hold cannot be pinned on one of them and a retry
	// fixes nothing.
	haltsShard attribution = iota
	// answersItsCaller is sync mode's window of one, whose writer is still
	// inside the [Cycle.write] that appended it ([Cycle.answerWriter]).
	answersItsCaller
	// dropsItsEntry is a replayed provisional entry, carried alone, whose
	// caller has the answer already ([Cycle.dropProvisional]).
	dropsItsEntry
)

// attribute is the drain's attribution rule. Both conjuncts are needed: a rule
// reading the cause alone would report a stranger's failure to a caller the
// moment a second call site spelled that cause, and wrong the other way is a
// halt where an answer was owed.
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

// settlement is what a drain's outcome does to the shard. [apply.Class] says
// what the transaction did; this says what it means here, which is a different
// question and the only one this package answers.
//
// The six are exhaustive and their watermark discipline splits two ways: only
// the first two put rows in the cold store, so only they may move it. The other
// four leave it where the cold store put it — a trim goes to the watermark, and
// one moved over rows that are not there strands a recovering owner.
type settlement int

const (
	// settlesForward: the rows are in the cold store, so the watermark moves
	// over them and the drain is counted and emitted.
	settlesForward settlement = iota
	// asksTheWatermark: the outcome is unreadable and the watermark is the only
	// witness. One at or above the drain's seqno means it committed after all,
	// and the drain settles forward; anything else halts.
	asksTheWatermark
	// answersItsWriter: sync mode's window of one, whose writer is still inside
	// the call that appended it ([Cycle.answerWriter]).
	answersItsWriter
	// dropsTheEntry: a replayed provisional entry whose caller has its answer
	// already ([Cycle.dropProvisional]).
	dropsTheEntry
	// haltsInvariant is a divergence this process owns, and it may never be
	// converted to ShardOwnershipLost.
	haltsInvariant
	// haltsLost is the fence working.
	haltsLost
)

// settlementOf is the rule, named so it cannot be read as performing one. A
// class it does not recognise halts on the invariant side deliberately: an
// outcome nobody enumerated is not one to carry on from, and the other arm
// would hand it to the next owner as an ordinary failover.
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
