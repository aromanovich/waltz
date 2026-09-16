package cycle

// The decisions of decide.go, asked directly and enumerated: the five moments a
// read can arrive in, both units across I10's bound, the recognised and
// unrecognised errors at each state, and every drain cause at every window size. The cycle-shaped tests beside this file say what a cycle
// does, which is a different claim.
//
// The attribution rule cannot be falsified through a cycle at all: the only
// cause carrying [answersCaller] is issued at one call site, where sync mode's
// window is one by construction.

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	p "go.temporal.io/server/common/persistence"

	"github.com/aromanovich/waltz/apply"
	"github.com/aromanovich/waltz/wal"
	"github.com/aromanovich/waltz/walmetrics"
)

// Who answers a read, and out of which sources.

// requireRoute holds a rule to its route and to the answer that route carries:
// nothing to refuse with on the routes that answer, the one concrete type the
// shard's read path matches on [refuseAsLost], the caller's own halt untouched
// on [refuseAsHalt], and the write path's own refusal on [refuseAsUnresolved].
func requireRoute(t *testing.T, want, got readRoute, refusal, halt error) {
	t.Helper()
	require.Equal(t, want, got, "route %d where the rule says %d, refusing with %v", got, want, refusal)

	switch want {
	case merge, passThrough, retryOnSuccessor:
		require.NoError(t, refusal, "a route that answers refuses nothing")
	case refuseAsLost:
		requireLost(t, refusal)
	case refuseAsHalt:
		require.Same(t, halt, refusal,
			"the halt's own error, unrebuilt: its cause is the only record of what diverged")
		require.False(t, errors.As(refusal, new(*p.ShardOwnershipLostError)),
			"a divergence this process owns must not leave here as a failover")
	case refuseAsUnresolved:
		requireRefusal(t, refusal)
		require.False(t, errors.As(refusal, new(*p.ShardOwnershipLostError)),
			"a shard that will answer again the moment its store does must not be re-acquired")
	}
}

// TestEveryMomentAReadCanArriveInRoutesBothReaders is the whole matrix: the
// five moments, each over its own inputs and both readers. The rows that are
// constraints rather than behaviour — halted-lost refuses a task read whatever
// the tail says, a stopped cycle refuses one whatever its state, an unreadable
// drain refuses both whoever is asking, and halted-invariant is never converted
// to ShardOwnershipLost — are marked where they sit.
func TestEveryMomentAReadCanArriveInRoutesBothReaders(t *testing.T) {
	// The value [Cycle.halted] would have built; every [refuseAsHalt] row
	// asserts this exact value comes back.
	halt := fmt.Errorf("%w (halted-invariant), shard %d: %w",
		ErrHalted, int(testShard), errors.New("a version assertion failed"))

	t.Run("the registry holds no cycle for the shard", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			who  reader
			want readRoute
		}{
			{"a mutable-state read", mutableStateRead, passThrough},
			{"a task read", taskRead, refuseAsLost},
		} {
			t.Run(tc.name, func(t *testing.T) {
				route, refusal := noCycleRoute(tc.who, testShard)
				requireRoute(t, tc.want, route, refusal, halt)
			})
		}
	})

	// Three states × two readers × the tail's two, and the stall over all of
	// them: a drain nobody could read the outcome of is a fact about the tail
	// and not about who holds the shard.
	t.Run("the cycle answers for itself", func(t *testing.T) {
		const at = wal.Seqno(2) // the drain a stalled tail is stalled at

		for _, tc := range []struct {
			name      string
			st        State
			tailEmpty bool
			stalled   wal.Seqno
			who       reader
			want      readRoute
		}{
			{"running, mutable-state read, empty tail", StateRunning, true, 0, mutableStateRead, merge},
			{"running, mutable-state read, held tail", StateRunning, false, 0, mutableStateRead, merge},
			{"running, task read, empty tail", StateRunning, true, 0, taskRead, merge},
			{"running, task read, held tail", StateRunning, false, 0, taskRead, merge},

			// Both readers, because both are owed something a merge cannot give:
			// the row would be older than what its own writer was acked, and the
			// page short the tasks of the drain nobody can place. The tail's own
			// two are here for totality — a stalled tail is never empty, since
			// the stalled drain acked the entries it stopped at.
			{"running and stalled, mutable-state read", StateRunning, false, at, mutableStateRead, refuseAsUnresolved},
			{"running and stalled, task read", StateRunning, false, at, taskRead, refuseAsUnresolved},
			{"running and stalled, mutable-state read, empty tail", StateRunning, true, at, mutableStateRead, refuseAsUnresolved},
			{"running and stalled, task read, empty tail", StateRunning, true, at, taskRead, refuseAsUnresolved},

			{"lost, mutable-state read, empty tail", StateHaltedLost, true, 0, mutableStateRead, passThrough},
			{"lost, mutable-state read, held tail", StateHaltedLost, false, 0, mutableStateRead, refuseAsLost},
			// The two the tail may not decide: another owner is acking into this
			// shard's log, and its entries are in neither this tail nor the cold
			// store.
			{"lost, task read, empty tail", StateHaltedLost, true, 0, taskRead, refuseAsLost},
			{"lost, task read, held tail", StateHaltedLost, false, 0, taskRead, refuseAsLost},

			// Halted-invariant: the shard has not moved on, so the tail rule is
			// the whole rule and both readers get the same answer.
			{"invariant, mutable-state read, empty tail", StateHaltedInvariant, true, 0, mutableStateRead, passThrough},
			{"invariant, mutable-state read, held tail", StateHaltedInvariant, false, 0, mutableStateRead, refuseAsHalt},
			{"invariant, task read, empty tail", StateHaltedInvariant, true, 0, taskRead, passThrough},
			{"invariant, task read, held tail", StateHaltedInvariant, false, 0, taskRead, refuseAsHalt},

			// A watermark that answered below the stalled seqno halts and leaves
			// the stall standing, so these two are reached: the halt is the
			// answer, being where this shard stopped for good.
			{"invariant and stalled, mutable-state read", StateHaltedInvariant, false, at, mutableStateRead, refuseAsHalt},
			{"lost and stalled, task read", StateHaltedLost, false, at, taskRead, refuseAsLost},
		} {
			t.Run(tc.name, func(t *testing.T) {
				route, refusal := loopRoute(tc.st, tc.tailEmpty, tc.stalled, tc.who, testShard, halt)
				requireRoute(t, tc.want, route, refusal, halt)
			})
		}
	})

	// The same input space, where the answer is a mirrored tail and a state the
	// loop left behind. A stopped cycle never reports [StateRunning] —
	// [Cycle.Retire] moves it before the goroutine ends — so those rows are the
	// rule being total over its inputs rather than a shape a cycle reaches.
	t.Run("the cycle's goroutine is gone", func(t *testing.T) {
		for _, tc := range []struct {
			name      string
			st        State
			tailEmpty bool
			who       reader
			want      readRoute
		}{
			{"running, mutable-state read, empty tail", StateRunning, true, mutableStateRead, passThrough},
			{"running, mutable-state read, held tail", StateRunning, false, mutableStateRead, refuseAsHalt},
			{"running, task read, empty tail", StateRunning, true, taskRead, refuseAsLost},
			{"running, task read, held tail", StateRunning, false, taskRead, refuseAsLost},

			{"lost, mutable-state read, empty tail", StateHaltedLost, true, mutableStateRead, passThrough},
			{"lost, mutable-state read, held tail", StateHaltedLost, false, mutableStateRead, refuseAsLost},
			// The shard's tail is the successor's window now, which a page must
			// not be short of and a stale row may be.
			{"lost, task read, empty tail", StateHaltedLost, true, taskRead, refuseAsLost},
			{"lost, task read, held tail", StateHaltedLost, false, taskRead, refuseAsLost},

			{"invariant, mutable-state read, empty tail", StateHaltedInvariant, true, mutableStateRead, passThrough},
			{"invariant, mutable-state read, held tail", StateHaltedInvariant, false, mutableStateRead, refuseAsHalt},
			{"invariant, task read, empty tail", StateHaltedInvariant, true, taskRead, refuseAsLost},
			{"invariant, task read, held tail", StateHaltedInvariant, false, taskRead, refuseAsLost},
		} {
			t.Run(tc.name, func(t *testing.T) {
				route, refusal := stoppedRoute(tc.st, tc.tailEmpty, tc.who, testShard, halt)
				requireRoute(t, tc.want, route, refusal, halt)
			})
		}
	})

	// The last moment is the task read's alone, and its inputs are two facts
	// about the registry rather than about a cycle.
	t.Run("the cycle that answered is no longer the shard's", func(t *testing.T) {
		for _, tc := range []struct {
			name         string
			stillCurrent bool
			retried      bool
			want         readRoute
		}{
			{"still the shard's cycle", true, false, merge},
			{"still the shard's cycle, on the rebuilt page", true, true, merge},
			{"superseded once", false, false, retryOnSuccessor},
			{"superseded twice inside one page read", false, true, refuseAsLost},
		} {
			t.Run(tc.name, func(t *testing.T) {
				route, refusal := supersededRoute(tc.stillCurrent, tc.retried, testShard)
				requireRoute(t, tc.want, route, refusal, halt)
			})
		}
	})
}

// ---------------------------------------------------------------------------
// What a write meets before the append.
// ---------------------------------------------------------------------------

// TestAWriteIsRefusedAtItsEdgeAndTheRefusalNamesWhy walks both of I10's units
// across the boundary and the unreadable outcome beside them, and asserts which
// one is reported: a shard tripping more than one has one metric to emit, and
// the tag is the diagnosis — entries means the applier is stalled, bytes a
// workflow near the server's own blob limits, unresolved an applier that cannot
// say what it did.
//
// The bound is `>=` in both units — at the limit is full — and every refusal is
// the concrete type the shard's write path reads as "definitely not committed".
func TestAWriteIsRefusedAtItsEdgeAndTheRefusalNamesWhy(t *testing.T) {
	cfg := Config{HardMaxEntries: 8, HardMaxBytes: 1024}

	for _, tc := range []struct {
		name    string
		entries int64
		bytes   int64
		stalled wal.Seqno
		limit   string // "" means the write may proceed
	}{
		{"an empty tail", 0, 0, 0, ""},
		{"one short in both units", 7, 1023, 0, ""},
		{"at the entry bound", 8, 0, 0, walmetrics.LimitEntries},
		{"one short of the entry bound, with bytes to spare", 7, 512, 0, ""},
		{"at the byte bound", 0, 1024, 0, walmetrics.LimitBytes},
		{"one short of the byte bound", 8191, 1023, 0, walmetrics.LimitEntries},
		// The bound reads the tail as it stands, so a mutation is never refused
		// for its own size and the tail may pass the limit by an entry.
		{"over the entry bound by one", 9, 0, 0, walmetrics.LimitEntries},
		{"one oversized mutation", 1, 2048, 0, walmetrics.LimitBytes},
		// Both at once is reported as bytes: that is the diagnosis an operator
		// cannot fix by waiting for the applier.
		{"over both bounds", 64, 4096, 0, walmetrics.LimitBytes},
		// And a stall outranks the sizes, because it is the one waiting will
		// not clear: a tail that cannot be applied over is why the other two
		// grow in the first place.
		{"a tail well inside both bounds, stalled", 1, 16, 4, walmetrics.LimitUnresolved},
		{"stalled and over both bounds", 64, 4096, 4, walmetrics.LimitUnresolved},
	} {
		t.Run(tc.name, func(t *testing.T) {
			refusal, limit := writeRefused(tc.entries, tc.bytes, tc.stalled, testShard, cfg)
			if tc.limit == "" {
				require.Nil(t, refusal, "a write is not refused until it is")
				require.Empty(t, limit, "and nothing is refused, so nothing named a reason")
				return
			}
			require.NotNil(t, refusal)
			require.Equal(t, tc.limit, limit)
			require.Equal(t, enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_LIMIT, refusal.Cause,
				"a persistence limit, not a tenant's doing")
			require.Equal(t, enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM, refusal.Scope,
				"SCOPE_SYSTEM keeps the retry inside the history client")
			require.False(t, p.OperationPossiblySucceeded(refusal),
				"a refusal raised before the append is definitely not committed, and the shard reads that off the type")
			require.Contains(t, refusal.Message, fmt.Sprintf("shard %d", testShard),
				"an operator reading one refusal must be told which shard tripped")
			if tc.limit == walmetrics.LimitUnresolved {
				require.Contains(t, refusal.Message, fmt.Sprintf("seqno %d", tc.stalled),
					"and which drain it is waiting on")
				return
			}
			require.Contains(t, refusal.Message, fmt.Sprintf("%d/%d entries", tc.entries, cfg.HardMaxEntries))
			require.Contains(t, refusal.Message, fmt.Sprintf("%d/%d bytes", tc.bytes, cfg.HardMaxBytes))
		})
	}
}

// ---------------------------------------------------------------------------
// The store boundary.
// ---------------------------------------------------------------------------

// TestOnlyAFenceBecomesShardOwnershipLost is [storeError]'s whole matrix.
// Recognition is checked before the state, so a value the shard already
// recognises travels untouched even on a halted-lost cycle; the two checks in
// the other order would rebuild a condition failure as a failover.
func TestOnlyAFenceBecomesShardOwnershipLost(t *testing.T) {
	// The three the store recognises directly, and one it does not.
	condition := &p.WorkflowConditionFailedError{Msg: "stale"}
	fenced := &p.ShardOwnershipLostError{ShardID: int32(testShard), Msg: "another node has it"}
	refused := &serviceerror.ResourceExhausted{Cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_LIMIT}
	unknown := errors.New("the transaction's outcome was not read back")
	halted := fmt.Errorf("%w (halted-invariant), shard %d: %w", ErrHalted, int(testShard), unknown)
	// The boundary type-switches rather than using errors.As, so a wrapped
	// condition failure is not one the shard would recognise either.
	wrapped := fmt.Errorf("cycle: encoding a mutation of shard %d: %w", int(testShard), condition)

	for _, tc := range []struct {
		name    string
		st      State
		err     error
		toLost  bool // ShardOwnershipLost, or the value handed back untouched
		wantNil bool
	}{
		{name: "a write that succeeded, running", st: StateRunning, err: nil, wantNil: true},
		{name: "a write that succeeded on a fenced cycle", st: StateHaltedLost, err: nil, wantNil: true},

		{name: "a condition failure, running", st: StateRunning, err: condition},
		{name: "apply's own fence, running", st: StateRunning, err: fenced},
		{name: "I10's refusal, running", st: StateRunning, err: refused},
		{name: "an unknown outcome, running", st: StateRunning, err: unknown},

		// The state maps exactly one way, and only for values the shard would
		// otherwise re-acquire over.
		{name: "an unknown outcome on a fenced cycle", st: StateHaltedLost, err: unknown, toLost: true},
		{name: "the halt's refusal on a fenced cycle", st: StateHaltedLost, err: halted, toLost: true},
		{name: "a wrapped condition failure on a fenced cycle", st: StateHaltedLost, err: wrapped, toLost: true},
		// …and not for a value that already says "definitely not committed",
		// whatever the cycle's state is.
		{name: "a condition failure on a fenced cycle", st: StateHaltedLost, err: condition},
		{name: "I10's refusal on a fenced cycle", st: StateHaltedLost, err: refused},
		{name: "apply's own fence on a fenced cycle", st: StateHaltedLost, err: fenced},

		// Halted-invariant maps nothing: the divergence stays this process's.
		{name: "an unknown outcome on a diverged cycle", st: StateHaltedInvariant, err: unknown},
		{name: "the halt's refusal on a diverged cycle", st: StateHaltedInvariant, err: halted},
		{name: "a condition failure on a diverged cycle", st: StateHaltedInvariant, err: condition},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := storeError(tc.st, testShard, tc.err)
			switch {
			case tc.wantNil:
				require.NoError(t, got, "a write that worked has nothing to translate")
			case tc.toLost:
				lost := requireLost(t, got)
				require.Contains(t, lost.Msg, tc.err.Error(), "and it carries what the cycle said")
			default:
				require.Same(t, tc.err, got,
					"the value must not be rebuilt on the way out: the shard type-switches on it")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// The drain's attribution.
// ---------------------------------------------------------------------------

// drainCauses is every declared cause, with what the attribution rule makes of
// it at a window of one. Written once, so a tenth cause is given a reading by
// both tests below rather than by whichever one somebody remembered.
var drainCauses = []struct {
	name  string
	cause drainCause
	atOne attribution
}{
	{"sync mode's own drain", drainSync, answersItsCaller},
	{"a replayed provisional entry", drainReplayProvisional, dropsItsEntry},
	{"the mutation watermark", drainWatermarkMutations, haltsShard},
	{"the byte watermark", drainWatermarkBytes, haltsShard},
	{"the age watermark", drainWatermarkAge, haltsShard},
	{"fold's refusal", drainRefusal, haltsShard},
	{"an explicit drain", drainExplicit, haltsShard},
	{"a read draining the window", drainRead, haltsShard},
	{"a previous owner's tail", drainReplay, haltsShard},
}

// TestAFailedAssertionIsAttributedOnlyToACallerInAWindowOfOne pins both halves
// of [attribute]: the cause must say a caller is still on the line, and the
// window must hold the one mutation that caller wrote. Dropping the window
// conjunct would let a caller be told its write failed on entries somebody else
// wrote, which stay in the log marked settled.
func TestAFailedAssertionIsAttributedOnlyToACallerInAWindowOfOne(t *testing.T) {
	for _, tc := range drainCauses {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.atOne, attribute(tc.cause, 1),
				"at a window of one the cause is the whole of the rule")

			// Every other window, including the empty one a drain never
			// reaches.
			for _, in := range []int{0, 2, 3, 256} {
				require.Equal(t, haltsShard, attribute(tc.cause, in),
					"a window of %d mixes writers who were acked long ago: a failure in it is nobody's answer", in)
			}
		})
	}

	// The two claims the matrix above stands in for.
	require.Equal(t, haltsShard, attribute(drainSync, 2),
		"a caller may only be told about a window that holds its own mutation and nothing else")
	require.Equal(t, haltsShard, attribute(drainReplayProvisional, 2),
		"a provisional entry is dropped only where it was carried alone")
	require.Equal(t, haltsShard, attribute(drainWatermarkMutations, 1),
		"a window of one whose caller was already acked has nobody to answer either")
}

// TestTheOutcomeOfEveryAppendError is the classifier's whole rule: the three
// refusals the contract names, and the default that everything else falls to.
// The default is the half worth enumerating — an error read as "wrote nothing"
// is a seqno whose fate nobody established handed to the next mutation — so the
// unrecognised cases below are the ones a transport actually produces, each
// asked in the shapes a caller wraps them in.
func TestTheOutcomeOfEveryAppendError(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want appendOutcome
	}{
		{"fenced", wal.ErrFenced, appendFenced},
		{"fenced, wrapped by a backend", fmt.Errorf("shard 3: %w", wal.ErrFenced), appendFenced},
		{"the seqno is taken", wal.ErrAlreadyWritten, appendTaken},
		{"taken, wrapped", fmt.Errorf("shard 3: %w", wal.ErrAlreadyWritten), appendTaken},
		{"a gap below it", wal.ErrGap, appendNothing},
		{"a gap, wrapped", fmt.Errorf("shard 3: %w", wal.ErrGap), appendNothing},

		{"a timeout", context.DeadlineExceeded, appendUnknown},
		{"the caller's cancellation", context.Canceled, appendUnknown},
		{"a zero epoch, which the contract admits but this cycle cannot send", wal.ErrZeroEpoch, appendUnknown},
		{"a bare transport failure", errors.New("connection reset by peer"), appendUnknown},
		{"a nil error, which no caller asks about", nil, appendUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, appendOutcomeOf(tc.err),
				"an append error classified as anything but %v: the three the contract names each "+
					"say the write is whole one way or the other, and everything else leaves it "+
					"unestablished", tc.want)
		})
	}

	require.Equal(t, appendUnknown, appendOutcome(0),
		"the zero value is not the unestablished outcome, so an outcome nobody set reads as one "+
			"somebody did — which is the rounding-down this classifier exists to refuse")
}

// TestTheSettlementOfEveryClass enumerates every apply class at every cause and
// window size.
//
// The watermark discipline is not asserted here and cannot be: which way a
// settlement settles is the branch [Cycle.drain] takes, not a property of the
// value. It is covered by cycle tests that drive the settlement branch.
func TestTheSettlementOfEveryClass(t *testing.T) {
	windows := []int{0, 1, 2, 3, 256}

	// The four classes whose meaning is a function of the class alone: what the
	// window held and who triggered it change nothing.
	for _, tc := range []struct {
		class apply.Class
		want  settlement
	}{
		{apply.ClassCommitted, settlesForward},
		{apply.ClassUnknownOutcome, asksTheWatermark},
		{apply.ClassShardLost, haltsLost},
		{apply.ClassRefused, haltsInvariant},
	} {
		t.Run(tc.class.String(), func(t *testing.T) {
			for _, c := range drainCauses {
				for _, in := range windows {
					require.Equal(t, tc.want, settlementOf(tc.class, c.cause, in),
						"%s at a window of %d", c.name, in)
				}
			}
		})
	}

	// The fifth class is the only one that reads the cause and the window, and
	// it reads them through [attribute] — so the rows here are that rule's,
	// mapped onto what the drain then does.
	t.Run("ClassInvariantViolated", func(t *testing.T) {
		// Three points and no loop over the nine causes: a loop here would have
		// to build its expectation by calling [attribute], which is what this
		// arm calls, so it would restate the implementation and move with it.
		// The rule itself is held across every cause and window by [attribute]'s
		// own matrix above, whose expectations are written out.
		require.Equal(t, answersItsWriter, settlementOf(apply.ClassInvariantViolated, drainSync, 1),
			"the one caller still inside its own write is told")
		require.Equal(t, dropsTheEntry, settlementOf(apply.ClassInvariantViolated, drainReplayProvisional, 1),
			"a provisional entry carried alone is dropped, not halted on")
		require.Equal(t, haltsInvariant, settlementOf(apply.ClassInvariantViolated, drainWatermarkMutations, 256),
			"a window of many is nobody's answer")
	})

	// An unrecognised class halts, and on the invariant side: converting it
	// would hand a divergence this process owns to the next owner as a failover.
	require.Equal(t, haltsInvariant, settlementOf(apply.Class(-1), drainExplicit, 1))
}
