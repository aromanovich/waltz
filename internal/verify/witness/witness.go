// Package witness checks the claims a run over the WAL layer must make about
// the layer's own counters, the same way for every run.
//
// It catches failures that leave suites green: a layer that ended up as
// passthrough, or a method that reaches the log when it should not. A run
// states what it was meant to be ([Expect]) and what its instruments saw
// ([Observed]); the witness says whether they agree.
//
// Everything here is a pure function of values a table test can build, so the
// witness itself can be tested.
//
// [Observed] requires a [cycle.Totals]. Summing [cycle.Cycle.Stats] over the
// shards held now would silently drop the counters of a shard re-acquired
// mid-run.
package witness

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/aromanovich/waltz/cycle"
	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/walmetrics"
	"github.com/aromanovich/waltz/wrapper"
)

// Window is the window the run's cycle kept. In Sync no read crosses a held
// workflow and the tail ends empty; in Windowed both must have happened, or
// the run was really sync. NoLayer is the control: the layer was out of the
// path and everything it counts must be zero.
//
// The zero value is not a window, and [Expect.Check] refuses it: an Expect
// with no window would assert nothing.
type Window int

const (
	_ Window = iota
	NoLayer
	Sync
	Windowed
)

// String names the window for error messages.
func (w Window) String() string {
	switch w {
	case NoLayer:
		return "no-layer"
	case Sync:
		return "sync"
	case Windowed:
		return "windowed"
	default:
		return fmt.Sprintf("window(%d)", int(w))
	}
}

// KindClaim claims that entries of Kind reached the log. Why says what in the
// run should have produced them, so a failure says what went missing.
type KindClaim struct {
	Kind mutation.Kind
	Why  string
}

// Emission is one recorded value of one metric series, with the tags it was
// recorded under.
type Emission struct {
	Value int64
	Tags  map[string]string
}

// Emissions is a run's captured metric emissions by series name. It is this
// package's own type, not metricstest's, so table tests can build literals.
// A run without a capture handler passes nil.
type Emissions map[string][]Emission

// Observed is what a run's instruments saw. Totals is required. Store and
// Emitted are optional (a live server's are not reachable from its test
// process); a nil one skips only the claims that read it.
//
// The strongest claims compare instruments: what the store sent against what
// the layer answered, and counters against emissions, since an unemitted
// series looks like idleness. They run whenever both sides are present.
type Observed struct {
	// Totals is the layer's account of the whole run, retired cycles
	// included, from [cycle.Manager.Totals].
	//
	// Acked is a log position, not a count. The condition-failure claims
	// read it as a count, which holds only if each shard's log started
	// empty; a run over inherited tails must not claim [Sync] with
	// ConditionFailures.
	Totals cycle.Totals
	// Store is the wrapper's counters of what was sent to the layer. Nil if
	// the run cannot reach the store.
	Store *wrapper.Counts
	// Emitted is the captured emissions; nil with no capture handler.
	Emitted Emissions
}

// Expect is what the run was meant to be: its window and what its suites
// drove at the layer. A false field claims absence: for example, no task
// reads means zero pages went through the merge.
type Expect struct {
	// Window is required; the zero value is refused.
	Window Window
	// ShardsAcquired claims the ShardObserver saw at least one acquire. In a
	// run that only acquires shards, any write means something else wrote.
	ShardsAcquired bool
	// MutableState: the suites make mutable-state writes or deletes.
	MutableState bool
	// HistoryTasks: the suites write history tasks (logged like state).
	HistoryTasks bool
	// TaskReads: the suites read task pages through the merge.
	TaskReads bool
	// Ranges: a range delete completed. Separate from HistoryTasks because
	// queue checkpoints run on a 30s timer, so a short run has none.
	Ranges bool
	// TailHeld claims the run ends with acked, unapplied entries in the tail.
	// A run that waits for a drain before sampling must not claim it.
	TailHeld bool
	// ConditionFailures claims the suites expect some conditions to fail.
	// Read only under [Sync], where a failed condition is answered after the
	// append, so acked entries exceed committed drains. In windowed
	// mode conditions are decided before the append, and the answered
	// failure series must always be empty.
	ConditionFailures bool
	// Kinds lists the entry kinds that must have reached the log (nil for
	// none). It claims presence, not amounts.
	Kinds []KindClaim
}

// The series the witness reads, taken from their declarations so a rename
// cannot silently empty a claim.
var (
	seriesIntercepted               = walmetrics.InterceptedWrites.Name()
	seriesDrains                    = walmetrics.Drains.Name()
	seriesDrainedMutations          = walmetrics.DrainedMutations.Name()
	seriesAnsweredConditionFailures = walmetrics.AnsweredConditionFailures.Name()
	seriesHalts                     = walmetrics.Halts.Name()
)

// Universal checks the claims every run makes, even one that opted out of all
// others: no shard halted, and trims that started committed.
func Universal(t cycle.Totals) []error {
	var errs []error
	if len(t.Halted) > 0 {
		errs = append(errs, fmt.Errorf("a shard halted: %v", t.Halted))
	}
	// A failed trim halts nothing and is retried, so nothing else notices the
	// log is no longer compacted. A run too short to start a trim is exempt.
	if t.Trims > 0 && t.TrimsCommitted == 0 {
		errs = append(errs, fmt.Errorf(
			"no trim committed: the cadence started %d and the log was never compacted", t.Trims))
	}
	return errs
}

// report records one violation; [Expect.Check] prefixes the claim's name.
type report func(format string, args ...any)

// claim is one check on a run. Its name labels failures and keys the
// leave-one-out table that shows each claim catches something unique.
type claim struct {
	Name string
	// Gate must hold for Check to apply (instrument present, coverage
	// claimed). Nil means every run with a layer makes the claim.
	Gate func(Expect, Observed) bool
	// Check reports once per violation.
	Check func(Expect, Observed, report)
}

// The gates, each one fact about the run or its instruments.
func hasStore(_ Expect, o Observed) bool        { return o.Store != nil }
func hasEmissions(_ Expect, o Observed) bool    { return o.Emitted != nil }
func acquiresShards(e Expect, _ Observed) bool  { return e.ShardsAcquired }
func writesState(e Expect, _ Observed) bool     { return e.MutableState }
func writesTasks(e Expect, _ Observed) bool     { return e.HistoryTasks }
func writesNoTasks(e Expect, _ Observed) bool   { return !e.HistoryTasks }
func writesNothing(e Expect, _ Observed) bool   { return !e.MutableState && !e.HistoryTasks }
func writesSomething(e Expect, _ Observed) bool { return e.MutableState || e.HistoryTasks }
func readsTasks(e Expect, _ Observed) bool      { return e.TaskReads }
func readsNoTasks(e Expect, _ Observed) bool    { return !e.TaskReads }
func completesRanges(e Expect, _ Observed) bool { return e.Ranges }
func expectsFailures(e Expect, _ Observed) bool { return e.ConditionFailures }
func inSync(e Expect, _ Observed) bool          { return e.Window == Sync }
func inWindowed(e Expect, _ Observed) bool      { return e.Window == Windowed }
func tasksOnly(e Expect, o Observed) bool       { return writesTasks(e, o) && !writesState(e, o) }
func drainsCommitted(_ Expect, o Observed) bool { return o.Totals.Drains > 0 }

// all is the conjunction of gates.
func all(gates ...func(Expect, Observed) bool) func(Expect, Observed) bool {
	return func(e Expect, o Observed) bool {
		for _, gate := range gates {
			if !gate(e, o) {
				return false
			}
		}
		return true
	}
}

// controlClaims check that a [NoLayer] run really had no layer; otherwise the
// control would include the behaviour it exists to exclude. With
// Options.Layer nil every counter is zero by construction.
var controlClaims = []claim{{
	Name: "C1 the control reached no shard",
	Check: func(_ Expect, o Observed, report report) {
		t := o.Totals
		if t.Shards != 0 || t.Acked != 0 || t.TaskReads != 0 {
			report("the run reached the layer (shards=%d acked=%d task-reads=%d): it is not the control it claims to be",
				t.Shards, t.Acked, t.TaskReads)
		}
	},
}, {
	Name: "C2 the control's store counted nothing",
	Gate: hasStore,
	Check: func(_ Expect, o Observed, report report) {
		if *o.Store != (wrapper.Counts{}) {
			report("the control's store counted something (%+v): it is not the control it claims to be", *o.Store)
		}
	},
}}

// layerClaims are checked for a run with a layer, in report order: layer
// reached, routed equalities, coverage, empty layer and its opposite, the sync
// and windowed inversion, emissions, claimed kinds.
var layerClaims = []claim{{
	Name: "W1 every acked entry named a request",
	// An unnameable kind is a bug in any run with a layer.
	Check: func(_ Expect, o Observed, report report) {
		if n := o.Totals.Kinds[mutation.KindInvalid]; n > 0 {
			report("%d entries were acked holding no request, or more than one: [mutation.Mutation.Kind] reports "+
				"KindInvalid for both, and either is a record the log should never have taken", n)
		}
	},
}, {
	Name: "W2 a shard was acquired through the layer",
	Gate: acquiresShards,
	Check: func(_ Expect, o Observed, report report) {
		if o.Totals.Shards == 0 {
			report("no shard was acquired through the layer: the ShardObserver never fired")
		}
	},
}, {
	Name: "W3 every read the store sent was answered by a shard",
	// Routed equality: a read counted at the store but not at a shard was
	// answered somewhere it should not have been.
	Gate: hasStore,
	Check: func(_ Expect, o Observed, report report) {
		if int64(o.Totals.Reads) != o.Store.Overlaid {
			report("the store counted %d reads at the layer and the shards answered %d", o.Store.Overlaid, o.Totals.Reads)
		}
	},
}, {
	Name: "W4 every task page the store sent was answered by a shard",
	Gate: hasStore,
	Check: func(_ Expect, o Observed, report report) {
		if int64(o.Totals.TaskReads) != o.Store.TaskReads {
			report("the store counted %d task pages at the layer and the shards answered %d", o.Store.TaskReads, o.Totals.TaskReads)
		}
	},
}, {
	Name: "W5 this run's task reads reached the merge",
	Gate: all(hasStore, readsTasks),
	Check: func(_ Expect, o Observed, report report) {
		if o.Store.TaskReads == 0 {
			report("this run reads task ranges; not one of them reached the merge")
		}
	},
}, {
	Name: "W6 no task page in a run that reads none",
	Gate: all(hasStore, readsNoTasks),
	Check: func(_ Expect, o Observed, report report) {
		if o.Store.TaskReads != 0 {
			report("%d task pages went through the merge in a run that reads none", o.Store.TaskReads)
		}
	},
}, {
	Name: "W7 this run's history tasks reached the log",
	// Zero here means the task path bypassed the accumulator.
	Gate: all(hasStore, writesTasks),
	Check: func(_ Expect, o Observed, report report) {
		if o.Store.TasksWritten == 0 {
			report("this run writes history tasks; not one of them reached the log")
		}
	},
}, {
	Name: "W8 no task traffic in a run that writes none",
	Gate: all(hasStore, writesNoTasks),
	Check: func(_ Expect, o Observed, report report) {
		if o.Store.TasksWritten != 0 || o.Store.TasksCompleted != 0 {
			report("%d task writes and %d range deletes reached the layer in a run that makes neither",
				o.Store.TasksWritten, o.Store.TasksCompleted)
		}
	},
}, {
	Name: "W9 this run's range completions reached the log",
	Gate: all(hasStore, completesRanges),
	Check: func(_ Expect, o Observed, report report) {
		if o.Store.TasksCompleted == 0 {
			report("this run range-completes tasks; not one of them reached the log")
		}
	},
}, {
	Name: "W10 a queue completed a range",
	Gate: completesRanges,
	Check: func(_ Expect, o Observed, report report) {
		if o.Totals.AckedRanges == 0 {
			report("no queue ever completed a range: the log carries no deletion record, so half of the " +
				"history-task path was not exercised at all")
		}
	},
}, {
	Name: "W11 an empty layer acked nothing",
	// A run that writes nothing leaves an empty log in every window; anything
	// else means the layer grew a path of its own.
	Gate: writesNothing,
	Check: func(_ Expect, o Observed, report report) {
		if o.Totals.Acked != 0 {
			report("%d entries were acked by a run that writes nothing: something else was writing", o.Totals.Acked)
		}
	},
}, {
	Name: "W12 an empty layer held no read",
	Gate: writesNothing,
	Check: func(_ Expect, o Observed, report report) {
		if o.Totals.ReadsHeld != 0 {
			report("%d reads crossed a window in a run that writes nothing into one", o.Totals.ReadsHeld)
		}
	},
}, {
	Name: "W13 an empty layer's store counted nothing",
	Gate: all(writesNothing, hasStore),
	Check: func(_ Expect, o Observed, report report) {
		if *o.Store != (wrapper.Counts{}) {
			report("the run writes no mutable state and no task, yet the store counted %+v: nothing may have entered the log", *o.Store)
		}
	},
}, {
	Name: "W14 an empty layer emitted nothing",
	Gate: all(writesNothing, hasEmissions),
	Check: func(_ Expect, o Observed, report report) {
		if n := len(o.Emitted[seriesIntercepted]); n != 0 {
			report("%d %s emissions were recorded by a run that writes nothing", n, seriesIntercepted)
		}
	},
}, {
	Name: "W15 the layer acked something",
	Gate: writesSomething,
	Check: func(_ Expect, o Observed, report report) {
		if o.Totals.Acked == 0 {
			report("the layer acked no mutation: the cluster ran on the plugin alone")
		}
	},
}, {
	Name: "W16 a write reached the layer",
	Gate: all(writesState, hasStore),
	Check: func(_ Expect, o Observed, report report) {
		if o.Store.Intercepted == 0 {
			report("no write reached the layer: this run was passthrough wearing another mode's name")
		}
	},
}, {
	Name: "W17 something was applied",
	Gate: writesState,
	Check: func(_ Expect, o Observed, report report) {
		if o.Totals.Drains == 0 || o.Totals.Applied == 0 {
			report("nothing was applied (drains=%d applied=%d): entries were acked and none reached the cold store",
				o.Totals.Drains, o.Totals.Applied)
		}
	},
}, {
	Name: "W18 sync mode's overlay is a no-op",
	// Sync mode drains before answering, so the accumulator is empty at every
	// call boundary and no read can cross a held workflow.
	Gate: inSync,
	Check: func(_ Expect, o Observed, report report) {
		if o.Totals.ReadsHeld != 0 {
			report("%d of %d reads crossed a workflow the window still held: sync mode's overlay is supposed to be a no-op",
				o.Totals.ReadsHeld, o.Totals.Reads)
		}
	},
}, {
	Name: "W19 sync mode's tail is empty at a drain boundary",
	// At a window of one every acked entry is resolved before its caller is
	// answered, so a restart has nothing to replay. TailEntries is not
	// Acked−Applied: expected condition failures separate those (see
	// [cycle.Stats]).
	Gate: inSync,
	Check: func(_ Expect, o Observed, report report) {
		if o.Totals.TailEntries != 0 {
			report("%d acked entries were still unresolved at the end: the tail is not empty at a drain boundary", o.Totals.TailEntries)
		}
	},
}, {
	Name: "W20 sync mode's merge is a no-op",
	Gate: inSync,
	Check: func(_ Expect, o Observed, report report) {
		if o.Totals.TaskReadsMerged != 0 {
			report("%d of %d task pages carried a task out of the window: sync mode drains inside every write",
				o.Totals.TaskReadsMerged, o.Totals.TaskReads)
		}
	},
}, {
	Name: "W21 sync mode drained a run that writes only tasks",
	Gate: all(inSync, tasksOnly),
	Check: func(_ Expect, o Observed, report report) {
		if o.Totals.Drains == 0 {
			report("task writes were acked and no drain committed: nothing was applied")
		}
	},
}, {
	Name: "W22 sync mode answered a condition failure rather than committing it",
	// A failed condition at a window of one is answered, not committed, so
	// acked exceeds drains. Reads Acked as a count; see [Observed.Totals].
	Gate: all(inSync, expectsFailures),
	Check: func(_ Expect, o Observed, report report) {
		if int64(o.Totals.Acked) <= int64(o.Totals.Drains) {
			report("every acked entry committed (acked=%d drains=%d): this run writes conditions it expects "+
				"to fail, so some of them were not evaluated", o.Totals.Acked, o.Totals.Drains)
		}
	},
}, {
	Name: "W23 a windowed read crossed a held workflow",
	// The inverse of W18 and W19: in windowed mode each must happen, or the
	// run was really sync.
	Gate: all(inWindowed, writesState),
	Check: func(_ Expect, o Observed, report report) {
		if o.Totals.ReadsHeld == 0 {
			report("no overlay read crossed a held workflow (reads=%d): the read path was answered from an "+
				"empty window every time, which is passthrough wearing another name", o.Totals.Reads)
		}
	},
}, {
	Name: "W24 a windowed run ends with a tail",
	Gate: func(e Expect, o Observed) bool { return inWindowed(e, o) && e.TailHeld },
	Check: func(_ Expect, o Observed, report report) {
		if o.Totals.TailEntries == 0 {
			report("the tail was empty at the end: %d entries acked, %d drains, nothing held — a tail that "+
				"empties at every call boundary collapses nothing", o.Totals.Acked, o.Totals.Drains)
		}
	},
}, {
	Name: "W25 a windowed page carried a task out of the window",
	Gate: all(inWindowed, readsTasks),
	Check: func(_ Expect, o Observed, report report) {
		if o.Totals.TaskReadsMerged == 0 {
			report("not one of %d task pages carried a task out of the window: nobody ever read a merged page",
				o.Totals.TaskReads)
		}
	},
}, {
	Name: "W26 a windowed range took a task out of a window",
	// A folded range drops tasks the window holds. Drops are counted at commit
	// (an uncommitted drain is replayed), so a run with no committed drain
	// reports zero honestly; W10 covers the deletion path itself.
	Gate: all(inWindowed, completesRanges, drainsCommitted),
	Check: func(_ Expect, o Observed, report report) {
		if o.Totals.DroppedTasks == 0 {
			report("%d ranges were folded over %d committed drains and not one of them took a task out of a "+
				"window: the run completes ranges over tasks it has just written", o.Totals.AckedRanges, o.Totals.Drains)
		}
	},
}, {
	Name: "W27 intercepted writes were emitted",
	// Counters against emissions: unit tests pin what each series means,
	// these pin that the composition emits them at all.
	Gate: all(hasEmissions, writesState, hasStore),
	Check: func(_ Expect, o Observed, report report) {
		if n := len(o.Emitted[seriesIntercepted]); int64(n) != o.Store.Intercepted {
			report("%s recorded %d values over %d intercepted writes", seriesIntercepted, n, o.Store.Intercepted)
		}
	},
}, {
	Name: "W28 drains were emitted",
	Gate: all(hasEmissions, writesState),
	Check: func(_ Expect, o Observed, report report) {
		if n := len(o.Emitted[seriesDrains]); n != o.Totals.Drains {
			report("%s recorded %d values over %d committed drains", seriesDrains, n, o.Totals.Drains)
		}
	},
}, {
	Name: "W29 nothing was counted as halting",
	Gate: all(hasEmissions, writesState),
	Check: func(_ Expect, o Observed, report report) {
		if n := len(o.Emitted[seriesHalts]); n != 0 {
			report("nothing halted, so nothing may have been counted as halting: %s recorded %d values", seriesHalts, n)
		}
	},
}, {
	Name: "W30 sync mode's answered failures were emitted",
	Gate: all(hasEmissions, writesState, inSync, expectsFailures),
	Check: func(_ Expect, o Observed, report report) {
		n, want := len(o.Emitted[seriesAnsweredConditionFailures]), int64(o.Totals.Acked)-int64(o.Totals.Drains)
		if int64(n) != want {
			report("%s recorded %d values and %d drains were answered rather than committed",
				seriesAnsweredConditionFailures, n, want)
		}
	},
}, {
	Name: "W31 a windowed run's drained mutations were emitted",
	Gate: all(hasEmissions, writesState, inWindowed),
	Check: func(_ Expect, o Observed, report report) {
		if n := len(o.Emitted[seriesDrainedMutations]); n != o.Totals.Drains {
			report("%s recorded %d values over %d committed drains", seriesDrainedMutations, n, o.Totals.Drains)
		}
	},
}, {
	Name: "W32 a windowed drain collapsed something",
	// One value per committed drain; a value above 1 shows an actual collapse,
	// which no counter can.
	Gate: all(hasEmissions, writesState, inWindowed),
	Check: func(_ Expect, o Observed, report report) {
		if maxEmitted(o.Emitted[seriesDrainedMutations]) <= 1 {
			report("every one of %d drains carried a single mutation: nothing was collapsed", o.Totals.Drains)
		}
	},
}, {
	Name: "W33 no condition reached a windowed drain",
	// In windowed mode every condition is decided before the append, so the
	// series of failures answered from a drain must be empty. A value means a
	// condition reached a transaction, or a drain answered a caller it did
	// not have.
	Gate: all(hasEmissions, writesState, inWindowed),
	Check: func(_ Expect, o Observed, report report) {
		if n := len(o.Emitted[seriesAnsweredConditionFailures]); n != 0 {
			report("%d condition failures were raised at a drain: every condition this mode meets is "+
				"decided before the append", n)
		}
	},
}, {
	Name: "W34 every claimed kind reached the log",
	Check: func(e Expect, o Observed, report report) {
		for _, want := range e.Kinds {
			if o.Totals.Kinds[want.Kind] == 0 {
				report("no %s entry reached the log: %s", want.Kind, want.Why)
			}
		}
	},
}}

// Check returns the failed claims, each prefixed with the claim's name; empty
// means the run is what Expect says. It is pure.
func (e Expect) Check(o Observed) []error {
	switch e.Window {
	case NoLayer, Sync, Windowed:
	default:
		return []error{fmt.Errorf("the Expect states no window (%v): a witness that asserts nothing is the failure it exists to catch", e.Window)}
	}

	errs := Universal(o.Totals)
	claims := layerClaims
	if e.Window == NoLayer {
		claims = controlClaims
	}
	for _, c := range claims {
		if c.Gate != nil && !c.Gate(e, o) {
			continue
		}
		// errors.New, not Errorf: a % in the message is not a verb.
		c.Check(e, o, func(format string, args ...any) {
			errs = append(errs, errors.New(c.Name+": "+fmt.Sprintf(format, args...)))
		})
	}
	return errs
}

// Describe renders the run as one line for people. It prints every counter
// except trims and replay drops, and only the kinds that fired.
//
// task-collisions is printed but not asserted: a merged page's two sources are
// disjoint by construction, so non-zero is worth investigating, not failing.
func Describe(o Observed) string {
	t := o.Totals
	line := fmt.Sprintf("shards=%d epochs=%d acked=%d applied=%d tail=%d drains=%d "+
		"reads=%d reads-held=%d task-reads=%d task-reads-merged=%d task-collisions=%d "+
		"acked-ranges=%d dropped-tasks=%d written-tasks=%d "+
		"refusals=%d replayed=%d halted=%v kinds=[%s]",
		t.Shards, t.Epochs, t.Acked, t.Applied, t.TailEntries, t.Drains,
		t.Reads, t.ReadsHeld, t.TaskReads, t.TaskReadsMerged, t.TaskCollisions,
		t.AckedRanges, t.DroppedTasks, t.WrittenTasks,
		t.Refusals, t.Replayed, t.Halted, describeKinds(t))
	if o.Store != nil {
		line += fmt.Sprintf(" store=[writes=%d task-writes=%d range-deletes=%d reads=%d task-pages=%d]",
			o.Store.Intercepted, o.Store.TasksWritten, o.Store.TasksCompleted, o.Store.Overlaid, o.Store.TaskReads)
	}
	if by := drainTriggers(o.Emitted); by != "" {
		line += " drains-by-trigger=[" + by + "]"
	}
	return line
}

// describeKinds lists the kinds with a non-zero count.
func describeKinds(t cycle.Totals) string {
	var parts []string
	for k, n := range t.Kinds {
		if n > 0 {
			parts = append(parts, fmt.Sprintf("%s:%d", mutation.Kind(k), n))
		}
	}
	return strings.Join(parts, " ")
}

// drainTriggers counts drains by [walmetrics.Drains]'s trigger tag, which
// shows how the window behaved (for example, all refusal force-drains means no
// size or age trigger was ever reached).
func drainTriggers(emitted Emissions) string {
	by := map[string]int{}
	for _, r := range emitted[seriesDrains] {
		by[r.Tags["trigger"]]++
	}
	parts := make([]string, 0, len(by))
	for trigger, n := range by {
		parts = append(parts, fmt.Sprintf("%s %d", trigger, n))
	}
	slices.Sort(parts)
	return strings.Join(parts, ", ")
}

// maxEmitted returns the largest recorded value.
func maxEmitted(rs []Emission) int64 {
	var largest int64
	for _, r := range rs {
		largest = max(largest, r.Value)
	}
	return largest
}
