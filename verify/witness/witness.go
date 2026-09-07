// Package witness is the WAL layer's witness as one judged module: the claims
// a run over the layer must be able to make about the layer's own counters,
// stated once and checked the same way by the acceptance and by a live run
// (#174).
//
// A witness exists because the failure it catches is silent in both directions,
// and green is what it looks like either way (#58, #82): a layer that came out
// empty *is* passthrough, and somebody else's suites are green over it; a
// method that starts transiting into the log when it should not leaves them
// just as green. So a run states what it was supposed to be ([Expect]), hands
// over what its instruments saw ([Observed]), and the witness says whether the
// two agree.
//
// Everything here is a pure function of values a table test can build — no
// cluster, no server, no environment. That is #154's rule applied to the whole
// witness rather than to the live run's half of it: the module whose job is to
// catch a silent pass must be judgeable itself. Until this package there were
// two witnesses in two vocabularies, and the larger one — the acceptance's —
// was the one module in the tree nothing judged. It also summed over
// [cycle.Manager.Shards], the *current* cycles, so a shard re-acquired
// mid-suite silently dropped its counters out of the witness — the exact loss
// [cycle.Totals] was built to close (#128, #153) — and, being a hand-written
// copy, it had already lost TaskCollisions, Replayed, Dropped and Trims.
// [Observed] takes a [cycle.Totals] and nothing weaker.
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

// Window is what window the run's cycle kept — the axis the witness's two
// central claims invert on, and neither reading is the other's default (#82):
// in sync mode no read ever crosses a held workflow and nothing is left in the
// tail, and in a windowed mode both must have happened, or the run was
// intercept mode wearing a window's name. NoLayer is the control's claim — the
// layer was out of the path entirely, and everything it counts must be zero.
//
// The zero value is deliberately not a window. An Expect that states no window
// is a witness that asserts nothing, which is the failure this package exists
// to catch, so [Expect.Check] refuses it rather than defaulting it.
type Window int

const (
	_ Window = iota
	NoLayer
	Sync
	Windowed
)

// String names the window for a refusal message.
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

// KindClaim is one coverage claim: this kind of entry reached the log, and the
// reason a zero is a loss rather than a fact. The why names what went missing
// — for the live slice, the suite that was added to drive exactly this kind
// (#144) — because a red run must tell its reader which half of the layer went
// missing rather than that something did.
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

// Emissions is a run's captured metric emissions, by series name. It is
// deliberately this package's own type rather than metricstest's: the judged
// module may not depend on upstream test machinery to be judged, and a table
// test builds an Emissions literal where it could not build a captured
// recording. The acceptance converts its capture handler's snapshot in a few
// lines; a live run has no capture handler and passes nil.
type Emissions map[string][]Emission

// Observed is what a run's instruments saw. Totals is required and is the
// layer's own account — the seam that cannot lose a counter (#153). The other
// two are optional because not every run has them: a live server's store
// counters and metric emissions are not reachable from its test process, so a
// nil instrument skips exactly the claims that read it, and the claims that
// remain are the ones the run can actually make.
//
// The sharpest claims here are equalities *between* instruments — the store's
// count of what it sent at the layer against the layer's count of what it
// answered, and the counters against the emissions (#59), because a series
// nobody emits looks exactly like a system doing no work. Those run whenever
// both sides of an equality are present.
type Observed struct {
	// Totals is the layer's own account of the whole run: every cycle the node
	// has held, retired ones included. Taken from [cycle.Manager.Totals] —
	// never assembled by hand from [cycle.Manager.Shards], which is how the
	// previous copy of this witness lost every re-acquired shard's counters.
	//
	// One caveat travels with Acked: it is a position, not a count, and the
	// claims that read it as "how many entries went into the log" (the
	// condition-failure arithmetic below) hold only over folders that started
	// empty — which the acceptance's fixture guarantees and a live run's
	// witness must not assume, and does not: no claim a live run makes does
	// that arithmetic.
	Totals cycle.Totals
	// Store is the wrapper's own traffic counters — what was sent *at* the
	// layer, counted on the way in, against which Totals says what the layer
	// did with it. Nil for a run that cannot reach the store it decorated.
	Store *wrapper.Counts
	// Emitted is the run's captured metric emissions. Nil for a run with no
	// capture handler.
	Emitted Emissions
}

// Expect is what the run was supposed to be: which window its cycle kept, and
// what its suites drove at the layer. Each false field is a claim of absence,
// not a shrug — a run that reads no task ranges must have routed zero pages at
// the merge, because a suite reading around the layer is as green as one
// reading through it (#80).
type Expect struct {
	// Window is required; see [Window] for why the zero value is refused.
	Window Window
	// ShardsAcquired claims at least one shard was acquired through the layer
	// — the ShardObserver fired. It is fencing's positive claim, where the
	// *absence* of everything else is the point: a run whose suites only watch
	// shards being acquired writes nothing, so a mutation there means
	// something else was writing.
	ShardsAcquired bool
	// MutableState is whether the run's suites make any of the six
	// mutable-state writes.
	MutableState bool
	// HistoryTasks is whether they write history tasks. Since #142 those go
	// into the log like the six, so the assertion inverted with the path
	// (#33): zero wherever there is no layer, non-zero wherever there is one.
	HistoryTasks bool
	// TaskReads is whether they read task pages through the merge.
	TaskReads bool
	// Ranges is whether a range delete was completed — and it is a separate
	// claim from HistoryTasks because the two come apart in time: a queue's
	// checkpoint is on a 30s timer per queue per shard, so a run shorter than
	// that legitimately raises no range, and a witness that demanded one
	// would fail honest runs (measured: a 30s two-suite live run reports
	// acked-ranges=0).
	Ranges bool
	// TailHeld claims the run *ends* with entries still in the tail — acked
	// and not applied — which is the windowed acceptance's half of the
	// inversion, sampled before the fixture's cleanup drains what is left. A
	// live run must not claim it: its witness deliberately waits for a drain
	// before sampling (WaitForDrain), so its tail may honestly be empty.
	TailHeld bool
	// ConditionFailures claims the run's suites write conditions they expect
	// to fail. Its sync-mode reading is the mode's carve-out stated outright
	// (#57): a condition failure at a window of one is *answered*, so entries
	// acked exceed drains committed, and a run where every acked entry
	// committed is a run that stopped expecting its own failures. Read only
	// under [Sync]: in a windowed mode every condition is decided before the
	// append (#138), and the claim there is the emission's emptiness, made
	// unconditionally.
	ConditionFailures bool
	// Kinds is the run's coverage claims, nil for none: which of the eleven
	// intercepted methods must have reached the log at all (#144). A claim
	// about which, never about how much.
	Kinds []KindClaim
}

// The series the witness reads, taken from the declarations rather than
// restated, so a renamed series fails to compile here instead of silently
// emptying a claim.
var (
	seriesIntercepted               = walmetrics.InterceptedWrites.Name()
	seriesDrains                    = walmetrics.Drains.Name()
	seriesDrainedMutations          = walmetrics.DrainedMutations.Name()
	seriesAnsweredConditionFailures = walmetrics.AnsweredConditionFailures.Name()
	seriesHalts                     = walmetrics.Halts.Name()
)

// Universal is the claims every run makes, whatever it was — including a run
// that opted out of every other claim. A halt is not a statement about what a
// run covered; it is a shard that stopped, and a passthrough or opted-out run
// that halted a shard has something to say about the layer either way.
func Universal(t cycle.Totals) []error {
	var errs []error
	if len(t.Halted) > 0 {
		errs = append(errs, fmt.Errorf("a shard halted: %v", t.Halted))
	}
	// A trim that fails halts nothing and is retried at the next cadence, so
	// nothing else in a run says it happened: the log stops being compacted and
	// every suite stays green. The claim is conditional on the cadence having
	// fired at all — a run too short to trim says nothing here, which is why
	// this is a comparison and not `TrimsCommitted == 0`.
	if t.Trims > 0 && t.TrimsCommitted == 0 {
		errs = append(errs, fmt.Errorf(
			"no trim committed: the cadence started %d and the log was never compacted", t.Trims))
	}
	return errs
}

// report is how a claim states a violation: one line per thing it saw, with
// the claim's own name put in front of it by [Expect.Check].
type report func(format string, args ...any)

// claim is one thing the witness says about a run — the shape verify/checker's
// assertions have, for the reason that package's leave-one-out gives: a claim
// with a name can be shown to catch a defect no other claim catches, and a
// failure tells its reader which half of the layer went missing rather than
// that something did.
type claim struct {
	// Name is what a failure reports under, and what the leave-one-out table is
	// keyed on.
	Name string
	// Gate is what must hold before the predicate means anything — an absent
	// instrument, or a coverage the run never claimed. Nil is a claim every run
	// with a layer makes.
	Gate func(Expect, Observed) bool
	// Check reports once per violation.
	Check func(Expect, Observed, report)
}

// The gates. Each names one fact about the run or its instruments, so that a
// claim's row reads as "this, when that" rather than as a nested condition.
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

// all is the conjunction of gates, so a claim standing on two facts names both
// instead of retesting one inside its predicate.
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

// controlClaims are what a [NoLayer] run says, and they have to be asserted
// rather than assumed: a "passthrough" run that quietly still had the layer in
// it would answer the attribution question (#130) with the layer's own
// behaviour, which is the one thing it exists to exclude. Every counter on both
// instruments is zero by construction when Options.Layer is nil, which is what
// makes these cheap.
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

// layerClaims are what a run with a layer says. The order is the order they are
// reported in, and it is the order they were written in: the ones that say the
// layer was reached at all, then the routed equalities, then the coverage
// claims, then the two windows' inversion, then the emissions.
var layerClaims = []claim{{
	Name: "W1 every acked entry named a request",
	// A kind the codec could not name is a bug wherever it appears, so this
	// claim is made for any layer window rather than for any particular
	// coverage.
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
	// The routed equalities, which the two layer windows make identically. The
	// directions are not symmetric and both matter — a read counted at the
	// store and not at a shard is a read answered somewhere it should not have
	// been.
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
	// A suite that routes none of its task pages at the merge is reading around
	// the layer, which is as green as reading through it (#80).
	Gate: all(hasStore, readsNoTasks),
	Check: func(_ Expect, o Observed, report report) {
		if o.Store.TaskReads != 0 {
			report("%d task pages went through the merge in a run that reads none", o.Store.TaskReads)
		}
	},
}, {
	Name: "W7 this run's history tasks reached the log",
	// #142 pinned where it is exercised: both task calls go into the log in the
	// runs that make them, and in no other run. This is the inversion of what
	// it used to say (#33) — the count is now zero without a layer and non-zero
	// wherever the run writes tasks, and a run reporting the old shape is a
	// task path that went round the accumulator.
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
				"history-task path was not exercised at all (#142)")
		}
	},
}, {
	Name: "W11 an empty layer acked nothing",
	// The empty layer: what a run that writes neither must leave behind —
	// decision D3 as an assertion, in every window that has a layer at all, and
	// the same claim in both windows on purpose: a suite that writes nothing
	// has an empty window whatever the window is configured to be, so a
	// difference here would be the layer having grown a path.
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
	// Sync mode's half of the inversion, and in this window each is a claim
	// about *nothing happening*. Sync mode drains before it answers, so the
	// accumulator is empty at every call boundary and no read can cross a
	// window that holds anything — which is what keeps the whole-folder byte
	// comparison meaningful with the read path wired in (#44/#58), and the
	// cheapest place to notice a window that stopped being one mutation.
	Gate: inSync,
	Check: func(_ Expect, o Observed, report report) {
		if o.Totals.ReadsHeld != 0 {
			report("%d of %d reads crossed a workflow the window still held: sync mode's overlay is supposed to be a no-op",
				o.Totals.ReadsHeld, o.Totals.Reads)
		}
	},
}, {
	Name: "W19 sync mode's tail is empty at a drain boundary",
	// The claim P3 rests its "no replay needed" boundary on (#43): at a window
	// of one every acked entry is resolved before its caller is answered, so a
	// restart here would have nothing to reconstruct.
	//
	// TailEntries is deliberately not Acked−Applied: sync mode's expected
	// condition failures legitimately leave those apart (see [cycle.Stats]),
	// and merging the two counters is the bug the cycle's own notes warn about.
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
	// The other half of sync mode's one carve-out (#57), stated outright
	// because it is the mechanism: a condition failure at a window of one is
	// *answered*, so entries acked exceed drains committed. This reads Acked as
	// a count, which the fresh-folder caveat on [Observed.Totals] is about.
	Gate: all(inSync, expectsFailures),
	Check: func(_ Expect, o Observed, report report) {
		if int64(o.Totals.Acked) <= int64(o.Totals.Drains) {
			report("every acked entry committed (acked=%d drains=%d): this run writes conditions it expects "+
				"to fail, so some of them were not evaluated", o.Totals.Acked, o.Totals.Drains)
		}
	},
}, {
	Name: "W23 a windowed read crossed a held workflow",
	// The inversion, and the reason the mode exists (#82): sync mode's claims
	// are that no read ever crossed a held workflow and that nothing was left
	// in the tail, and here each must be false or the run was intercept mode
	// wearing a window's name — the same silent failure the witness was built
	// for, one phase further on.
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
	// The drop, where a drain committed to count it: a range folding in takes
	// the tasks the window already holds out of the batch. It is counted at the
	// commit and not at the fold, because an uncommitted drain's entries stay
	// in the log and replay folds them again — so a run whose window never
	// filled commits nothing and legitimately reports zero, which is the mode
	// working rather than the mechanism missing. What says the deletion path
	// ran at all is W10.
	Gate: all(inWindowed, completesRanges, drainsCommitted),
	Check: func(_ Expect, o Observed, report report) {
		if o.Totals.DroppedTasks == 0 {
			report("%d ranges were folded over %d committed drains and not one of them took a task out of a "+
				"window: the run completes ranges over tasks it has just written", o.Totals.AckedRanges, o.Totals.Drains)
		}
	},
}, {
	Name: "W27 intercepted writes were emitted",
	// Every one of those numbers went out as a metric (#59). Asserting the
	// counters and the emissions against each other is what makes this more
	// than a "something was recorded" check: the unit tests pin what each
	// series means, and these pin that the composition emits them at all —
	// which is the failure mode a metrics stack actually has, since a series
	// nobody emits looks exactly like a system doing no work.
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
	// wal_drained_mutations records one value per committed drain, so the run
	// says outright that the accumulator folded a batch rather than a sequence
	// of windows of one — the collapse itself, which no counter above can show.
	Gate: all(hasEmissions, writesState, inWindowed),
	Check: func(_ Expect, o Observed, report report) {
		if maxEmitted(o.Emitted[seriesDrainedMutations]) <= 1 {
			report("every one of %d drains carried a single mutation: nothing was collapsed", o.Totals.Drains)
		}
	},
}, {
	Name: "W33 no condition reached a windowed drain",
	// Where the run's expected condition failures were answered, and since #138
	// the answer is: all of them, before the append. The series counts failures
	// that came back from a *drain* and were attributed to the one caller in
	// its window, which in this mode nothing is — so a non-zero here is a
	// condition that reached a transaction: either the check let one through,
	// or a drain answered a batch whose caller it did not have.
	Gate: all(hasEmissions, writesState, inWindowed),
	Check: func(_ Expect, o Observed, report report) {
		if n := len(o.Emitted[seriesAnsweredConditionFailures]); n != 0 {
			report("%d condition failures were raised at a drain: every condition this mode meets is "+
				"decided before the append (#138)", n)
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

// Check says whether the run the instruments saw is the run Expect claims, as a
// list of failed claims — empty for a run that is what it says it is. Each
// error is prefixed with the name of the claim that raised it, so a red run
// says which half of the layer went missing rather than that something did.
// Pure; the callers turn the errors into their own failure shape.
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
		// errors.New over a second Errorf: the formatted message is data by
		// this point and a % in it is not a verb.
		c.Check(e, o, func(format string, args ...any) {
			errs = append(errs, errors.New(c.Name+": "+fmt.Sprintf(format, args...)))
		})
	}
	return errs
}

// Describe is the run as one line, for a person to read beside whatever the
// witness said. It prints every counter and only the kinds that fired — the
// zeroes are the interesting half, but printing every name on every line to
// say so buries the ones that matter, and [Expect.Check] is where a missing
// kind is a failure rather than a fact.
//
// task-collisions is printed and not asserted, and the asymmetry is the point
// (#152): the two sources a merged page draws from are disjoint by
// construction, so a non-zero here is the one observation that would say that
// construction broke — worth looking into rather than worth failing on. It
// reaches this line at all because #153 made the counter survive the
// [cycle.Totals] seam.
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

// describeKinds prints only the kinds a run actually saw.
func describeKinds(t cycle.Totals) string {
	var parts []string
	for k, n := range t.Kinds {
		if n > 0 {
			parts = append(parts, fmt.Sprintf("%s:%d", mutation.Kind(k), n))
		}
	}
	return strings.Join(parts, " ")
}

// drainTriggers summarises what asked for the drains, which at the acceptance's
// two windows is the run's most informative single number: at the knee the size
// watermark never fires inside a test and every drain is a fold.ErrRefused
// force-drain, while at window 2 the mutation watermark dominates (#70).
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

// maxEmitted is the largest value a series was recorded with.
func maxEmitted(rs []Emission) int64 {
	var largest int64
	for _, r := range rs {
		largest = max(largest, r.Value)
	}
	return largest
}
