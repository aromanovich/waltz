package witness

// The witness's own tests: the module whose job is to catch a silent pass must
// be judgeable itself, and a witness that cannot go red is that failure with an
// extra step. So the negative cases are the deliverable and the green ones are
// the control: each healthy run below is spoiled in exactly one way, and the
// claim is that the witness says so — by name, so that a red run tells its
// reader which half of the layer went missing rather than that something did.
//
// The shapes are the ones a run over the layer takes: the mutable-state suite
// and the task suite, each under both layer windows, plus the two suites that
// write nothing and the control that has no layer at all — constructed here
// rather than captured, since nothing in this package runs a suite. A live
// run's shape — Totals alone, no store, no emissions — is [Universal] and the
// claims an absent instrument leaves standing; verify/e2e drives that shape
// over a server it boots in-process, and is the witness's only caller.

import (
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/aromanovich/waltz/cycle"
	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/wrapper"
)

func messages(errs []error) string {
	var parts []string
	for _, err := range errs {
		parts = append(parts, err.Error())
	}
	return strings.Join(parts, "\n")
}

// series is n recordings of one value each, the shape a counter series takes in
// a capture; the acceptance's emission-length claims count entries, not sums.
func series(n int, value int64) []Emission {
	rs := make([]Emission, n)
	for i := range rs {
		rs[i] = Emission{Value: value}
	}
	return rs
}

// syncRun is the mutable-state suite under sync mode, healthy: 24 writes
// intercepted, 4 of them expected condition failures answered rather than
// committed, every read answered from an empty window.
func syncRun() (Expect, Observed) {
	t := cycle.Totals{Shards: 4, Epochs: 4, Acked: 24, Applied: 24}
	t.Drains = 20
	// 20 drains at the shipped cadence of 16: one compaction, and it committed.
	t.Trims = 1
	t.TrimsCommitted = 1
	t.Reads = 100
	t.Kinds[mutation.KindCreate] = 10
	t.Kinds[mutation.KindUpdate] = 14
	store := wrapper.Counts{Intercepted: 24, Overlaid: 100}
	emitted := Emissions{
		seriesIntercepted:               series(24, 1),
		seriesDrains:                    series(20, 1),
		seriesAnsweredConditionFailures: series(4, 1),
	}
	e := Expect{Window: Sync, ShardsAcquired: true, MutableState: true, ConditionFailures: true}
	return e, Observed{Totals: t, Store: &store, Emitted: emitted}
}

// windowedRun is the same suite at a window bigger than one, healthy: reads
// crossing held workflows, a tail still standing at the end, drains that
// carried batches, and no condition ever reaching a drain.
func windowedRun() (Expect, Observed) {
	t := cycle.Totals{Shards: 4, Epochs: 4, Acked: 300, Applied: 260, TailEntries: 40}
	t.Drains = 5
	t.Reads = 200
	t.ReadsHeld = 150
	t.Kinds[mutation.KindCreate] = 100
	t.Kinds[mutation.KindUpdate] = 200
	store := wrapper.Counts{Intercepted: 300, Overlaid: 200}
	emitted := Emissions{
		seriesIntercepted:      series(300, 1),
		seriesDrains:           series(5, 1),
		seriesDrainedMutations: series(5, 52),
	}
	e := Expect{Window: Windowed, ShardsAcquired: true, MutableState: true,
		ConditionFailures: true, TailHeld: true}
	return e, Observed{Totals: t, Store: &store, Emitted: emitted}
}

// taskWindowed is the task suite at window 2, healthy: task writes and range
// deletes in the log, pages merged from two sources, and drains whose ranges
// took tasks out of the window.
func taskWindowed() (Expect, Observed) {
	t := cycle.Totals{Shards: 4, Epochs: 4, Acked: 60, Applied: 55, TailEntries: 5}
	t.Drains = 3
	t.TaskReads = 40
	t.TaskReadsMerged = 9
	t.AckedRanges = 12
	t.DroppedTasks = 5
	t.WrittenTasks = 30
	t.Kinds[mutation.KindAddTasks] = 30
	t.Kinds[mutation.KindRangeCompleteTasks] = 12
	store := wrapper.Counts{TasksWritten: 30, TasksCompleted: 12, TaskReads: 40}
	e := Expect{Window: Windowed, ShardsAcquired: true, HistoryTasks: true, TaskReads: true, Ranges: true}
	return e, Observed{Totals: t, Store: &store}
}

// taskSync is the task suite under sync mode, healthy: the same writes, every
// one drained inside its own call, so the merge is a no-op.
func taskSync() (Expect, Observed) {
	t := cycle.Totals{Shards: 4, Epochs: 4, Acked: 42, Applied: 42}
	t.Drains = 42
	t.TaskReads = 40
	t.AckedRanges = 12
	t.DroppedTasks = 2
	t.WrittenTasks = 30
	t.Kinds[mutation.KindAddTasks] = 30
	t.Kinds[mutation.KindRangeCompleteTasks] = 12
	store := wrapper.Counts{TasksWritten: 30, TasksCompleted: 12, TaskReads: 40}
	e := Expect{Window: Sync, ShardsAcquired: true, HistoryTasks: true, TaskReads: true, Ranges: true}
	return e, Observed{Totals: t, Store: &store}
}

// emptyRun is a suite that writes neither mutable state nor tasks, under a
// layer: everything it could have sent must be absent.
func emptyRun() (Expect, Observed) {
	t := cycle.Totals{Shards: 4, Epochs: 4}
	store := wrapper.Counts{}
	e := Expect{Window: Sync, ShardsAcquired: true}
	return e, Observed{Totals: t, Store: &store, Emitted: Emissions{}}
}

// control is the passthrough run: no layer, and both instruments all zero.
func control() (Expect, Observed) {
	store := wrapper.Counts{}
	return Expect{Window: NoLayer}, Observed{Store: &store}
}

// TestAWitnessedRunPassesEveryShape is the control's half, and it is the half
// that would go unnoticed if it broke: a witness that fails everything catches
// every silent pass and is still useless, because the first honest run turns
// it off.
func TestAWitnessedRunPassesEveryShape(t *testing.T) {
	for name, shape := range map[string]func() (Expect, Observed){
		"sync":          syncRun,
		"windowed":      windowedRun,
		"task-sync":     taskSync,
		"task-windowed": taskWindowed,
		"empty":         emptyRun,
		"control":       control,
	} {
		e, o := shape()
		require.Empty(t, e.Check(o), "shape %q rejected a healthy run", name)
	}
}

// TestTheExpectMustStateAWindow: the zero value is refused rather than
// defaulted, because an Expect that states no window is a witness that asserts
// nothing — the failure the package exists to catch, reachable by forgetting
// one field.
func TestTheExpectMustStateAWindow(t *testing.T) {
	errs := Expect{MutableState: true}.Check(Observed{})
	require.Len(t, errs, 1)
	require.Contains(t, errs[0].Error(), "states no window")
}

// TestTheUniversalClaimsCanFail is what [Universal] says, and it is the half
// the leave-one-out below cannot reach: a halt and an uncommitted trim are
// claims every run makes whatever it covered, so they belong to no claim in
// either table and have no row there.
func TestTheUniversalClaimsCanFail(t *testing.T) {
	for _, c := range []struct {
		name  string
		spoil func(*Observed)
		says  string
	}{
		{"a shard halted", func(o *Observed) { o.Totals.Halted = []string{"shard 3"} }, "a shard halted"},
		{"no trim committed", func(o *Observed) { o.Totals.TrimsCommitted = 0 }, "no trim committed"},
	} {
		t.Run(c.name, func(t *testing.T) {
			e, o := syncRun()
			c.spoil(&o)
			errs := e.Check(o)
			require.NotEmpty(t, errs, "the witness accepted a run where %s", c.name)
			require.Contains(t, messages(errs), c.says)
		})
	}
}

// TestTheRoutedEqualitiesHoldInBothDirections: a read counted at the store and
// not at a shard is a read answered somewhere it should not have been, and a
// shard answering reads the store never sent is the same bug the other way.
// Both instruments have to be present for either claim to be made at all.
func TestTheRoutedEqualitiesHoldInBothDirections(t *testing.T) {
	e, o := syncRun()
	o.Store.Overlaid = 90
	require.Contains(t, messages(e.Check(o)), "the store counted 90 reads at the layer and the shards answered 100")

	e, o = syncRun()
	o.Totals.Reads = 90
	require.Contains(t, messages(e.Check(o)), "the store counted 100 reads at the layer and the shards answered 90")

	e, o = taskWindowed()
	o.Store.TaskReads = 39
	require.Contains(t, messages(e.Check(o)), "the store counted 39 task pages at the layer and the shards answered 40")
}

// TestReadingAroundTheLayerIsAsRedAsReadingThroughIt: a suite that reads
// task ranges and routes none of them at the merge is reading around the
// layer, which is as green as reading through it — and a suite that reads none
// must have routed none.
func TestReadingAroundTheLayerIsAsRedAsReadingThroughIt(t *testing.T) {
	e, o := taskWindowed()
	o.Totals.TaskReads = 0
	o.Store.TaskReads = 0
	o.Totals.TaskReadsMerged = 0
	require.Contains(t, messages(e.Check(o)), "not one of them reached the merge")

	e, o = syncRun()
	o.Totals.TaskReads = 3
	o.Store.TaskReads = 3
	require.Contains(t, messages(e.Check(o)), "went through the merge in a run that reads none")
}

// TestTheEmptyLayerIsAssertedNotAssumed: what a run that writes neither path
// must leave behind — D3 as an assertion — because a method that starts
// transiting into the log when it should not leaves every suite green.
func TestTheEmptyLayerIsAssertedNotAssumed(t *testing.T) {
	for _, c := range []struct {
		name  string
		spoil func(*Observed)
		says  string
	}{
		{"an entry was acked", func(o *Observed) { o.Totals.Acked = 2 }, "something else was writing"},
		{"a read crossed a window", func(o *Observed) { o.Totals.ReadsHeld = 1 }, "writes nothing into one"},
		{"the store counted a write", func(o *Observed) { o.Store.Intercepted = 1 }, "nothing may have entered the log"},
		{"a write was emitted", func(o *Observed) { o.Emitted[seriesIntercepted] = series(1, 1) }, "emissions were recorded by a run that writes nothing"},
	} {
		t.Run(c.name, func(t *testing.T) {
			e, o := emptyRun()
			c.spoil(&o)
			errs := e.Check(o)
			require.NotEmpty(t, errs, "the empty-layer witness accepted a run where %s", c.name)
			require.Contains(t, messages(errs), c.says)
		})
	}
}

// TestTheControlIsAControl: a "passthrough" run that quietly still had the
// layer in it answers the question of what a difference is attributable to with
// the layer's own behaviour, which is the one thing it exists to exclude.
func TestTheControlIsAControl(t *testing.T) {
	for _, c := range []struct {
		name  string
		spoil func(*Observed)
	}{
		{"a shard was acquired", func(o *Observed) { o.Totals.Shards = 1 }},
		{"a mutation was acked", func(o *Observed) { o.Totals.Acked = 1 }},
		{"a task page was routed", func(o *Observed) { o.Totals.TaskReads = 1 }},
		{"the store counted a write", func(o *Observed) { o.Store.Intercepted = 1 }},
	} {
		t.Run(c.name, func(t *testing.T) {
			e, o := control()
			c.spoil(&o)
			require.Contains(t, messages(e.Check(o)), "not the control it claims to be")
		})
	}
}

// TestTheHistoryTaskPathIsPinnedWhereItIsExercised: both task calls go into the
// log in the runs that make them and in no other run — the count is zero
// without a layer and non-zero wherever the run writes tasks, and a run
// reporting the opposite is a task path that went round the accumulator.
func TestTheHistoryTaskPathIsPinnedWhereItIsExercised(t *testing.T) {
	e, o := taskWindowed()
	o.Store.TasksWritten = 0
	require.Contains(t, messages(e.Check(o)), "this run writes history tasks; not one of them reached the log")

	e, o = taskWindowed()
	o.Store.TasksCompleted = 0
	require.Contains(t, messages(e.Check(o)), "range-completes tasks; not one of them reached the log")

	e, o = taskWindowed()
	o.Totals.AckedRanges = 0
	require.Contains(t, messages(e.Check(o)), "no queue ever completed a range")

	e, o = taskWindowed()
	o.Totals.DroppedTasks = 0
	require.Contains(t, messages(e.Check(o)), "not one of them took a task out of a window")

	// The drop is counted at the commit, not at the fold, so a run whose
	// window never filled commits nothing and legitimately reports zero —
	// the mode working rather than the mechanism missing.
	e, o = taskWindowed()
	o.Totals.Drains = 0
	o.Totals.DroppedTasks = 0
	o.Totals.TailEntries = 60
	require.Empty(t, e.Check(o), "a window that never filled has no drop to report")

	e, o = syncRun()
	o.Store.TasksWritten = 7
	require.Contains(t, messages(e.Check(o)), "in a run that makes neither")
}

// TestAnAbsentInstrumentSkipsExactlyItsClaims: a live run has no reachable
// store counters and no capture handler, and the claims that read them are
// skipped rather than failed — while everything the layer's own account can
// say is still said.
func TestAnAbsentInstrumentSkipsExactlyItsClaims(t *testing.T) {
	e, o := windowedRun()
	o.Store = nil
	o.Emitted = nil
	require.Empty(t, e.Check(o), "a run with only the layer's own account is still a witnessed run")

	// And the layer's own claims still bite without the other instruments.
	e, o = windowedRun()
	o.Store = nil
	o.Emitted = nil
	o.Totals.ReadsHeld = 0
	require.Contains(t, messages(e.Check(o)), "passthrough wearing")
}

// TestKindClaimsFailByName: a coverage claim is about *which*, never about how
// much, and a missing kind fails by name with the reason the caller attached —
// a red run must say which suite went missing, not that something did.
func TestKindClaimsFailByName(t *testing.T) {
	e, o := syncRun()
	e.Kinds = []KindClaim{
		{Kind: mutation.KindCreate, Why: "nothing started a workflow"},
		{Kind: mutation.KindDelete, Why: "the tombstone suite went missing"},
	}
	errs := e.Check(o)
	require.Len(t, errs, 1, "one missing kind is one failure")
	require.Contains(t, errs[0].Error(), mutation.KindDelete.String())
	require.Contains(t, errs[0].Error(), "the tombstone suite went missing")
}

// TestDescribeReportsTheInstrumentsItWasGiven: the line is what a run leaves
// behind for a person to read, so the counters pinned below cannot go missing
// from it silently, the zero kinds stay off it, and the optional instruments
// appear exactly when they were sampled. The rest of the line is unheld:
// epochs, the six task counters, refusals, replayed, halted and two of the
// store's labels could each be dropped from Describe's format string and this
// stays green.
func TestDescribeReportsTheInstrumentsItWasGiven(t *testing.T) {
	_, o := windowedRun()
	o.Emitted[seriesDrains] = []Emission{
		{Value: 1, Tags: map[string]string{"trigger": "refused"}},
		{Value: 1, Tags: map[string]string{"trigger": "refused"}},
		{Value: 1, Tags: map[string]string{"trigger": "mutations"}},
	}
	line := Describe(o)
	for _, want := range []string{
		"shards=4", "acked=300", "applied=260", "tail=40", "drains=5",
		"reads=200", "reads-held=150", "kinds=[",
		"store=[writes=300", "reads=200 task-pages=0]",
		"drains-by-trigger=[mutations 1, refused 2]",
	} {
		require.Contains(t, line, want)
	}

	bare := Describe(Observed{})
	require.NotContains(t, bare, "store=[", "an instrument nobody sampled stays off the line")
	require.NotContains(t, bare, "drains-by-trigger=[")
	require.Contains(t, bare, "kinds=[]", "a run that acked nothing names no kind")
}

// windowedEmpty is a suite that writes nothing at a window bigger than one.
// It exists for one row of the leave-one-out below: a held read is both "an
// empty layer held a read" and "sync mode's overlay is a no-op", and only a
// windowed empty run separates them.
func windowedEmpty() (Expect, Observed) {
	t := cycle.Totals{Shards: 4, Epochs: 4}
	store := wrapper.Counts{}
	e := Expect{Window: Windowed, ShardsAcquired: true}
	return e, Observed{Totals: t, Store: &store, Emitted: Emissions{}}
}

// claimNames is every claim the witness can raise, in the order it raises them.
func claimNames() []string {
	names := make([]string, 0, len(controlClaims)+len(layerClaims))
	for _, c := range controlClaims {
		names = append(names, c.Name)
	}
	for _, c := range layerClaims {
		names = append(names, c.Name)
	}
	return names
}

// fired is which claims raised each error, by the name Check puts in front of
// it. An error matching no claim is [Universal]'s or the window guard's and is
// reported as itself, so a row cannot pass by tripping one of those instead.
func fired(errs []error) []string {
	var out []string
	for _, err := range errs {
		name := err.Error()
		for _, c := range claimNames() {
			if strings.HasPrefix(err.Error(), c+": ") {
				name = c
				break
			}
		}
		out = append(out, name)
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// TestEachClaimHasADefectOnlyItCatches is the leave-one-out, and it is what
// makes the list of claims worth having: for every claim there is a run that
// exactly it rejects. A claim with no such run is either redundant — some other
// claim already covers the defect — or unreachable, and both look like a witness
// doing its job until the day the other claim is edited.
//
// This is stronger than "the witness went red": a spoil that trips three claims
// satisfies a substring match on the message, so nothing says whether the claim
// under test was one of the three.
//
// A row spoils exactly one thing. Where doing so unavoidably moves a second
// number — a store counter and the series that counts it are one fact observed
// twice — the row moves both and says so, because the alternative is a row that
// passes by tripping the claim next to the one it names.
func TestEachClaimHasADefectOnlyItCatches(t *testing.T) {
	defects := []struct {
		claim string
		shape func() (Expect, Observed)
		spoil func(*Expect, *Observed)
	}{
		{"C1 the control reached no shard", control, func(_ *Expect, o *Observed) { o.Totals.Shards = 1 }},
		{"C2 the control's store counted nothing", control, func(_ *Expect, o *Observed) { o.Store.Intercepted = 1 }},

		{"W1 every acked entry named a request", syncRun, func(_ *Expect, o *Observed) {
			o.Totals.Kinds[mutation.KindInvalid] = 2
		}},
		{"W2 a shard was acquired through the layer", syncRun, func(_ *Expect, o *Observed) { o.Totals.Shards = 0 }},
		{"W3 every read the store sent was answered by a shard", syncRun, func(_ *Expect, o *Observed) {
			o.Store.Overlaid = 90
		}},
		{"W4 every task page the store sent was answered by a shard", taskWindowed, func(_ *Expect, o *Observed) {
			o.Store.TaskReads = 39
		}},
		{"W5 this run's task reads reached the merge", taskWindowed, func(_ *Expect, o *Observed) {
			// Both sides of W4's equality, so that the run is short a task read
			// rather than disagreeing with itself about one.
			o.Store.TaskReads, o.Totals.TaskReads = 0, 0
		}},
		{"W6 no task page in a run that reads none", syncRun, func(_ *Expect, o *Observed) {
			o.Store.TaskReads, o.Totals.TaskReads = 1, 1
		}},
		{"W7 this run's history tasks reached the log", taskWindowed, func(_ *Expect, o *Observed) {
			o.Store.TasksWritten = 0
		}},
		{"W8 no task traffic in a run that writes none", syncRun, func(_ *Expect, o *Observed) {
			o.Store.TasksWritten = 1
		}},
		{"W9 this run's range completions reached the log", taskWindowed, func(_ *Expect, o *Observed) {
			o.Store.TasksCompleted = 0
		}},
		{"W10 a queue completed a range", taskWindowed, func(_ *Expect, o *Observed) { o.Totals.AckedRanges = 0 }},

		{"W11 an empty layer acked nothing", emptyRun, func(_ *Expect, o *Observed) { o.Totals.Acked = 1 }},
		{"W12 an empty layer held no read", windowedEmpty, func(_ *Expect, o *Observed) { o.Totals.ReadsHeld = 1 }},
		{"W13 an empty layer's store counted nothing", emptyRun, func(_ *Expect, o *Observed) {
			o.Store.Intercepted = 1
		}},
		{"W14 an empty layer emitted nothing", emptyRun, func(_ *Expect, o *Observed) {
			o.Emitted[seriesIntercepted] = series(1, 1)
		}},

		{"W15 the layer acked something", taskSync, func(_ *Expect, o *Observed) { o.Totals.Acked = 0 }},
		{"W16 a write reached the layer", syncRun, func(_ *Expect, o *Observed) {
			// The series counts the same writes, so a store that intercepted
			// none and a series that recorded 24 is two defects, not one.
			o.Store.Intercepted = 0
			o.Emitted[seriesIntercepted] = nil
		}},
		{"W17 something was applied", syncRun, func(_ *Expect, o *Observed) { o.Totals.Applied = 0 }},

		{"W18 sync mode's overlay is a no-op", syncRun, func(_ *Expect, o *Observed) { o.Totals.ReadsHeld = 1 }},
		{"W19 sync mode's tail is empty at a drain boundary", syncRun, func(_ *Expect, o *Observed) {
			o.Totals.TailEntries = 3
		}},
		{"W20 sync mode's merge is a no-op", syncRun, func(_ *Expect, o *Observed) { o.Totals.TaskReadsMerged = 1 }},
		{"W21 sync mode drained a run that writes only tasks", taskSync, func(_ *Expect, o *Observed) {
			o.Totals.Drains = 0
		}},
		{"W22 sync mode answered a condition failure rather than committing it", syncRun, func(_ *Expect, o *Observed) {
			// Every acked entry committed: the drains rise to meet them, and
			// the series that counts the answered ones goes with them.
			o.Totals.Drains = 24
			o.Emitted[seriesDrains] = series(24, 1)
			o.Emitted[seriesAnsweredConditionFailures] = nil
		}},

		{"W23 a windowed read crossed a held workflow", windowedRun, func(_ *Expect, o *Observed) {
			o.Totals.ReadsHeld = 0
		}},
		{"W24 a windowed run ends with a tail", windowedRun, func(_ *Expect, o *Observed) {
			o.Totals.TailEntries = 0
		}},
		{"W25 a windowed page carried a task out of the window", taskWindowed, func(_ *Expect, o *Observed) {
			o.Totals.TaskReadsMerged = 0
		}},
		{"W26 a windowed range took a task out of a window", taskWindowed, func(_ *Expect, o *Observed) {
			o.Totals.DroppedTasks = 0
		}},

		{"W27 intercepted writes were emitted", syncRun, func(_ *Expect, o *Observed) {
			o.Emitted[seriesIntercepted] = series(23, 1)
		}},
		{"W28 drains were emitted", syncRun, func(_ *Expect, o *Observed) {
			o.Emitted[seriesDrains] = series(19, 1)
		}},
		{"W29 nothing was counted as halting", syncRun, func(_ *Expect, o *Observed) {
			o.Emitted[seriesHalts] = series(1, 1)
		}},
		{"W30 sync mode's answered failures were emitted", syncRun, func(_ *Expect, o *Observed) {
			o.Emitted[seriesAnsweredConditionFailures] = series(3, 1)
		}},
		{"W31 a windowed run's drained mutations were emitted", windowedRun, func(_ *Expect, o *Observed) {
			o.Emitted[seriesDrainedMutations] = series(4, 52)
		}},
		{"W32 a windowed drain collapsed something", windowedRun, func(_ *Expect, o *Observed) {
			o.Emitted[seriesDrainedMutations] = series(5, 1)
		}},
		{"W33 no condition reached a windowed drain", windowedRun, func(_ *Expect, o *Observed) {
			o.Emitted[seriesAnsweredConditionFailures] = series(1, 1)
		}},
		{"W34 every claimed kind reached the log", syncRun, func(e *Expect, _ *Observed) {
			e.Kinds = []KindClaim{{Kind: mutation.KindDelete, Why: "the suite deletes workflows"}}
		}},
	}

	covered := map[string]bool{}
	for _, d := range defects {
		t.Run(d.claim, func(t *testing.T) {
			e, o := d.shape()
			require.Empty(t, e.Check(o), "the shape this row spoils is not healthy to begin with")
			d.spoil(&e, &o)
			require.Equal(t, []string{d.claim}, fired(e.Check(o)),
				"the defect this row stages must be caught by %s and by nothing else", d.claim)
		})
		covered[d.claim] = true
	}

	// Every claim needs a row, or the claim it lacks is the one nobody has
	// shown to be reachable. This is a statement about this table, not about
	// the code's shape: both lists are values in this package.
	for _, name := range claimNames() {
		require.True(t, covered[name], "no defect is staged for %s: it is redundant, unreachable, or untested", name)
	}
}
