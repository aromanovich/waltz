package checker

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/aromanovich/waltz/wal"
)

// The leave-one-out table (#94's, carried forward with two entries changed).
//
// An assertion is admitted when a defect *only it* catches is demonstrated, not
// argued — so this file is a correct run, and then that run broken one way at a
// time, with each break asserted to fire exactly one assertion. An assertion
// with no defect of its own is decoration; a defect no assertion catches is a
// hole with a real regression waiting in it. The table below is keyed on the
// catalogue and walked from it, so neither half can go quiet.
//
// #94's D7 — "a refused write that landed" — is withdrawn along with the
// assertion that caught it, and the withdrawal is the point of #95's finding
// rather than a gap: a refusal bounds nothing about the cold store, because a
// drain error returned through Manager.Write arrives after that call's entry is
// durable and from outside is the same Go type as a refusal that provably wrote
// nothing.
// TestARefusalIsNotAClaimAboutTheColdStore is that as a test.

// ---------------------------------------------------------------------------
// a correct run
// ---------------------------------------------------------------------------

const shard = wal.ShardID(1)

func run(id string) Workflow { return Workflow{"ns", "wf", id} }

// world is the run every test below starts from: three runs on one shard, five
// entries, everything acked, the last call on one of them a delete.
type world struct {
	entries []Observed
	calls   []Call
	present map[Workflow]bool
}

func correct() world {
	a, b, c := run("a"), run("b"), run("c")
	return world{
		entries: []Observed{
			{Seqno: 1, Epoch: 2, Subject: a, Digest: 0x1},
			{Seqno: 2, Epoch: 2, Subject: b, Digest: 0x2},
			{Seqno: 3, Epoch: 2, Subject: a, Digest: 0x3},
			{Seqno: 4, Epoch: 2, Subject: c, Digest: 0x4},
			{Seqno: 5, Epoch: 2, Subject: c, Digest: 0x5},
		},
		calls: []Call{
			{Node: "n1", Seq: 1, Shard: shard, Kind: "create", Subject: a, Effect: Exists, Outcome: Acked},
			{Node: "n1", Seq: 2, Shard: shard, Kind: "create", Subject: b, Effect: Exists, Outcome: Acked},
			{Node: "n1", Seq: 3, Shard: shard, Kind: "update", Subject: a, Effect: Exists, Outcome: Acked},
			{Node: "n1", Seq: 4, Shard: shard, Kind: "create", Subject: c, Effect: Exists, Outcome: Acked},
			{Node: "n1", Seq: 5, Shard: shard, Kind: "delete", Subject: c, Effect: Removed, Outcome: Acked},
		},
		present: map[Workflow]bool{a: true, b: true},
	}
}

func (w world) sample(watermark wal.Seqno) Sample {
	return Sample{
		Shard:           shard,
		Entries:         slices.Clone(w.entries),
		WatermarkBefore: watermark,
		WatermarkAfter:  watermark,
		HasWatermark:    true,
	}
}

// judge drives the whole instrument over a world: two samples, so the
// assertions that need memory are answerable, then the record, then quiesce.
func (w world) judge(t *testing.T, samples ...Sample) *Journal {
	t.Helper()
	return w.judgeUnder(t, Chaos(), samples...)
}

func (w world) judgeUnder(t *testing.T, p Policy, samples ...Sample) *Journal {
	t.Helper()
	j, err := w.judgeAllowingRefusal(p, samples...)
	require.NoError(t, err)
	return j
}

func (w world) judgeAllowingRefusal(p Policy, samples ...Sample) (*Journal, error) {
	j := New(p)
	if len(samples) == 0 {
		samples = []Sample{w.sample(5), w.sample(5)}
	}
	for _, s := range samples {
		j.Observe(s)
	}
	j.Record(w.calls...)
	return j, j.Quiesced(context.Background(), w.presence())
}

func (w world) presence() Present {
	return func(_ context.Context, c Call) (bool, error) { return w.present[c.Subject], nil }
}

func TestACorrectRunIsGreen(t *testing.T) {
	w := correct()
	j := w.judge(t)
	require.Empty(t, j.Findings(), "a correct run must be green: %v", j.Findings())

	// A green result over nothing is the failure mode a census exists to
	// expose, so the baseline asserts it saw a run.
	c := j.Census()
	require.Equal(t, 2, c.Samples)
	require.Equal(t, 5, c.Calls)
	require.Equal(t, 5, c.Acked)
	require.Equal(t, 3, c.Judged, "every run's last call was acked, so every run is judged")
	require.Equal(t, map[wal.ShardID]int{shard: 5}, c.Remembered)
}

// ---------------------------------------------------------------------------
// one defect at a time
// ---------------------------------------------------------------------------

// defect is one break of the correct run, and the world it has to be injected
// into for the break to be the thing under test.
type defect struct {
	name string
	// policy is the run the defect is injected into. It is the zero value —
	// meaning [Chaos] — everywhere but D1, and that exception is not
	// bookkeeping: a trim that ran ahead of the watermark is a defect of the
	// trim, so it can only happen in a run that trims. Injected into a run that
	// declared it keeps its whole log, the same world is also A10 — which is
	// correct and says something else, and a table that let the two overlap
	// would stop being leave-one-out.
	policy Policy
	// refusesToQuiesce is D1's alone: a log trimmed above its watermark can
	// never become quiescent, so the instrument both reports the finding and
	// then declines to ask the cold store anything. Both halves are the right
	// answer and the table states them.
	refusesToQuiesce bool
	breakIt          func(w *world) []Sample
}

// catalogue is every assertion this package makes, in the order it makes them.
func catalogue() []string {
	names := make([]string, 0, len(perSample)+len(overRun))
	for _, a := range perSample {
		names = append(names, a.Name)
	}
	for _, a := range overRun {
		names = append(names, a.Name)
	}
	return names
}

func TestEachAssertionHasADefectOnlyItCatches(t *testing.T) {
	trimming := Chaos()
	trimming.WholeLog = false

	defects := map[string][]defect{
		"A1 gap-freedom": {{
			name: "D6 a hole in the log",
			breakIt: func(w *world) []Sample {
				w.entries = append(w.entries[:2], w.entries[3:]...)
				s := w.sample(5)
				return []Sample{s, s}
			},
		}},
		"A2 epoch monotonic": {{
			name: "D4 a zombie appends under a fenced-off epoch",
			breakIt: func(w *world) []Sample {
				w.entries[4].Epoch = 1
				s := w.sample(5)
				return []Sample{s, s}
			},
		}},
		"A3 entries are immutable": {{
			name: "D5 an entry is rewritten in place",
			breakIt: func(w *world) []Sample {
				first := w.sample(5)
				w.entries[2].Digest = 0xdead
				return []Sample{first, w.sample(5)}
			},
		}},
		"A4 watermark monotonic": {{
			name: "D2 the watermark goes backwards",
			breakIt: func(w *world) []Sample {
				return []Sample{w.sample(5), w.sample(3)}
			},
		}},
		"A5 recoverable": {{
			name:             "D1 the trim runs ahead of the watermark",
			policy:           trimming,
			refusesToQuiesce: true,
			breakIt: func(w *world) []Sample {
				// The log's bottom is gone while the watermark is still low, so
				// nothing holds the entries between them: an unrecoverable shard,
				// and no other assertion says a word about it.
				w.entries = w.entries[3:]
				s := w.sample(1)
				return []Sample{s, s}
			},
		}},
		"A6 applied ⊆ logged": {{
			name: "D3 apply runs ahead of the ack",
			breakIt: func(w *world) []Sample {
				s := w.sample(9)
				return []Sample{s, s}
			},
		}},
		"A7 acked ⟹ the store agrees": {{
			name: "D7 an acked write is lost",
			breakIt: func(w *world) []Sample {
				delete(w.present, run("b"))
				return nil
			},
		}, {
			name: "D8 an acked delete did not happen",
			breakIt: func(w *world) []Sample {
				// The other direction, and the one a Present wired to a constant
				// yes is caught by.
				w.present[run("c")] = true
				return nil
			},
		}},
		"A9 no entry without a call": {{
			name: "D9 an entry no call accounts for",
			breakIt: func(w *world) []Sample {
				w.entries = append(w.entries, Observed{Seqno: 6, Epoch: 2, Subject: run("b"), Digest: 0x6})
				// Applied as well as logged, so the world is quiescent and A7 is
				// answerable: the defect is an entry nobody asked for, not a
				// drain that has not run.
				s := w.sample(6)
				return []Sample{s, s}
			},
		}},
		"A10 the run kept its whole log": {{
			name: "D10 the trim fires in a run declared WholeLog",
			breakIt: func(w *world) []Sample {
				w.entries = w.entries[2:]
				s := w.sample(5)
				return []Sample{s, s}
			},
		}},
	}

	for _, fires := range catalogue() {
		t.Run(fires, func(t *testing.T) {
			require.NotEmpty(t, defects[fires],
				"%s has no defect of its own: an assertion nothing breaks catches nothing either", fires)
			for _, d := range defects[fires] {
				t.Run(d.name, func(t *testing.T) {
					w := correct()
					samples := d.breakIt(&w)
					policy := d.policy
					if policy == (Policy{}) {
						policy = Chaos()
					}
					j, err := w.judgeAllowingRefusal(policy, samples...)
					if d.refusesToQuiesce {
						require.ErrorContains(t, err, "not quiescent")
					} else {
						require.NoError(t, err)
					}
					require.Equal(t, []string{fires}, j.Fired(),
						"%s must fire %s and nothing else; it fired %v", d.name, fires, j.Findings())
				})
			}
		})
	}

	// The other direction: a defect naming an assertion the catalogue no longer
	// holds is a table judging nothing.
	for fires := range defects {
		require.Contains(t, catalogue(), fires, "no assertion is named %q", fires)
	}
}

// ---------------------------------------------------------------------------
// the identity control
// ---------------------------------------------------------------------------

// TestAnUnknownOutcomeIsNotAFinding is the checker's identity control, and the
// parallel with the oracle's is exact: the exception is admitted by *the record*
// and never by an assertion's author, and it is scoped to the one call.
//
// It is not a rare case. #95's first kill produced one on the first try — the
// call the process was inside when it died — and a partition produces one per
// deadline.
func TestAnUnknownOutcomeIsNotAFinding(t *testing.T) {
	w := correct()
	// The last call on run c — the delete — was in flight when the node died.
	// Its row may or may not be there, so neither answer is a finding.
	w.calls[4].Outcome = Unknown
	w.calls[4].Dangling = true

	for _, there := range []bool{true, false} {
		w.present[run("c")] = there
		j := w.judge(t)
		require.Empty(t, j.Findings(),
			"an unknown outcome with the row present=%v was read as a claim: %v", there, j.Findings())
	}

	j := w.judge(t)
	c := j.Census()
	require.Equal(t, 1, c.Unknown)
	require.Equal(t, 1, c.Dangling)
	require.Equal(t, 2, c.Judged, "the run whose last call is unknown is not judged; the other two are")
}

// TestARefusalIsNotAClaimAboutTheColdStore is #95's correction as a test, and it
// is the reason #94's A8 is withdrawn rather than kept.
//
// A losing claimant's refused call was measured to be in the log and applied by
// the winner's replay: `Cycle.add` returns a drain's error to whoever's call
// triggered the drain, *after* that call's own entry is durable. Some refusals
// are provably pre-append — backpressure, the condition authority, a halted
// cycle — and from outside the two arrive as the same Go type. So the instrument
// asserts [Present] for [Acked] and for nothing else.
func TestARefusalIsNotAClaimAboutTheColdStore(t *testing.T) {
	w := correct()
	w.calls[4].Outcome = Refused

	for _, there := range []bool{true, false} {
		w.present[run("c")] = there
		j := w.judge(t)
		require.Empty(t, j.Findings(),
			"a refusal with the row present=%v was read as a claim about the cold store: %v", there, j.Findings())
	}

	// And A9 still counts it, because a refused call may well have appended.
	// That is the same fact read the other way round, and dropping refusals
	// from the count would make A9 accuse the layer of inventing exactly the
	// entry #95 measured.
	j := w.judge(t)
	require.Equal(t, 1, j.Census().Refused)
	require.Empty(t, j.Findings())
}

// TestASupersededCallIsNotAssertedOn pins the narrowing A7 rests on. A create's
// row is legitimately gone once the delete lands, so "this call's effect is
// visible" is a statement about the final state and only the last call on a run
// makes one.
func TestASupersededCallIsNotAssertedOn(t *testing.T) {
	w := correct()
	// Run c: created, then deleted. The create is acked and its row is absent,
	// which is correct behaviour and must not be a finding.
	require.Equal(t, Exists, w.calls[3].Effect)
	require.Equal(t, Removed, w.calls[4].Effect)
	require.False(t, w.present[run("c")])

	j := w.judge(t)
	require.Empty(t, j.Findings())
}

// TestTwoDriversOnOneRunIsRefusedRatherThanJudged: if two drivers both name a
// run, no call on it is the last one and every claim A7 makes about it is
// arbitrary. That is an instrument that cannot judge, not a layer that failed,
// so it is an error and not a finding.
func TestTwoDriversOnOneRunIsRefusedRatherThanJudged(t *testing.T) {
	w := correct()
	w.calls = append(w.calls, Call{Node: "n2", Seq: 1, Shard: shard, Subject: run("a"), Outcome: Acked})

	j := New(Chaos())
	j.Observe(w.sample(5))
	j.Record(w.calls...)
	err := j.Quiesced(context.Background(), w.presence())
	require.ErrorContains(t, err, "claimed by both")
}

// TestTheOwnershipGuardIsKeyedOnTheNodeIdAndNotTheIncarnation is why a chaos
// case that kills the same shard's owner over and over gives every successor a
// **fresh node id** rather than a new incarnation of one (#112).
//
// Both are expressible: the record is a file per incarnation and `seq` is per
// process (#95), so a restarted node is a first-class thing here. But the guard
// above is the only thing standing between "two drivers wrote this run" and an
// A7 asserted about a call that was not the last one, and it compares
// [Call.Node] — two incarnations of one id are one driver as far as it can see.
// A repeated-kill run gives every generation a seed of its own, so a seed
// accidentally repeated is exactly the mistake the guard exists to catch, and
// under one shared id it would be caught by nothing.
//
// The incarnation keeps the job it was given, which is a different one: a node
// restarted onto the same record path numbering a second run from 1.
func TestTheOwnershipGuardIsKeyedOnTheNodeIdAndNotTheIncarnation(t *testing.T) {
	// The same mistake twice — a second driver on a run that already has one —
	// told apart only by whether it announced itself as a different node.
	second := Call{Seq: 1, Shard: shard, Subject: run("a"), Outcome: Acked}

	byID := correct()
	byID.calls = append(byID.calls, withNode(second, "n2", 1))
	_, err := byID.judgeAllowingRefusal(Chaos())
	require.ErrorContains(t, err, "claimed by both",
		"a successor with an id of its own is what the guard can see")

	byIncarnation := correct()
	byIncarnation.calls = append(byIncarnation.calls, withNode(second, "n1", 2))
	_, err = byIncarnation.judgeAllowingRefusal(Chaos())
	require.NoError(t, err,
		"the guard is keyed on the node id, so a successor reusing it is invisible to it — "+
			"which is the whole reason a chaos case gives each generation an id of its own")
}

func withNode(c Call, node string, incarnation int64) Call {
	c.Node, c.Incarnation = node, incarnation
	return c
}

// TestAColdStoreReadThatFailedIsNotGreen: an instrument whose read errored has
// judged nothing, and swallowing that is how a run is green for the wrong
// reason.
func TestAColdStoreReadThatFailedIsNotGreen(t *testing.T) {
	w := correct()
	j := New(Chaos())
	j.Observe(w.sample(5))
	j.Record(w.calls...)
	err := j.Quiesced(context.Background(), func(context.Context, Call) (bool, error) {
		return false, context.DeadlineExceeded
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Empty(t, j.Findings(), "a failed read is an error, never a finding")
}

// ---------------------------------------------------------------------------
// A6's gate
// ---------------------------------------------------------------------------

// TestA6IsGatedOnTheRunAndNotWeakened is #94's unpredicted finding, kept as a
// regression.
//
// A6 is a false positive against a trimming log: an idle correct shard ends with
// its log trimmed to the watermark and the watermark far above what is left. The
// obvious weakening — "…or the log is empty" — would leave a watermark free to
// be arbitrarily wrong exactly where it is least observable, which is the shape
// the oracle's admission law exists to forbid. So it is *gated* on a declared
// property of the run instead.
func TestA6IsGatedOnTheRunAndNotWeakened(t *testing.T) {
	trimmed := Sample{Shard: shard, WatermarkBefore: 20, WatermarkAfter: 20, HasWatermark: true}

	keeping := New(Chaos())
	require.True(t, keeping.Policy().WholeLog)
	keeping.Observe(trimmed)
	require.Equal(t, []string{"A6 applied ⊆ logged"}, keeping.Fired(),
		"under a run that declared it keeps its log, an empty log below a high watermark IS the defect")

	trimming := Chaos()
	trimming.WholeLog = false
	shipped := New(trimming)
	shipped.Observe(trimmed)
	require.Empty(t, shipped.Findings(),
		"under a trimming run the same world is a correct idle shard, and A6 must say nothing")
}

// ---------------------------------------------------------------------------
// the three breaks the acceptance names
// ---------------------------------------------------------------------------

// TestAPresentThatAlwaysAnswersYesIsCaught is the first of #101's three
// deliberate breaks of the wiring. It is caught by A7 in the direction only a
// delete exercises, which is why the driver's stream has to contain deletions.
func TestAPresentThatAlwaysAnswersYesIsCaught(t *testing.T) {
	w := correct()
	j := New(Chaos())
	j.Observe(w.sample(5))
	j.Record(w.calls...)
	require.NoError(t, j.Quiesced(context.Background(),
		func(context.Context, Call) (bool, error) { return true, nil }))
	require.Equal(t, []string{"A7 acked ⟹ the store agrees"}, j.Fired())
}

// TestADriverThatRecordsOutcomesOnlyIsCaught is the second break, and it is the
// one that says why the record is two lines per call.
//
// A driver that writes a line only once its call has returned cannot write one
// for the call it was inside when it died. That is wrong in both directions at
// once, and the test pins both:
//
//   - the entry that call left is in the log with nothing accounting for it,
//     which is **A9**;
//   - and the call before it becomes the last one recorded on its run, so **A7**
//     is asserted about a state a later mutation has already moved past — a
//     finding manufactured out of a correct layer.
//
// A record that both hides real findings and invents fake ones is the argument
// for two fsynced lines per call, and it is why the honest run below is green
// on the very same world.
func TestADriverThatRecordsOutcomesOnlyIsCaught(t *testing.T) {
	w := correct()
	// The node died inside its last call: the entry is there, the outcome line
	// is not. With two lines per call this is an ordinary unknown.
	w.calls[4].Outcome = Unknown
	w.calls[4].Dangling = true

	honest := w.judge(t)
	require.Empty(t, honest.Findings(), "a dangling call is the third class, not a finding")

	// The same run recorded the other way: what a driver writing outcomes only
	// would have on disk is every completed call and nothing for the one in
	// flight.
	w.calls = outcomesOnly(w.calls)
	broken := New(Chaos())
	broken.Observe(w.sample(5))
	broken.Record(w.calls...)
	require.NoError(t, broken.Quiesced(context.Background(), w.presence()))
	require.Equal(t, []string{
		"A7 acked ⟹ the store agrees",
		"A9 no entry without a call",
	}, broken.Fired())
}

// outcomesOnly is the record a driver that wrote one line per call, after the
// call, would have left: every call whose outcome came back, and nothing for the
// one it died inside.
func outcomesOnly(calls []Call) []Call {
	var out []Call
	for _, c := range calls {
		if c.Dangling {
			continue
		}
		out = append(out, c)
	}
	return out
}

// TestAJournalThatForgetsMissesWhatOnlyMemorySees is the third break.
//
// It is demonstrated against defects rather than against a correct run, and that
// is the honest shape rather than a convenience: under a run that keeps its
// whole log, one look *is* the history, so a forgetful journal answers A1, A2,
// A5, A6, A9 and A10 exactly as a remembering one does. What memory is
// load-bearing for is the two things the world holds one copy of — an entry that
// was overwritten (A3) and a watermark that went backwards (A4) — and neither
// leaves a trace in any single sample.
func TestAJournalThatForgetsMissesWhatOnlyMemorySees(t *testing.T) {
	for _, tc := range []struct {
		defect  string
		fires   string
		samples func(w *world) []Sample
	}{
		{
			defect: "D5 an entry is rewritten in place",
			fires:  "A3 entries are immutable",
			samples: func(w *world) []Sample {
				first := w.sample(5)
				w.entries[2].Digest = 0xdead
				return []Sample{first, w.sample(5)}
			},
		},
		{
			defect: "D2 the watermark goes backwards",
			fires:  "A4 watermark monotonic",
			samples: func(w *world) []Sample {
				return []Sample{w.sample(5), w.sample(3)}
			},
		},
	} {
		t.Run(tc.defect, func(t *testing.T) {
			w := correct()
			samples := tc.samples(&w)

			remembering := New(Chaos())
			for _, s := range samples {
				remembering.Observe(s)
			}
			require.Equal(t, []string{tc.fires}, remembering.Fired())

			// A journal that forgets is the post-mortem look #94 rejected,
			// spelled as a wiring break: a fresh journal per observation.
			var forgetful []Finding
			for _, s := range samples {
				j := New(Chaos())
				j.Observe(s)
				forgetful = append(forgetful, j.Findings()...)
			}
			require.Empty(t, forgetful,
				"%s left a trace in a single sample, so this break proves nothing about memory", tc.defect)
		})
	}
}
