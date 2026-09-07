package checker

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// [CensusOf] is the half of a census the record answers on its own, and it is
// exported because the chaos harness asks it about one node at a time — a
// generation's quota, a partitioned node's silence, the judged runs split by
// the driver that wrote them. It used to be a copy there, checked by a
// fifteen-minute cluster run; here it is checked by a table.

// TestACensusCountsEveryOutcomeClassAndTheDanglingWithin is the census as an
// arithmetic claim: the three classes partition the calls, and Dangling counts
// *within* the unknown class rather than beside it — a run that reported them
// as a fourth class would double-count exactly the calls a kill produced.
func TestACensusCountsEveryOutcomeClassAndTheDanglingWithin(t *testing.T) {
	a, b, c := run("a"), run("b"), run("c")
	got := CensusOf([]Call{
		{Node: "n1", Seq: 1, Subject: a, Outcome: Acked},
		{Node: "n1", Seq: 2, Subject: b, Outcome: Refused},
		{Node: "n1", Seq: 3, Subject: c, Outcome: Unknown},
		{Node: "n1", Seq: 4, Subject: c, Outcome: Unknown, Dangling: true},
	})

	require.Equal(t, 4, got.Calls)
	require.Equal(t, 1, got.Acked)
	require.Equal(t, 1, got.Refused)
	require.Equal(t, 2, got.Unknown)
	require.Equal(t, 1, got.Dangling)
	require.Equal(t, got.Acked+got.Refused+got.Unknown, got.Calls,
		"the three classes must partition the calls, or a census can be green over calls nobody counted")
	require.Zero(t, CensusOf(nil).Calls, "no calls is a census of nothing, not a panic")
}

// TestOnlyARunWhoseLastCallWasAckedIsJudged pins the one number here that is
// not a tally: Judged is what A7 will state something about, so it is per
// *run* and it is the run's last call that decides — an earlier ack on a run
// that later went unknown is a final state nobody knows, which is the whole
// reason A7 is narrow (see [Journal.Quiesced]).
func TestOnlyARunWhoseLastCallWasAckedIsJudged(t *testing.T) {
	acked, thenUnknown, thenRefused := run("acked"), run("unknown"), run("refused")
	got := CensusOf([]Call{
		{Node: "n1", Seq: 1, Subject: acked, Outcome: Acked},
		{Node: "n1", Seq: 2, Subject: thenUnknown, Outcome: Acked},
		{Node: "n1", Seq: 3, Subject: thenRefused, Outcome: Acked},
		{Node: "n1", Seq: 4, Subject: thenUnknown, Outcome: Unknown, Dangling: true},
		{Node: "n1", Seq: 5, Subject: thenRefused, Outcome: Refused},
	})

	require.Equal(t, 3, got.Acked)
	require.Equal(t, 1, got.Judged,
		"three runs were acked at some point and only one of them ended that way")
}

// TestTheJournalsCensusIsCensusOfPlusWhatOnlyItKnows is the reason the function
// is exported rather than copied: the harness's per-node counts and the run's
// own totals are the same arithmetic, and a second implementation of it drifts
// silently — the copy said nothing about Judged at all, so the two numbers a
// green run is asserted on came from two places.
func TestTheJournalsCensusIsCensusOfPlusWhatOnlyItKnows(t *testing.T) {
	w := correct()
	j := w.judge(t)

	whole := j.Census()
	fromCalls := CensusOf(w.calls)

	require.Equal(t, fromCalls.Calls, whole.Calls)
	require.Equal(t, fromCalls.Acked, whole.Acked)
	require.Equal(t, fromCalls.Refused, whole.Refused)
	require.Equal(t, fromCalls.Unknown, whole.Unknown)
	require.Equal(t, fromCalls.Dangling, whole.Dangling)
	require.Equal(t, fromCalls.Judged, whole.Judged)

	// And the two fields a slice of calls cannot answer, which is what the
	// journal is for: what it looked at, and what it still remembers of a log
	// the trim has been taking away.
	require.Zero(t, fromCalls.Samples)
	require.Nil(t, fromCalls.Remembered)
	require.Equal(t, 2, whole.Samples)
	require.NotEmpty(t, whole.Remembered)
}
