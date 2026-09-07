package checker

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestTheDeclarationIsWhatSetsTheTrim is #101's fourth deliverable as a test.
//
// #94 concluded that a chaos run raises TrimEvery so the log is the run's
// history; #95 found the conclusion necessary and not sufficient, because the
// trim fires on TrimEvery drains **or** TrimAfter age and the default 60 s
// erased an entire run. Whatever declares WholeLog therefore has to be what sets
// the policy — a declaration and a command line that could disagree is a promise
// nobody keeps.
func TestTheDeclarationIsWhatSetsTheTrim(t *testing.T) {
	keeping := Chaos()
	require.True(t, keeping.WholeLog)
	flags := keeping.NodeFlags()
	require.Contains(t, flags, "-"+FlagTrimEvery)
	require.Contains(t, flags, "-"+FlagTrimAfter,
		"both triggers or neither: raising TrimEvery alone was measured to erase a whole run")

	trimming := keeping
	trimming.WholeLog = false
	flags = trimming.NodeFlags()
	require.NotContains(t, flags, "-"+FlagTrimEvery)
	require.NotContains(t, flags, "-"+FlagTrimAfter,
		"a run that did not declare WholeLog runs at the shipped cadence, visibly")
}

// TestEveryPolicyKnobReachesTheNode: a knob the policy holds and does not render
// is a run configured by hope. The check is by name, since the names are the
// contract with the node binary.
func TestEveryPolicyKnobReachesTheNode(t *testing.T) {
	flags := Chaos().NodeFlags()
	for _, name := range []string{
		FlagWindowMutations, FlagWindowBytes, FlagWindowAge,
		FlagTrimEvery, FlagTrimAfter, FlagCallTimeout,
	} {
		require.True(t, slices.Contains(flags, "-"+name), "%s is never passed to a node: %v", name, flags)
	}
}

func TestAPolicyThatCannotMeanAnythingIsRefused(t *testing.T) {
	require.NoError(t, Chaos().Validate())

	for _, tc := range []struct {
		name   string
		break_ func(p *Policy)
	}{
		{"no window", func(p *Policy) { p.Mutations = 0 }},
		{"no byte bound", func(p *Policy) { p.Bytes = 0 }},
		{"no age bound", func(p *Policy) { p.Age = 0 }},
		{"a negative deadline", func(p *Policy) { p.CallTimeout = -1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := Chaos()
			tc.break_(&p)
			require.Error(t, p.Validate())
		})
	}
}
