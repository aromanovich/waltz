package wrapper

// The interception table against the kinds it is indexed by. What each row
// *means* is the metrics and intercept suites' business; this is the one
// direction those cannot state — a kind with no row at all, which turns every
// write of that kind into a refusal.

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/aromanovich/waltz/mutation"
)

// TestEveryInterceptedKindHasARow: the eight kinds the record format has a
// shape for are exactly the eight the wrapper takes into the layer, so every
// one of them needs a row. A ninth kind added to mutation without one fails
// here rather than at whichever metric goes untagged.
func TestEveryInterceptedKindHasARow(t *testing.T) {
	var s ExecutionStore
	for k := mutation.KindInvalid + 1; int(k) < mutation.KindCount; k++ {
		row := interception[k]
		t.Run(k.String(), func(t *testing.T) {
			require.NotEmpty(t, row.op, "no metrics op tag")
			require.NotNil(t, row.counter, "no counter")
			require.NotNil(t, row.counter(&s), "the counter selector answers nothing")
		})
	}
}

// TestTheOpTagsAreTheStoreMethodNames: the tag is what an operator reads off
// the metric, and the only thing that makes it meaningful is that it names the
// persistence method the write arrived on. A row copied from its neighbour is
// the failure this catches.
func TestTheOpTagsAreTheStoreMethodNames(t *testing.T) {
	// Against intercepted rather than a literal of its own: that map's keys are
	// held to being real ExecutionStore methods by the reflection over the
	// interface in intercept_test.go, where a third hand-written copy of these
	// eight pairs would be held to nothing.
	require.Len(t, intercepted, mutation.KindCount-1, "every kind but the zero one")
	for name, k := range intercepted {
		require.Equal(t, name, interception[k].op, "kind %s", k)
	}

	seen := map[string]bool{}
	for k := mutation.KindInvalid + 1; int(k) < mutation.KindCount; k++ {
		op := interception[k].op
		require.False(t, seen[op], "two kinds tag their metric %q", op)
		seen[op] = true
	}
}
