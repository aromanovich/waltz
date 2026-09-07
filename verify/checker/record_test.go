package checker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	p "go.temporal.io/server/common/persistence"

	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/verify/mutgen"
)

func newRecordAt(t *testing.T, node string, incarnation int64) (*Record, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "record.jsonl")
	r, err := NewRecord(path, node, incarnation)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })
	return r, path
}

func stream(t *testing.T, n int) []mutation.Mutation {
	t.Helper()
	cfg := mutgen.Default()
	cfg.Seed = 7
	cfg.ShardID = 1
	cfg.Workflows = 4
	g, err := mutgen.New(cfg)
	require.NoError(t, err)
	ms, err := g.Take(n)
	require.NoError(t, err)
	return ms
}

// TestACallWithNoOutcomeIsTheThirdClass is why the record is two fsynced lines
// per call: the gap between them is where a kill -9 lands, and a call caught
// there is one nobody knows the outcome of — not one that failed, and not one
// that never happened.
func TestACallWithNoOutcomeIsTheThirdClass(t *testing.T) {
	rec, path := newRecordAt(t, "n1", 1)
	ms := stream(t, 3)

	first, err := rec.Call(1, 2, ms[0])
	require.NoError(t, err)
	require.NoError(t, rec.Outcome(first, Acked, ""))

	second, err := rec.Call(1, 2, ms[1])
	require.NoError(t, err)
	require.NoError(t, rec.Outcome(second, Refused, "condition failed"))

	// The third call is written down and the process dies before its outcome.
	_, err = rec.Call(1, 2, ms[2])
	require.NoError(t, err)

	lines, err := ReadRecord(path)
	require.NoError(t, err)
	calls := Calls(lines)
	require.Len(t, calls, 3)
	require.Equal(t, Acked, calls[0].Outcome)
	require.Equal(t, Refused, calls[1].Outcome)
	require.Equal(t, Unknown, calls[2].Outcome)
	require.True(t, calls[2].Dangling)
	require.False(t, calls[0].Dangling)

	// And the identity came along, which is what A7 looks the call up by and
	// what A9 counts entries against.
	for i, c := range calls {
		if c.Effect == NoClaim {
			// A history-task record names no run (#142); the kind is still on
			// the line, which is what a reader of the record needs.
			require.Empty(t, c.Subject.Run)
			require.Equal(t, ms[i].Kind().String(), c.Kind)
			continue
		}
		require.NotEmpty(t, c.Subject.Run, "call %d has no subject", i)
		require.Equal(t, ms[i].Kind().String(), c.Kind)
	}
}

// TestASecondRunOnOneRecordNeedsAnIncarnationOfItsOwn closes the trap #95 left
// written down in its own source: `seq` is per process, so a node restarted onto
// the same record path numbers a second run from 1, and the two runs' outcome
// lines then point at each other's calls.
func TestASecondRunOnOneRecordNeedsAnIncarnationOfItsOwn(t *testing.T) {
	first, path := newRecordAt(t, "n1", 0)
	_, err := first.Call(1, 2, stream(t, 1)[0])
	require.NoError(t, err)
	require.NoError(t, first.Close())

	_, err = NewRecord(path, "n1", 0)
	require.ErrorContains(t, err, "already holds a run")

	second, err := NewRecord(path, "n1", 2)
	require.NoError(t, err)
	t.Cleanup(func() { _ = second.Close() })

	id, err := second.Call(1, 3, stream(t, 1)[0])
	require.NoError(t, err)
	require.EqualValues(t, 1, id, "seq restarts, which is exactly why the incarnation has to be there")

	lines, err := ReadRecord(path)
	require.NoError(t, err)
	require.Len(t, lines, 2)
	require.EqualValues(t, 0, lines[0].Incarnation)
	require.EqualValues(t, 2, lines[1].Incarnation)
}

// TestAPartialTrailingLineIsDropped: the record is appended and fsynced per
// line, so a kill can at worst leave the last one short — and an instrument that
// treated that as a broken file could not read the record of the very node it
// came to kill.
func TestAPartialTrailingLineIsDropped(t *testing.T) {
	rec, path := newRecordAt(t, "n1", 1)
	id, err := rec.Call(1, 2, stream(t, 1)[0])
	require.NoError(t, err)
	require.NoError(t, rec.Outcome(id, Acked, ""))
	require.NoError(t, rec.Close())

	b, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, append(b, []byte(`{"seq":3,"even`)...), 0o644))

	lines, err := ReadRecord(path)
	require.NoError(t, err)
	require.Len(t, lines, 2)
	require.Len(t, Calls(lines), 1)
}

// TestIdentifyNamesTheSubjectAndTheEffect drives the whole generated stream
// through the one function both ends use — the driver stamping a call line, and
// the sampler reading an entry — so a request shape that carries its identity
// somewhere else is a failure here rather than a silent A7 that judges nothing.
func TestIdentifyNamesTheSubjectAndTheEffect(t *testing.T) {
	kinds := map[mutation.Kind]int{}
	for _, m := range stream(t, 400) {
		subject, effect, err := Identify(m)
		require.NoError(t, err)
		kinds[m.Kind()]++
		switch m.Kind() {
		case mutation.KindAddTasks, mutation.KindRangeCompleteTasks:
			// The two records that are about a queue rather than about a run
			// (#142). They are identified and make no claim, which is what lets
			// a driver record them without A7 asserting something it cannot see.
			require.Equal(t, NoClaim, effect, "%v", m.Kind())
			require.Empty(t, subject.Run)
			continue
		}
		require.NotEmpty(t, subject.Namespace)
		require.NotEmpty(t, subject.Workflow)
		require.NotEmpty(t, subject.Run, "%v carries no run id", m.Kind())
		if m.Kind() == mutation.KindDelete {
			require.Equal(t, Removed, effect)
		} else {
			require.Equal(t, Exists, effect, "%v", m.Kind())
		}
	}
	// A stream with no deletion in it would make the "always answers yes" break
	// unfalsifiable, since A7's absent direction is the only thing that catches
	// it. This is that requirement on the driver, asserted where it is cheap.
	require.Greater(t, kinds[mutation.KindDelete], 0, "the stream carries no deletion: %v", kinds)
}

// TestClassifyIsThreeValuedAndUnknownIsTheDefault pins the correction #95
// forced: the closed set of definite answers is a list, and anything not on it
// is unknown rather than assumed, because the direction of that mistake is a
// checker accusing a correct layer.
func TestClassifyIsThreeValuedAndUnknownIsTheDefault(t *testing.T) {
	require.Equal(t, Acked, Classify(nil))
	require.Equal(t, Refused, Classify(&p.WorkflowConditionFailedError{}))
	require.Equal(t, Refused, Classify(&serviceerror.ResourceExhausted{}))
	require.Equal(t, Refused, Classify(&p.ShardOwnershipLostError{}))
	require.Equal(t, Unknown, Classify(context.DeadlineExceeded))
	require.Equal(t, Unknown, Classify(errors.New("connection reset by peer")))

	require.True(t, Fenced(&p.ShardOwnershipLostError{}))
	require.False(t, Fenced(&p.WorkflowConditionFailedError{}))
}
