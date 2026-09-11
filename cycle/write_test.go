package cycle

// The boundary the wrapper writes through: which of the cycle's answers become
// the store's own error types and which do not.
//
// Every case here is about what ContextImpl.handleWriteErrorLocked makes of the
// value. It is a type switch with no errors.As, so the answer decides what the
// history service does next: keep the shard and report the failure, stop the
// shard because it has been stolen, or re-acquire it in the background because
// nobody knows whether the write landed.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	p "go.temporal.io/server/common/persistence"

	"github.com/aromanovich/waltz/cold"
	"github.com/aromanovich/waltz/internal/verify/basetest"
	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/wal"
	"github.com/aromanovich/waltz/wal/memwal"
)

// newManager builds a manager over a memwal and a fake applier, in sync mode
// unless shape says otherwise.
func newManager(t *testing.T, apply cold.Applier, shape func(*Config)) *Manager {
	t.Helper()
	cfg := Defaults()
	cfg.Sync = true
	if shape != nil {
		shape(&cfg)
	}
	m, err := NewManager(testDeps(memwal.New(), apply), Fixed(cfg))
	require.NoError(t, err)
	t.Cleanup(func() { m.Close(context.Background()) })
	return m
}

// write is [Manager.Write] over an empty cold store — empty and not absent,
// since a caller bringing no rows is refused with [ErrNoBaseRow] rather
// than believed. The windows below open with a create, which asserts absence,
// so an empty store is what their caller would have found. The delegated read
// itself is condition_test.go's.
func write(ctx context.Context, m *Manager, mut mutation.Mutation, epoch wal.Epoch) error {
	return m.Write(ctx, mut, epoch, basetest.New().Rows())
}

// TestAWriteToAShardThisNodeDoesNotHoldIsRefused: this is the one case that
// must not fall through to the store below. A write the layer let past is a
// write the log never saw, and the next drain would assert a base version that
// write had already moved.
func TestAWriteToAShardThisNodeDoesNotHoldIsRefused(t *testing.T) {
	m := newManager(t, &fakeApplier{}, nil)
	ns, wf, run := ids()

	err := write(context.Background(), m, mkCreate(ns, wf, run), 7)
	requireLost(t, err)
}

// TestAWriteUnderAnEpochThisNodeDoesNotHoldIsRefused is I11, the fencing the
// plugin's own AssertShard(rangeID) does on every such request. Without it a
// shard context already fenced out would have its write re-stamped with
// whatever epoch this node currently holds.
func TestAWriteUnderAnEpochThisNodeDoesNotHoldIsRefused(t *testing.T) {
	ctx := context.Background()
	applier := &fakeApplier{}
	m := newManager(t, applier, nil)
	require.NoError(t, m.ShardAcquired(ctx, testShard, 7))
	ns, wf, run := ids()

	requireLost(t, write(ctx, m, mkCreate(ns, wf, run), 6))
	require.Empty(t, applier.drains, "a refused write reaches nothing")
	require.EqualValues(t, 0, m.Shard(testShard).Stats().CommitSeqno, "and acks nothing")

	require.NoError(t, write(ctx, m, mkCreate(ns, wf, run), 7), "the epoch this node holds is taken")
}

// TestADeleteNamesNoEpochAndIsTakenAnyway: the tombstone requests carry no
// rangeID, so a zero epoch means "the caller named none" rather than "epoch 0".
// They are still fenced by the drain transaction's epoch CAS.
func TestADeleteNamesNoEpochAndIsTakenAnyway(t *testing.T) {
	ctx := context.Background()
	m := newManager(t, &fakeApplier{}, nil)
	require.NoError(t, m.ShardAcquired(ctx, testShard, 7))
	ns, wf, run := ids()

	require.NoError(t, write(ctx, m, mkCreate(ns, wf, run), 7))
	require.NoError(t, write(ctx, m, mutation.Mutation{Delete: &p.DeleteWorkflowExecutionRequest{
		ShardID:     int32(testShard),
		NamespaceID: ns,
		WorkflowID:  wf,
		RunID:       run,
	}}, 0))
}

// TestAFencedShardAnswersShardOwnershipLost: the apply transaction's failed
// epoch CAS is already the store's own type and travels untouched, and every
// refusal the halted cycle answers afterwards becomes one.
func TestAFencedShardAnswersShardOwnershipLost(t *testing.T) {
	ctx := context.Background()
	fenced := &p.ShardOwnershipLostError{ShardID: int32(testShard), Msg: "another node has it"}
	m := newManager(t, &fakeApplier{errs: []error{fenced}}, nil)
	require.NoError(t, m.ShardAcquired(ctx, testShard, 7))
	ns, wf, run := ids()

	err := write(ctx, m, mkCreate(ns, wf, run), 7)
	require.True(t, err == error(fenced), //nolint:errorlint // identity is the assertion
		"apply's own value must not be rebuilt on the way out, got %v", err)
	require.Equal(t, StateHaltedLost, m.Shard(testShard).State())

	requireLost(t, write(ctx, m, mkCreate(ns, wf, run), 7))
}

// TestAHaltedInvariantIsNotAFailover: a divergence this process owns must not
// leave here as ShardOwnershipLost, which would hand the bug to the next owner
// as an ordinary failover. Unrecognised is not unhandled — the switch's default
// arm re-acquires the shard in the background.
func TestAHaltedInvariantIsNotAFailover(t *testing.T) {
	ctx := context.Background()
	// Async: at a window of one a condition failure is the caller's answer
	// rather than a halt.
	m := newManager(t, &fakeApplier{errs: []error{&p.WorkflowConditionFailedError{Msg: "stale"}}},
		func(c *Config) { c.Sync = false; c.Mutations = 2 })
	require.NoError(t, m.ShardAcquired(ctx, testShard, 7))
	ns, wf, run := ids()

	require.NoError(t, write(ctx, m, mkCreate(ns, wf, run), 7))
	require.Error(t, write(ctx, m, mkUpdate(ns, wf, run, 2), 7))
	require.Equal(t, StateHaltedInvariant, m.Shard(testShard).State())

	err := write(ctx, m, mkUpdate(ns, wf, run, 3), 7)
	require.ErrorIs(t, err, ErrHalted)
	require.False(t, errors.As(err, new(*p.ShardOwnershipLostError)),
		"a halted-invariant shard must never look like a shard somebody else took: %v", err)
}

// TestTheRefusalReachesTheBoundaryUntouched: I10's refusal is already one of
// the store's own types — the one the shard's write path reads as "definitely
// not committed" — so the boundary must hand it back unrebuilt.
func TestTheRefusalReachesTheBoundaryUntouched(t *testing.T) {
	ctx := context.Background()
	m := newManager(t, &fakeApplier{}, func(c *Config) {
		neverDrains(c)
		c.HardMaxEntries = 1
	})
	require.NoError(t, m.ShardAcquired(ctx, testShard, 7))
	ns, wf, run := ids()

	require.NoError(t, write(ctx, m, mkCreate(ns, wf, run), 7))
	err := write(ctx, m, mkUpdate(ns, wf, run, 2), 7)
	_, ok := err.(*serviceerror.ResourceExhausted) //nolint:errorlint // the concrete type is the assertion
	require.True(t, ok, "the boundary answered %T, which handleWriteErrorLocked would not recognise", err)
	require.False(t, p.OperationPossiblySucceeded(err))
}

// TestAConditionTheWindowAnswersIsTheCallersOwnError is the unwrapped-error
// claim on the condition authority's path; the assertion is a direct type
// assertion, since a wrapped copy would satisfy errors.As and is what is
// forbidden.
//
// A condition failure found at drain time is attributable only at a window of
// one, because every other caller has been acked. One the accumulator answers
// is attributable at any window: its subject is the mutation in this caller's
// own call, and the check runs before the append, so nothing was acked.
func TestAConditionTheWindowAnswersIsTheCallersOwnError(t *testing.T) {
	ctx := context.Background()
	applier := &fakeApplier{}
	m := newManager(t, applier, neverDrains)
	require.NoError(t, m.ShardAcquired(ctx, testShard, 7))
	ns, wf, run := ids()

	require.NoError(t, write(ctx, m, mkCreate(ns, wf, run), 7))
	require.NoError(t, write(ctx, m, mkUpdate(ns, wf, run, 2), 7))

	// The same version again: a writer that read the run before the update.
	// Sequentially the store answers it; so does the window.
	err := write(ctx, m, mkUpdate(ns, wf, run, 2), 7)
	failed, ok := err.(*p.WorkflowConditionFailedError) //nolint:errorlint // the concrete type is the assertion
	require.True(t, ok, "the boundary answered %T, which handleWriteErrorLocked would not recognise: %v", err, err)
	require.EqualValues(t, 2, failed.DBRecordVersion, "the version the window holds, as the store would have reported it")
	require.False(t, p.OperationPossiblySucceeded(err))

	// Checked before the append, so a refused write provably wrote nothing and
	// the caller may retry off a fresh read.
	require.EqualValues(t, 2, m.Shard(testShard).Stats().CommitSeqno, "the refused write acked nothing")
	require.Empty(t, applier.drains, "and reached no transaction")
	require.Equal(t, StateRunning, m.Shard(testShard).State(),
		"an ordinary condition failure must not halt the shard: that is the divergence this answers")

	require.NoError(t, write(ctx, m, mkUpdate(ns, wf, run, 3), 7),
		"and the shard keeps taking writes that do follow the window")
}

// TestAWindowedWriteRetainsTheCallersOwnRequest is the ownership half of
// wrapper.ShardWriter.Write: past a Write the window has not drained, the
// layer holds the caller's request itself. That is what puts the drain's
// rangeID stamp (apply) in a struct whose caller returned long ago, and a
// copy taken anywhere between the seam and the drain would answer the doc's
// promise with a mutation nobody can observe.
func TestAWindowedWriteRetainsTheCallersOwnRequest(t *testing.T) {
	ctx := context.Background()
	applier := &fakeApplier{}
	m := newManager(t, applier, func(c *Config) {
		c.Sync = false
		c.Mutations, c.Bytes, c.Age = 2, 1<<30, time.Hour
	})
	require.NoError(t, m.ShardAcquired(ctx, testShard, 7))
	ns, wf, run := ids()

	held := mkCreate(ns, wf, run)
	require.NoError(t, write(ctx, m, held, 7))
	require.Empty(t, applier.drains, "the window holds it, and the caller is gone")

	require.NoError(t, write(ctx, m, mkCreate(ids()), 7), "the second write trips the watermark")
	require.Len(t, applier.drains, 1)
	require.Same(t, held.Create, applier.drains[0][0].Request.Create,
		"the drain must reach the caller's own request rather than a copy of it")
}

func requireLost(t *testing.T, err error) *p.ShardOwnershipLostError {
	t.Helper()
	lost, ok := err.(*p.ShardOwnershipLostError) //nolint:errorlint // the concrete type is the assertion
	require.True(t, ok, "expected the store's own ShardOwnershipLostError, got %T: %v", err, err)
	require.EqualValues(t, testShard, wal.ShardID(lost.ShardID))
	return lost
}
