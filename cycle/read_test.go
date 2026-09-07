package cycle

// The overlay from the cycle's side: which source answers a read, what a halted
// or retired shard says, and what the counters see. The merge's own shape table
// is fold's and is tested there.

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	p "go.temporal.io/server/common/persistence"

	"github.com/aromanovich/waltz/apply"
	"github.com/aromanovich/waltz/fold"
	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/wal"
	"github.com/aromanovich/waltz/wal/waltest"
)

// coldStore is the base row the wrapper's thunk would have read: what it holds,
// how often it was asked, and what it fails with instead.
type coldStore struct {
	mu    sync.Mutex
	calls int
	info  string
	err   error

	// seq and at stamp when the row was read, against the same counter the
	// drain stamps its commit on. Order is the only way to tell a read that
	// waited for the drain from one answered after it by chance.
	seq *atomic.Int64
	at  int64
}

func (c *coldStore) read(context.Context) (*p.InternalGetWorkflowExecutionResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	if c.seq != nil {
		c.at = c.seq.Add(1)
	}
	if c.err != nil {
		return nil, c.err
	}
	return &p.InternalGetWorkflowExecutionResponse{
		State: &p.InternalWorkflowMutableState{
			ExecutionInfo: &commonpb.DataBlob{Data: []byte(c.info)},
		},
		DBRecordVersion: 1,
	}, nil
}

func (c *coldStore) set(info string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.info = info
}

func (c *coldStore) asked() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func (c *coldStore) readAt() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

func getExec(ns, wf, run string) *p.GetWorkflowExecutionRequest {
	return &p.GetWorkflowExecutionRequest{
		ShardID: int32(testShard), NamespaceID: ns, WorkflowID: wf, RunID: run,
	}
}

func getCurrent(ns, wf string) *p.GetCurrentExecutionRequest {
	return &p.GetCurrentExecutionRequest{ShardID: int32(testShard), NamespaceID: ns, WorkflowID: wf}
}

// blockingApplier holds a drain inside Apply until the test releases it, then
// lands its writes in the cold store: a transaction committing, as a reader
// sees it.
type blockingApplier struct {
	started chan struct{}
	release chan struct{}
	commit  func()
}

func (a *blockingApplier) Apply(context.Context, wal.ShardID, wal.Epoch, fold.Batch) error {
	close(a.started)
	<-a.release
	a.commit()
	return nil
}

// TestAReadArrivingMidDrainWaitsForItsOutcome is why a read is a request on the
// cycle's loop rather than a lock around the accumulator. The window empties
// when a drain starts and the cold store holds the writes only when the
// transaction commits, so a read served in that interval returns a state that
// never existed.
//
// This test does not falsify that exclusion and cannot: a read is a request on
// the channel the drain occupies, so it cannot be issued past one, and no
// premature read can be constructed here to watch it fail. What is pinned is
// the ordering — the read is answered once, from the cold store, after the
// commit, with what the drain wrote. Losing the property itself would have to
// be caught by the mode-level acceptance.
func TestAReadArrivingMidDrainWaitsForItsOutcome(t *testing.T) {
	e := newEnv(t, func(c *Config) { c.Mutations = 1 })
	ns, wf, run := ids()

	var seq atomic.Int64
	var committedAt int64
	base := &coldStore{info: "before the drain", seq: &seq}

	held := &blockingApplier{started: make(chan struct{}), release: make(chan struct{})}
	held.commit = func() {
		base.set("what the drain wrote")
		committedAt = seq.Add(1)
	}
	e.c.deps.Writer = held

	added := make(chan error, 1)
	go func() { added <- e.c.write(context.Background(), mkCreate(ns, wf, run), coldRows()) }()
	<-held.started

	type answer struct {
		resp *p.InternalGetWorkflowExecutionResponse
		err  error
	}
	read := make(chan answer, 1)
	go func() {
		resp, err := e.c.getWorkflowExecution(context.Background(), getExec(ns, wf, run), base.read)
		read <- answer{resp, err}
	}()

	close(held.release)
	require.NoError(t, <-added)
	got := <-read

	require.NoError(t, got.err)
	require.Equal(t, 1, base.asked(), "the window was drained, so this read is the cold store's to answer")
	require.Greater(t, base.readAt(), committedAt,
		"the cold store was asked before the drain's outcome was known: the answer is a write undone")
	require.Equal(t, "what the drain wrote", string(got.resp.State.ExecutionInfo.Data))
}

// TestAReadIsAnsweredFromTheWindow is the ordinary case: a run the window holds
// as a snapshot comes back without the cold store being asked at all.
func TestAReadIsAnsweredFromTheWindow(t *testing.T) {
	e := newEnv(t, nil)
	ns, wf, run := ids()
	base := &coldStore{info: "cold"}

	require.NoError(t, e.add(t, mkCreate(ns, wf, run)))
	resp, err := e.c.getWorkflowExecution(context.Background(), getExec(ns, wf, run), base.read)
	require.NoError(t, err)
	require.Nil(t, resp.State.ExecutionInfo, "a created run's state is the window's, not the store's")
	require.Zero(t, base.asked(), "a snapshot answers alone: the round trip is saved, not taken and discarded")

	require.NoError(t, e.add(t, mkUpdate(ns, wf, run, 2)))
	resp, err = e.c.getWorkflowExecution(context.Background(), getExec(ns, wf, run), base.read)
	require.NoError(t, err)
	require.Equal(t, int64(2), resp.DBRecordVersion, "the tail's version, which the next write will assert")
	require.Zero(t, base.asked(), "the update folded into the create's snapshot, which still answers alone")

	stats := e.c.Stats()
	require.Equal(t, 2, stats.Reads)
	require.Equal(t, 2, stats.ReadsHeld)
}

// TestAnUnheldRunIsTheColdStoresAnswer: for a run the window does not hold, the
// base's answer is the answer, and its NotFound travels back unwrapped like
// every other error on this path.
func TestAnUnheldRunIsTheColdStoresAnswer(t *testing.T) {
	e := newEnv(t, nil)
	ns, wf, run := ids()
	require.NoError(t, e.add(t, mkCreate(ns, wf, run)))

	base := &coldStore{info: "cold"}
	resp, err := e.c.getWorkflowExecution(context.Background(), getExec(ns, wf, "another-run"), base.read)
	require.NoError(t, err)
	require.Equal(t, "cold", string(resp.State.ExecutionInfo.Data))
	require.Equal(t, 1, base.asked())

	missing := serviceerror.NewNotFound("workflow execution not found")
	broken := &coldStore{err: missing}
	_, err = e.c.getWorkflowExecution(context.Background(), getExec(ns, wf, "another-run"), broken.read)
	require.True(t, err == error(missing), //nolint:errorlint // identity is the assertion
		"the base's error must come back unwrapped, got %v", err)

	require.Equal(t, 0, e.c.Stats().ReadsHeld, "neither read crossed a workflow the window holds")
}

// TestADeletedExecutionReadsAsDeleted: the tombstone is answered by the cycle,
// not by the cold store, which still holds the row until the drain.
func TestADeletedExecutionReadsAsDeleted(t *testing.T) {
	e := newEnv(t, nil)
	ns, wf, run := ids()
	require.NoError(t, e.add(t, mkCreate(ns, wf, run)))
	require.NoError(t, e.add(t, mutation.Mutation{Delete: &p.DeleteWorkflowExecutionRequest{
		ShardID: int32(testShard), NamespaceID: ns, WorkflowID: wf, RunID: run,
	}}))

	base := &coldStore{info: "still in the cold store"}
	_, err := e.c.getWorkflowExecution(context.Background(), getExec(ns, wf, run), base.read)
	var notFound *serviceerror.NotFound
	require.ErrorAs(t, err, &notFound)
	require.Zero(t, base.asked(), "a tombstone answers alone")
}

// TestInSyncModeTheOverlayIsANoOp: with Sync on, the accumulator is empty at
// every call boundary, so every read is the base's own answer and ReadsHeld is
// 0 by construction — which is what keeps the acceptance's byte-for-byte
// comparison of sync mode against the unwrapped server meaningful.
func TestInSyncModeTheOverlayIsANoOp(t *testing.T) {
	e := newEnv(t, func(c *Config) { c.Sync = true })
	ns, wf, run := ids()
	require.NoError(t, e.add(t, mkCreate(ns, wf, run)))
	require.NoError(t, e.add(t, mkUpdate(ns, wf, run, 2)))

	base := &coldStore{info: "cold"}
	resp, err := e.c.getWorkflowExecution(context.Background(), getExec(ns, wf, run), base.read)
	require.NoError(t, err)
	require.Equal(t, "cold", string(resp.State.ExecutionInfo.Data))
	require.Equal(t, int64(1), resp.DBRecordVersion, "the base's version, untouched")

	_, err = e.c.getCurrentExecution(context.Background(), getCurrent(ns, wf), func(context.Context) (*p.InternalGetCurrentExecutionResponse, error) {
		return &p.InternalGetCurrentExecutionResponse{RunID: run}, nil
	})
	require.NoError(t, err)

	stats := e.c.Stats()
	require.Equal(t, 2, stats.Reads)
	require.Zero(t, stats.ReadsHeld, "sync mode drains before it returns, so no read can cross a held workflow")
}

// TestTheCurrentRowIsAnsweredByTheCycle: the cycle asks the window for the
// current row and turns "no current row" into the store's own NotFound.
func TestTheCurrentRowIsAnsweredByTheCycle(t *testing.T) {
	e := newEnv(t, nil)
	ns, wf, run := ids()
	require.NoError(t, e.add(t, mkCreate(ns, wf, run)))

	asked := 0
	base := func(context.Context) (*p.InternalGetCurrentExecutionResponse, error) {
		asked++
		return &p.InternalGetCurrentExecutionResponse{RunID: "some-older-run"}, nil
	}

	resp, err := e.c.getCurrentExecution(context.Background(), getCurrent(ns, wf), base)
	require.NoError(t, err)
	require.Equal(t, run, resp.RunID, "the window's own write, not the store's row")
	require.Zero(t, asked)

	require.NoError(t, e.add(t, mutation.Mutation{DeleteCurrent: &p.DeleteCurrentWorkflowExecutionRequest{
		ShardID: int32(testShard), NamespaceID: ns, WorkflowID: wf, RunID: run,
	}}))
	_, err = e.c.getCurrentExecution(context.Background(), getCurrent(ns, wf), base)
	var notFound *serviceerror.NotFound
	require.ErrorAs(t, err, &notFound, "the window wrote the row and then removed it")
}

// TestAHaltedShardAnswersReadsFromWhatItCanVouchFor: the halt rule for the
// mutable-state reads turns on the tail rather than on the state, since a halt
// leaves the tail counters alone and they are what says whether the cold store
// still holds everything acked.
func TestAHaltedShardAnswersReadsFromWhatItCanVouchFor(t *testing.T) {
	t.Run("an empty tail is passthrough", func(t *testing.T) {
		e := newEnv(t, func(c *Config) { c.Mutations = 1 })
		ns, wf, run := ids()
		require.NoError(t, e.add(t, mkCreate(ns, wf, run)))
		// The drain committed, so the tail is empty when the fence lands.
		e.log.OnAppend(waltest.Once(wal.ErrFenced))
		require.Error(t, e.add(t, mkUpdate(ns, wf, run, 2)))
		require.Equal(t, StateHaltedLost, e.c.State())

		base := &coldStore{info: "cold"}
		resp, err := e.c.getWorkflowExecution(context.Background(), getExec(ns, wf, run), base.read)
		require.NoError(t, err, "everything acked is in the cold store, so it can answer")
		require.Equal(t, "cold", string(resp.State.ExecutionInfo.Data))
	})

	t.Run("an unapplied tail on halted-lost is ShardOwnershipLost", func(t *testing.T) {
		e := newEnv(t, nil)
		ns, wf, run := ids()
		require.NoError(t, e.add(t, mkCreate(ns, wf, run)))
		e.apply.errs = []error{&p.ShardOwnershipLostError{ShardID: int32(testShard)}}
		require.Error(t, e.c.drainNow(context.Background()))
		require.Equal(t, StateHaltedLost, e.c.State())

		base := &coldStore{info: "cold"}
		_, err := e.c.getWorkflowExecution(context.Background(), getExec(ns, wf, run), base.read)
		var lost *p.ShardOwnershipLostError
		require.ErrorAs(t, err, &lost)
		require.IsType(t, &p.ShardOwnershipLostError{}, err,
			"unwrapped: the shard's read path matches this one concrete type and nothing else")
		require.Zero(t, base.asked(), "the layer knows the cold store is incomplete")
	})

	t.Run("an unapplied tail on halted-invariant stays unrecognised", func(t *testing.T) {
		e := newEnv(t, nil)
		ns, wf, run := ids()
		require.NoError(t, e.add(t, mkCreate(ns, wf, run)))
		e.apply.errs = []error{&apply.InvariantViolationError{Cause: errors.New("a version assertion failed")}}
		require.Error(t, e.c.drainNow(context.Background()))
		require.Equal(t, StateHaltedInvariant, e.c.State())

		base := &coldStore{info: "cold"}
		_, err := e.c.getWorkflowExecution(context.Background(), getExec(ns, wf, run), base.read)
		require.ErrorIs(t, err, ErrHalted)
		var lost *p.ShardOwnershipLostError
		require.NotErrorIs(t, err, lost,
			"a divergence this process owns must not be handed on as an ordinary failover")
		require.Zero(t, base.asked())
	})
}

// TestARetiredCycleAnswersOffItsMirroredTail: with no loop left to ask, the
// tail is read off the mirror Add already consults. Empty means the cold store
// holds everything acked and the read is answerable; non-empty means it does
// not.
func TestARetiredCycleAnswersOffItsMirroredTail(t *testing.T) {
	t.Run("drained and stopped", func(t *testing.T) {
		e := newEnv(t, func(c *Config) { c.Mutations = 1 })
		ns, wf, run := ids()
		require.NoError(t, e.add(t, mkCreate(ns, wf, run)))
		e.c.Retire()

		base := &coldStore{info: "cold"}
		resp, err := e.c.getWorkflowExecution(context.Background(), getExec(ns, wf, run), base.read)
		require.NoError(t, err)
		require.Equal(t, "cold", string(resp.State.ExecutionInfo.Data))
	})

	t.Run("retired holding a tail", func(t *testing.T) {
		e := newEnv(t, nil)
		ns, wf, run := ids()
		require.NoError(t, e.add(t, mkCreate(ns, wf, run)))
		e.c.Retire()

		base := &coldStore{info: "cold"}
		_, err := e.c.getWorkflowExecution(context.Background(), getExec(ns, wf, run), base.read)
		var lost *p.ShardOwnershipLostError
		require.ErrorAs(t, err, &lost)
		require.Zero(t, base.asked())
	})
}

// TestAShardThisNodeDoesNotHoldIsPassthrough is the registry's rule for the two
// mutable-state reads, and it is the opposite of what Write does with the same
// case: a write around the log is fatal, while refusing a read would refuse
// every role that legitimately reads a shard without owning it.
func TestAShardThisNodeDoesNotHoldIsPassthrough(t *testing.T) {
	m, err := NewManager(Deps{Log: newLog(), Writer: &fakeApplier{}, Recoverer: &fakeWatermark{}, Registry: testRegistry()}, Fixed(Defaults()))
	require.NoError(t, err)

	base := &coldStore{info: "cold"}
	resp, err := m.GetWorkflowExecution(context.Background(), getExec("ns", "wf", "run"), base.read)
	require.NoError(t, err)
	require.Equal(t, "cold", string(resp.State.ExecutionInfo.Data))

	current, err := m.GetCurrentExecution(context.Background(), getCurrent("ns", "wf"), func(context.Context) (*p.InternalGetCurrentExecutionResponse, error) {
		return &p.InternalGetCurrentExecutionResponse{RunID: "run"}, nil
	})
	require.NoError(t, err)
	require.Equal(t, "run", current.RunID)
}
