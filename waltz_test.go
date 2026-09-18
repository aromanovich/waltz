package waltz

// Configuration and composition without a cluster: the mode, policy and graph.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/service/history/tasks"

	"github.com/aromanovich/waltz/baserow"
	"github.com/aromanovich/waltz/cycle"
	"github.com/aromanovich/waltz/fold"
	"github.com/aromanovich/waltz/internal/verify/coldtest"
	"github.com/aromanovich/waltz/internal/verify/mutbuild"
	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/wal"
	"github.com/aromanovich/waltz/wal/memwal"
)

// Every decision the layer takes samples the policy and the budget assertion
// samples it first, so a caller that states none has to be refused where the
// other required inputs are — otherwise it is a nil dereference inside a
// composition that had already been accepted.
func TestTheCompositionRefusesAPolicyNobodyStated(t *testing.T) {
	_, err := Compose(Backends{}, nil, DefaultTaskCategories(), nil, nil)
	require.ErrorContains(t, err, "no policy")
}

// The layer decodes an inherited tail with the registry this cluster is
// configured for, not the default set: an unknown category id is fatal to a
// replay, so a default registry on a cluster with archival enabled would refuse
// exactly the entries carrying archival tasks.
func TestTheRegistryIsTheServersOwn(t *testing.T) {
	dc := dynamicconfig.NewCollection(dynamicconfig.NewNoopClient(), log.NewNoopLogger())

	off := TaskCategories(dc, &config.Config{})
	_, found := off.Categories().GetCategoryByID(tasks.CategoryArchival.ID())
	require.False(t, found, "archival is off, so the category is not in the registry")

	on := TaskCategories(dc, &config.Config{
		Archival: config.Archival{History: config.HistoryArchival{State: "enabled"}},
	})
	_, found = on.Categories().GetCategoryByID(tasks.CategoryArchival.ID())
	require.True(t, found,
		"archival is on, so the entries this cluster writes can name a category a default registry has never heard of")
}

// composed is the composition over the backends this library ships: a log in
// memory and a writer that commits nothing, so cfg is the whole of what varies
// between the callers.
func composed(t *testing.T, cfg cycle.Config) (*Layer, *coldtest.Cold) {
	t.Helper()
	cold := coldtest.New()
	layer, err := Compose(
		Backends{Log: memwal.New(), Cold: cold},
		cycle.Fixed(cfg),
		DefaultTaskCategories(),
		log.NewNoopLogger(), nil)
	require.NoError(t, err)
	return layer, cold
}

// The composition over a log in memory, which is what makes [Backends] a seam
// rather than a parameter list: the same Compose a binary calls, over a writer
// that commits nothing, with no cluster anywhere. A caller that could not reach
// this graph would build a second cycle.NewManager instead, which is the one
// thing this package asks callers not to do.
func TestTheCompositionIsReachableWithoutAColdStore(t *testing.T) {
	ctx := context.Background()
	const shard, epoch = wal.ShardID(3), wal.Epoch(7)

	layer, _ := composed(t, cycle.Defaults())
	t.Cleanup(func() { require.NoError(t, layer.Shutdown(ctx, time.Minute)) })

	// Through Options, because that is the face the binary hands the factory:
	// the registry is the acquire's observer and the write path at once.
	require.NoError(t, layer.Options().Layer.ShardAcquired(ctx, shard, epoch))

	_, ok := layer.ShardStats(shard)
	require.True(t, ok, "the acquire installed a cycle for the shard")
	require.Equal(t, 1, layer.Totals().Shards, "and the layer counts it")
}

// closingLog counts the closes of the log it wraps, which is the only thing
// about [wal.Log.Close] a composition can be asked.
type closingLog struct {
	wal.Log
	closes int
}

func (l *closingLog) Close() { l.closes++; l.Log.Close() }

// A shutdown releases the log, not only the cycles that were writing to it. A
// backend holding a connection or a pinged ownership transaction has nothing
// above it but the composition that knows when the last append has happened —
// so a drain that stops the cycles and leaves the backend open keeps shards
// this process has stopped writing to.
func TestShutdownReleasesTheLog(t *testing.T) {
	logs := &closingLog{Log: memwal.New()}
	cold := coldtest.New()
	layer, err := Compose(
		Backends{Log: logs, Cold: cold},
		cycle.Fixed(cycle.Defaults()),
		DefaultTaskCategories(),
		log.NewNoopLogger(), nil)
	require.NoError(t, err)

	require.Zero(t, logs.closes, "a composed layer has not released anything yet")
	require.NoError(t, layer.Shutdown(context.Background(), time.Minute))
	require.Equal(t, 1, logs.closes, "the shutdown released the log")
}

// TestTheShutdownDrainOutlivesTheContextThatAsksForIt: a shutdown drain runs
// where a context has just been cancelled — that is what shutdown is — so the
// budget goes on a context of the layer's own. A drain that inherited the
// caller's cancellation would return at once and leave a tail behind, which is
// not a failure anybody reads: the next owner replays it and the run looks
// clean.
func TestTheShutdownDrainOutlivesTheContextThatAsksForIt(t *testing.T) {
	const shard, epoch = wal.ShardID(3), wal.Epoch(7)

	layer, cold := composed(t, cycle.Defaults())

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, layer.Options().Layer.ShardAcquired(ctx, shard, epoch))
	require.NoError(t, layer.Options().Layer.Write(ctx, mutation.Mutation{Create: aCreate(shard)}, epoch, baserow.New(emptyStore{})))
	require.Zero(t, cold.Drains(), "a windowed write of one mutation reaches no watermark")

	cancel()
	require.NoError(t, layer.Shutdown(ctx, time.Minute))

	require.Equal(t, 1, cold.Drains(),
		"the window the shutdown found went to the cold store, cancelled context and all")
}

// TestOneCompositionEmitsToOneHandler drives the composition the way a binary
// running several services in one process does: one options value, one
// NewFactory per service, each with that service's metrics handler. Both halves
// of the layer — the wrapper's counters and the cycles' — must land in the same
// handler, because they are read against each other: what the stores sent at
// the layer over what the drains carried is not a ratio when the two numbers
// are on different scrapes.
//
// Which handler wins is walmetrics.Emitter.Use's decision; what this asserts is
// that one decision covers both halves.
func TestOneCompositionEmitsToOneHandler(t *testing.T) {
	ctx := context.Background()
	const shard, epoch = wal.ShardID(3), wal.Epoch(7)

	// Sync, so the write this test makes drains inside itself and the cycles'
	// half of the numbers exists by the time the assertions run.
	cfg := cycle.Defaults()
	cfg.Sync = true

	layer, _ := composed(t, cfg)
	t.Cleanup(func() { require.NoError(t, layer.Shutdown(ctx, time.Minute)) })

	factory := layer.AbstractFactory(baseFactory{base: coldStores{}})

	history, matching := metricstest.NewCaptureHandler(), metricstest.NewCaptureHandler()
	factory.NewFactory(config.CustomDatastoreConfig{}, resolver.NewNoopResolver(), "test", log.NewNoopLogger(), history)
	// The second service's persistence graph, and the stores this test writes
	// through: whichever service builds last must not take the numbers with it.
	stores := factory.NewFactory(config.CustomDatastoreConfig{}, resolver.NewNoopResolver(), "test", log.NewNoopLogger(), matching)

	shards, err := stores.NewShardStore()
	require.NoError(t, err)
	require.NoError(t, shards.UpdateShard(ctx, &p.InternalUpdateShardRequest{
		ShardID: int32(shard), RangeID: int64(epoch), PreviousRangeID: int64(epoch) - 1,
	}))

	store, err := stores.NewExecutionStore()
	require.NoError(t, err)

	first, second := history.StartCapture(), matching.StartCapture()
	create := aCreate(shard)
	create.RangeID = int64(epoch)
	_, err = store.CreateWorkflowExecution(ctx, create)
	require.NoError(t, err)

	emitted := first.Snapshot()
	require.Len(t, emitted["wal_intercepted_writes"], 1,
		"the wrapper's own series went somewhere else: the store built for the second service is emitting through a handler of its own")
	require.Len(t, emitted["wal_drains"], 1,
		"the cycles' series went somewhere else")
	require.Empty(t, second.Snapshot(),
		"the second service's handler took part of the layer's numbers with it")
}

// aCreate is the smallest brand-new workflow the write path folds and encodes.
// Smallest in the literal sense: it carries no execution-state blob, so it does
// not survive a round trip through the codec, which rebuilds the state from that
// blob and hands back a nil one for a fold to dereference. Fine for a write
// driven through the layer, which folds the request as given; a test that wants
// an entry *in a log* builds one with `internal/verify/mutbuild` instead.
func aCreate(shard wal.ShardID) *p.InternalCreateWorkflowExecutionRequest {
	run := uuid.NewString()
	return &p.InternalCreateWorkflowExecutionRequest{
		ShardID: int32(shard),
		Mode:    p.CreateWorkflowModeBrandNew,
		NewWorkflowSnapshot: p.InternalWorkflowSnapshot{
			NamespaceID: uuid.NewString(),
			WorkflowID:  "one-emitter",
			RunID:       run,
			ExecutionState: &persistencespb.WorkflowExecutionState{
				CreateRequestId: uuid.NewString(),
				RunId:           run,
				State:           enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
				Status:          enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			},
			DBRecordVersion: 1,
		},
	}
}

// baseFactory stands in for the abstract factory of the plugin holding the cold
// store: it hands back the same stores whatever service asks, which is what
// makes the handler the only thing that differs between the two NewFactory
// calls above.
type baseFactory struct{ base p.DataStoreFactory }

func (f baseFactory) NewFactory(
	config.CustomDatastoreConfig, resolver.ServiceResolver, string, log.Logger, metrics.Handler,
) p.DataStoreFactory {
	return f.base
}

// coldStores is the cold half in the shape intercept mode needs of it: a shard
// store whose rangeID lands, and an execution store answering the two
// pre-window reads with the truth about a run that does not exist. Every other
// method is promoted from a nil interface, so an intercepted call that fell
// through to the cold store panics here rather than being answered.
type coldStores struct{ p.DataStoreFactory }

func (coldStores) NewShardStore() (p.ShardStore, error)         { return landingShardStore{}, nil }
func (coldStores) NewExecutionStore() (p.ExecutionStore, error) { return emptyStore{}, nil }

type landingShardStore struct{ p.ShardStore }

func (landingShardStore) UpdateShard(context.Context, *p.InternalUpdateShardRequest) error {
	return nil
}

type emptyStore struct{ p.ExecutionStore }

func (emptyStore) GetWorkflowExecution(context.Context, *p.GetWorkflowExecutionRequest) (*p.InternalGetWorkflowExecutionResponse, error) {
	return nil, &serviceerror.NotFound{Message: "no such run"}
}

func (emptyStore) GetCurrentExecution(context.Context, *p.GetCurrentExecutionRequest) (*p.InternalGetCurrentExecutionResponse, error) {
	return nil, &serviceerror.NotFound{Message: "no current row"}
}

// GetCurrentExecutionWithLastWriteVersion is how the layer takes the
// current-row read ([baserow.Store]).
func (emptyStore) GetCurrentExecutionWithLastWriteVersion(
	context.Context, *p.GetCurrentExecutionRequest,
) (*p.InternalGetCurrentExecutionResponse, int64, error) {
	return nil, 0, &serviceerror.NotFound{Message: "no current row"}
}

// TestAShutdownThatLeavesATailSaysWhichShardsHoldIt is the report no other
// moment can give. The entries are in the log and a successor's replay is what
// they are for, so a node restarting into the same configuration needs nothing
// from this answer — but a node coming back without the `wal` section composes
// no log, cannot see that they exist, and will not replay them. A shutdown that
// answered nil is the only evidence that taking the layer out is safe.
func TestAShutdownThatLeavesATailSaysWhichShardsHoldIt(t *testing.T) {
	ctx := context.Background()
	const shard, epoch = wal.ShardID(5), wal.Epoch(2)

	layer, err := Compose(
		Backends{Log: memwal.New(), Cold: refusingCold{Cold: coldtest.New()}},
		cycle.Fixed(cycle.Defaults()),
		DefaultTaskCategories(),
		log.NewNoopLogger(), nil)
	require.NoError(t, err)

	require.NoError(t, layer.Options().Layer.ShardAcquired(ctx, shard, epoch))
	require.NoError(t, layer.Options().Layer.Write(ctx,
		mutation.Mutation{Create: aCreate(shard)}, epoch, baserow.New(emptyStore{})),
		"the write was not acked, so there is no tail for the shutdown to fail to empty")

	err = layer.Shutdown(ctx, time.Minute)
	require.Error(t, err, "the shutdown applied nothing and reported a clean stop")

	var undrained *UndrainedError
	require.ErrorAs(t, err, &undrained)
	require.Len(t, undrained.Shards, 1)
	left := undrained.Shards[0]
	require.Equal(t, shard, left.Shard)
	require.Equal(t, epoch, left.Epoch, "a residue that cannot name its epoch names no log position")
	require.Equal(t, 1, left.Entries)
	require.Error(t, left.Cause, "the drain's own answer is what tells a halt from a budget that ran out")
	require.ErrorContains(t, err, "will not replay them")
}

// TestAShutdownSeesATailNoRequestEverMadeItLookAt is the same report for the
// shard nothing asked about. A cycle replays lazily, on the first request that
// reaches it, so one installed by an acquire and then left alone has never read
// its watermark and never seen the log — and what it inherited is a dead owner's
// acked entries, which is the case the whole recovery path exists for.
//
// A shutdown that answered nil there is the dangerous answer rather than a
// merely incomplete one: the operations runbook removes the `wal` section once
// every node's shutdown has answered nil, and passthrough composes no log, so
// those entries are never replayed by anyone.
func TestAShutdownSeesATailNoRequestEverMadeItLookAt(t *testing.T) {
	ctx := context.Background()
	const shard, dead, epoch = wal.ShardID(5), wal.Epoch(1), wal.Epoch(2)

	// A previous owner's acked entry, left in the log by a node that died. Built
	// rather than hand-written: it has to survive the codec, which carries the
	// execution state as a blob and rebuilds it on the way out.
	wal1 := memwal.New()
	require.NoError(t, wal1.Fence(ctx, shard, dead))
	payload, err := mutation.Encode(
		mutbuild.For(int32(shard)).Create(uuid.NewString(), "one-emitter", uuid.NewString()))
	require.NoError(t, err)
	require.NoError(t, wal1.Append(ctx, shard, dead, wal.FirstSeqno, payload))

	layer, err := Compose(
		Backends{Log: wal1, Cold: refusingCold{Cold: coldtest.New()}},
		cycle.Fixed(cycle.Defaults()),
		DefaultTaskCategories(),
		log.NewNoopLogger(), nil)
	require.NoError(t, err)

	// The shard is taken and then nothing asks it anything.
	require.NoError(t, layer.Options().Layer.ShardAcquired(ctx, shard, epoch))

	err = layer.Shutdown(ctx, time.Minute)
	require.Error(t, err, "the log holds an acked entry no drain applied, and the shutdown called that clean")

	var undrained *UndrainedError
	require.ErrorAs(t, err, &undrained)
	require.Len(t, undrained.Shards, 1)
	require.Equal(t, shard, undrained.Shards[0].Shard)
}

// TestARetiredShardStillReportsWhatItHolds: RetireShard is the verb for staging
// what a process that died leaves behind, and the question straight after it is
// what the shard was holding. The cycle stays the shard's — nothing removes it
// from the registry — so the two narrow reads a caller has answer for it, and a
// stopped cycle has no loop to count with. Answering zero for the tail is the
// one number there that reads as a fact rather than as an absence, and it is
// exactly the fact that is false: the entries are in the log.
func TestARetiredShardStillReportsWhatItHolds(t *testing.T) {
	ctx := context.Background()
	const shard, epoch = wal.ShardID(9), wal.Epoch(2)

	layer, _ := composed(t, cycle.Defaults())
	require.NoError(t, layer.Options().Layer.ShardAcquired(ctx, shard, epoch))
	require.NoError(t, layer.Options().Layer.Write(ctx,
		mutation.Mutation{Create: aCreate(shard)}, epoch, baserow.New(emptyStore{})))

	before, ok := layer.ShardStats(shard)
	require.True(t, ok)
	require.Equal(t, 1, before.TailEntries)
	require.Equal(t, 1, layer.Totals().TailEntries)

	require.True(t, layer.RetireShard(shard))

	after, ok := layer.ShardStats(shard)
	require.True(t, ok, "the cycle is still the shard's")
	require.Equal(t, 1, after.TailEntries,
		"the acked entry is in the log whether or not a goroutine is left to say so")
	require.Equal(t, 1, layer.Totals().TailEntries)
}

// unreadableCold is a store whose watermark cannot be read, which is what one
// that is down looks like to a cycle that has never started: it has no floor to
// replay from, so it cannot say what its shard holds.
type unreadableCold struct{ *coldtest.Cold }

func (unreadableCold) Watermark(context.Context, wal.ShardID) (wal.Seqno, bool, error) {
	return 0, false, errors.New("unreadableCold: the watermark cannot be read")
}

// TestAShutdownThatCouldNotLookSaysSo is the other half of the rule that a
// shutdown does not call a shard clean it never looked at: the looking can fail.
// A start that cannot read the watermark leaves the tail at its floor, which is
// zero — the same zero a shard that drained everything reports — so a count is
// not what makes this a residue. The cause is.
func TestAShutdownThatCouldNotLookSaysSo(t *testing.T) {
	ctx := context.Background()
	const shard, epoch = wal.ShardID(7), wal.Epoch(2)

	layer, err := Compose(
		Backends{Log: memwal.New(), Cold: unreadableCold{Cold: coldtest.New()}},
		cycle.Fixed(cycle.Defaults()),
		DefaultTaskCategories(),
		log.NewNoopLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, layer.Options().Layer.ShardAcquired(ctx, shard, epoch))

	err = layer.Shutdown(ctx, time.Minute)
	var undrained *UndrainedError
	require.ErrorAs(t, err, &undrained, "the shutdown could not establish what the shard holds and called it clean")
	require.Len(t, undrained.Shards, 1)
	require.Equal(t, shard, undrained.Shards[0].Shard)
	require.Zero(t, undrained.Shards[0].Entries,
		"nothing was acked through this cycle: what is missing is the answer, not the entries")
	require.Error(t, undrained.Shards[0].Cause, "which is the whole of what makes it a residue")
}

// TestAResidueForAShardTakenAwayNamesTheFence: the shutdown looks at a shard
// nothing asked it about, and what it finds there may be a successor's. It
// reports a residue — this node cannot establish what the shard holds, and
// nothing that reports zero entries may be read as a clean one — and the cause
// is what makes the report actionable rather than a loop: no restart of this
// node will ever drain a shard it does not own, and the entries are the new
// owner's, whose own shutdown is where they appear.
func TestAResidueForAShardTakenAwayNamesTheFence(t *testing.T) {
	ctx := context.Background()
	const shard, mine, theirs = wal.ShardID(6), wal.Epoch(2), wal.Epoch(3)

	logs := memwal.New()
	layer, err := Compose(
		Backends{Log: logs, Cold: coldtest.New()},
		cycle.Fixed(cycle.Defaults()),
		DefaultTaskCategories(),
		log.NewNoopLogger(), nil)
	require.NoError(t, err)

	require.NoError(t, layer.Options().Layer.ShardAcquired(ctx, shard, mine))

	// Another node takes the shard and writes, while nothing has asked this one
	// anything — so its cycle has never looked at the log.
	require.NoError(t, logs.Fence(ctx, shard, theirs))
	payload, err := mutation.Encode(
		mutbuild.For(int32(shard)).Create(uuid.NewString(), "one-emitter", uuid.NewString()))
	require.NoError(t, err)
	require.NoError(t, logs.Append(ctx, shard, theirs, wal.FirstSeqno, payload))

	err = layer.Shutdown(ctx, time.Minute)
	var undrained *UndrainedError
	require.ErrorAs(t, err, &undrained)
	require.Len(t, undrained.Shards, 1)
	require.Equal(t, shard, undrained.Shards[0].Shard)
	require.ErrorContains(t, undrained.Shards[0].Cause, cycle.FencedAway,
		"the cause is what tells an operator to finish the new owner's shutdown rather than restart this one")
}

// TestAShutdownAfterACleanRetireIsQuiet is the other side of the rule above: a
// close that could not establish what its shard holds is a residue, and a cycle
// that was already retired is not that case. Its mirror is the established
// answer — which is why a residue reads it there — so a shutdown finding no loop
// left to ask has nothing open to report. Reporting it would tell an operator
// that removing the layer strands something, over a shard that drained.
func TestAShutdownAfterACleanRetireIsQuiet(t *testing.T) {
	ctx := context.Background()
	const shard, epoch = wal.ShardID(4), wal.Epoch(3)

	// Sync, so the write drains inside itself and the tail is empty after it.
	cfg := cycle.Defaults()
	cfg.Sync = true
	layer, cold := composed(t, cfg)

	require.NoError(t, layer.Options().Layer.ShardAcquired(ctx, shard, epoch))
	require.NoError(t, layer.Options().Layer.Write(ctx,
		mutation.Mutation{Create: aCreate(shard)}, epoch, baserow.New(emptyStore{})))
	require.Equal(t, 1, cold.Drains(), "the entry is in the cold store, so the shard holds nothing")

	require.True(t, layer.RetireShard(shard))
	require.NoError(t, layer.Shutdown(ctx, time.Minute))
}

// TestAShutdownWithoutABudgetIsRefused: zero is a deadline already past, so a
// caller reading it the way most of Go does would drain nothing and be handed
// every shard back as a residue — a report indistinguishable from a cold store
// that is down.
func TestAShutdownWithoutABudgetIsRefused(t *testing.T) {
	ctx := context.Background()
	layer, _ := composed(t, cycle.Defaults())
	t.Cleanup(func() { require.NoError(t, layer.Shutdown(ctx, time.Minute)) })

	for _, budget := range []time.Duration{0, -time.Second} {
		err := layer.Shutdown(ctx, budget)
		require.ErrorContains(t, err, "is not a budget")

		var undrained *UndrainedError
		require.NotErrorAs(t, err, &undrained,
			"a refused argument is not a shard holding a tail, and a caller type-switching must not read it as one")
	}
}

// refusingCold is a cold store whose drain never commits and whose watermark
// says no drain ever has, which is what a store that is down looks like from
// inside a shutdown: the window is gone, the entries stay acked, and the tail
// outlives the process.
type refusingCold struct{ *coldtest.Cold }

func (refusingCold) Apply(context.Context, wal.ShardID, wal.Epoch, fold.Batch) error {
	return errors.New("refusingCold: this drain does not commit")
}
