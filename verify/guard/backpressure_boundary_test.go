package guard

// Where backpressure meets the unforked server: the one assertion that cannot
// be made inside either package, because it is about both (#47, built in #56).
//
// The cycle raises the refusal and the wrapper carries it out, and neither may
// import the other — so this file, like cycle_wiring_test.go beside it, is
// where the composition is checked. Two claims, and the second is the reason
// the first matters:
//
//   - the value the layer refuses with is one the shard's write path calls
//     "definitely not committed": the shard stays loaded, its queues back off
//     and their tasks stay out of the DLQ (#47 measured all three in the pinned
//     server);
//   - it reaches that arm only while it is *unwrapped*.
//     ContextImpl.handleWriteErrorLocked switches on the concrete type
//     (service/history/shard/context_impl.go:1407), so a single
//     fmt.Errorf("...: %w", err) anywhere on the way out drops the refusal into
//     the switch's default arm — a background shard re-acquire, which is
//     backpressure converted into the failover it exists to prevent.
//
// The switch itself is unexported. Its list is not: persistence.
// OperationPossiblySucceeded is the same set of concrete types, exported, and
// the same decision — the server's own transaction code asks it exactly the
// question this test asks (service/history/workflow/transaction_impl.go:75).

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/mock"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"

	"github.com/aromanovich/waltz"
	"github.com/aromanovich/waltz/cycle"
	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/verify/basetest"
	"github.com/aromanovich/waltz/verify/coldtest"
	"github.com/aromanovich/waltz/wal"
	"github.com/aromanovich/waltz/wal/memwal"
	"github.com/aromanovich/waltz/wrapper"
)

func TestTheBackpressureRefusalIsDefinitelyNotCommitted(t *testing.T) {
	refusal := tripATail(t)

	// The type assertion, not errors.As: this is the shape the switch matches.
	_, ok := refusal.(*serviceerror.ResourceExhausted) //nolint:errorlint // the concrete type is the assertion
	require.True(t, ok, "the layer refuses with %T, which handleWriteErrorLocked would not recognise", refusal)
	require.False(t, p.OperationPossiblySucceeded(refusal),
		"a refused write must be one the shard knows did not happen")

	wrapped := fmt.Errorf("wal: appending to shard 3: %w", refusal)
	require.True(t, errors.As(wrapped, new(*serviceerror.ResourceExhausted)),
		"errors.As still matches, which is exactly why it is not what the shard uses")
	require.True(t, p.OperationPossiblySucceeded(wrapped),
		"one %w and the same refusal becomes an unknown outcome: a re-acquire, a new rangeID, a failover")
}

// TestTheWrapperCarriesTheRefusalOut is the other half, on the path the
// refusal actually travels: whatever the store below answers with, the store
// above answers with the same value — not a copy, not a wrapping of it. In
// passthrough mode the base store is the plugin; in intercept mode (#57) it is
// the cycle, and the obligation does not change.
func TestTheWrapperCarriesTheRefusalOut(t *testing.T) {
	refusal := tripATail(t)
	ctrl := gomock.NewController(t)
	base := mock.NewMockExecutionStore(ctrl)
	req := &p.InternalUpdateWorkflowExecutionRequest{ShardID: 3}
	base.EXPECT().UpdateWorkflowExecution(gomock.Any(), req).Return(refusal).Times(1)

	store, err := wrapper.NewExecutionStore(base, wrapper.Options{})
	require.NoError(t, err, "passthrough reads no pre-window row, so any store serves it")

	got := store.UpdateWorkflowExecution(context.Background(), req)
	require.True(t, got == refusal, //nolint:errorlint // identity is the assertion
		"the refusal must come out of the wrapper unwrapped, got %v", got)
}

// TestTheRefusalSurvivesTheWholeInterceptPath is that obligation on the path it
// will actually travel in production (#57): the binary's own composition, a real
// log, a real wrapper, and no mock anywhere between the tail and the caller.
// What it adds over the two tests above is the two boundaries they each stop
// short of — the cycle's answer becoming the registry's, and the registry's
// becoming the store's — either of which could wrap the value without any of the
// packages' own tests noticing.
//
// It goes through waltz.Compose and not a registry of its own. A log in memory
// and a refusing writer are what waltz.Backends is a parameter for, and a file
// that could not pass them would build a second registry — so the path under
// test would be the production path everywhere except at its root.
func TestTheRefusalSurvivesTheWholeInterceptPath(t *testing.T) {
	ctx := context.Background()
	const shard, epoch = wal.ShardID(3), wal.Epoch(7)

	cfg := cycle.Defaults()
	cfg.Sync = false
	cfg.Mutations, cfg.Bytes, cfg.Age = 1<<30, 1<<30, time.Hour
	cfg.HardMaxEntries = 1

	layer, err := waltz.Compose(
		waltz.Backends{Log: memwal.New(), Writer: noDrain(), Recoverer: coldtest.New()},
		cycle.Fixed(cfg),
		waltz.DefaultTaskCategories(),
		nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { layer.Shutdown(ctx, time.Minute) })

	// One options value builds both stores, as the factory does in production:
	// the registry is the acquire's observer and the write path at once, and
	// there is one field to say so.
	opts := layer.Options()

	// The acquire the wrapper's ShardStore reports, so the registry has a cycle
	// for the shard at the epoch the writes will carry.
	shards := wrapper.NewShardStore(passingShardStore{}, opts)
	require.NoError(t, shards.UpdateShard(ctx, &p.InternalUpdateShardRequest{
		ShardID: int32(shard), RangeID: int64(epoch), PreviousRangeID: int64(epoch) - 1,
	}))

	store, err := wrapper.NewExecutionStore(unreachableStore{t: t}, opts)
	require.NoError(t, err)

	first := aMutation(shard)
	first.Create.RangeID = int64(epoch)
	_, err = store.CreateWorkflowExecution(ctx, first.Create)
	require.NoError(t, err, "the first write fits a tail bound of one")

	second := aMutation(shard)
	second.Create.RangeID = int64(epoch)
	_, err = store.CreateWorkflowExecution(ctx, second.Create)

	_, ok := err.(*serviceerror.ResourceExhausted) //nolint:errorlint // the concrete type is the assertion
	require.True(t, ok, "the layer answered %T through the whole path, which the shard would not recognise", err)
	require.False(t, p.OperationPossiblySucceeded(err),
		"a refused write must be one the shard knows did not happen")

	// The base store above answers the condition authority's two reads and
	// fails every other call, which is the point: apart from that residual read
	// an intercepted write must not reach the store below, so a wrapper that
	// fell through fails by name rather than passing.
}

// unreachableStore is the base store this test's writes must not reach, in the
// only two senses left available.
//
// It used to be a plain `nil`, which said "must not reach" as loudly as Go can
// — until the condition authority (#74) started handing the layer the base
// store's own two reads. A method value taken on a nil interface panics *where
// it is taken*, so the test crashed on its first write, before the refusal it
// is about, and took the whole root test binary down with it — and with that,
// everything `make test-all` runs after this file (#105).
//
// So the fixture had to become a real store, and in becoming one it corrected
// the claim it was making. In a **windowed** cycle — which this test builds,
// since backpressure needs a tail — an intercepted write legitimately reads the
// pre-window current row through this store: that is fold.Delegated.Settle
// closing the residual, and it is skipped only in sync mode. NotFound is the
// honest answer here, because none of these workflows exists.
//
// Every other method fails the test by name, which is what the nil was for and
// is strictly better than a segfault: a fall-through now says which call fell
// through. The two reads are spelled out rather than left to the embedded
// interface for the reason above — a promoted method value on a nil embedded
// interface panics where it is taken.
type unreachableStore struct {
	p.ExecutionStore
	t *testing.T
}

func (u unreachableStore) GetWorkflowExecution(_ context.Context, _ *p.GetWorkflowExecutionRequest) (*p.InternalGetWorkflowExecutionResponse, error) {
	return nil, &serviceerror.NotFound{Message: "no such run"}
}

func (u unreachableStore) GetCurrentExecution(_ context.Context, _ *p.GetCurrentExecutionRequest) (*p.InternalGetCurrentExecutionResponse, error) {
	return nil, &serviceerror.NotFound{Message: "no current row"}
}

// GetCurrentExecutionWithLastWriteVersion is the shape the layer actually takes
// that read through (baserow.Store, #138): the response type
// cannot carry the row's last_write_version and the condition authority cannot
// refuse an assertion without it.
func (u unreachableStore) GetCurrentExecutionWithLastWriteVersion(
	_ context.Context, _ *p.GetCurrentExecutionRequest,
) (*p.InternalGetCurrentExecutionResponse, int64, error) {
	return nil, 0, &serviceerror.NotFound{Message: "no current row"}
}

// CreateWorkflowExecution is the one write the path could fall through to, and
// the one the nil base store was really guarding. The rest of the interface
// stays promoted from the embedded nil: nothing here can reach it without
// taking a method value first, which is the case this file already covers.
func (u unreachableStore) CreateWorkflowExecution(context.Context, *p.InternalCreateWorkflowExecutionRequest) (*p.InternalCreateWorkflowExecutionResponse, error) {
	u.t.Errorf("an intercepted write reached the base store's CreateWorkflowExecution")
	return nil, errors.New("unreachableStore: CreateWorkflowExecution")
}

// passingShardStore is the plugin's half of the acquire, in the only shape this
// test needs: the rangeID lands, and nothing else about a shard is asked.
type passingShardStore struct{ p.ShardStore }

func (passingShardStore) UpdateShard(context.Context, *p.InternalUpdateShardRequest) error {
	return nil
}

// tripATail returns the refusal a real cycle raises when its tail is full —
// the value under test is the layer's own, not a reconstruction of it here.
//
// It goes in through [cycle.Manager.Write] because that is the only door: the
// cycle's own append is unexported, so that I11's epoch check cannot be walked
// around by anyone holding a *Cycle. The refusal's identity survives that door —
// cycle's storeError hands back untouched every value
// persistence.OperationPossiblySucceeded recognises as definitely-not-committed,
// and TestTheBackpressureRefusalIsDefinitelyNotCommitted asserts that predicate
// is false for exactly this value. The same shape in-package is
// cycle/write_test.go's TestTheRefusalReachesTheBoundaryUntouched.
func tripATail(t *testing.T) error {
	t.Helper()
	ctx := context.Background()
	const shard, epoch = wal.ShardID(3), wal.Epoch(7)

	cfg := cycle.Defaults()
	cfg.Mutations, cfg.Bytes, cfg.Age = 1<<30, 1<<30, time.Hour
	cfg.HardMaxEntries = 1

	cold := noDrain()
	manager, err := cycle.NewManager(cycle.Deps{
		Log:       memwal.New(),
		Writer:    cold,
		Recoverer: cold,
		Registry:  tasks.NewDefaultTaskCategoryRegistry(),
	}, cycle.Fixed(cfg))
	require.NoError(t, err)
	// The acquire fences the log and installs the cycle, in that order.
	require.NoError(t, manager.ShardAcquired(ctx, shard, epoch))
	// Retire and not manager.Close: Close drains, and the cold store is refusing
	// to say no drain belongs in this test.
	t.Cleanup(func() { manager.Shard(shard).Retire() })

	// The condition authority's two reads over a cold store that holds nothing —
	// a fresh shard's. They are required and not optional ([cycle.ErrNoBaseRow]):
	// the wrapper hands the store's own over on every write, and a caller
	// bringing none would have its delegated assertions acked without anyone
	// evaluating them. The mutation below is a BrandNew create, whose
	// current-row assertion is "must not exist", so an empty store is what it
	// stands on.
	rows := basetest.New().Rows()
	require.NoError(t, manager.Write(ctx, aMutation(shard), epoch, rows))
	err = manager.Write(ctx, aMutation(shard), epoch, rows)
	require.Error(t, err, "the second write is past a tail bound of one")
	return err
}

// noDrain is the cycle's two collaborators in the smallest shape that gets a
// tail full: no drain ever runs, and the shard has never had a watermark.
func noDrain() *coldtest.Cold {
	return coldtest.Refusing(errors.New("no drain should have run: the window watermark is out of reach"))
}

func aMutation(shard wal.ShardID) mutation.Mutation {
	run := uuid.NewString()
	return mutation.Mutation{Create: &p.InternalCreateWorkflowExecutionRequest{
		ShardID: int32(shard),
		Mode:    p.CreateWorkflowModeBrandNew,
		NewWorkflowSnapshot: p.InternalWorkflowSnapshot{
			NamespaceID: uuid.NewString(),
			WorkflowID:  "backpressure",
			RunID:       run,
			ExecutionState: &persistencespb.WorkflowExecutionState{
				CreateRequestId: uuid.NewString(),
				RunId:           run,
				State:           enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
				Status:          enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			},
			DBRecordVersion: 1,
		},
	}}
}
