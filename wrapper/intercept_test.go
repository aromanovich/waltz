package wrapper

// Intercept mode's partition of the 28 ExecutionStore methods: eight writes go
// into the WAL, three reads are answered by the layer (two through the overlay,
// one through the task merge), one is refused, and the other sixteen transit.
// Both ways of getting it wrong are silent in a functional suite — a twelfth
// method taken into the layer, or one of the eleven left transiting past the
// accumulator — so the partition is driven by reflection over the whole
// interface and both sides are checked in the same loop.
//
// Nothing here needs a cluster: the store below is upstream's gomock mock and
// the layer below is a recorder.

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/common/metrics"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/mock"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"

	"github.com/aromanovich/waltz/baserow"
	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/wal"
)

// intercepted is the write half of the partition, written out rather than
// derived: every ExecutionStore method intercept mode takes into the WAL, and
// the record kind it becomes.
var intercepted = map[string]mutation.Kind{
	"CreateWorkflowExecution":          mutation.KindCreate,
	"UpdateWorkflowExecution":          mutation.KindUpdate,
	"ConflictResolveWorkflowExecution": mutation.KindConflictResolve,
	"SetWorkflowExecution":             mutation.KindSet,
	"DeleteWorkflowExecution":          mutation.KindDelete,
	"DeleteCurrentWorkflowExecution":   mutation.KindDeleteCurrent,
	"AddHistoryTasks":                  mutation.KindAddTasks,
	"RangeCompleteHistoryTasks":        mutation.KindRangeCompleteTasks,
}

// answered is the read half: the reads whose answer one of the writes can
// change, so the layer and not the store below decides what comes back. The
// first two go through the overlay, the third through the merge. A method in
// none of these three maps is asserted to transit.
var answered = map[string]bool{
	"GetWorkflowExecution": true,
	"GetCurrentExecution":  true,
	"GetHistoryTasks":      true,
}

// refused is the third part of the partition: the method intercept mode answers
// with an error rather than a result. It reaches neither the store below nor the
// layer. Emptying this map would put the single-key completion back to deleting
// rows the window has not written yet.
var refused = map[string]bool{
	"CompleteHistoryTask": true,
}

// recordingLayer is a fake layer that records what it was handed and answers
// with err. Its reads do not call the base thunk unless callBase says so.
//
// The embedded [ShardLayer] is left nil: it satisfies the halves this fake has
// no opinion about, and a call reaching one panics naming the method instead of
// passing silently as a no-op would. Explicitly declared methods shadow the
// embedded ones, so a wrong signature is still a compile error.
//
// Use is declared rather than left to the embedding, because handing the
// handler over is not a half a layer may decline: [ShardLayer] carries
// [MetricsSink] and NewFactory calls it on every composed layer.
type recordingLayer struct {
	ShardLayer

	got    []mutation.Mutation
	epochs []wal.Epoch
	err    error

	// reads names the store methods that reached the read path, and callBase
	// makes the layer use the closure it was handed.
	reads    []string
	callBase bool

	// baseRows is the pair of reads the write path was handed for the condition
	// authority's residual. Kept rather than called, so that a test can ask
	// where they go.
	baseRows *baserow.Rows
}

func (w *recordingLayer) Write(
	_ context.Context,
	m mutation.Mutation,
	epoch wal.Epoch,
	base *baserow.Rows,
) error {
	w.got = append(w.got, m)
	w.epochs = append(w.epochs, epoch)
	w.baseRows = base
	return w.err
}

func (w *recordingLayer) GetWorkflowExecution(
	ctx context.Context,
	_ *p.GetWorkflowExecutionRequest,
	base func(context.Context) (*p.InternalGetWorkflowExecutionResponse, error),
) (*p.InternalGetWorkflowExecutionResponse, error) {
	w.reads = append(w.reads, "GetWorkflowExecution")
	if w.callBase {
		return base(ctx)
	}
	return nil, w.err
}

func (w *recordingLayer) GetCurrentExecution(
	ctx context.Context,
	_ *p.GetCurrentExecutionRequest,
	base func(context.Context) (*p.InternalGetCurrentExecutionResponse, error),
) (*p.InternalGetCurrentExecutionResponse, error) {
	w.reads = append(w.reads, "GetCurrentExecution")
	if w.callBase {
		return base(ctx)
	}
	return nil, w.err
}

func (w *recordingLayer) GetHistoryTasks(
	ctx context.Context,
	req *p.GetHistoryTasksRequest,
	base func(context.Context, *p.GetHistoryTasksRequest) (*p.InternalGetHistoryTasksResponse, error),
) (*p.InternalGetHistoryTasksResponse, error) {
	w.reads = append(w.reads, "GetHistoryTasks")
	if w.callBase {
		return base(ctx, req)
	}
	return nil, w.err
}

var _ ShardLayer = (*recordingLayer)(nil)

// Use takes the handler and drops it; what the hand-off is asserted with are
// the two fakes in metrics_test.go, which override this.
func (w *recordingLayer) Use(metrics.Handler) {}

func TestInterceptModeTakesTheElevenAndOnlyTheEleven(t *testing.T) {
	iface := reflect.TypeFor[p.ExecutionStore]()
	require.Len(t, intercepted, 8, "the WAL's record format has eight shapes (invariant I1)")
	require.Len(t, answered, 3, "two mutable-state reads through the overlay, one task read through the merge")
	require.Len(t, refused, 1, "the single-key completion is the one method the record format has no shape for")

	for method := range iface.Methods() {
		t.Run(method.Name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			base := versioned(ctrl)
			layer := &recordingLayer{err: errors.New(method.Name + ": the layer's own error")}
			store := newStore(t, base, Options{Layer: layer})

			args := callArgs(method.Type)
			kind, isIntercepted := intercepted[method.Name]
			switch {
			case refused[method.Name]:
				// It reaches neither side. No expectation is set on the base, so
				// a call to it fails the controller naming it.
				got := reflect.ValueOf(store).MethodByName(method.Name).Call(args)
				assertOnlyTheLayersError(t, got, ErrCompleteHistoryTaskUnsupported, method.Name)
				require.Empty(t, layer.got, "%s must not reach the WAL", method.Name)
				require.Empty(t, layer.reads, "%s is not a read", method.Name)

			case answered[method.Name]:
				// The three. No expectation is set on the base and this layer
				// does not call the thunk, so a cold-store read fails the
				// controller: the wrapper reads nothing on its own account.
				got := reflect.ValueOf(store).MethodByName(method.Name).Call(args)
				require.Equal(t, []string{method.Name}, layer.reads,
					"%s must be answered by the layer", method.Name)
				require.Empty(t, layer.got, "%s is a read, not a write", method.Name)
				assertOnlyTheLayersError(t, got, layer.err, method.Name)

			case isIntercepted:
				// The eight. No expectation is set on the base at all, so any
				// call to it, the store's own method included, fails the
				// controller.
				got := reflect.ValueOf(store).MethodByName(method.Name).Call(args)
				require.Len(t, layer.got, 1, "%s must ack exactly one mutation", method.Name)
				require.Equal(t, kind, layer.got[0].Kind(),
					"%s built a %s record", method.Name, layer.got[0].Kind())
				require.Same(t, args[1].Interface(), request(layer.got[0]),
					"%s must hand down the caller's own request", method.Name)
				require.Empty(t, layer.reads, "%s is a write, not a read", method.Name)
				assertOnlyTheLayersError(t, got, layer.err, method.Name)

			default:
				// The other 16 must reach the same base method they reach in
				// passthrough and nothing else; gomock fails any other call.
				want := returnValues(t, ctrl, method.Name, method.Type)
				expectOnce(t, base, method.Name, args, want)
				got := reflect.ValueOf(store).MethodByName(method.Name).Call(args)
				require.Len(t, got, len(want))
				for j := range got {
					assertSame(t, want[j], got[j], method.Name)
				}
				require.Empty(t, layer.got,
					"%s must not reach the WAL: the record format has eight shapes and this is not one", method.Name)
				require.Empty(t, layer.reads,
					"%s must not reach the layer's read path: the covered set is three reads", method.Name)
			}
		})
	}
}

// assertOnlyTheLayersError checks that the call returned want by identity and
// nil for every other result. Condition failures, I10's refusal and the
// overlay's ShardOwnershipLost all travel this path and the shard type-switches
// on what arrives, so a wrapped error lands in its default arm.
func assertOnlyTheLayersError(t *testing.T, got []reflect.Value, want error, method string) {
	t.Helper()
	last := got[len(got)-1]
	require.True(t, last.Interface() == want, //nolint:errorlint // identity is the assertion
		"%s must return the layer's error unwrapped, got %v", method, last.Interface())
	for j := range len(got) - 1 {
		require.True(t, got[j].IsNil(), "%s must answer nothing beside a failure", method)
	}
}

// TestTheOverlayIsHandedTheStoresOwnRead: the closure the layer is given must
// reach the base method the caller asked for, with the caller's own request.
// In intercept mode these three do not call the base themselves, so the
// wrong-base-method mistake TestEveryMethodTransits catches elsewhere is
// invisible to it here.
func TestTheOverlayIsHandedTheStoresOwnRead(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	base := versioned(ctrl)
	layer := &recordingLayer{callBase: true}
	store := newStore(t, base, Options{Layer: layer})

	execReq := &p.GetWorkflowExecutionRequest{ShardID: 3, WorkflowID: "wf", RunID: "run"}
	execResp := &p.InternalGetWorkflowExecutionResponse{DBRecordVersion: 7}
	base.EXPECT().GetWorkflowExecution(gomock.Any(), execReq).Return(execResp, nil).Times(1)
	gotExec, err := store.GetWorkflowExecution(ctx, execReq)
	require.NoError(t, err)
	require.Same(t, execResp, gotExec)

	currReq := &p.GetCurrentExecutionRequest{ShardID: 3, WorkflowID: "wf"}
	currResp := &p.InternalGetCurrentExecutionResponse{RunID: "run"}
	base.EXPECT().GetCurrentExecution(gomock.Any(), currReq).Return(currResp, nil).Times(1)
	gotCurrent, err := store.GetCurrentExecution(ctx, currReq)
	require.NoError(t, err)
	require.Same(t, currResp, gotCurrent)

	// The task read's closure takes a request, because the merge asks the store
	// below for a different page than the caller asked for.
	taskReq := &p.GetHistoryTasksRequest{ShardID: 3, TaskCategory: tasks.CategoryTransfer, BatchSize: 10}
	taskResp := &p.InternalGetHistoryTasksResponse{NextPageToken: []byte("base's own")}
	base.EXPECT().GetHistoryTasks(gomock.Any(), taskReq).Return(taskResp, nil).Times(1)
	gotTasks, err := store.GetHistoryTasks(ctx, taskReq)
	require.NoError(t, err)
	require.Same(t, taskResp, gotTasks)

	require.Equal(t, int64(2), store.Counts().Overlaid)
	require.Equal(t, int64(1), store.Counts().TaskReads,
		"the task read is counted apart from the two mutable-state ones")
	require.Zero(t, store.Counts().Intercepted, "a read acks nothing")
}

// TestTheWritePathIsHandedTheStoresOwnReads: the condition authority verifies
// the assertions the window does not determine against the pre-window row, and
// the pair it is handed is the only way it can reach one. A pair from any store
// but this store's base would verify conditions against somebody else's row.
func TestTheWritePathIsHandedTheStoresOwnReads(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	base := versioned(ctrl)
	layer := &recordingLayer{}
	store := newStore(t, base, Options{Layer: layer})

	require.NoError(t, store.SetWorkflowExecution(ctx, &p.InternalSetWorkflowExecutionRequest{ShardID: 3}))
	require.NotNil(t, layer.baseRows, "the write path was handed no way to read a pre-window row")

	execReq := &p.GetWorkflowExecutionRequest{ShardID: 3, WorkflowID: "wf", RunID: "run"}
	execResp := &p.InternalGetWorkflowExecutionResponse{DBRecordVersion: 7}
	base.EXPECT().GetWorkflowExecution(gomock.Any(), execReq).Return(execResp, nil).Times(1)
	gotExec, err := layer.baseRows.Run(ctx, 3, "", "wf", "run")
	require.NoError(t, err)
	require.Same(t, execResp, gotExec)

	// The current row goes through the store's versioned read rather than the
	// one on p.ExecutionStore: without last_write_version the condition it
	// decides can be confirmed but never refused.
	base.currentResp, base.currentVersion = &p.InternalGetCurrentExecutionResponse{RunID: "run"}, 11
	gotCurrent, gotVersion, err := layer.baseRows.Current(ctx, 3, "", "wf")
	require.NoError(t, err)
	require.Same(t, base.currentResp, gotCurrent)
	require.EqualValues(t, 11, gotVersion, "the row's last_write_version reached the layer")
	require.Equal(t, &p.GetCurrentExecutionRequest{ShardID: 3, WorkflowID: "wf"}, base.currentReq,
		"and the shard the write named is the shard the read went to")

	require.Zero(t, store.Counts().Overlaid, "a read the write path makes is not a read of the overlay's")
}

// versionedStore is a base store that answers [baserow.Store], which a bare
// mock of p.ExecutionStore cannot, the versioned read not being on that
// interface.
type versionedStore struct {
	*mock.MockExecutionStore

	currentReq     *p.GetCurrentExecutionRequest
	currentResp    *p.InternalGetCurrentExecutionResponse
	currentVersion int64
}

func (s *versionedStore) GetCurrentExecutionWithLastWriteVersion(
	_ context.Context, req *p.GetCurrentExecutionRequest,
) (*p.InternalGetCurrentExecutionResponse, int64, error) {
	s.currentReq = req
	return s.currentResp, s.currentVersion, nil
}

// versioned is the base store intercept mode can be built over: upstream's mock
// with the versioned read added.
func versioned(ctrl *gomock.Controller) *versionedStore {
	return &versionedStore{MockExecutionStore: mock.NewMockExecutionStore(ctrl)}
}

// newStore is [NewExecutionStore] for a test that stages a store the mode can
// be served over. The one that does not is below.
func newStore(t *testing.T, base p.ExecutionStore, opts Options) *ExecutionStore {
	t.Helper()
	store, err := NewExecutionStore(base, opts)
	require.NoError(t, err)
	return store
}

// A store with no version-carrying current-row read cannot verify a delegated
// current-row assertion, so the store is not built. The refusal is returned
// rather than raised, and its caller is the factory the server is still inside:
// an incompatible store makes the binary fail at startup, not on a request path.
func TestAnUnpatchedCheckoutIsRefusedAtConstruction(t *testing.T) {
	ctrl := gomock.NewController(t)
	unpatched := mock.NewMockExecutionStore(ctrl)

	store, err := NewExecutionStore(unpatched, Options{Layer: &recordingLayer{}})
	require.ErrorIs(t, err, baserow.ErrNoVersionedRead)
	require.Nil(t, store)

	// Passthrough reads no pre-window row, so the same store serves it.
	transiting, err := NewExecutionStore(unpatched, Options{})
	require.NoError(t, err)
	require.NotNil(t, transiting)
}

// TestTheFactoryReturnsTheRefusalRatherThanRaisingIt: the whole point of the
// error above is the caller it reaches. NewExecutionStore is reached through
// Temporal's factory interface, which already returns one.
func TestTheFactoryReturnsTheRefusalRatherThanRaisingIt(t *testing.T) {
	ctrl := gomock.NewController(t)
	base := mock.NewMockDataStoreFactory(ctrl)
	base.EXPECT().NewExecutionStore().Return(mock.NewMockExecutionStore(ctrl), nil).Times(1)

	store, err := NewDataStoreFactory(base, Options{Layer: &recordingLayer{}}).NewExecutionStore()
	require.ErrorIs(t, err, baserow.ErrNoVersionedRead)
	// The interface value, not what it points at: require.Nil is reflective and
	// is satisfied by a non-nil p.ExecutionStore holding a nil *ExecutionStore,
	// which is a store whose first method call dereferences nil.
	require.True(t, store == nil, "the refusal came back as a non-nil interface over a nil store: %#v", store)
}

// request returns the one request a mutation holds, whatever its kind.
func request(m mutation.Mutation) any {
	switch m.Kind() {
	case mutation.KindCreate:
		return m.Create
	case mutation.KindUpdate:
		return m.Update
	case mutation.KindConflictResolve:
		return m.ConflictResolve
	case mutation.KindSet:
		return m.Set
	case mutation.KindDelete:
		return m.Delete
	case mutation.KindDeleteCurrent:
		return m.DeleteCurrent
	case mutation.KindAddTasks:
		return m.AddTasks
	case mutation.KindRangeCompleteTasks:
		return m.RangeCompleteTasks
	default:
		return nil
	}
}

// TestTheEpochTravelsWithTheWrite: the request's rangeID is the epoch
// (invariant I11) and travels with the mutation, so the layer can refuse a
// write from a shard context already fenced out. The two deletes carry no
// rangeID, and a zero epoch says so.
func TestTheEpochTravelsWithTheWrite(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	base := versioned(ctrl)
	writes := &recordingLayer{}
	store := newStore(t, base, Options{Layer: writes})

	_, err := store.CreateWorkflowExecution(ctx, &p.InternalCreateWorkflowExecutionRequest{ShardID: 3, RangeID: 11})
	require.NoError(t, err)
	require.NoError(t, store.UpdateWorkflowExecution(ctx, &p.InternalUpdateWorkflowExecutionRequest{ShardID: 3, RangeID: 12}))
	require.NoError(t, store.ConflictResolveWorkflowExecution(ctx, &p.InternalConflictResolveWorkflowExecutionRequest{ShardID: 3, RangeID: 13}))
	require.NoError(t, store.SetWorkflowExecution(ctx, &p.InternalSetWorkflowExecutionRequest{ShardID: 3, RangeID: 14}))
	require.NoError(t, store.DeleteWorkflowExecution(ctx, &p.DeleteWorkflowExecutionRequest{ShardID: 3}))
	require.NoError(t, store.DeleteCurrentWorkflowExecution(ctx, &p.DeleteCurrentWorkflowExecutionRequest{ShardID: 3}))

	require.Equal(t, []wal.Epoch{11, 12, 13, 14, 0, 0}, writes.epochs)
}

// TestTheEventsGoDownBeforeTheMutation: event history stays out of the WAL in
// v1 (decision D3), so a write's event blobs go to the cold store through
// AppendHistoryNodes before the mutation is acked. Otherwise the log holds a
// mutable state pointing at history nodes nobody wrote — an entry that is
// durable and correct, which is why no functional suite sees it. Every
// intercepted kind is driven, so a request shape whose events go nowhere fails
// by name; which slots each shape has is
// [mutation.Mutation.EventSlots]' own suite.
func TestTheEventsGoDownBeforeTheMutation(t *testing.T) {
	ctx := context.Background()
	current, reset, fresh := events("current"), events("reset"), events("new")
	batch := func(e ...*p.InternalAppendHistoryNodesRequest) []*p.InternalAppendHistoryNodesRequest {
		return e
	}

	cases := []struct {
		kind mutation.Kind
		call func(*ExecutionStore) error
		// want is every append the write must make, in order.
		want []*p.InternalAppendHistoryNodesRequest
	}{
		{
			kind: mutation.KindCreate,
			call: func(s *ExecutionStore) error {
				_, err := s.CreateWorkflowExecution(ctx, &p.InternalCreateWorkflowExecutionRequest{
					ShardID:              3,
					NewWorkflowNewEvents: batch(fresh),
				})
				return err
			},
			want: batch(fresh),
		},
		{
			kind: mutation.KindUpdate,
			call: func(s *ExecutionStore) error {
				return s.UpdateWorkflowExecution(ctx, &p.InternalUpdateWorkflowExecutionRequest{
					ShardID:                 3,
					UpdateWorkflowNewEvents: batch(current),
					NewWorkflowNewEvents:    batch(fresh),
				})
			},
			want: batch(current, fresh),
		},
		{
			kind: mutation.KindConflictResolve,
			call: func(s *ExecutionStore) error {
				return s.ConflictResolveWorkflowExecution(ctx, &p.InternalConflictResolveWorkflowExecutionRequest{
					ShardID:                        3,
					CurrentWorkflowEventsNewEvents: batch(current),
					ResetWorkflowEventsNewEvents:   batch(reset),
					NewWorkflowEventsNewEvents:     batch(fresh),
				})
			},
			want: batch(current, reset, fresh),
		},
		{
			kind: mutation.KindSet,
			call: func(s *ExecutionStore) error {
				return s.SetWorkflowExecution(ctx, &p.InternalSetWorkflowExecutionRequest{ShardID: 3})
			},
		},
		{
			kind: mutation.KindDelete,
			call: func(s *ExecutionStore) error {
				return s.DeleteWorkflowExecution(ctx, &p.DeleteWorkflowExecutionRequest{ShardID: 3})
			},
		},
		{
			kind: mutation.KindDeleteCurrent,
			call: func(s *ExecutionStore) error {
				return s.DeleteCurrentWorkflowExecution(ctx, &p.DeleteCurrentWorkflowExecutionRequest{ShardID: 3})
			},
		},
		{
			kind: mutation.KindAddTasks,
			call: func(s *ExecutionStore) error {
				return s.AddHistoryTasks(ctx, &p.InternalAddHistoryTasksRequest{ShardID: 3})
			},
		},
		{
			kind: mutation.KindRangeCompleteTasks,
			call: func(s *ExecutionStore) error {
				return s.RangeCompleteHistoryTasks(ctx, &p.RangeCompleteHistoryTasksRequest{ShardID: 3})
			},
		},
	}

	covered := map[mutation.Kind]bool{}
	for _, c := range cases {
		covered[c.kind] = true
		t.Run(c.kind.String(), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			base := versioned(ctrl)
			writes := &recordingLayer{}
			store := newStore(t, base, Options{Layer: writes})

			var appended []*p.InternalAppendHistoryNodesRequest
			base.EXPECT().AppendHistoryNodes(gomock.Any(), gomock.Any()).Times(len(c.want)).
				DoAndReturn(func(_ context.Context, r *p.InternalAppendHistoryNodesRequest) error {
					require.Empty(t, writes.got, "the mutation was acked before its events went down")
					appended = append(appended, r)
					return nil
				})

			require.NoError(t, c.call(store))
			require.Equal(t, c.want, appended)
			require.Len(t, writes.got, 1, "and then the mutation")
			require.Equal(t, c.kind, writes.got[0].Kind())
		})
	}

	for k := mutation.KindInvalid + 1; int(k) < mutation.KindCount; k++ {
		require.True(t, covered[k], "no write drives kind %s past the event slots", k)
	}
}

// TestAnUnwrittenEventIsAnUnackedMutation: if the events cannot be written the
// mutation must not be acked. An entry whose events are missing is a state the
// cold store can never be brought to, and replay would apply it forever.
func TestAnUnwrittenEventIsAnUnackedMutation(t *testing.T) {
	ctrl := gomock.NewController(t)
	base := versioned(ctrl)
	writes := &recordingLayer{}
	store := newStore(t, base, Options{Layer: writes})

	broken := errors.New("the history node did not land")
	base.EXPECT().AppendHistoryNodes(gomock.Any(), gomock.Any()).Return(broken).Times(1)

	_, err := store.CreateWorkflowExecution(context.Background(), &p.InternalCreateWorkflowExecutionRequest{
		ShardID:              3,
		NewWorkflowNewEvents: []*p.InternalAppendHistoryNodesRequest{events("new")},
	})
	require.True(t, err == broken, //nolint:errorlint // identity is the assertion
		"the base store's error must come out unwrapped, got %v", err)
	require.Empty(t, writes.got, "nothing may be acked once the events failed")
}

// TestACreateAnswersTheStoresOwnResponse: the response type has no fields, so
// what is pinned is a non-nil pointer beside a nil error. The ExecutionManager
// dereferences it.
func TestACreateAnswersTheStoresOwnResponse(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := newStore(t, versioned(ctrl), Options{Layer: &recordingLayer{}})

	resp, err := store.CreateWorkflowExecution(context.Background(),
		&p.InternalCreateWorkflowExecutionRequest{ShardID: 3})
	require.NoError(t, err)
	require.NotNil(t, resp)
}

// TestTheTaskPathIsCountedApartFromTheSix keeps a shard whose traffic is queue
// entries distinguishable from one writing workflows.
func TestTheTaskPathIsCountedApartFromTheSix(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	base := versioned(ctrl)
	store := newStore(t, base, Options{Layer: &recordingLayer{}})

	add := &p.InternalAddHistoryTasksRequest{ShardID: 3}
	require.NoError(t, store.AddHistoryTasks(ctx, add))
	require.NoError(t, store.AddHistoryTasks(ctx, add))
	require.NoError(t, store.RangeCompleteHistoryTasks(ctx, &p.RangeCompleteHistoryTasksRequest{ShardID: 3}))
	_, err := store.CreateWorkflowExecution(ctx, &p.InternalCreateWorkflowExecutionRequest{ShardID: 3})
	require.NoError(t, err)

	require.Equal(t, Counts{Intercepted: 1, TasksWritten: 2, TasksCompleted: 1}, store.Counts())
}

// TestPassthroughCountsNothingOnTheTaskPath: with no layer both calls reach
// their base methods and nothing is counted.
func TestPassthroughCountsNothingOnTheTaskPath(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	base := versioned(ctrl)
	store := newStore(t, base, Options{})

	add := &p.InternalAddHistoryTasksRequest{ShardID: 3}
	rangeReq := &p.RangeCompleteHistoryTasksRequest{ShardID: 3}
	base.EXPECT().AddHistoryTasks(gomock.Any(), add).Return(nil).Times(1)
	base.EXPECT().RangeCompleteHistoryTasks(gomock.Any(), rangeReq).Return(nil).Times(1)

	require.NoError(t, store.AddHistoryTasks(ctx, add))
	require.NoError(t, store.RangeCompleteHistoryTasks(ctx, rangeReq))
	require.Equal(t, Counts{}, store.Counts())
}

func events(id string) *p.InternalAppendHistoryNodesRequest {
	return &p.InternalAppendHistoryNodesRequest{
		Node: p.InternalHistoryNode{
			NodeID: 1,
			Events: &commonpb.DataBlob{Data: []byte(id)},
		},
	}
}
