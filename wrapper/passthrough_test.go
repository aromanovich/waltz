package wrapper

// The passthrough claim for all 44 hand-written forwarding methods (28 + 6 +
// 10), most of which no functional suite here touches. Each is driven by
// reflection against upstream's gomock mock with a single expectation on the
// method under test, so forwarding to the wrong base method or to none fails
// naming it. Arguments go in as matchers, so the base is asserted to see the
// caller's own request, and results are checked by identity: an error this path
// wrapped would stop being the sentinel the shard type-switches on.

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/mock"
	"go.uber.org/mock/gomock"
)

func TestEveryMethodTransits(t *testing.T) {
	cases := []struct {
		name  string
		iface reflect.Type
		build func(t *testing.T, ctrl *gomock.Controller) (base, wrapped any)
		// decorated names the methods whose first result is this layer's own
		// value rather than the base's: the two stores the factory wraps.
		decorated map[string]reflect.Type
	}{
		{
			name:  "ExecutionStore",
			iface: reflect.TypeFor[p.ExecutionStore](),
			build: func(t *testing.T, ctrl *gomock.Controller) (any, any) {
				base := mock.NewMockExecutionStore(ctrl)
				return base, newStore(t, base, Options{})
			},
		},
		{
			name:  "ShardStore",
			iface: reflect.TypeFor[p.ShardStore](),
			build: func(_ *testing.T, ctrl *gomock.Controller) (any, any) {
				base := mock.NewMockShardStore(ctrl)
				return base, NewShardStore(base, Options{})
			},
		},
		{
			name:  "DataStoreFactory",
			iface: reflect.TypeFor[p.DataStoreFactory](),
			build: func(_ *testing.T, ctrl *gomock.Controller) (any, any) {
				base := mock.NewMockDataStoreFactory(ctrl)
				return base, NewDataStoreFactory(base, Options{})
			},
			decorated: map[string]reflect.Type{
				"NewExecutionStore": reflect.TypeFor[*ExecutionStore](),
				"NewShardStore":     reflect.TypeFor[*ShardStore](),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.NotZero(t, tc.iface.NumMethod())
			for method := range tc.iface.Methods() {
				t.Run(method.Name, func(t *testing.T) {
					ctrl := gomock.NewController(t)
					base, wrapped := tc.build(t, ctrl)

					args := callArgs(method.Type)
					want := returnValues(t, ctrl, method.Name, method.Type)
					if _, ok := tc.decorated[method.Name]; ok {
						// A constructor that failed has no store to decorate;
						// TestAFailedConstructorIsNotDecorated has that half.
						want = succeeding(method.Type, want)
					}
					expectOnce(t, base, method.Name, args, want)

					got := reflect.ValueOf(wrapped).MethodByName(method.Name).Call(args)
					require.Len(t, got, len(want))
					for j := range got {
						if typ, ok := tc.decorated[method.Name]; ok && j == 0 {
							require.Equal(t, typ, reflect.TypeOf(got[j].Interface()),
								"%s must return this layer's store, not the base's", method.Name)
							require.Same(t, want[j].Interface(), decorated(got[j].Interface()),
								"%s must decorate the store the base factory built", method.Name)
							continue
						}
						assertSame(t, want[j], got[j], method.Name)
					}
				})
			}
		})
	}
}

// callArgs builds one argument per parameter. Pointers are allocated rather
// than left nil, so a wrapper reading a request field (UpdateShard does) is
// exercised rather than crashed; the context is a real one because gomock
// matches a nil interface by a different rule than everything else.
func callArgs(sig reflect.Type) []reflect.Value {
	args := make([]reflect.Value, sig.NumIn())
	for i := range args {
		typ := sig.In(i)
		switch {
		case typ == reflect.TypeFor[context.Context]():
			args[i] = reflect.ValueOf(context.Background())
		case typ.Kind() == reflect.Pointer:
			args[i] = reflect.New(typ.Elem())
		default:
			args[i] = reflect.Zero(typ)
		}
	}
	return args
}

// returnValues invents a distinguishable value per result, so that "the wrapper
// returned what the base returned" is a claim about identity rather than about
// two zero values agreeing. Store results are interfaces and come from
// upstream's mocks, which are tellable apart.
func returnValues(t *testing.T, ctrl *gomock.Controller, method string, sig reflect.Type) []reflect.Value {
	t.Helper()
	stores := map[reflect.Type]any{
		reflect.TypeFor[p.TaskStore]():            mock.NewMockTaskStore(ctrl),
		reflect.TypeFor[p.ShardStore]():           mock.NewMockShardStore(ctrl),
		reflect.TypeFor[p.MetadataStore]():        mock.NewMockMetadataStore(ctrl),
		reflect.TypeFor[p.ExecutionStore]():       mock.NewMockExecutionStore(ctrl),
		reflect.TypeFor[p.Queue]():                mock.NewMockQueue(ctrl),
		reflect.TypeFor[p.QueueV2]():              mock.NewMockQueueV2(ctrl),
		reflect.TypeFor[p.ClusterMetadataStore](): mock.NewMockClusterMetadataStore(ctrl),
		reflect.TypeFor[p.NexusEndpointStore]():   mock.NewMockNexusEndpointStore(ctrl),
		reflect.TypeFor[p.HistoryBranchUtil]():    &p.HistoryBranchUtilImpl{},
	}

	out := make([]reflect.Value, sig.NumOut())
	for i := range out {
		typ := sig.Out(i)
		switch {
		case typ == reflect.TypeFor[error]():
			out[i] = reflect.ValueOf(fmt.Errorf("%s: the base store's own error", method))
		case stores[typ] != nil:
			out[i] = reflect.ValueOf(stores[typ]).Convert(typ)
		case typ.Kind() == reflect.Pointer:
			out[i] = reflect.New(typ.Elem())
		case typ.Kind() == reflect.String:
			out[i] = reflect.ValueOf("transit:" + method)
		case typ.Kind() == reflect.Bool:
			out[i] = reflect.ValueOf(true)
		default:
			// A result type with no distinguishable value would turn this
			// into a comparison of two zero values, so it fails instead: a
			// new store on the factory belongs in the map above.
			t.Fatalf("%s returns a %s, which this test has no distinguishable value for", method, typ)
		}
	}
	return out
}

// succeeding blanks the error result, leaving every other value alone. Which
// one it is comes from the signature, not from the value's own type.
func succeeding(sig reflect.Type, out []reflect.Value) []reflect.Value {
	for i := range out {
		if sig.Out(i) == reflect.TypeFor[error]() {
			out[i] = reflect.Zero(sig.Out(i))
		}
	}
	return out
}

// decorated returns the base store one of this layer's stores wraps, or nil for
// anything else.
func decorated(v any) any {
	switch s := v.(type) {
	case *ExecutionStore:
		return s.base
	case *ShardStore:
		return s.base
	default:
		return nil
	}
}

// expectOnce sets the single expectation the method under test is allowed to
// satisfy. The arguments go in as plain values, which gomock compares with
// reflect.DeepEqual, so the base is asserted to see what the caller sent.
func expectOnce(t *testing.T, base any, method string, args, returns []reflect.Value) {
	t.Helper()
	recorder := reflect.ValueOf(base).MethodByName("EXPECT").Call(nil)[0]
	matchers := make([]reflect.Value, len(args))
	for i, arg := range args {
		matchers[i] = reflect.ValueOf(any(arg.Interface()))
	}
	call, ok := recorder.MethodByName(method).Call(matchers)[0].Interface().(*gomock.Call)
	require.True(t, ok, "%s: the mock recorder returned something other than a *gomock.Call", method)
	call.Times(1)
	if len(returns) > 0 {
		rets := make([]any, len(returns))
		for i, r := range returns {
			rets[i] = r.Interface()
		}
		call.Return(rets...)
	}
}

func assertSame(t *testing.T, want, got reflect.Value, method string) {
	t.Helper()
	w, g := want.Interface(), got.Interface()
	if err, ok := w.(error); ok {
		// Identity, not errors.Is: a wrapped error satisfies errors.Is and is
		// exactly what this path may not produce.
		require.True(t, g == any(err), //nolint:errorlint // identity is the assertion
			"%s must return the base store's error unwrapped, got %v", method, g)
		return
	}
	if want.Kind() == reflect.Pointer {
		require.Same(t, w, g, "%s must return the base store's value", method)
		return
	}
	require.Equal(t, w, g, "%s must return the base store's value", method)
}

// TestTheWholeSurfaceIsCovered guards the guard: enumerating the current
// interface would still pass if that interface shrank. The explicit counts are
// those of temporal v1.29.6, and a move in any of
// them is a change to what the wrapper owes.
func TestTheWholeSurfaceIsCovered(t *testing.T) {
	require.Equal(t, 28, reflect.TypeFor[p.ExecutionStore]().NumMethod(),
		"the ExecutionStore surface moved; the wrapper and #44's count both need rereading")
	require.Equal(t, 6, reflect.TypeFor[p.ShardStore]().NumMethod(),
		"the ShardStore surface moved; the wrapper and #44's count both need rereading")
	require.Equal(t, 10, reflect.TypeFor[p.DataStoreFactory]().NumMethod(),
		"the DataStoreFactory surface moved; a new store would be silently unwrapped")
}

// TestNoObserverIsNotAnObserver: with the zero Options the acquire branch must
// be inert rather than panicking on the nil layer at the first shard bump.
func TestNoObserverIsNotAnObserver(t *testing.T) {
	ctrl := gomock.NewController(t)
	base := mock.NewMockShardStore(ctrl)
	req := &p.InternalUpdateShardRequest{ShardID: 3, RangeID: 8, PreviousRangeID: 7}
	base.EXPECT().UpdateShard(gomock.Any(), req).Return(nil).Times(1)

	require.NoError(t, NewShardStore(base, Options{}).UpdateShard(context.Background(), req))
}

// TestAFailedConstructorIsNotDecorated: a store that could not be built comes
// back as a nil store and the base's own error. Decorating nil would turn a
// startup failure into a nil dereference on the first write.
func TestAFailedConstructorIsNotDecorated(t *testing.T) {
	ctrl := gomock.NewController(t)
	base := mock.NewMockDataStoreFactory(ctrl)
	broken := errors.New("no connection to the database")
	base.EXPECT().NewExecutionStore().Return(nil, broken).Times(1)
	base.EXPECT().NewShardStore().Return(nil, broken).Times(1)
	f := NewDataStoreFactory(base, Options{})

	// The interface value, not what it points at: require.Nil is reflective and
	// would take a non-nil interface holding a nil store, which is the shape
	// that dereferences nil at the first call.
	exec, err := f.NewExecutionStore()
	require.True(t, exec == nil, "a decorated nil store came back: %#v", exec)
	require.True(t, err == broken, "got %v", err) //nolint:errorlint // identity is the assertion

	shards, err := f.NewShardStore()
	require.True(t, shards == nil, "a decorated nil store came back: %#v", shards)
	require.True(t, err == broken, "got %v", err) //nolint:errorlint // identity is the assertion
}
