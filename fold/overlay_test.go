package fold_test

// The overlay: what a read is told while the window holds writes the cold store
// has not seen — the shape table, the version rule, the current-row order.

import (
	"maps"
	"reflect"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"

	"github.com/aromanovich/waltz/fold"
)

// baseRow builds the cold store's answer in its read's shape: blobs in every
// collection, the version on the response rather than on the state.
func baseRow(version int64, opts ...func(*p.InternalWorkflowMutableState)) *p.InternalGetWorkflowExecutionResponse {
	state := &p.InternalWorkflowMutableState{
		ActivityInfos:  map[int64]*commonpb.DataBlob{1: blob("base-activity-1")},
		TimerInfos:     map[string]*commonpb.DataBlob{"t": blob("base-timer")},
		ExecutionInfo:  blob("base-info"),
		ExecutionState: blob("base-state"),
		NextEventID:    5,
		Checksum:       blob("base-checksum"),
	}
	for _, opt := range opts {
		opt(state)
	}
	return &p.InternalGetWorkflowExecutionResponse{State: state, DBRecordVersion: version}
}

func baseBuffered(names ...string) func(*p.InternalWorkflowMutableState) {
	return func(s *p.InternalWorkflowMutableState) {
		for _, name := range names {
			s.BufferedEvents = append(s.BufferedEvents, blob(name))
		}
	}
}

func baseSignalRequested(ids ...string) func(*p.InternalWorkflowMutableState) {
	return func(s *p.InternalWorkflowMutableState) { s.SignalRequestedIDs = ids }
}

// render performs the read as the cycle does: base fetched only if needed.
func render(a *fold.Accumulator, run string, base *p.InternalGetWorkflowExecutionResponse) (
	*p.InternalGetWorkflowExecutionResponse, bool, fold.RunShape,
) {
	view := a.ViewRun(nsID, wfID, run)
	if !view.NeedsBase() {
		base = nil
	}
	resp, found := view.Render(base)
	return resp, found, view.Shape
}

func activity(t *testing.T, resp *p.InternalGetWorkflowExecutionResponse, id int64) string {
	t.Helper()
	b, ok := resp.State.ActivityInfos[id]
	require.True(t, ok, "activity %d is not in the answer", id)
	return string(b.Data)
}

// TestTheOverlayShapeTable covers one case per shape, and whether that shape
// consults the cold store's row.
func TestTheOverlayShapeTable(t *testing.T) {
	t.Run("nothing held is the base untouched", func(t *testing.T) {
		a := fold.New(shard)
		add(t, a, mkUpdate(runY, 3))

		base := baseRow(9)
		resp, found, shape := render(a, runX, base)
		require.Equal(t, fold.RunAbsent, shape)
		require.True(t, found)
		require.Same(t, base, resp, "an untouched base is the base itself, not a copy of it")
	})

	t.Run("a snapshot answers alone", func(t *testing.T) {
		a := fold.New(shard)
		add(t, a, mkCreate(runX, snapActivity(2, "window-activity-2")))

		// The plugin's snapshot path clears the run's collection tables before
		// writing its own, so merging the base in would answer with rows on
		// their way out.
		resp, found, shape := render(a, runX, baseRow(9))
		require.Equal(t, fold.RunSnapshot, shape)
		require.True(t, found)
		require.Equal(t, "window-activity-2", activity(t, resp, 2))
		require.NotContains(t, resp.State.ActivityInfos, int64(1),
			"the base's leftovers may not be merged into a snapshot's answer")
		require.Equal(t, "info-v1", string(resp.State.ExecutionInfo.Data))
	})

	t.Run("a delta is base merged with the window", func(t *testing.T) {
		a := fold.New(shard)
		add(t, a,
			mkUpdate(runX, 2, upsertActivity(2, "window-activity-2")),
			mkUpdate(runX, 3, deleteActivity(1)),
		)

		resp, found, shape := render(a, runX, baseRow(1))
		require.Equal(t, fold.RunDelta, shape)
		require.True(t, found)
		require.Equal(t, "window-activity-2", activity(t, resp, 2), "the window's upsert")
		require.NotContains(t, resp.State.ActivityInfos, int64(1), "the window's delete")
		require.Equal(t, "base-timer", string(resp.State.TimerInfos["t"].Data),
			"a collection the window never touched comes from the base")
		require.Equal(t, "info-v3", string(resp.State.ExecutionInfo.Data), "scalars come from the tail")
	})

	t.Run("a tombstone is NotFound whatever the base holds", func(t *testing.T) {
		a := fold.New(shard)
		add(t, a, mkDelete(runX))

		resp, found, shape := render(a, runX, baseRow(9))
		require.Equal(t, fold.RunTombstone, shape)
		require.False(t, found, "a deleted execution must read as deleted")
		require.Nil(t, resp)
	})

	t.Run("a create behind a tombstone answers alone", func(t *testing.T) {
		a := fold.New(shard)
		add(t, a, mkDelete(runX), mkCreate(runX, snapActivity(2, "second-life")))

		resp, found, shape := render(a, runX, baseRow(9))
		require.Equal(t, fold.RunSnapshot, shape)
		require.True(t, found)
		require.Equal(t, "second-life", activity(t, resp, 2))
		require.NotContains(t, resp.State.ActivityInfos, int64(1))
	})

	t.Run("the new run of a continue-as-new is a snapshot", func(t *testing.T) {
		a := fold.New(shard)
		m := mkUpdate(runX, 2)
		newRun := snapshot(runY, 1)
		newRun.ExecutionInfoBlob = blob("info-new-run")
		m.Update.NewWorkflowSnapshot = &newRun
		add(t, a, m)

		_, _, shape := render(a, runX, baseRow(1))
		require.Equal(t, fold.RunDelta, shape, "the continued run keeps its delta")

		resp, found, shape := render(a, runY, nil)
		require.Equal(t, fold.RunSnapshot, shape)
		require.True(t, found)
		require.Equal(t, "info-new-run", string(resp.State.ExecutionInfo.Data))
	})
}

// fullBaseRow is baseRow with every field of the cold store's answer set, so a
// field missing from a merged read is the overlay's doing and not the fixture's.
func fullBaseRow(version int64) *p.InternalGetWorkflowExecutionResponse {
	return baseRow(version,
		func(s *p.InternalWorkflowMutableState) {
			s.ChildExecutionInfos = map[int64]*commonpb.DataBlob{1: blob("base-child")}
			s.RequestCancelInfos = map[int64]*commonpb.DataBlob{1: blob("base-cancel")}
			s.SignalInfos = map[int64]*commonpb.DataBlob{1: blob("base-signal")}
			s.ChasmNodes = map[string]p.InternalChasmNode{"c": {Data: blob("base-chasm")}}
			s.DBRecordVersion = version
		},
		baseSignalRequested("base-signal-id"),
		baseBuffered("base-batch"),
	)
}

// fullSnapshot fills a window snapshot's collections, the counterpart of
// fullBaseRow on the arm that answers without the cold store.
func fullSnapshot(s *p.InternalWorkflowSnapshot) {
	s.ActivityInfos = map[int64]*commonpb.DataBlob{1: blob("window-activity")}
	s.TimerInfos = map[string]*commonpb.DataBlob{"t": blob("window-timer")}
	s.ChildExecutionInfos = map[int64]*commonpb.DataBlob{1: blob("window-child")}
	s.RequestCancelInfos = map[int64]*commonpb.DataBlob{1: blob("window-cancel")}
	s.SignalInfos = map[int64]*commonpb.DataBlob{1: blob("window-signal")}
	s.ChasmNodes = map[string]p.InternalChasmNode{"c": {Data: blob("window-chasm")}}
	s.SignalRequestedIDs = map[string]struct{}{"window-signal-id": {}}
}

func withChecksum(name string) func(*p.InternalWorkflowMutation) {
	return func(m *p.InternalWorkflowMutation) { m.Checksum = blob(name) }
}

// TestEveryFieldOfAReadAnswerIsFilled enumerates the answer off Temporal's own
// type, which is what makes a field added upstream fail here rather than come
// back zero. Two arms, because the answer is assembled twice over: a delta over
// the cold store's row, whose collections must survive snapshotOfBase, and a
// snapshot the window holds, which answers with no base at all.
//
// It claims a field is filled and not what with — which source each comes from
// is judged case by case in the tests around it. What a zero costs, and which
// five of the thirteen used to be droppable, are in .claude/rules/fold.md.
func TestEveryFieldOfAReadAnswerIsFilled(t *testing.T) {
	answer := reflect.TypeFor[p.InternalWorkflowMutableState]()
	require.NotZero(t, answer.NumField(), "the answer's type has no fields: this judges nothing")

	requireFilled := func(t *testing.T, state *p.InternalWorkflowMutableState, from string) {
		t.Helper()
		v := reflect.ValueOf(state).Elem()
		for f := range answer.Fields() {
			require.Falsef(t, v.FieldByIndex(f.Index).IsZero(),
				"%s is zero in a read answered from %s, and the fixture fills it: no arm of the "+
					"overlay carries it, so the caller is told the run does not have it and "+
					"writes the run back without it", f.Name, from)
		}
	}

	t.Run("a delta over the cold store's row", func(t *testing.T) {
		a := fold.New(shard)
		// Collections untouched, so what reaches the answer is the base's: a
		// delta that upserted into them would refill what snapshotOfBase dropped.
		add(t, a, mkUpdate(runX, 2, withChecksum("delta-checksum")))

		resp, found, shape := render(a, runX, fullBaseRow(1))
		require.True(t, found)
		require.Equal(t, fold.RunDelta, shape)
		requireFilled(t, resp.State, "the window's delta over the cold store's row")
	})

	t.Run("a snapshot the window holds", func(t *testing.T) {
		a := fold.New(shard)
		// The update rides the snapshot (I8) and is what puts a buffered batch
		// on the run; its scalars replace the create's, so both must be set.
		add(t, a,
			mkCreate(runX, fullSnapshot),
			mkUpdate(runX, 2, withBuffered("window-batch"), withChecksum("window-checksum")),
		)

		resp, found, shape := render(a, runX, nil)
		require.True(t, found)
		require.Equal(t, fold.RunSnapshot, shape)
		requireFilled(t, resp.State, "the window's own snapshot")
	})
}

// answerSources is every collection of a read answer beside the one value
// the fixtures put in it, so an assertion can say which *source* filled it and
// not merely that something did. Held to the type by the subtest below, since
// the whole point is that a collection nobody reads is a collection nobody
// notices going missing or arriving crossed with its neighbour.
var answerSources = []struct {
	field        string
	of           func(*p.InternalWorkflowMutableState) []string
	base, window string
}{
	{"ActivityInfos", func(s *p.InternalWorkflowMutableState) []string { return blobsOf(s.ActivityInfos) },
		"base-activity-1", "window-activity"},
	{"TimerInfos", func(s *p.InternalWorkflowMutableState) []string { return blobsOf(s.TimerInfos) },
		"base-timer", "window-timer"},
	{"ChildExecutionInfos", func(s *p.InternalWorkflowMutableState) []string { return blobsOf(s.ChildExecutionInfos) },
		"base-child", "window-child"},
	{"RequestCancelInfos", func(s *p.InternalWorkflowMutableState) []string { return blobsOf(s.RequestCancelInfos) },
		"base-cancel", "window-cancel"},
	{"SignalInfos", func(s *p.InternalWorkflowMutableState) []string { return blobsOf(s.SignalInfos) },
		"base-signal", "window-signal"},
	{"ChasmNodes", func(s *p.InternalWorkflowMutableState) []string {
		out := make([]string, 0, len(s.ChasmNodes))
		for _, n := range s.ChasmNodes {
			out = append(out, string(n.Data.Data))
		}
		slices.Sort(out)
		return out
	}, "base-chasm", "window-chasm"},
}

func blobsOf[K comparable](m map[K]*commonpb.DataBlob) []string {
	out := make([]string, 0, len(m))
	for _, b := range m {
		out = append(out, string(b.Data))
	}
	slices.Sort(out)
	return out
}

// TestEveryCollectionOfAReadAnswerComesFromItsOwnSource is the half
// [TestEveryFieldOfAReadAnswerIsFilled] states it does not cover. That one
// asserts a field is non-zero, which a field crossed with its neighbour
// satisfies: four of the six collections are map[int64]*DataBlob, so assigning
// any of them from any other compiles, fills both, and passes.
//
// The answer is assembled by three hand-filled mirrors — snapshotOfBase on the
// way in, copySnapshot for a window's own state, mutableStateOf on the way out
// — and each names all six by hand. Nine such crossings left the whole of
// `go test ./...` green, every collection but the activities in every mirror.
//
// What it costs is not a stale answer. The caller writes the run back from
// what it read, and a snapshot-bearing write clears the run's tables first —
// so a read that answers the signals as the activities has the next write put
// the signals in the activity table and delete the activities, both acked.
func TestEveryCollectionOfAReadAnswerComesFromItsOwnSource(t *testing.T) {
	t.Run("the table names every collection the answer carries", func(t *testing.T) {
		a := fold.New(shard)
		add(t, a, mkCreate(runX, fullSnapshot))
		resp, _, _ := render(a, runX, nil)

		named := make([]string, 0, len(answerSources))
		for _, c := range answerSources {
			named = append(named, c.field)
		}
		carried := slices.Sorted(maps.Keys(answerCollections(t, resp.State)))
		slices.Sort(named)
		require.Equal(t, carried, named,
			"a collection of the answer with no row here is one nothing says the source of")
	})

	t.Run("a delta over the cold store's row", func(t *testing.T) {
		a := fold.New(shard)
		// No collection upserts, so every collection in the answer is the base's,
		// carried across by snapshotOfBase.
		add(t, a, mkUpdate(runX, 2))

		resp, found, shape := render(a, runX, fullBaseRow(1))
		require.True(t, found)
		require.Equal(t, fold.RunDelta, shape)
		for _, c := range answerSources {
			require.Equalf(t, []string{c.base}, c.of(resp.State),
				"%s in a delta's answer must be the base row's own %s", c.field, c.field)
		}
	})

	t.Run("a snapshot the window holds", func(t *testing.T) {
		a := fold.New(shard)
		add(t, a, mkCreate(runX, fullSnapshot))

		resp, found, shape := render(a, runX, nil)
		require.True(t, found)
		require.Equal(t, fold.RunSnapshot, shape)
		for _, c := range answerSources {
			require.Equalf(t, []string{c.window}, c.of(resp.State),
				"%s in a snapshot's answer must be the window's own %s", c.field, c.field)
		}
	})
}

// TestOnlyADeltaAsksTheColdStore: only a delta and an unheld run need a base.
func TestOnlyADeltaAsksTheColdStore(t *testing.T) {
	a := fold.New(shard)
	add(t, a, mkCreate(runX), mkDelete(runY))

	require.False(t, a.ViewRun(nsID, wfID, runX).NeedsBase(), "a snapshot answers alone")
	require.False(t, a.ViewRun(nsID, wfID, runY).NeedsBase(), "a tombstone answers alone")
	require.True(t, a.ViewRun(nsID, wfID, "run-z").NeedsBase(), "an unheld run is the base")

	b := fold.New(shard)
	add(t, b, mkUpdate(runX, 2))
	require.True(t, b.ViewRun(nsID, wfID, runX).NeedsBase(), "a delta needs something to fold onto")
}

// TestTheOverlayReturnsTheTailsVersion: a read answers with the version the
// window will write. The caller's next conditional write asserts what it read,
// and the base's older version would fail that condition.
func TestTheOverlayReturnsTheTailsVersion(t *testing.T) {
	t.Run("a delta writes the tail's", func(t *testing.T) {
		a := fold.New(shard)
		add(t, a, mkUpdate(runX, 2), mkUpdate(runX, 3), mkUpdate(runX, 4))

		resp, _, _ := render(a, runX, baseRow(1))
		require.Equal(t, int64(4), resp.DBRecordVersion, "the version the window will write, not the base's 1")
		require.Equal(t, int64(4), resp.State.DBRecordVersion, "and the same value in both fields")
	})

	t.Run("a snapshot writes its own", func(t *testing.T) {
		a := fold.New(shard)
		add(t, a, mkSet(runX, 6))

		resp, _, _ := render(a, runX, baseRow(1))
		require.Equal(t, int64(6), resp.DBRecordVersion)
		require.Equal(t, int64(6), resp.State.DBRecordVersion)
	})

	t.Run("an unheld run keeps the base's", func(t *testing.T) {
		a := fold.New(shard)
		resp, _, _ := render(a, runX, baseRow(9))
		require.Equal(t, int64(9), resp.DBRecordVersion)
	})
}

// answerCollections is every map a read's answer carries, enumerated off
// Temporal's own type so that the two rules below reach a collection added
// upstream without an edit. Both are about maps the answer hands out, and a map
// the fixture left nil is one neither rule reaches through.
func answerCollections(t *testing.T, state *p.InternalWorkflowMutableState) map[string]reflect.Value {
	t.Helper()
	out, v := map[string]reflect.Value{}, reflect.ValueOf(state).Elem()
	for f := range reflect.TypeFor[p.InternalWorkflowMutableState]().Fields() {
		if m := v.FieldByIndex(f.Index); m.Kind() == reflect.Map {
			require.Falsef(t, m.IsNil(), "%s is nil in the answer, so nothing here reaches through it", f.Name)
			out[f.Name] = m
		}
	}
	require.NotEmptyf(t, out, "the answer carries no map: this has judged nothing")
	return out
}

// emptyEveryCollection writes into every map the answer carries, as a caller
// free to do what it likes with what it was handed would.
func emptyEveryCollection(t *testing.T, state *p.InternalWorkflowMutableState) {
	t.Helper()
	for _, m := range answerCollections(t, state) {
		for _, k := range m.MapKeys() {
			m.SetMapIndex(k, reflect.Value{})
		}
	}
}

func collectionSizes(t *testing.T, state *p.InternalWorkflowMutableState) map[string]int {
	t.Helper()
	out := map[string]int{}
	for name, m := range answerCollections(t, state) {
		out[name] = m.Len()
	}
	return out
}

// TestTheOverlayIsReadOnlyOnTheAccumulator: the drain after any number of reads
// equals the drain without them. An answer holding the accumulator's own map
// would let a later fold rewrite a response already returned.
//
// The write into the answer is every collection it carries and not a chosen one:
// copySnapshot copies the snapshot whole and then clones its maps one line per
// map, so a map added upstream is shared until somebody adds the line.
func TestTheOverlayIsReadOnlyOnTheAccumulator(t *testing.T) {
	build := func() *fold.Accumulator {
		a := fold.New(shard)
		add(t, a,
			mkCreate(runX, fullSnapshot, snapTask("task-create")),
			mkUpdate(runY, 2, upsertActivity(7, "updated"), withTask("task-update"), withBuffered("batch")),
		)
		return a
	}

	quiet := reqs(build().Drain())

	a := build()
	snapshotResp, _, _ := render(a, runX, nil)
	deltaResp, _, _ := render(a, runY, fullBaseRow(1))
	emptyEveryCollection(t, snapshotResp.State)
	emptyEveryCollection(t, deltaResp.State)
	deltaResp.State.BufferedEvents = append(deltaResp.State.BufferedEvents, blob("extra"))
	noisy := reqs(a.Drain())

	require.Equal(t, quiet, noisy, "a window that was read is the window that was not")
}

// TestTheOverlayDoesNotWriteThroughTheBase is the same rule for the caller's
// base row, which belongs to whoever answered the thunk. Two moments, and the
// second is the one snapshotOfBase answers for: the fold must not reach the row
// while it renders, and what it hands back must not be the row's own maps —
// there too the copy is a line per collection.
func TestTheOverlayDoesNotWriteThroughTheBase(t *testing.T) {
	a := fold.New(shard)
	add(t, a, mkUpdate(runX, 2, upsertActivity(2, "window"), deleteActivity(1)))

	base := fullBaseRow(1)
	held := collectionSizes(t, base.State)

	resp, _, _ := render(a, runX, base)
	require.Equal(t, "window", activity(t, resp, 2))

	require.Len(t, base.State.ActivityInfos, held["ActivityInfos"], "the base row gained the window's upsert")
	require.Contains(t, base.State.ActivityInfos, int64(1), "the base row lost a key to the window's delete")
	require.Len(t, base.State.BufferedEvents, 1, "the base's batches were appended to")

	emptyEveryCollection(t, resp.State)
	require.Equal(t, held, collectionSizes(t, base.State),
		"a collection of the answer is the base row's own map, so a caller writing into what it "+
			"was handed empties the row it still holds")
}

// TestTheOverlaysBufferedEvents: the store's batches first, then the window's;
// the store's are dropped by a clear or by a snapshot.
func TestTheOverlaysBufferedEvents(t *testing.T) {
	t.Run("the store's, then the window's", func(t *testing.T) {
		a := fold.New(shard)
		add(t, a, mkUpdate(runX, 2, withBuffered("window-1")), mkUpdate(runX, 3, withBuffered("window-2")))

		resp, _, _ := render(a, runX, baseRow(1, baseBuffered("store-1")))
		require.Equal(t, []string{"store-1", "window-1", "window-2"}, blobNames(resp.State.BufferedEvents))
	})

	t.Run("a clear drops the store's", func(t *testing.T) {
		a := fold.New(shard)
		add(t, a,
			mkUpdate(runX, 2, withBuffered("dropped")),
			mkUpdate(runX, 3, withClearBuffered()),
			mkUpdate(runX, 4, withBuffered("kept")),
		)

		resp, _, _ := render(a, runX, baseRow(1, baseBuffered("store-1")))
		require.Equal(t, []string{"kept"}, blobNames(resp.State.BufferedEvents),
			"the clear drops the store's rows and the batches accumulated before it")
	})

	t.Run("a snapshot drops the store's without being asked", func(t *testing.T) {
		a := fold.New(shard)
		add(t, a, mkUpdate(runX, 2, withBuffered("dropped")), mkSet(runX, 3))

		resp, _, _ := render(a, runX, nil)
		require.Empty(t, resp.State.BufferedEvents,
			"a snapshot replaces the run's rows wholesale, buffered events included")
	})
}

func blobNames(blobs []*commonpb.DataBlob) []string {
	var names []string
	for _, b := range blobs {
		names = append(names, string(b.Data))
	}
	return names
}

// TestTheOverlayMergesSignalRequestedIDs: the one collection the cold store
// returns as a list and the window holds as a set. The merge is ordered.
func TestTheOverlayMergesSignalRequestedIDs(t *testing.T) {
	a := fold.New(shard)
	add(t, a, mkUpdate(runX, 2, func(m *p.InternalWorkflowMutation) {
		m.UpsertSignalRequestedIDs = map[string]struct{}{"c": {}}
		m.DeleteSignalRequestedIDs = map[string]struct{}{"a": {}}
	}))

	resp, _, _ := render(a, runX, baseRow(1, baseSignalRequested("a", "b")))
	require.Equal(t, []string{"b", "c"}, resp.State.SignalRequestedIDs,
		"the base's set less the window's deletes, plus its upserts, in a deterministic order")
}

// ---------------------------------------------------------------------------
// The current-execution row is written by the window's last writer rather than
// by the merged request, so "the run's state is in the tail" and "the current
// row is in the tail" are different questions.
// ---------------------------------------------------------------------------

// withRealState gives a snapshot an execution state that survives a round trip:
// the current-row answer is the one read that decodes.
func withRealState(t *testing.T, run string) func(*p.InternalWorkflowSnapshot) {
	return func(s *p.InternalWorkflowSnapshot) {
		t.Helper()
		st := realState(t, run, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING)
		b, err := serialization.WorkflowExecutionStateToBlob(st)
		require.NoError(t, err)
		s.ExecutionState, s.ExecutionStateBlob = st, b
	}
}

func currentRow(run string) *p.InternalGetCurrentExecutionResponse {
	return &p.InternalGetCurrentExecutionResponse{
		RunID:          run,
		ExecutionState: &persistencespb.WorkflowExecutionState{RunId: run},
	}
}

// completedCurrentRow builds the row a start over a reused workflow ID asserts
// on: the previous run, finished ([fold.DelegatedCurrent.Verify]).
func completedCurrentRow(run string) *p.InternalGetCurrentExecutionResponse {
	return &p.InternalGetCurrentExecutionResponse{
		RunID: run,
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId: run,
			State: enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		},
	}
}

// renderCurrent is the current-execution read as the cycle performs it.
func renderCurrent(t *testing.T, a *fold.Accumulator, base *p.InternalGetCurrentExecutionResponse) (
	*p.InternalGetCurrentExecutionResponse, bool, fold.CurrentShape,
) {
	t.Helper()
	view := a.ViewCurrent(nsID, wfID)
	if !view.NeedsBase() {
		base = nil
	}
	resp, found, err := view.Render(base)
	require.NoError(t, err)
	return resp, found, view.Shape
}

// TestTheCurrentRowIsAnsweredInOrder: each rule is reached only because the
// earlier ones did not fire, and swapping any two changes an answer.
func TestTheCurrentRowIsAnsweredInOrder(t *testing.T) {
	t.Run("removed wins over everything", func(t *testing.T) {
		a := fold.New(shard)
		add(t, a, mkCreate(runX, withRealState(t, runX)), mkDeleteCurrent(runX))

		resp, found, shape := renderCurrent(t, a, currentRow(runX))
		require.Equal(t, fold.CurrentGone, shape)
		require.False(t, found, "the window wrote the row and then removed it")
		require.Nil(t, resp)
	})

	t.Run("the window's own write, decoded", func(t *testing.T) {
		a := fold.New(shard)
		add(t, a, mkCreate(runX, withRealState(t, runX)))

		resp, found, shape := renderCurrent(t, a, currentRow(runY))
		require.Equal(t, fold.CurrentWritten, shape)
		require.True(t, found)
		require.Equal(t, runX, resp.RunID, "not the base's run")
		require.Equal(t, runX, resp.ExecutionState.RunId)
		require.Equal(t, "request-"+runX, resp.ExecutionState.CreateRequestId,
			"the row's content as the sequential path would have left it")
	})

	t.Run("a write outranks a delete of another run", func(t *testing.T) {
		a := fold.New(shard)
		add(t, a, mkCreate(runX, withRealState(t, runX)), mkDeleteCurrent(runY))

		resp, found, shape := renderCurrent(t, a, currentRow(runY))
		require.Equal(t, fold.CurrentWritten, shape,
			"a delete-current naming another run is a sequential no-op, so the window's write stands")
		require.True(t, found)
		require.Equal(t, runX, resp.RunID)
	})

	t.Run("a guard is evaluated against the base", func(t *testing.T) {
		a := fold.New(shard)
		add(t, a, mkDeleteCurrent(runX))

		_, found, shape := renderCurrent(t, a, currentRow(runX))
		require.Equal(t, fold.CurrentGuarded, shape)
		require.False(t, found, "the base names the deleted run, so the row goes")

		resp, found, _ := renderCurrent(t, a, currentRow(runY))
		require.True(t, found, "the base names another run: the store's delete would have done nothing")
		require.Equal(t, runY, resp.RunID)
	})

	t.Run("two guards, either of which removes the row", func(t *testing.T) {
		// Sequentially the second delete sees whatever the first left, so the
		// row is gone if the base names either run.
		a := fold.New(shard)
		add(t, a, mkDeleteCurrent(runX), mkDeleteCurrent(runY))

		_, foundX, _ := renderCurrent(t, a, currentRow(runX))
		_, foundY, _ := renderCurrent(t, a, currentRow(runY))
		_, foundZ, _ := renderCurrent(t, a, currentRow("run-z"))
		require.False(t, foundX)
		require.False(t, foundY)
		require.True(t, foundZ)
	})

	t.Run("a window that never touched the row", func(t *testing.T) {
		a := fold.New(shard)
		// BypassCurrent is the store's mode for writing no current row.
		m := mkUpdate(runX, 2)
		m.Update.Mode = p.UpdateWorkflowModeBypassCurrent
		add(t, a, m)

		base := currentRow(runX)
		resp, found, shape := renderCurrent(t, a, base)
		require.Equal(t, fold.CurrentUnheld, shape)
		require.True(t, found)
		require.Same(t, base, resp)
	})

	t.Run("a workflow the window does not hold at all", func(t *testing.T) {
		a := fold.New(shard)
		base := currentRow(runX)
		resp, found, shape := renderCurrent(t, a, base)
		require.Equal(t, fold.CurrentUnheld, shape)
		require.True(t, found)
		require.Same(t, base, resp)
	})
}

// TestOnlyAGuardedOrUnheldCurrentRowAsksTheColdStore: written and gone are
// answered from the window alone, guarded and unheld need the base.
func TestOnlyAGuardedOrUnheldCurrentRowAsksTheColdStore(t *testing.T) {
	written := fold.New(shard)
	add(t, written, mkCreate(runX, withRealState(t, runX)))
	require.False(t, written.ViewCurrent(nsID, wfID).NeedsBase())

	gone := fold.New(shard)
	add(t, gone, mkCreate(runX, withRealState(t, runX)), mkDeleteCurrent(runX))
	require.False(t, gone.ViewCurrent(nsID, wfID).NeedsBase())

	guarded := fold.New(shard)
	add(t, guarded, mkDeleteCurrent(runX))
	require.True(t, guarded.ViewCurrent(nsID, wfID).NeedsBase())

	require.True(t, fold.New(shard).ViewCurrent(nsID, wfID).NeedsBase())
}
