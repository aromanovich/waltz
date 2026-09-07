package cycle

// The counter seam: nothing in [Counters] may hold a number that must not be
// summed, and nothing in it may go unsummed by [Totals]. A counter dropped at
// that seam reads as a run that did nothing, which no other assertion notices.

import (
	"context"
	"reflect"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"

	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/verify/coldtasks"
	"github.com/aromanovich/waltz/wal"
)

// fillCounters gives every counter a distinct non-zero value, by reflection so
// a field added later is filled without touching this file. It fails the test
// on any field that is not an int or an array of ints.
func fillCounters(t *testing.T, c *Counters) {
	t.Helper()
	v := reflect.ValueOf(c).Elem()
	n := int64(0)
	next := func() int64 { n++; return n }
	for i := range v.NumField() {
		f, name := v.Field(i), v.Type().Field(i).Name
		switch {
		case f.Kind() == reflect.Int:
			f.SetInt(next())
		case f.Kind() == reflect.Array && f.Type().Elem().Kind() == reflect.Int:
			for j := range f.Len() {
				f.Index(j).SetInt(next())
			}
		default:
			// Positions may not live here: a new cycle inherits a log's
			// seqnos, so summing one counts the same entries once per
			// acquire.
			t.Fatalf("Counters.%s is a %s: every field here must be an int or an "+
				"array of ints, because that is what Counters.add can be total over "+
				"and what \"a count of something a cycle did\" means. If this is a "+
				"position rather than a count, it belongs on Stats or Totals.",
				name, f.Type())
		}
	}
}

// TestCountersAreSummableAndEveryOneIsSummed adds the same value twice: a merge
// written with `=` where it meant `+=` copies correctly onto an empty
// accumulator and loses every earlier epoch on a full one, which is the shape
// [Manager] uses it in.
func TestCountersAreSummableAndEveryOneIsSummed(t *testing.T) {
	var one Counters
	fillCounters(t, &one)

	var got Counters
	got.add(one)
	got.add(one)

	v, want := reflect.ValueOf(got), reflect.ValueOf(one)
	for i := range v.NumField() {
		name := v.Type().Field(i).Name
		f, w := v.Field(i), want.Field(i)
		if f.Kind() == reflect.Int {
			require.Equal(t, 2*w.Int(), f.Int(),
				"Counters.add does not sum %s: a counter it forgets is a counter that "+
					"reads zero in every witness, however busy the node was", name)
			continue
		}
		for j := range f.Len() {
			require.Equal(t, 2*w.Index(j).Int(), f.Index(j).Int(),
				"Counters.add does not sum %s[%d]", name, j)
		}
	}
}

// TestARetiredCyclesCountersReachTheNodesTotals pins that a shard changing
// hands leaves everything its cycle counted in the node's reading.
func TestARetiredCyclesCountersReachTheNodesTotals(t *testing.T) {
	ctx := context.Background()
	logs := newLog()
	m, err := NewManager(Deps{
		Log: logs, Writer: &fakeApplier{}, Recoverer: &fakeWatermark{}, Registry: testRegistry(),
	}, Fixed(func() Config {
		// A window nothing reaches, so the cycle counts and never drains.
		c := Defaults()
		c.Mutations, c.Bytes = 1<<20, 1<<30
		return c
	}()))
	require.NoError(t, err)
	t.Cleanup(func() { m.Close(ctx) })

	require.NoError(t, m.ShardAcquired(ctx, testShard, 4))
	first := m.Shard(testShard)

	ns := uuid.NewString()
	cold := coldtasks.New()
	require.NoError(t, first.write(ctx, mkTasks(ns, "wf", "run", 2, map[tasks.Category][]p.InternalHistoryTask{
		tasks.CategoryTransfer: {immediate(20)},
	}), rowsHolding("wf", "run", 1)))

	// The key both sources hold, which is what a collision is.

	cold.Hold(tasks.CategoryTransfer, immediate(20))
	minKey, maxKey := immediateRange()
	paginate(t, first, cold.Read, taskReq(tasks.CategoryTransfer, minKey, maxKey, 100))

	held := first.Stats()
	require.Equal(t, 1, held.TaskCollisions, "the fixture did not produce the collision it is here for")
	require.Equal(t, held.Counters, m.Totals().Counters, "a held cycle's counters are the node's")

	// The shard changes hands, which stops that cycle and its goroutine.
	require.NoError(t, m.ShardAcquired(ctx, testShard, 5))
	require.NotSame(t, first, m.Shard(testShard))

	total := m.Totals()
	require.Equal(t, held.Counters, total.Counters,
		"the retired cycle's counters left the node's reading with it")
	require.Equal(t, 1, total.TaskCollisions,
		"TaskCollisions is a count of something a cycle did and it did not survive the Totals seam (#152)")
	require.Equal(t, 2, total.Epochs)
	require.Equal(t, 1, total.Shards)
}

// TestATrimCommittingOnTheWayOutIsInTheNodesTotals: taking a superseded
// cycle's count and stopping it are one call, because as two they cannot both
// be right — before the stop the loop can still count, and after it there is no
// loop to ask, so a count taken first is a count taken early.
//
// The trim is where that gap is reproducible rather than merely racy: it is the
// one counter written off the loop, by the detached goroutine, and joined by
// [trim.Trimmer.Wait] — which [Cycle.Retire] calls after the loop is gone. A
// reading taken before the stop is therefore a trim behind, deterministically,
// and this fixture holds the trim exactly there: the fake log blocks until the
// cycle's own loop has closed `done`.
func TestATrimCommittingOnTheWayOutIsInTheNodesTotals(t *testing.T) {
	ctx := context.Background()
	logs := newLog()
	m, err := NewManager(Deps{
		Log: logs, Writer: &fakeApplier{}, Recoverer: &fakeWatermark{}, Registry: testRegistry(),
	}, Fixed(func() Config {
		// Drain on every write and trim on every drain, so one mutation is one
		// trim and the cadence needs no clock.
		c := Defaults()
		c.Sync, c.Mutations, c.TrimEvery = true, 1, 1
		return c
	}()))
	require.NoError(t, err)
	t.Cleanup(func() { m.Close(ctx) })

	require.NoError(t, m.ShardAcquired(ctx, testShard, 4))
	first := m.Shard(testShard)

	// The trim runs beside the loop and finishes only once the loop is gone, so
	// it commits inside Retire's Wait and after any reading taken before it.
	logs.OnTrim(func(int) error { <-first.done; return nil })

	require.NoError(t, first.write(ctx, mkCreate(ids()), coldRows()))
	require.Equal(t, 1, first.Stats().Trims, "the cadence did not fire, so there is no trim to lose")
	require.Zero(t, first.Stats().TrimsCommitted, "and it has not reached the log yet, which is the fixture")

	require.NoError(t, m.ShardAcquired(ctx, testShard, 5))
	require.NotSame(t, first, m.Shard(testShard))

	require.Equal(t, []wal.Seqno{1}, logs.Trims(), "the held trim did reach the log")
	require.Equal(t, 1, m.Totals().TrimsCommitted,
		"and what it did on the way out is in the node's reading, not dropped with the cycle")
}

// TestTotalsSumsEveryCounterOverEveryCycle is the same claim over the whole
// type: a retired cycle's block is filled by reflection, so a counter no
// fixture here drives is still asserted to arrive.
func TestTotalsSumsEveryCounterOverEveryCycle(t *testing.T) {
	ctx := context.Background()
	m, err := NewManager(Deps{
		Log: newLog(), Writer: &fakeApplier{}, Recoverer: &fakeWatermark{}, Registry: testRegistry(),
	}, Fixed(Defaults()))
	require.NoError(t, err)
	t.Cleanup(func() { m.Close(ctx) })

	var want Counters
	fillCounters(t, &want)
	m.held.retire(want)

	require.Equal(t, want, m.Totals().Counters)

	// A live cycle beside it: the two are added rather than one replacing the
	// other.
	require.NoError(t, m.ShardAcquired(ctx, testShard, 4))
	require.NoError(t, m.Shard(testShard).write(ctx, mkCreate(ids()), coldRows()))

	got := m.Totals()
	require.Equal(t, want.Reads, got.Reads, "a counter the live cycle never touched")
	require.Greater(t, got.Kinds[mutation.KindCreate], want.Kinds[mutation.KindCreate],
		"the live cycle's own count was not added to the retired one's")
}

// TestTheCountersAreNotAControlSurface pins that the merge stays unexported, so
// nothing outside this package can steer a number it came to read. An exported
// method on the embedded [Counters] would be promoted onto [Stats] and
// [Totals] alike.
func TestTheCountersAreNotAControlSurface(t *testing.T) {
	for _, v := range []any{Counters{}, Stats{}, Totals{}} {
		value := reflect.TypeOf(v)
		byValue := map[string]bool{}
		for method := range value.Methods() {
			byValue[method.Name] = true
		}
		// Only a pointer receiver can change the value it is called on, so the
		// difference between the two method sets is exactly the exported
		// mutators. A value-receiver String() or ratio is fine.
		ptr := reflect.PointerTo(value)
		for method := range ptr.Methods() {
			if name := method.Name; !byValue[name] {
				t.Fatalf("%s has an exported mutator %s: Totals is the witness's reading and "+
					"nobody's control surface, and an exported merge on the embedded Counters "+
					"is promoted onto both structs", value, name)
			}
		}
	}
}
