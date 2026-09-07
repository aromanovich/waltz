package window

// The type's own arithmetic, without a cycle around it.

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// TestTakeEmptiesTheWindowAndReportsIt is the one mutator's claim: a caller
// cannot empty the window without being handed the bytes the tail goes on
// holding.
func TestTakeEmptiesTheWindowAndReportsIt(t *testing.T) {
	var w Window
	require.True(t, w.Empty())

	w.Add(100, epoch)
	w.Add(40, epoch.Add(2*time.Second))
	mutations, bytes := w.Size()
	require.Equal(t, 2, mutations)
	require.Equal(t, 140, bytes)

	held := w.Take(epoch.Add(5 * time.Second))
	require.Equal(t, 5*time.Second, held.Age(), "the age is the oldest mutation's, not the newest")
	require.Equal(t, 140, held.Release(), "the bytes stay in the tail until the transaction commits")
	require.Zero(t, held.Release(), "and they are released once: a second settle subtracts nothing")

	require.True(t, w.Empty())
	mutations, bytes = w.Size()
	require.Zero(t, mutations)
	require.Zero(t, bytes)
}

// TestAnEmptyWindowHasNoAge: oldest means nothing while the window is empty, so
// a take reports zero rather than the distance from the zero time — which is
// the age a drain would otherwise emit.
func TestAnEmptyWindowHasNoAge(t *testing.T) {
	var w Window

	held := w.Take(epoch)
	require.Zero(t, held.Age())
	require.Zero(t, held.Release())

	require.False(t, w.Aged(epoch.Add(time.Hour), time.Second),
		"the age watermark drains a tail nothing is pushing on, and there is nothing here")

	// The age runs from the mutation that opened the window.
	w.Add(10, epoch.Add(time.Minute))
	require.True(t, w.Aged(epoch.Add(time.Minute+time.Second), time.Second))
}

// TestTheCountIsAnsweredBeforeTheBytes: a window over both watermarks reports
// the count, since the trigger is what carries the two apart downstream.
func TestTheCountIsAnsweredBeforeTheBytes(t *testing.T) {
	at := Watermarks{Mutations: 2, Bytes: 100}

	var w Window
	require.Equal(t, NoTrip, w.Trips(at))

	w.Add(500, epoch)
	require.Equal(t, TripBytes, w.Trips(at), "one mutation, over the byte watermark")

	w.Add(1, epoch)
	require.Equal(t, TripMutations, w.Trips(at), "over both, and the count is the answer")

	w.Take(epoch)
	require.Equal(t, NoTrip, w.Trips(at))
}

// TestAnEmptyWindowTripsNothing pins the reading of a zero watermark: "drain
// every write", which is a rule about a window something was folded into.
func TestAnEmptyWindowTripsNothing(t *testing.T) {
	var w Window
	require.Equal(t, NoTrip, w.Trips(Watermarks{}))

	w.Add(1, epoch)
	require.Equal(t, TripMutations, w.Trips(Watermarks{}))
}

// TestTheAgeWatermarkIsNotTheSizeWatermark is why the two are separate methods:
// replay consults the size rule and must not consult the age one.
func TestTheAgeWatermarkIsNotTheSizeWatermark(t *testing.T) {
	var w Window
	w.Add(10, epoch)

	require.True(t, w.Aged(epoch.Add(time.Minute), time.Second))
	require.Equal(t, NoTrip, w.Trips(Watermarks{Mutations: 256, Bytes: 1 << 20}))
}
