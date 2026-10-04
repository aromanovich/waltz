// Package window tracks the size and age of what a cycle has folded since its
// last drain. It is a package so that emptying it outside [Window.Take] does
// not compile.
//
// Its bytes are not the tail's: the window empties when a drain starts, the
// tail when that drain's transaction resolves. Nothing here emits metrics;
// the drain does, once it has an outcome.
//
// It may not name wal.Log or wal.Entry: the window bounds a drain, it does
// not read the log.
package window

import "time"

// Window is what a cycle has folded and not yet drained. Owned by the loop;
// the zero value is empty.
type Window struct {
	mutations int
	// bytes sums encoded payload lengths, the same unit the tail counts.
	bytes int
	// oldest is when the first mutation landed; meaningless while empty.
	oldest time.Time
}

// Add records one folded mutation of encoded payload length size. now is used
// only when the window was empty.
func (w *Window) Add(size int, now time.Time) {
	if w.Empty() {
		w.oldest = now
	}
	w.mutations++
	w.bytes += size
}

// Size returns the window's mutation count and bytes, for reports.
func (w *Window) Size() (mutations, bytes int) { return w.mutations, w.bytes }

// Empty reports whether nothing has been folded since the last take.
func (w *Window) Empty() bool { return w.mutations == 0 }

// Taken is what one take handed over. Its bytes cannot be written as a plain
// number, so the tail only releases bytes a window produced, and only once
// ([Taken.Release]). A double settle therefore cannot drive the tail below
// zero, where it would never trip I10 again.
//
// Leaving a Taken unsettled is legal: a halt drops its window, since the
// entries are acked and the cycle is finished.
type Taken struct {
	bytes int
	age   time.Duration
}

// Age is how long the oldest mutation had waited when the window was taken;
// zero for an empty window. It may be read any number of times.
func (t Taken) Age() time.Duration { return t.age }

// Release returns the bytes once; later calls return 0.
func (t *Taken) Release() int {
	bytes := t.bytes
	t.bytes = 0
	return bytes
}

// Take empties the window and returns its bytes, which the tail holds until
// the drain resolves, and the age of its oldest mutation. It omits the
// mutation count: a drain reports its batch's count, which can be zero.
func (w *Window) Take(now time.Time) Taken {
	if w.Empty() {
		return Taken{} // an empty window has no age
	}
	t := Taken{bytes: w.bytes, age: now.Sub(w.oldest)}
	*w = Window{}
	return t
}

// Aged reports whether the first mutation landed at least age ago. False
// while empty.
func (w *Window) Aged(now time.Time, age time.Duration) bool {
	return !w.Empty() && now.Sub(w.oldest) >= age
}

// Watermarks are the size triggers a window drains at.
type Watermarks struct {
	Mutations int
	Bytes     int
}

// Trip is which size trigger a window has reached, if either.
type Trip int

const (
	NoTrip Trip = iota
	TripMutations
	TripBytes
)

// Trips applies the size triggers only; replay uses these but not
// [Window.Aged]. A window over both reports TripMutations. An empty window
// trips nothing, so a zero trigger means "drain every write".
func (w *Window) Trips(at Watermarks) Trip {
	switch {
	case w.Empty():
		return NoTrip
	case w.mutations >= at.Mutations:
		return TripMutations
	case w.bytes >= at.Bytes:
		return TripBytes
	}
	return NoTrip
}
