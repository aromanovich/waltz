// Package window is the size and age of what a cycle has folded since its last
// drain. A package so that emptying it outside [Window.Take] does not compile.
//
// Its bytes are not the tail's: this empties when a drain starts, the tail only
// when that drain's transaction commits. Nothing here publishes — the drain's
// numbers are emitted once the transaction has an outcome.
//
// It counts what the loop folded and may not name wal.Log or wal.Entry: the
// window bounds a drain, it does not read the log.
package window

import "time"

// Window is what a cycle has folded and not yet drained. Loop-owned, and the
// zero value is an empty window.
type Window struct {
	mutations int
	// bytes is the entries' encoded payload lengths, which is what the tail
	// counts too.
	bytes int
	// oldest is when the mutation that opened this window landed, and means
	// nothing while the window is empty.
	oldest time.Time
}

// Add takes one folded mutation in; size is its encoded payload length. now is
// read only when this mutation opens the window.
func (w *Window) Add(size int, now time.Time) {
	if w.Empty() {
		w.oldest = now
	}
	w.mutations++
	w.bytes += size
}

// Size is the window as a report reads it.
func (w *Window) Size() (mutations, bytes int) { return w.mutations, w.bytes }

// Empty reports a window nothing has been folded into since the last take.
func (w *Window) Empty() bool { return w.mutations == 0 }

// Taken is what one take handed over. It exists so the bytes cannot be stated
// as a number: the only value the tail will release is one a window produced,
// and it releases it once ([Taken.Release]), so a branch that settles twice
// subtracts twice from nothing rather than driving the tail below zero — a tail
// that never trips I10 again, which is unbounded memory by the road the bound
// exists to close.
//
// A take nobody settles is legal and deliberate: a halt drops its window, the
// entries behind it being acked and the cycle finished.
type Taken struct {
	bytes int
	age   time.Duration
}

// Age is how long the window's oldest mutation had been waiting when it was
// taken. Zero for a window that was empty. Reportable any number of times: it
// is the drain's metric, not the tail's arithmetic.
func (t Taken) Age() time.Duration { return t.age }

// Release hands the bytes to the tail, once. Every call after the first reports
// none, which is what makes a double settle arithmetically inert.
func (t *Taken) Release() int {
	bytes := t.bytes
	t.bytes = 0
	return bytes
}

// Take empties the window and reports the bytes the tail goes on holding until
// the drain commits, and the age of its oldest mutation.
//
// Not the mutation count: what a drain applied is its batch's own, and a window
// that folded entries can still fold to nothing.
func (w *Window) Take(now time.Time) Taken {
	if w.Empty() {
		return Taken{} // an empty window has no age
	}
	t := Taken{bytes: w.bytes, age: now.Sub(w.oldest)}
	*w = Window{}
	return t
}

// Aged reports a window whose first mutation landed at least age ago. False
// while empty: there is nothing for a tick to drain.
func (w *Window) Aged(now time.Time, age time.Duration) bool {
	return !w.Empty() && now.Sub(w.oldest) >= age
}

// Watermarks is the size a window drains at, in the two units it counts.
type Watermarks struct {
	Mutations int
	Bytes     int
}

// Trip is which size watermark a window has reached, if either.
type Trip int

const (
	NoTrip Trip = iota
	TripMutations
	TripBytes
)

// Trips is the size rule only; the age rule is [Window.Aged], because replay
// consults one and not the other.
//
// The count is answered first, so a window over both reports it. An empty
// window trips nothing: a zero watermark means "drain every write".
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
