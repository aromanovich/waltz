package tailstate

// The type's own arithmetic, without a cycle around it. What a cycle's call
// sites do with it is TestTheMirrorFollowsEveryTailMove's claim.

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/aromanovich/waltz/cycle/window"
	"github.com/aromanovich/waltz/wal"
	"github.com/aromanovich/waltz/walmetrics"
)

// took is a window that folded exactly size bytes, taken: the only way to hand
// the tail anything, which is what Settle's parameter is for.
func took(size int) *window.Taken {
	var w window.Window
	w.Add(size, time.Time{})
	t := w.Take(time.Time{})
	return &t
}

// TestTheTailsTwoFloorsStaySeparate is what [Tail.Settle]'s mark exists for:
// resolved and applied are two numbers. Merging them is a bug in both
// directions — trimming past what the cold store holds strands a recovering
// owner, and counting a settled entry trips backpressure on a healthy shard.
func TestTheTailsTwoFloorsStaySeparate(t *testing.T) {
	// The noop emitter.
	m := NewMirror(walmetrics.New(nil))
	tl := New(m)

	tl.Floor(wal.FirstSeqno - 1)
	tl.Ack(wal.FirstSeqno, 100)
	require.Equal(t, 1, tl.Entries())
	require.False(t, tl.Empty())

	tl.Settle(wal.FirstSeqno, took(100), KeepWatermark)
	require.True(t, tl.Empty(), "a settled entry is out of the tail the bound reads")
	require.EqualValues(t, wal.FirstSeqno-1, tl.Applied(),
		"and the watermark did not move: trim goes to applied, and the cold store holds nothing")
	require.Zero(t, tl.Bytes())

	tl.Ack(wal.FirstSeqno+1, 40)
	tl.Settle(wal.FirstSeqno+1, took(40), MoveWatermark)
	require.True(t, tl.Empty())
	require.EqualValues(t, wal.FirstSeqno+1, tl.Applied(), "a committed drain is the one settle that moves it")

	entries, bytes := m.Size()
	require.Zero(t, entries)
	require.Zero(t, bytes)
	require.True(t, m.Empty())
}

// TestAFloorPlantsAllFourNumbers is what makes [Tail.Floor] safe to run more
// than once. A cycle whose replay is abandoned reads the watermark again and
// re-acks everything above it, so a floor that left the bytes behind would have
// I10 counting one incident's memory once per attempt — and unlike the entry
// count, which the positions carry, the bytes are only ever released by a
// settle that never comes for an attempt nobody adopted.
func TestAFloorPlantsAllFourNumbers(t *testing.T) {
	m := NewMirror(walmetrics.New(nil))
	tl := New(m)

	tl.Floor(wal.FirstSeqno - 1)
	tl.Ack(wal.FirstSeqno, 100)
	tl.Ack(wal.FirstSeqno+1, 40)
	require.Equal(t, 140, tl.Bytes())

	// The attempt is abandoned: nothing settles, and the floor is planted
	// again at the watermark the cold store still holds.
	tl.Floor(wal.FirstSeqno - 1)
	require.Zero(t, tl.Bytes(), "the abandoned attempt's bytes are still counted")
	require.True(t, tl.Empty())
	require.EqualValues(t, wal.FirstSeqno-1, tl.Commit())

	entries, bytes := m.Size()
	require.Zero(t, entries)
	require.Zero(t, bytes, "and the mirror is what a write is refused off")
}

// TestAStalledTailSettlesNothing is the floor an unreadable outcome leaves.
// Applied is the position that matters most — a trim goes to it, so moving it
// over entries the cold store may hold none of authorises their deletion from
// the log as well — and the bytes stay with the entries, since a tail whose two
// counts disagree is the bound reading a number no window will hand back.
func TestAStalledTailSettlesNothing(t *testing.T) {
	tl := New(NewMirror(walmetrics.New(nil)))
	tl.Floor(wal.FirstSeqno - 1)
	tl.Ack(wal.FirstSeqno, 100)
	tl.Ack(wal.FirstSeqno+1, 40)

	// The drain of both entries comes back with no outcome anybody could read.
	unreadable := errors.New("the cold store did not answer")
	tl.Stall(wal.FirstSeqno+1, took(140), unreadable)
	unresolved, stalled := tl.Stalled()
	require.True(t, stalled)
	require.EqualValues(t, wal.FirstSeqno+1, unresolved.Seqno)
	require.Equal(t, unreadable, unresolved.Cause, "the halt this may still become is attributed to it")
	require.Equal(t, 140, tl.Bytes(), "the entries are still acked, so their bytes are still counted")
	require.Equal(t, 2, tl.Entries())

	tl.Ack(wal.FirstSeqno+2, 60)
	tl.Settle(wal.FirstSeqno+2, took(60), MoveWatermark)
	require.EqualValues(t, wal.FirstSeqno-1, tl.Applied(), "nothing settles over a drain nobody could read")
	require.Equal(t, 3, tl.Entries())
	require.Equal(t, 200, tl.Bytes(), "and its bytes are held with its entry rather than half of it")
	require.False(t, tl.Empty())

	// The watermark answers at last, and it had committed.
	tl.Resolve()
	require.EqualValues(t, wal.FirstSeqno+1, tl.Applied())
	require.Equal(t, 60, tl.Bytes(), "the stalled drain's bytes leave with the answer, and only those")
	unresolved, stalled = tl.Stalled()
	require.False(t, stalled)
	require.NoError(t, unresolved.Cause, "a cause outliving its stall would attribute the wrong drain")
}

// TestAStalledTailIsOnTheMirrorToo is what the goroutines with no loop to ask
// read: the stall is what a write is refused on before it is queued, and the
// non-empty tail is what a stopped cycle's read is refused on — an unresolved
// drain being exactly the case where the cold store cannot be said to hold what
// this cycle acked.
func TestAStalledTailIsOnTheMirrorToo(t *testing.T) {
	m := NewMirror(walmetrics.New(nil))
	tl := New(m)
	tl.Floor(wal.FirstSeqno - 1)
	tl.Ack(wal.FirstSeqno, 100)
	tl.Stall(wal.FirstSeqno, took(100), errors.New("the cold store did not answer"))

	require.False(t, tl.Empty())
	require.False(t, m.Empty())
	entries, bytes := m.Size()
	require.EqualValues(t, 1, entries)
	require.EqualValues(t, 100, bytes)
	seqno, stalled := m.StalledAt()
	require.True(t, stalled)
	require.EqualValues(t, wal.FirstSeqno, seqno)

	// And a floor is the other way out of one: a successor reads the watermark
	// itself and re-acks everything above it.
	tl.Floor(wal.FirstSeqno - 1)
	_, stalled = tl.Stalled()
	require.False(t, stalled)
	_, stalled = m.StalledAt()
	require.False(t, stalled)
	require.Zero(t, tl.Bytes())
	require.True(t, m.Empty())
}

// TestASecondSettleOfOneWindowReleasesNothing is what makes the parameter a
// value rather than a number. Subtracting one window's bytes twice puts the
// tail below zero, and a tail below zero never trips I10 again — unbounded
// memory by the road the bound exists to close.
func TestASecondSettleOfOneWindowReleasesNothing(t *testing.T) {
	tl := New(NewMirror(walmetrics.New(nil)))
	tl.Floor(wal.FirstSeqno - 1)
	tl.Ack(wal.FirstSeqno, 100)
	tl.Ack(wal.FirstSeqno+1, 40)

	held := took(100)
	tl.Settle(wal.FirstSeqno, held, KeepWatermark)
	require.Equal(t, 40, tl.Bytes())

	tl.Settle(wal.FirstSeqno, held, KeepWatermark)
	require.Equal(t, 40, tl.Bytes(), "the window was already released")
	require.GreaterOrEqual(t, tl.Bytes(), 0, "and the tail never goes below zero")
}
