// Package waltest is the conformance suite for the [wal] contract, and beside
// it [Faulty]: a backend that keeps the contract, wrapped so that a chosen call
// fails.
//
// It asserts external behaviour of [wal.Log] only, and imports the contract
// and an assertion library but never a backend.
package waltest

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/aromanovich/waltz/wal"
)

// RunContractSuite runs the contract tests against a backend. The backend must
// start with no shards in it: the suite picks fresh shard names but cannot
// empty a backend still holding another run's logs.
func RunContractSuite(t *testing.T, log wal.Log) {
	t.Helper()

	tests := []struct {
		name string
		run  func(f *fixture)
	}{
		{"AppendsComeBackInOrder", testAppendsComeBackInOrder},
		{"GapIsRefused", testGapIsRefused},
		{"DuplicateSeqnoIsAlreadyWritten", testDuplicateSeqnoIsAlreadyWritten},
		{"ReadFromAnyPosition", testReadFromAnyPosition},
		{"ArgumentsTheContractRefuses", testArgumentsTheContractRefuses},
		{"TrimRemovesUpToAndNothingElse", testTrimRemovesUpToAndNothingElse},
		{"PayloadsAreNobodyElsesMemory", testPayloadsAreNobodyElsesMemory},
		{"TrimOfALogWithNothingInIt", testTrimOfALogWithNothingInIt},
		{"AppendBelowATrimIsRefused", testAppendBelowATrimIsRefused},
		{"ShardsAreIndependent", testShardsAreIndependent},
		{"AppendNeedsAFenceAtItsEpoch", testAppendNeedsAFenceAtItsEpoch},
		{"ZeroEpochIsRefused", testZeroEpochIsRefused},
		{"FenceCutsOffLowerEpochs", testFenceCutsOffLowerEpochs},
		{"FencedOutranksAMissingPredecessor", testFencedOutranksAMissingPredecessor},
		{"FenceAtTheSameEpochIsIdempotent", testFenceAtTheSameEpochIsIdempotent},
		{"FenceAtALowerEpochIsRefused", testFenceAtALowerEpochIsRefused},
		{"EpochGrowsWithoutChangingOwner", testEpochGrowsWithoutChangingOwner},
		{"TwoWritersContendForOneShard", testTwoWritersContendForOneShard},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(newFixture(t, log))
		})
	}
}

// Guarantee 1: what was appended at a seqno comes back at that seqno, in order.
func testAppendsComeBackInOrder(f *fixture) {
	shard, epoch := f.newShard(), wal.Epoch(7)
	f.fence(shard, epoch)

	f.appendRun(shard, epoch, wal.FirstSeqno, 5)

	f.expectLog(shard, entriesFrom(epoch, wal.FirstSeqno, 5))
}

// Guarantee 4: an append that would leave a hole fails with [wal.ErrGap] and
// writes nothing, so a pipelined append that arrived out of order is a retry
// rather than a corruption.
func testGapIsRefused(f *fixture) {
	shard, epoch := f.newShard(), wal.Epoch(3)
	f.fence(shard, epoch)

	second, third := wal.FirstSeqno+1, wal.FirstSeqno+2

	f.expectError(wal.ErrGap, f.appendErr(shard, epoch, second, payloadFor(second)))
	f.append(shard, epoch, wal.FirstSeqno, payloadFor(wal.FirstSeqno))
	f.expectError(wal.ErrGap, f.appendErr(shard, epoch, third, payloadFor(third)))

	f.expectLog(shard, entriesFrom(epoch, wal.FirstSeqno, 1))

	f.append(shard, epoch, second, payloadFor(second))
	f.append(shard, epoch, third, payloadFor(third))

	f.expectLog(shard, entriesFrom(epoch, wal.FirstSeqno, 3))
}

// The retry rule: re-appending after an ambiguous failure is safe, says whether
// the first attempt landed, and never overwrites.
func testDuplicateSeqnoIsAlreadyWritten(f *fixture) {
	shard, epoch := f.newShard(), wal.Epoch(2)
	f.fence(shard, epoch)

	f.append(shard, epoch, wal.FirstSeqno, payloadFor(wal.FirstSeqno))
	f.append(shard, epoch, wal.FirstSeqno+1, payloadFor(wal.FirstSeqno+1))

	other := []byte("a different payload for the same seqno")
	f.expectError(wal.ErrAlreadyWritten,
		f.appendErr(shard, epoch, wal.FirstSeqno, other))
	// Every taken seqno, not just the log's first: a backend answering off its
	// lower end would pass the line above and let this one overwrite.
	f.expectError(wal.ErrAlreadyWritten,
		f.appendErr(shard, epoch, wal.FirstSeqno+1, other))

	f.expectLog(shard, entriesFrom(epoch, wal.FirstSeqno, 2))
}

// Guarantee 5: a read starts wherever replay resumes, in or outside the log's
// range, and returns windows.
func testReadFromAnyPosition(f *fixture) {
	shard, epoch := f.newShard(), wal.Epoch(11)
	f.fence(shard, epoch)

	const count = 6
	f.appendRun(shard, epoch, wal.FirstSeqno, count)
	last := wal.FirstSeqno + count - 1

	f.expectEntries(shard, wal.FirstSeqno+2, 10, entriesFrom(epoch, wal.FirstSeqno+2, count-2))
	f.expectEntries(shard, last, 10, entriesFrom(epoch, last, 1))
	f.expectEntries(shard, last+1, 10, nil)

	// The window after a limit starts where the limit stopped.
	f.expectEntries(shard, wal.FirstSeqno, 2, entriesFrom(epoch, wal.FirstSeqno, 2))
	f.expectEntries(shard, wal.FirstSeqno+2, 2, entriesFrom(epoch, wal.FirstSeqno+2, 2))

	// A from below the first seqno reads from the first entry.
	f.expectEntries(shard, 0, 10, entriesFrom(epoch, wal.FirstSeqno, count))
}

// The other half of guarantee 5: a trim removes exactly up to its watermark and
// leaves the log usable, the shard's ownership included.
func testTrimRemovesUpToAndNothingElse(f *fixture) {
	shard, epoch := f.newShard(), wal.Epoch(5)
	f.fence(shard, epoch)

	const count = 5
	f.appendRun(shard, epoch, wal.FirstSeqno, count)
	last := wal.FirstSeqno + count - 1

	f.trim(shard, wal.FirstSeqno-1)
	f.expectLog(shard, entriesFrom(epoch, wal.FirstSeqno, count))

	f.trim(shard, last-2)
	f.expectLog(shard, entriesFrom(epoch, last-1, 2))

	// Repeating a trim is harmless, and the shard is still this epoch's.
	f.trim(shard, last-2)
	f.append(shard, epoch, last+1, payloadFor(last+1))
	f.expectLog(shard, entriesFrom(epoch, last-1, 3))

	// A trim at or past the tail takes everything below the tail and leaves the
	// log appendable. Whether the tail row survives is the backend's business:
	// one that checks appends against a stored predecessor has to keep it.
	f.trim(shard, last+1)
	f.expectTrimmedTo(shard, last+1)
	f.trim(shard, last+100)
	f.expectTrimmedTo(shard, last+1)
	f.append(shard, epoch, last+2, payloadFor(last+2))
}

// Payload ownership, both directions. A caller may reuse the buffer it appended
// from as soon as the call returns, and may keep and overwrite what a read
// handed it; neither reaches the log. A backend keeping its entries in memory
// is the one that could fail this by handing out its own, but so would one
// caching a page it read, so the obligation is the contract's rather than any
// backend's.
//
// Non-aliasing between two entries of one read is asserted by writing into one
// and reading the other, because pointer identity is not what the contract
// promises.
func testPayloadsAreNobodyElsesMemory(f *fixture) {
	shard, epoch := f.newShard(), wal.Epoch(21)
	f.fence(shard, epoch)

	buf := []byte("the caller's own buffer")
	f.append(shard, epoch, wal.FirstSeqno, buf)
	copy(buf, "OVERWRITTEN AFTER THE APPEND RETURNED!!")

	f.append(shard, epoch, wal.FirstSeqno+1, payloadFor(wal.FirstSeqno+1))

	got, err := f.readLog(shard)
	require.NoErrorf(f.t, err, "reading shard %d", shard)
	require.Len(f.t, got, 2)
	require.Equal(f.t, []byte("the caller's own buffer"), got[0].Payload,
		"the log kept the caller's slice and saw a write the caller made after Append returned")

	// Two entries of one read, then the same entry across two reads.
	copy(got[0].Payload, "CLOBBERED")
	require.Equal(f.t, payloadFor(wal.FirstSeqno+1), got[1].Payload,
		"writing into one entry's payload reached another entry of the same read")

	again, err := f.readLog(shard)
	require.NoErrorf(f.t, err, "re-reading shard %d", shard)
	require.Equal(f.t, []byte("the caller's own buffer"), again[0].Payload,
		"writing into a payload the log handed out reached the log")
}

// A trim of a log holding nothing is the ordinary case rather than a corner: a
// healthy shard is drained, and the cycle trims to its watermark whether or not
// anything is below it. The log must still be a log afterwards — appendable at
// [wal.FirstSeqno], since nothing has occupied it.
func testTrimOfALogWithNothingInIt(f *fixture) {
	fenced, unfenced := f.newShard(), f.newShard()
	epoch := wal.Epoch(15)

	// Trim carries no epoch and so cannot ask who owns the shard: on one nobody
	// has claimed, every entry it names is one of the entries that are not there.
	f.trim(unfenced, wal.FirstSeqno)

	f.fence(fenced, epoch)
	f.trim(fenced, wal.FirstSeqno)

	for _, shard := range []wal.ShardID{fenced, unfenced} {
		f.fence(shard, epoch)
		f.append(shard, epoch, wal.FirstSeqno, payloadFor(wal.FirstSeqno))
		f.expectLog(shard, entriesFrom(epoch, wal.FirstSeqno, 1))
	}
}

// The seqnos a trim took are spent rather than free again. Which refusal says
// so is deliberately not settled — a backend that keeps its log's last entry
// answers [wal.ErrGap] where one that keeps its next seqno answers
// [wal.ErrAlreadyWritten] — but a backend that *accepts* the append puts a hole
// in a log the whole layer above reads as gap-free, and acks a commitSeqno
// below entries it still holds, which the retry rule then hands to a second
// writer as its own.
//
// [wal.FirstSeqno] is the seqno this is about: a backend deriving the answer
// from the row below the entry has that row survive every trim, whatever it is
// keeping down there for itself.
func testAppendBelowATrimIsRefused(f *fixture) {
	shard, epoch := f.newShard(), wal.Epoch(9)
	f.fence(shard, epoch)

	const count = 3
	f.appendRun(shard, epoch, wal.FirstSeqno, count)
	last := wal.FirstSeqno + count - 1
	f.trim(shard, last)

	before, err := f.readLog(shard)
	require.NoErrorf(f.t, err, "reading shard %d", shard)

	refused := func(at wal.Epoch, seqno wal.Seqno) {
		f.t.Helper()
		err := f.appendErr(shard, at, seqno, []byte("a second writer, from the beginning"))
		require.Errorf(f.t, err, "appending at the trimmed seqno %d", seqno)
		if !errors.Is(err, wal.ErrAlreadyWritten) && !errors.Is(err, wal.ErrGap) {
			f.t.Fatalf("appending at the trimmed seqno %d of shard %d was refused with %v, "+
				"which is neither of the two refusals the contract admits here", seqno, shard, err)
		}
	}
	for seqno := wal.FirstSeqno; seqno <= last; seqno++ {
		refused(epoch, seqno)
	}

	after, err := f.readLog(shard)
	require.NoErrorf(f.t, err, "re-reading shard %d", shard)
	require.Equal(f.t, before, after, "a refused append wrote something")

	// And the log goes on where it left off rather than where it starts.
	f.append(shard, epoch, last+1, payloadFor(last+1))
	f.expectEntries(shard, last+1, 10, entriesFrom(epoch, last+1, 1))

	// The failover this is really about: a fence changes ownership and nothing
	// else, so the successor continues the log rather than starting one. A
	// backend keeping the answer beside the ownership its new owner just
	// rewrote would hand that owner the log from the beginning.
	successor := epoch + 1
	f.fence(shard, successor)
	refused(successor, wal.FirstSeqno)
	f.append(shard, successor, last+2, payloadFor(last+2))
}

// Seqnos, epochs and trims of one shard say nothing about another: shards fail
// over one at a time.
func testShardsAreIndependent(f *fixture) {
	first, second := f.newShard(), f.newShard()
	firstEpoch, secondEpoch := wal.Epoch(4), wal.Epoch(9)

	f.fence(first, firstEpoch)

	f.expectLog(second, nil)

	f.fence(second, secondEpoch)
	f.append(first, firstEpoch, wal.FirstSeqno, payloadFor(wal.FirstSeqno))
	f.append(first, firstEpoch, wal.FirstSeqno+1, payloadFor(wal.FirstSeqno+1))

	f.append(second, secondEpoch, wal.FirstSeqno, []byte("second shard"))
	f.append(second, secondEpoch, wal.FirstSeqno+1, []byte("second shard again"))
	f.expectLog(second, []wal.Entry{
		{Seqno: wal.FirstSeqno, Epoch: secondEpoch, Payload: []byte("second shard")},
		{Seqno: wal.FirstSeqno + 1, Epoch: secondEpoch, Payload: []byte("second shard again")},
	})

	// Trimming one shard leaves the other's identical seqnos alone.
	f.trim(second, wal.FirstSeqno)
	f.expectLog(first, entriesFrom(firstEpoch, wal.FirstSeqno, 2))
	f.expectLog(second, []wal.Entry{
		{Seqno: wal.FirstSeqno + 1, Epoch: secondEpoch, Payload: []byte("second shard again")},
	})
}

// The arguments the contract does not admit are refused rather than
// interpreted. A backend that answers any of them with a nil error hands its
// caller a loop that never ends or an ack for entries it does not hold, and
// both look like the log working.
func testArgumentsTheContractRefuses(f *fixture) {
	shard, epoch := f.newShard(), wal.Epoch(5)
	f.fence(shard, epoch)
	f.append(shard, epoch, wal.FirstSeqno, payloadFor(wal.FirstSeqno))
	one := entriesFrom(epoch, wal.FirstSeqno, 1)

	// An append of nothing would be an ack — of what, at a seqno it names and
	// does not occupy. An empty payload is an entry; a nil one is not.
	require.Error(f.t, f.appendErr(shard, epoch, wal.FirstSeqno+1, nil),
		"an append with no payload is refused")
	// Seqnos below FirstSeqno are not entries, whatever a backend keeps there.
	require.Error(f.t, f.appendErr(shard, epoch, wal.FirstSeqno-1, payloadFor(0)),
		"an append below the first seqno is refused")

	got, err := f.readErr(shard, wal.FirstSeqno, 0)
	require.Errorf(f.t, err,
		"a read of no entries is refused rather than answered with %d of them", len(got))
	got, err = f.readErr(shard, wal.FirstSeqno, -1)
	require.Errorf(f.t, err, "a negative limit is refused rather than answered with %d entries", len(got))

	// The one argument out of range that is clamped rather than refused: a read
	// from below the first seqno is where a caller starts when it wants the
	// whole log.
	f.expectEntries(shard, wal.FirstSeqno-1, 10, one)

	f.expectLog(shard, one)
}

// The bottom of guarantee 2: an epoch that has not fenced the shard cannot
// write to it, a writer that renewed its epoch without re-fencing included.
func testAppendNeedsAFenceAtItsEpoch(f *fixture) {
	shard, epoch := f.newShard(), wal.Epoch(8)

	f.expectError(wal.ErrFenced, f.appendErr(shard, epoch, wal.FirstSeqno, payloadFor(wal.FirstSeqno)))
	f.expectLog(shard, nil)

	f.fence(shard, epoch)
	f.append(shard, epoch, wal.FirstSeqno, payloadFor(wal.FirstSeqno))

	// An epoch above the fence is refused as firmly as one below it: entries
	// written without a fence are indistinguishable from a zombie's.
	f.expectError(wal.ErrFenced,
		f.appendErr(shard, epoch+1, wal.FirstSeqno+1, payloadFor(wal.FirstSeqno+1)))
	f.expectLog(shard, entriesFrom(epoch, wal.FirstSeqno, 1))
}

// Epoch 0 means "nobody owns this": an absent fence row and an unfenced log both
// report it. The refusal must be [wal.ErrZeroEpoch]; [wal.ErrFenced] reads as a
// lost shard and would send a caller that forgot an epoch into a failover.
func testZeroEpochIsRefused(f *fixture) {
	shard, epoch := f.newShard(), wal.Epoch(4)

	f.expectError(wal.ErrZeroEpoch, f.fenceErr(shard, 0))

	// On a claimed shard too: refused for being zero, not for losing a race.
	f.fence(shard, epoch)
	f.expectError(wal.ErrZeroEpoch, f.appendErr(shard, 0, wal.FirstSeqno, payloadFor(wal.FirstSeqno)))
	f.expectLog(shard, nil)
}

// Invariant I4 in its simplest form: the shard changed hands, and the ex-owner,
// which nobody tells, learns that from its next append.
func testFenceCutsOffLowerEpochs(f *fixture) {
	shard := f.newShard()
	zombie, owner := wal.Epoch(4), wal.Epoch(9)

	f.fence(shard, zombie)
	f.append(shard, zombie, wal.FirstSeqno, payloadFor(wal.FirstSeqno))

	f.fence(shard, owner)

	second := wal.FirstSeqno + 1
	f.expectError(wal.ErrFenced, f.appendErr(shard, zombie, second, payloadFor(second)))
	f.expectLog(shard, entriesFrom(zombie, wal.FirstSeqno, 1))

	// The new owner inherits the log rather than starting one: the seqno
	// sequence continues, and each entry keeps the epoch that wrote it.
	f.append(shard, owner, second, payloadFor(second))
	f.expectLog(shard, []wal.Entry{
		{Seqno: wal.FirstSeqno, Epoch: zombie, Payload: payloadFor(wal.FirstSeqno)},
		{Seqno: second, Epoch: owner, Payload: payloadFor(second)},
	})

	// A taken seqno gets ErrFenced, not ErrAlreadyWritten: the latter is an ack,
	// and would have the zombie take the entry that replaced it for its own.
	f.expectError(wal.ErrFenced, f.appendErr(shard, zombie, second, payloadFor(second)))
}

// The other half of the ordering [wal.ErrFenced] wins: it outranks [wal.ErrGap]
// too. [wal.ErrGap] means "retry once the predecessor lands", and for an
// ex-owner the predecessor never will, so a caller answering the two as they
// are documented never learns that the shard is gone.
//
// The same append under the epoch that owns the shard is refused as a gap,
// which is what makes this a statement about the order rather than a second
// fencing case: both refusals are live at that seqno, and only one may be said.
func testFencedOutranksAMissingPredecessor(f *fixture) {
	shard := f.newShard()
	zombie, owner := wal.Epoch(6), wal.Epoch(14)

	// An unfenced shard refuses as fenced whether or not the append fits.
	overFirst := wal.FirstSeqno + 1
	f.expectError(wal.ErrFenced, f.appendErr(shard, zombie, overFirst, payloadFor(overFirst)))

	f.fence(shard, zombie)
	f.append(shard, zombie, wal.FirstSeqno, payloadFor(wal.FirstSeqno))
	f.fence(shard, owner)

	// Two above the tail, so the entry below it is one no writer has reached.
	overHole := wal.FirstSeqno + 2
	f.expectError(wal.ErrGap, f.appendErr(shard, owner, overHole, payloadFor(overHole)))
	f.expectError(wal.ErrFenced, f.appendErr(shard, zombie, overHole, payloadFor(overHole)))

	f.expectLog(shard, entriesFrom(zombie, wal.FirstSeqno, 1))
}

// The restart that is not a failover: fencing at the epoch the log already
// carries must be a no-op, not an error and not a log that lost anything.
func testFenceAtTheSameEpochIsIdempotent(f *fixture) {
	shard, epoch := f.newShard(), wal.Epoch(6)

	f.fence(shard, epoch)
	f.append(shard, epoch, wal.FirstSeqno, payloadFor(wal.FirstSeqno))

	f.fence(shard, epoch)

	f.expectLog(shard, entriesFrom(epoch, wal.FirstSeqno, 1))
	f.append(shard, epoch, wal.FirstSeqno+1, payloadFor(wal.FirstSeqno+1))
	f.expectLog(shard, entriesFrom(epoch, wal.FirstSeqno, 2))
}

// A zombie acquiring at its stale epoch is refused with the owner's claim
// untouched: a half-succeeding fence would hand the log to a failover's loser.
func testFenceAtALowerEpochIsRefused(f *fixture) {
	shard := f.newShard()
	zombie, owner := wal.Epoch(4), wal.Epoch(9)

	f.fence(shard, owner)
	f.expectError(wal.ErrFenced, f.fenceErr(shard, zombie))

	f.append(shard, owner, wal.FirstSeqno, payloadFor(wal.FirstSeqno))
	f.expectError(wal.ErrFenced,
		f.appendErr(shard, zombie, wal.FirstSeqno+1, payloadFor(wal.FirstSeqno+1)))
	f.expectLog(shard, entriesFrom(owner, wal.FirstSeqno, 1))
}

// Invariant I11's corner: the epoch is the shard's rangeID, renewed whenever the
// shard exhausts its ID range, so it grows without a failover. The log accepts
// the growth and the writer appends where it left off.
func testEpochGrowsWithoutChangingOwner(f *fixture) {
	shard := f.newShard()
	before, after := wal.Epoch(12), wal.Epoch(13)

	f.fence(shard, before)
	f.appendRun(shard, before, wal.FirstSeqno, 2)

	f.fence(shard, after)

	third := wal.FirstSeqno + 2
	f.append(shard, after, third, payloadFor(third))
	f.expectLog(shard, []wal.Entry{
		{Seqno: wal.FirstSeqno, Epoch: before, Payload: payloadFor(wal.FirstSeqno)},
		{Seqno: wal.FirstSeqno + 1, Epoch: before, Payload: payloadFor(wal.FirstSeqno + 1)},
		{Seqno: third, Epoch: after, Payload: payloadFor(third)},
	})

	// The renewal is still a fence, so an append in flight across it fails
	// rather than landing under the wrong epoch.
	f.expectError(wal.ErrFenced, f.appendErr(shard, before, third+1, payloadFor(third+1)))
}

// The chaos test of invariant I4: two processes both believe they own one shard.
// Afterwards every acked entry must be present under the epoch that acked it,
// with no seqno acked twice, no hole, and epochs never going backwards.
func testTwoWritersContendForOneShard(f *fixture) {
	// Attempts are a cap, not a plan: the claimants stop once both have won
	// entries and been cut off, and a fence that lost the race leaves nothing to
	// be cut off from, so a skewed run needs room to retry. The cap turns a run
	// that never contends into a failure rather than a hang.
	const (
		maxAttempts     = 32
		appendsPerRound = 4
	)
	shard := f.newShard()

	// One counter, because rangeID is one: the server hands out a strictly
	// greater epoch per acquire (I11), so two claimants never hold the same one.
	var epochs atomic.Uint64

	var (
		claimants [2]claimant
		wg        sync.WaitGroup
	)
	// Both claimants are built before either starts, since each reads the
	// other's progress. Do not fold the two loops together.
	for i := range claimants {
		claimants[i] = claimant{f: f, shard: shard, epochs: &epochs}
	}
	for i := range claimants {
		wg.Go(func() {
			c := &claimants[i]
			for range maxAttempts {
				c.round(appendsPerRound)
				// Report the first violation, not what a later round
				// overwrote it with.
				if c.err != nil {
					return
				}
				// Stopping as soon as this one is satisfied would leave the other
				// owning an uncontested log with nobody left to fence it.
				if claimants[0].done.Load() && claimants[1].done.Load() {
					return
				}
			}
		})
	}
	wg.Wait()

	var acked []wal.Entry
	for i := range claimants {
		require.NoErrorf(f.t, claimants[i].err, "claimant %d", i)
		require.Truef(f.t, claimants[i].contended(),
			"claimant %d wrote %d entries and was cut off %d times in %d attempts: "+
				"the run never had two writers on the shard at once, so it says nothing about fencing",
			i, len(claimants[i].won), claimants[i].lost, maxAttempts)
		acked = append(acked, claimants[i].won...)
	}
	slices.SortFunc(acked, func(a, b wal.Entry) int { return cmp.Compare(a.Seqno, b.Seqno) })

	for i, entry := range acked {
		if i > 0 {
			// A seqno acked to two writers is the failure I4 exists to prevent.
			// Checked before contiguity, which would report it as a hole.
			require.NotEqualf(f.t, acked[i-1].Seqno, entry.Seqno,
				"seqno %d was acked to both epoch %d and epoch %d",
				entry.Seqno, acked[i-1].Epoch, entry.Epoch)
			require.GreaterOrEqualf(f.t, entry.Epoch, acked[i-1].Epoch,
				"epoch went backwards at seqno %d: %d after %d",
				entry.Seqno, entry.Epoch, acked[i-1].Epoch)
		}
		require.Equalf(f.t, wal.FirstSeqno+wal.Seqno(i), entry.Seqno,
			"the acked entries have a hole below seqno %d", entry.Seqno)
	}

	got, err := f.readLog(shard)
	require.NoError(f.t, err)
	require.Equal(f.t, acked, got, "the log is not what the claimants were told it is")
}

// claimant is one contender of [testTwoWritersContendForOneShard]. It records
// failures instead of asserting them, because require in a goroutine other than
// the test's stops that goroutine and nothing else: the run goes on with one
// claimant, whom nobody is left to fence, and can end up failing for never
// having contended rather than for the violation.
type claimant struct {
	f      *fixture
	shard  wal.ShardID
	epochs *atomic.Uint64

	// won holds the entries the log acked to this claimant, lost counts the
	// appends refused because the shard had moved on.
	won  []wal.Entry
	lost int
	// err is anything the contract does not allow, kept for the test to report.
	err error
	// done is the only field the other claimant reads, hence atomic: set once
	// this one has seen both sides of a failover or stopped on an error.
	done atomic.Bool
}

func (c *claimant) contended() bool { return len(c.won) > 0 && c.lost > 0 }

// round is one acquire-and-write: take the shard at a fresh epoch, replay the
// log to find where to write, then write until the log says the shard is not
// this claimant's.
func (c *claimant) round(appends int) {
	defer func() { c.done.Store(c.err != nil || c.contended()) }()

	// The yields make the interleaving the suite's own: a backend answering from
	// memory parks on nothing, so on a one-P runtime one claimant would take all
	// its rounds before the other started and nothing would contend.
	runtime.Gosched()

	epoch := wal.Epoch(c.epochs.Add(1))

	if err := c.f.fenceErr(c.shard, epoch); err != nil {
		if !errors.Is(err, wal.ErrFenced) {
			c.err = fmt.Errorf("fencing shard %d at epoch %d: %w", c.shard, epoch, err)
		}
		// Nothing written and nothing to be cut off from: not a round that counts.
		return
	}

	// Where to append is read, not remembered: the tail may be another's.
	entries, err := c.f.readLog(c.shard)
	if err != nil {
		c.err = err
		return
	}
	seqno := wal.FirstSeqno
	if len(entries) > 0 {
		seqno = entries[len(entries)-1].Seqno + 1
	}

	for range appends {
		// Between appends too: an uninterrupted round raced nobody.
		runtime.Gosched()

		entry := wal.Entry{Seqno: seqno, Epoch: epoch, Payload: payloadFrom(epoch, seqno)}
		err := c.f.appendErr(c.shard, epoch, seqno, entry.Payload)
		if err == nil {
			c.won = append(c.won, entry)
			seqno++
			continue
		}
		if !errors.Is(err, wal.ErrFenced) {
			// Holding the fence and appending where the replay said the log
			// ends, this claimant can only be refused as fenced: a taken seqno
			// or a gap means somebody else got in, the failure I4 forbids.
			c.err = fmt.Errorf("appending at seqno %d of shard %d under epoch %d: %w",
				seqno, c.shard, epoch, err)
			return
		}
		c.lost++

		// An ex-owner is never told it lost the shard, so the log must keep
		// refusing, the seqno it just refused included.
		err = c.f.appendErr(c.shard, epoch, seqno, entry.Payload)
		if !errors.Is(err, wal.ErrFenced) {
			c.err = fmt.Errorf("appending at seqno %d of shard %d under the fenced-off epoch %d: "+
				"got %v, want a fenced error", seqno, c.shard, epoch, err)
		}
		return
	}
}

// fixture is the per-test plumbing: the log, a context and fresh shard IDs.
type fixture struct {
	t   *testing.T
	log wal.Log
	ctx context.Context
}

// shardCounter hands out shard IDs. Process-wide, so two suites sharing a
// backend get shards of their own.
var shardCounter atomic.Uint32

func newFixture(t *testing.T, log wal.Log) *fixture {
	// Long enough that a backend talking to a slow local cluster is not a
	// failure, short enough that a hung one fails here rather than at the test
	// binary's timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	return &fixture{t: t, log: log, ctx: ctx}
}

// newShard returns a shard ID with an empty, unfenced log.
func (f *fixture) newShard() wal.ShardID {
	return wal.ShardID(shardCounter.Add(1))
}

func (f *fixture) fence(shard wal.ShardID, epoch wal.Epoch) {
	f.t.Helper()
	if err := f.log.Fence(f.ctx, shard, epoch); err != nil {
		f.t.Fatalf("fencing shard %d at epoch %d: %v", shard, epoch, err)
	}
}

// fenceErr is fixture.fence returning the error instead of failing the test.
func (f *fixture) fenceErr(shard wal.ShardID, epoch wal.Epoch) error {
	return f.log.Fence(f.ctx, shard, epoch)
}

func (f *fixture) append(shard wal.ShardID, epoch wal.Epoch, seqno wal.Seqno, payload []byte) {
	f.t.Helper()
	if err := f.log.Append(f.ctx, shard, epoch, seqno, payload); err != nil {
		f.t.Fatalf("appending seqno %d of shard %d under epoch %d: %v", seqno, shard, epoch, err)
	}
}

// appendErr is fixture.append returning the error instead of failing the test.
func (f *fixture) appendErr(shard wal.ShardID, epoch wal.Epoch, seqno wal.Seqno, payload []byte) error {
	return f.log.Append(f.ctx, shard, epoch, seqno, payload)
}

// appendRun appends count entries from first, one per call. The log it leaves
// is entriesFrom(epoch, first, count).
func (f *fixture) appendRun(shard wal.ShardID, epoch wal.Epoch, first wal.Seqno, count int) {
	f.t.Helper()
	for i := range wal.Seqno(count) {
		f.append(shard, epoch, first+i, payloadFor(first+i))
	}
}

// readErr is a read returning what the backend said, for the reads a test
// expects to be refused.
func (f *fixture) readErr(shard wal.ShardID, from wal.Seqno, limit int) ([]wal.Entry, error) {
	return f.log.ReadFrom(f.ctx, shard, from, limit)
}

func (f *fixture) trim(shard wal.ShardID, upTo wal.Seqno) {
	f.t.Helper()
	if err := f.log.Trim(f.ctx, shard, upTo); err != nil {
		f.t.Fatalf("trimming shard %d up to %d: %v", shard, upTo, err)
	}
}

// readLog reads a shard's whole log the way a replay does. It returns the error
// instead of failing the test, because the chaos test's claimants read from
// goroutines of their own.
func (f *fixture) readLog(shard wal.ShardID) ([]wal.Entry, error) {
	// Big enough that the suite's logs come back in one read.
	const window = 64

	var entries []wal.Entry
	for e, err := range wal.Entries(f.ctx, f.log, shard, wal.FirstSeqno, window) {
		if err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, nil
}

// expectError checks that an operation failed the way the contract says it
// fails. A backend may add context to the error but must keep it matchable.
func (f *fixture) expectError(want, got error) {
	f.t.Helper()
	require.ErrorIs(f.t, got, want)
}

// expectLog reads a shard from the start with a limit above anything this suite
// writes, so what comes back is the whole log.
func (f *fixture) expectLog(shard wal.ShardID, want []wal.Entry) {
	f.t.Helper()
	f.expectEntries(shard, wal.FirstSeqno, 10, want)
}

func (f *fixture) expectEntries(shard wal.ShardID, from wal.Seqno, limit int, want []wal.Entry) {
	f.t.Helper()

	got, err := f.log.ReadFrom(f.ctx, shard, from, limit)
	require.NoErrorf(f.t, err, "reading shard %d from %d", shard, from)
	// A backend may return a nil slice or an empty one; the contract does not care.
	if len(want) == 0 {
		require.Emptyf(f.t, got, "reading shard %d from %d (limit %d)", shard, from, limit)
		return
	}
	require.Equalf(f.t, want, got, "reading shard %d from %d (limit %d)", shard, from, limit)
}

// expectTrimmedTo checks a log trimmed at or past its tail: every entry below
// the tail is gone, and the tail row itself may or may not be.
func (f *fixture) expectTrimmedTo(shard wal.ShardID, tail wal.Seqno) {
	f.t.Helper()

	got, err := f.log.ReadFrom(f.ctx, shard, wal.FirstSeqno, 10)
	require.NoErrorf(f.t, err, "reading shard %d", shard)
	if len(got) > 1 {
		f.t.Fatalf("shard %d trimmed past its tail %d still holds %d entries, the lowest at seqno %d",
			shard, tail, len(got), got[0].Seqno)
	}
	if len(got) == 1 {
		require.Equalf(f.t, tail, got[0].Seqno,
			"shard %d trimmed past its tail %d kept the wrong entry", shard, tail)
	}
}

// payloadFor builds a payload naming its seqno, so a misplaced entry is
// recognisable rather than merely unequal.
func payloadFor(seqno wal.Seqno) []byte {
	return []byte(fmt.Sprintf("payload of entry %d", seqno))
}

// payloadFrom is payloadFor with the writing epoch in it.
func payloadFrom(epoch wal.Epoch, seqno wal.Seqno) []byte {
	return []byte(fmt.Sprintf("payload of entry %d, written under epoch %d", seqno, epoch))
}

// entriesFrom is what a log written by payloadFor under one epoch looks like.
func entriesFrom(epoch wal.Epoch, first wal.Seqno, count int) []wal.Entry {
	entries := make([]wal.Entry, 0, count)
	for i := range wal.Seqno(count) {
		entries = append(entries, wal.Entry{
			Seqno:   first + i,
			Epoch:   epoch,
			Payload: payloadFor(first + i),
		})
	}
	return entries
}
