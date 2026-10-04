// Package waltest is the conformance suite for the [wal] contract, plus
// [Faulty], which wraps a correct backend so chosen calls fail. Three more
// decorators break the contract on purpose, only to prove checks against:
// [Expiring], [Unfenced] and [Truncating].
//
// Two obligations the suite cannot test are functions a deployment runs
// against its own storage. [CheckRetention] covers time, which a
// millisecond suite cannot ([Expiring] proves it). [CheckReopen] covers
// storage: every suite case reads back through the value that wrote, so data
// or an epoch kept only in memory passes ([Unfenced] proves its ownership
// half).
//
// Not covered at all: a fence racing a displaced owner's append, which needs
// two processes; see [RunContractSuite].
//
// It tests only external behaviour of [wal.Log] and never imports a backend.
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
//
// A green run does not prove fencing across processes. Every case uses this
// one value, so a backend that keeps the owning epoch only in memory passes
// every fencing case, yet in a deployment lets two writers ack at one seqno.
// The backend's author must test that against real storage: fence at a
// higher epoch from a second process, append from the first, and read the
// outcome from the log.
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
		{"ACancelledContextChangesNothing", testACancelledContextChangesNothing},
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
		{"APageEndsAtItsLimitAndNotAtAByteBudget", testAPageEndsAtItsLimitAndNotAtAByteBudget},
		{"TrimRunsBesideAppends", testTrimRunsBesideAppends},
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
// writes nothing, so an out-of-order pipelined append can be retried.
func testGapIsRefused(f *fixture) {
	shard, epoch := f.newShard(), wal.Epoch(3)
	f.fence(shard, epoch)

	second, third := wal.FirstSeqno+1, wal.FirstSeqno+2

	f.expectError(f.appendErr(shard, epoch, second, payloadFor(second)), wal.ErrGap)
	f.append(shard, epoch, wal.FirstSeqno, payloadFor(wal.FirstSeqno))
	f.expectError(f.appendErr(shard, epoch, third, payloadFor(third)), wal.ErrGap)

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
	f.expectError(f.appendErr(shard, epoch, wal.FirstSeqno, other), wal.ErrAlreadyWritten)
	// Every taken seqno, not just the first.
	f.expectError(f.appendErr(shard, epoch, wal.FirstSeqno+1, other), wal.ErrAlreadyWritten)

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
	// Well past the end too: an offset-indexed slice allows last+1 but no further.
	f.expectEntries(shard, last+5, 10, nil)

	// The window after a limit starts where the limit stopped.
	f.expectEntries(shard, wal.FirstSeqno, 2, entriesFrom(epoch, wal.FirstSeqno, 2))
	f.expectEntries(shard, wal.FirstSeqno+2, 2, entriesFrom(epoch, wal.FirstSeqno+2, 2))

	// A from below the first seqno reads from the first entry.
	f.expectEntries(shard, 0, 10, entriesFrom(epoch, wal.FirstSeqno, count))
}

// The other half of guarantee 5: a trim removes exactly up to its upTo and
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

	// A trim below the current lower end removes nothing. A backend computing
	// `upTo - base + 1` in unsigned arithmetic underflows here and silently
	// deletes the whole log.
	f.trim(shard, wal.FirstSeqno)
	f.expectLog(shard, entriesFrom(epoch, last-1, 2))

	// Repeating a trim is harmless, and the shard is still this epoch's.
	f.trim(shard, last-2)
	f.append(shard, epoch, last+1, payloadFor(last+1))
	f.expectLog(shard, entriesFrom(epoch, last-1, 3))

	// A trim at or past the tail removes everything below it and leaves the
	// log appendable. Whether the tail row survives is up to the backend.
	f.trim(shard, last+1)
	f.expectTrimmedTo(shard, last+1)
	f.trim(shard, last+100)
	f.expectTrimmedTo(shard, last+1)
	f.append(shard, epoch, last+2, payloadFor(last+2))
}

// Payload ownership, both directions: a caller may reuse its append buffer
// once Append returns, and may overwrite what a read returned; neither
// reaches the log. Non-aliasing within one read is checked by writing one
// entry and reading another, since the contract does not promise pointer
// identity.
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

// Trimming an empty log is common: the cycle trims to its watermark whether
// or not anything is below it. The log must stay appendable at
// [wal.FirstSeqno].
func testTrimOfALogWithNothingInIt(f *fixture) {
	fenced, unfenced := f.newShard(), f.newShard()
	epoch := wal.Epoch(15)

	// Trim carries no epoch, so it also works on a shard nobody has fenced.
	f.trim(unfenced, wal.FirstSeqno)

	f.fence(fenced, epoch)
	f.trim(fenced, wal.FirstSeqno)

	for _, shard := range []wal.ShardID{fenced, unfenced} {
		f.fence(shard, epoch)
		f.append(shard, epoch, wal.FirstSeqno, payloadFor(wal.FirstSeqno))
		f.expectLog(shard, entriesFrom(epoch, wal.FirstSeqno, 1))
	}
}

// Trimmed seqnos stay spent. The contract accepts either [wal.ErrGap] or
// [wal.ErrAlreadyWritten] as the refusal, but accepting the append would put
// a hole in a log the layer reads as gap-free. [wal.FirstSeqno] matters most:
// a backend that checks the row below must still refuse it after a trim.
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

	f.append(shard, epoch, last+1, payloadFor(last+1))
	f.expectEntries(shard, last+1, 10, entriesFrom(epoch, last+1, 1))

	// After failover the successor continues the log; a fence must not reset
	// it to the beginning.
	successor := epoch + 1
	f.fence(shard, successor)
	refused(successor, wal.FirstSeqno)
	f.append(shard, successor, last+2, payloadFor(last+2))
}

// Seqnos, epochs and trims of one shard do not affect another.
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

// Invalid arguments are refused, not interpreted. Accepting one would give
// the caller an endless loop or an ack for entries the log does not hold.
func testArgumentsTheContractRefuses(f *fixture) {
	shard, epoch := f.newShard(), wal.Epoch(5)
	f.fence(shard, epoch)
	f.append(shard, epoch, wal.FirstSeqno, payloadFor(wal.FirstSeqno))
	one := entriesFrom(epoch, wal.FirstSeqno, 1)

	// An empty payload is an entry; a nil one is not.
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

	// The one out-of-range argument that is clamped, not refused: reading from
	// below the first seqno reads the whole log.
	f.expectEntries(shard, wal.FirstSeqno-1, 10, one)

	f.expectLog(shard, one)
}

// The context obligations stated on [wal.Log]:
//   - a call whose context is already cancelled leaves the log unchanged;
//   - its error matches context.Canceled, or the layer above treats it as an
//     unknown outcome and halts a healthy shard;
//   - an invalid argument is reported before the context, or a malformed call
//     looks like a timeout and is retried forever.
func testACancelledContextChangesNothing(f *fixture) {
	shard, epoch := f.newShard(), wal.Epoch(6)
	f.fence(shard, epoch)
	f.append(shard, epoch, wal.FirstSeqno, payloadFor(wal.FirstSeqno))
	one := entriesFrom(epoch, wal.FirstSeqno, 1)

	dead, cancel := context.WithCancel(f.ctx)
	cancel()

	f.expectError(f.log.Append(dead, shard, epoch, wal.FirstSeqno+1, payloadFor(wal.FirstSeqno+1)),
		context.Canceled)
	f.expectLog(shard, one)

	f.expectError(f.log.Fence(dead, shard, epoch+1), context.Canceled)
	// The fence did not take, so the old epoch still writes.
	f.append(shard, epoch, wal.FirstSeqno+1, payloadFor(wal.FirstSeqno+1))
	two := entriesFrom(epoch, wal.FirstSeqno, 2)

	f.expectError(f.log.Trim(dead, shard, wal.FirstSeqno), context.Canceled)
	f.expectLog(shard, two)

	_, err := f.log.ReadFrom(dead, shard, wal.FirstSeqno, 10)
	f.expectError(err, context.Canceled)

	f.expectError(f.log.Append(dead, shard, 0, wal.FirstSeqno+2, payloadFor(0)), wal.ErrZeroEpoch)
	f.expectError(f.log.Fence(dead, shard, 0), wal.ErrZeroEpoch)
	f.expectLog(shard, two)
}

// Guarantee 2: an epoch that has not fenced the shard cannot write to it,
// including a writer that bumped its epoch without re-fencing.
func testAppendNeedsAFenceAtItsEpoch(f *fixture) {
	shard, epoch := f.newShard(), wal.Epoch(8)

	f.expectError(f.appendErr(shard, epoch, wal.FirstSeqno, payloadFor(wal.FirstSeqno)), wal.ErrFenced)
	f.expectLog(shard, nil)

	f.fence(shard, epoch)
	f.append(shard, epoch, wal.FirstSeqno, payloadFor(wal.FirstSeqno))

	// An epoch above the fence is refused too: unfenced writes look like a
	// zombie's.
	f.expectError(f.appendErr(shard, epoch+1, wal.FirstSeqno+1, payloadFor(wal.FirstSeqno+1)), wal.ErrFenced)
	f.expectLog(shard, entriesFrom(epoch, wal.FirstSeqno, 1))
}

// Epoch 0 means "unowned". The refusal must be [wal.ErrZeroEpoch]:
// [wal.ErrFenced] would send a caller that forgot its epoch into a failover.
func testZeroEpochIsRefused(f *fixture) {
	shard, epoch := f.newShard(), wal.Epoch(4)

	f.expectError(f.fenceErr(shard, 0), wal.ErrZeroEpoch)

	// On a claimed shard too: refused for being zero, not for losing a race.
	f.fence(shard, epoch)
	f.expectError(f.appendErr(shard, 0, wal.FirstSeqno, payloadFor(wal.FirstSeqno)), wal.ErrZeroEpoch)
	f.expectLog(shard, nil)
}

// Invariant I4: after the shard changes hands, the old owner learns it from
// its next append.
func testFenceCutsOffLowerEpochs(f *fixture) {
	shard := f.newShard()
	zombie, owner := wal.Epoch(4), wal.Epoch(9)

	f.fence(shard, zombie)
	f.append(shard, zombie, wal.FirstSeqno, payloadFor(wal.FirstSeqno))

	f.fence(shard, owner)

	second := wal.FirstSeqno + 1
	f.expectError(f.appendErr(shard, zombie, second, payloadFor(second)), wal.ErrFenced)
	f.expectLog(shard, entriesFrom(zombie, wal.FirstSeqno, 1))

	// The new owner continues the log; each entry keeps its writer's epoch.
	f.append(shard, owner, second, payloadFor(second))
	f.expectLog(shard, []wal.Entry{
		{Seqno: wal.FirstSeqno, Epoch: zombie, Payload: payloadFor(wal.FirstSeqno)},
		{Seqno: second, Epoch: owner, Payload: payloadFor(second)},
	})

	// ErrFenced, not ErrAlreadyWritten: the latter is an ack, and the zombie
	// would take the new owner's entry as its own.
	f.expectError(f.appendErr(shard, zombie, second, payloadFor(second)), wal.ErrFenced)
}

// [wal.ErrFenced] also outranks [wal.ErrGap]. ErrGap means "retry once the
// predecessor lands", which for an ex-owner never happens, so it would never
// learn the shard is gone. The owner's identical append gets ErrGap, so both
// refusals apply at that seqno and this tests their order.
func testFencedOutranksAMissingPredecessor(f *fixture) {
	shard := f.newShard()
	zombie, owner := wal.Epoch(6), wal.Epoch(14)

	// An unfenced shard refuses as fenced whether or not the append fits.
	overFirst := wal.FirstSeqno + 1
	f.expectError(f.appendErr(shard, zombie, overFirst, payloadFor(overFirst)), wal.ErrFenced)

	f.fence(shard, zombie)
	f.append(shard, zombie, wal.FirstSeqno, payloadFor(wal.FirstSeqno))
	f.fence(shard, owner)

	// Two above the tail, so its predecessor is missing.
	overHole := wal.FirstSeqno + 2
	f.expectError(f.appendErr(shard, owner, overHole, payloadFor(overHole)), wal.ErrGap)
	f.expectError(f.appendErr(shard, zombie, overHole, payloadFor(overHole)), wal.ErrFenced)

	f.expectLog(shard, entriesFrom(zombie, wal.FirstSeqno, 1))
}

// A restart without failover: fencing at the current epoch is a no-op.
func testFenceAtTheSameEpochIsIdempotent(f *fixture) {
	shard, epoch := f.newShard(), wal.Epoch(6)

	f.fence(shard, epoch)
	f.append(shard, epoch, wal.FirstSeqno, payloadFor(wal.FirstSeqno))

	f.fence(shard, epoch)

	f.expectLog(shard, entriesFrom(epoch, wal.FirstSeqno, 1))
	f.append(shard, epoch, wal.FirstSeqno+1, payloadFor(wal.FirstSeqno+1))
	f.expectLog(shard, entriesFrom(epoch, wal.FirstSeqno, 2))
}

// A fence at a stale epoch is refused and leaves the owner's claim intact.
func testFenceAtALowerEpochIsRefused(f *fixture) {
	shard := f.newShard()
	zombie, owner := wal.Epoch(4), wal.Epoch(9)

	f.fence(shard, owner)
	f.expectError(f.fenceErr(shard, zombie), wal.ErrFenced)

	f.append(shard, owner, wal.FirstSeqno, payloadFor(wal.FirstSeqno))
	f.expectError(f.appendErr(shard, zombie, wal.FirstSeqno+1, payloadFor(wal.FirstSeqno+1)), wal.ErrFenced)
	f.expectLog(shard, entriesFrom(owner, wal.FirstSeqno, 1))
}

// Invariant I11: the epoch is the shard's rangeID, which also grows without a
// failover when the shard exhausts its ID range. The writer continues where
// it left off.
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

	// The renewal is still a fence: an in-flight append at the old epoch fails.
	f.expectError(f.appendErr(shard, before, third+1, payloadFor(third+1)), wal.ErrFenced)
}

// Invariant I4 under contention: two writers both believe they own one shard.
// Afterwards every acked entry must be present under the epoch that acked it,
// with no seqno acked twice, no hole, and epochs never going backwards.
func testTwoWritersContendForOneShard(f *fixture) {
	// maxAttempts is a cap: claimants stop once both have won entries and been
	// cut off. It turns a run that never contends into a failure, not a hang.
	const (
		maxAttempts     = 32
		appendsPerRound = 4
	)
	shard := f.newShard()

	// One shared counter: each acquire gets a strictly greater epoch (I11).
	var epochs atomic.Uint64

	var (
		claimants [2]claimant
		wg        sync.WaitGroup
	)
	// Build both before starting either, since each reads the other's
	// progress. Do not merge the two loops.
	for i := range claimants {
		claimants[i] = claimant{f: f, shard: shard, epochs: &epochs}
	}
	// Release both together, or on a loaded machine the second may start only
	// after the first used its whole cap, and the run fails as uncontended.
	start := make(chan struct{})
	for i := range claimants {
		wg.Go(func() {
			c := &claimants[i]
			<-start
			for range maxAttempts {
				c.round(appendsPerRound)
				// Keep the first violation.
				if c.err != nil {
					return
				}
				// Stop only when both are done, so neither is left uncontested.
				if claimants[0].done.Load() && claimants[1].done.Load() {
					return
				}
			}
		})
	}
	close(start)
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
			// A seqno acked twice is what I4 prevents. Checked before
			// contiguity, which would misreport it as a hole.
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

// A page ends only at its limit or at the end of the log. Callers such as
// [wal.Entries] and replay treat a short page as the end, so a backend that
// also stops at a byte budget (a gRPC message, a driver's row buffer) makes
// replay silently rebuild only a prefix of the tail. The other cases use tiny
// payloads and cannot catch this. Replay pages are bounded by count only, and
// one large workflow can fill one.
func testAPageEndsAtItsLimitAndNotAtAByteBudget(f *fixture) {
	const (
		entries = 24
		// 6 MB in total: over a 4 MB message and a 1 MB page, at a realistic
		// entry size.
		size = 256 << 10
	)
	shard, epoch := f.newShard(), wal.Epoch(9)
	f.fence(shard, epoch)

	for i := range wal.Seqno(entries) {
		seqno := wal.FirstSeqno + i
		f.append(shard, epoch, seqno, bigPayloadFor(seqno, size))
	}

	got, err := f.readErr(shard, wal.FirstSeqno, entries)
	require.NoError(f.t, err)
	require.Len(f.t, got, entries,
		"a page short of its limit is the end of the log to every caller, so a read that stops at a byte budget truncates a replay in silence")
	for i, e := range got {
		seqno := wal.FirstSeqno + wal.Seqno(i)
		require.Equal(f.t, seqno, e.Seqno)
		require.Equalf(f.t, bigPayloadFor(seqno, size), e.Payload, "entry %d came back changed", seqno)
	}
}

// [wal.Log.Trim] always runs concurrently with appends in a deployment. The
// other trim cases run on a quiet log, so a backend whose trim is a
// read-modify-write over the appended region passes them, yet silently loses
// an entry acked during the trim and later hands its seqno out again.
func testTrimRunsBesideAppends(f *fixture) {
	const (
		entries = 200
		// How far a trim stays behind the last ack, as a drain's watermark is
		// always below the tail.
		behind = wal.Seqno(8)
	)
	shard, epoch := f.newShard(), wal.Epoch(5)
	f.fence(shard, epoch)

	var (
		mu        sync.Mutex
		acked     wal.Seqno
		trimmed   wal.Seqno
		trims     int
		appending = true
		failed    error
	)
	// Recorded, not asserted: require in another goroutine stops only that
	// goroutine.
	fail := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		if failed == nil {
			failed = err
		}
	}

	var wg sync.WaitGroup
	wg.Go(func() {
		defer func() {
			mu.Lock()
			appending = false
			mu.Unlock()
		}()
		for i := range entries {
			seqno := wal.FirstSeqno + wal.Seqno(i)
			if err := f.appendErr(shard, epoch, seqno, payloadFor(seqno)); err != nil {
				fail(fmt.Errorf("appending seqno %d beside a trim: %w", seqno, err))
				return
			}
			mu.Lock()
			acked = seqno
			mu.Unlock()
			// Yield so the two overlap on a one-P runtime with an in-memory
			// backend, which never parks on I/O.
			runtime.Gosched()
		}
	})
	wg.Go(func() {
		for {
			mu.Lock()
			upTo, last, live := acked, trimmed, appending
			mu.Unlock()

			if upTo > behind && upTo-behind > last {
				upTo -= behind
				if err := f.log.Trim(f.ctx, shard, upTo); err != nil {
					fail(fmt.Errorf("trimming up to %d beside an append: %w", upTo, err))
					return
				}
				mu.Lock()
				trimmed, trims = upTo, trims+1
				mu.Unlock()
			}
			if !live {
				return
			}
			runtime.Gosched()
		}
	})
	wg.Wait()

	require.NoError(f.t, failed)
	require.Positive(f.t, trims,
		"no trim ran while the appends were going, so the run says nothing about the two together")
	require.Equal(f.t, wal.FirstSeqno+wal.Seqno(entries)-1, acked)

	got, err := f.readLog(shard)
	require.NoError(f.t, err)
	require.NotEmpty(f.t, got, "every trim stayed below what was acked, so the log cannot be empty")

	require.Equal(f.t, acked, got[len(got)-1].Seqno,
		"the entry acked last is gone from the log, which is what a trim that is not isolated from a concurrent append takes")
	require.LessOrEqual(f.t, got[0].Seqno, trimmed+1,
		"entries above the last trim are missing: a trim removed more than it was given")
	for i, e := range got {
		require.Equalf(f.t, got[0].Seqno+wal.Seqno(i), e.Seqno, "the log has a hole below seqno %d", e.Seqno)
		require.Equalf(f.t, payloadFor(e.Seqno), e.Payload, "seqno %d came back with another entry's payload", e.Seqno)
	}

	// The next seqno is unchanged by the trims.
	next := acked + 1
	f.append(shard, epoch, next, payloadFor(next))
}

// claimant is one writer in [testTwoWritersContendForOneShard]. It records
// failures instead of asserting, since require in a goroutine stops only that
// goroutine.
type claimant struct {
	f      *fixture
	shard  wal.ShardID
	epochs *atomic.Uint64

	// won is the entries acked to this claimant; lost counts appends refused
	// as fenced.
	won  []wal.Entry
	lost int
	// err is the first contract violation seen.
	err error
	// done is set once this claimant has both won and lost, or failed. The
	// other claimant reads it, hence atomic.
	done atomic.Bool
}

func (c *claimant) contended() bool { return len(c.won) > 0 && c.lost > 0 }

// round fences at a fresh epoch, reads the log to find the next seqno, then
// appends until fenced off.
func (c *claimant) round(appends int) {
	defer func() { c.done.Store(c.err != nil || c.contended()) }()

	// Yield so claimants interleave on a one-P runtime with an in-memory
	// backend, which never parks on I/O.
	runtime.Gosched()

	epoch := wal.Epoch(c.epochs.Add(1))

	if err := c.f.fenceErr(c.shard, epoch); err != nil {
		if !errors.Is(err, wal.ErrFenced) {
			c.err = fmt.Errorf("fencing shard %d at epoch %d: %w", c.shard, epoch, err)
		}
		// Lost the fence race: nothing written, the round does not count.
		return
	}

	// Read the tail; the other claimant may have written it.
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
		// Yield between appends too.
		runtime.Gosched()

		entry := wal.Entry{Seqno: seqno, Epoch: epoch, Payload: payloadFrom(epoch, seqno)}
		err := c.f.appendErr(c.shard, epoch, seqno, entry.Payload)
		if err == nil {
			c.won = append(c.won, entry)
			seqno++
			continue
		}
		if !errors.Is(err, wal.ErrFenced) {
			// Having fenced and read the tail, only ErrFenced is allowed; a
			// taken seqno or a gap means another writer got in, violating I4.
			c.err = fmt.Errorf("appending at seqno %d of shard %d under epoch %d: %w",
				seqno, c.shard, epoch, err)
			return
		}
		c.lost++

		// The log must keep refusing the ex-owner, at the same seqno too.
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

// shardCounter hands out shard IDs process-wide, so suites sharing a backend
// never share a shard.
var shardCounter atomic.Uint32

func newFixture(t *testing.T, log wal.Log) *fixture {
	// Tolerates a slow local cluster; fails a hung backend before the test
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
	if err := f.fenceErr(shard, epoch); err != nil {
		f.t.Fatalf("fencing shard %d at epoch %d: %v", shard, epoch, err)
	}
}

// fenceErr is fixture.fence returning the error instead of failing the test.
func (f *fixture) fenceErr(shard wal.ShardID, epoch wal.Epoch) error {
	return f.log.Fence(f.ctx, shard, epoch)
}

func (f *fixture) append(shard wal.ShardID, epoch wal.Epoch, seqno wal.Seqno, payload []byte) {
	f.t.Helper()
	if err := f.appendErr(shard, epoch, seqno, payload); err != nil {
		f.t.Fatalf("appending seqno %d of shard %d under epoch %d: %v", seqno, shard, epoch, err)
	}
}

// appendErr is fixture.append returning the error instead of failing the test.
func (f *fixture) appendErr(shard wal.ShardID, epoch wal.Epoch, seqno wal.Seqno, payload []byte) error {
	return f.log.Append(f.ctx, shard, epoch, seqno, payload)
}

// appendRun appends entriesFrom(epoch, first, count), one per call.
func (f *fixture) appendRun(shard wal.ShardID, epoch wal.Epoch, first wal.Seqno, count int) {
	f.t.Helper()
	for i := range wal.Seqno(count) {
		f.append(shard, epoch, first+i, payloadFor(first+i))
	}
}

// readErr returns the backend's answer, for reads expected to be refused.
func (f *fixture) readErr(shard wal.ShardID, from wal.Seqno, limit int) ([]wal.Entry, error) {
	return f.log.ReadFrom(f.ctx, shard, from, limit)
}

func (f *fixture) trim(shard wal.ShardID, upTo wal.Seqno) {
	f.t.Helper()
	if err := f.log.Trim(f.ctx, shard, upTo); err != nil {
		f.t.Fatalf("trimming shard %d up to %d: %v", shard, upTo, err)
	}
}

// readLog reads a shard's whole log in pages, as replay does. It returns the
// error rather than failing, because claimants call it from goroutines.
func (f *fixture) readLog(shard wal.ShardID) ([]wal.Entry, error) {
	// Most of the suite's logs fit in one page.
	const window = 64
	return readAll(f.ctx, f.log, shard, window)
}

// readAll reads a shard's whole log into a slice.
func readAll(ctx context.Context, log wal.Log, shard wal.ShardID, page int) ([]wal.Entry, error) {
	entries := make([]wal.Entry, 0, page)
	for e, err := range wal.Entries(ctx, log, shard, wal.FirstSeqno, page) {
		if err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, nil
}

// expectError checks the error matches want; a backend may wrap it.
func (f *fixture) expectError(got, want error) {
	f.t.Helper()
	require.ErrorIs(f.t, got, want)
}

// expectLog checks a shard's whole log; its callers write at most 10 entries.
func (f *fixture) expectLog(shard wal.ShardID, want []wal.Entry) {
	f.t.Helper()
	f.expectEntries(shard, wal.FirstSeqno, 10, want)
}

func (f *fixture) expectEntries(shard wal.ShardID, from wal.Seqno, limit int, want []wal.Entry) {
	f.t.Helper()

	got, err := f.log.ReadFrom(f.ctx, shard, from, limit)
	require.NoErrorf(f.t, err, "reading shard %d from %d", shard, from)
	// Nil and empty are equivalent.
	if len(want) == 0 {
		require.Emptyf(f.t, got, "reading shard %d from %d (limit %d)", shard, from, limit)
		return
	}
	require.Equalf(f.t, want, got, "reading shard %d from %d (limit %d)", shard, from, limit)
}

// expectTrimmedTo checks a log trimmed at or past its tail: everything below
// the tail is gone; the tail row may remain.
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
// recognisable.
func payloadFor(seqno wal.Seqno) []byte {
	return []byte(fmt.Sprintf("payload of entry %d", seqno))
}

// bigPayloadFor is payloadFor padded to size with a seqno-derived byte, so
// truncation cannot make two entries look alike.
func bigPayloadFor(seqno wal.Seqno, size int) []byte {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(seqno)
	}
	copy(payload, payloadFor(seqno))
	return payload
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
