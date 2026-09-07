// The contract is asserted by the suite in waltest and by nothing else. The two
// tests below it pin the promises this backend makes as a substitute, where it
// could differ from every real backend in a way the contract has no words for.
package memwal_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/aromanovich/waltz/wal"
	"github.com/aromanovich/waltz/wal/memwal"
	"github.com/aromanovich/waltz/wal/waltest"
)

// TestMemWALContractSuite runs the conformance suite against a Backend of its
// own: the suite is entitled to one with no shards.
func TestMemWALContractSuite(t *testing.T) {
	waltest.RunContractSuite(t, memwal.New())
}

// TestPayloadsAreNobodyElsesMemory pins the copy on both sides of the boundary.
// A backend that serializes keeps this by accident; here a caller reusing an
// append buffer, or writing into an entry it read back, would corrupt the log.
func TestPayloadsAreNobodyElsesMemory(t *testing.T) {
	ctx := context.Background()
	log := memwal.New()
	const shard, epoch = wal.ShardID(1), wal.Epoch(1)
	require.NoError(t, log.Fence(ctx, shard, epoch))

	// The caller reuses the buffer it appended, as an encoder with a scratch
	// slice does.
	buffer := []byte("the entry as it was written")
	require.NoError(t, log.Append(ctx, shard, epoch, wal.FirstSeqno, buffer))
	copy(buffer, "OVERWRITTEN AFTER THE APPEND")

	entries, err := log.ReadFrom(ctx, shard, wal.FirstSeqno, 10)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, []byte("the entry as it was written"), entries[0].Payload,
		"the log handed out the caller's own buffer at append time")

	// And the caller writes into what it read, as a decoder unmarshalling in
	// place does.
	copy(entries[0].Payload, "OVERWRITTEN AFTER THE READ")

	entries, err = log.ReadFrom(ctx, shard, wal.FirstSeqno, 10)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, []byte("the entry as it was written"), entries[0].Payload,
		"the log handed out its own memory at read time")
}

// TestATrimmedLogRemembersWhereItIs pins the next seqno surviving a trim that
// takes every entry. That such an append is refused at all is the suite's claim
// now; what is here is this backend's answer to it — every seqno the log gave
// out is *taken*, which is what having the position rather than deriving it can
// say, and what the contract leaves each backend to decide.
func TestATrimmedLogRemembersWhereItIs(t *testing.T) {
	ctx := context.Background()
	log := memwal.New()
	const shard, epoch = wal.ShardID(1), wal.Epoch(1)
	require.NoError(t, log.Fence(ctx, shard, epoch))

	const count = 3
	for i := range wal.Seqno(count) {
		seqno := wal.FirstSeqno + i
		require.NoError(t, log.Append(ctx, shard, epoch, seqno, []byte("entry")))
	}
	require.NoError(t, log.Trim(ctx, shard, wal.FirstSeqno+count))

	entries, err := log.ReadFrom(ctx, shard, wal.FirstSeqno, 10)
	require.NoError(t, err)
	require.Empty(t, entries, "the trim left entries behind")

	// Every seqno the log gave out is refused as taken: the entries are gone,
	// what they occupied is not. A backend deriving the answer from its rows
	// says ErrGap here, the rows that answer would come from being what the
	// trim deleted, and the contract picks neither.
	for i := range wal.Seqno(count) {
		err := log.Append(ctx, shard, epoch, wal.FirstSeqno+i, []byte("again"))
		require.ErrorIsf(t, err, wal.ErrAlreadyWritten,
			"appending at the trimmed seqno %d", wal.FirstSeqno+i)
	}
	// And the log continues where it left off rather than where it starts.
	require.NoError(t, log.Append(ctx, shard, epoch, wal.FirstSeqno+count, []byte("the next entry")))

	entries, err = log.ReadFrom(ctx, shard, wal.FirstSeqno, 10)
	require.NoError(t, err)
	require.Equal(t, []wal.Entry{
		{Seqno: wal.FirstSeqno + count, Epoch: epoch, Payload: []byte("the next entry")},
	}, entries)
}
