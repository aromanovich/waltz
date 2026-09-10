// The contract is asserted by the suite in waltest and by nothing else. The
// tests below it pin the promises this backend makes as a substitute, where it
// could differ from every real backend in a way the contract has no words for —
// and, in the retention case, prove an instrument the suite cannot carry.
package memwal_test

import (
	"context"
	"testing"
	"time"

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

// TestTheRetentionCheckIsNotVacuous is where waltest.CheckRetention is proved,
// since a deployment running it against its own storage has no way to tell a
// pass from a check that would have passed anything. Both directions, on a
// window short enough to be a unit test: this backend keeps what it acked, and
// one wrapped so its entries expire does not — which is the shape a retention
// window, a TTL on a table and a compaction that drops old records all have.
//
// The check itself costs the window it is given, which is why it is a function a
// deployment calls rather than a case in the suite.
func TestTheRetentionCheckIsNotVacuous(t *testing.T) {
	ctx := context.Background()
	const shard, epoch, window = wal.ShardID(4), wal.Epoch(7), 20 * time.Millisecond

	require.NoError(t, waltest.CheckRetention(ctx, memwal.New(), shard, epoch, window),
		"this backend takes entries away at a trim and at nothing else")

	expires := waltest.Expiring(memwal.New(), window/4)
	err := waltest.CheckRetention(ctx, expires, shard, epoch, window)
	require.Error(t, err, "a log whose entries expire passed a check for entries that expire")
	require.Contains(t, err.Error(), "holds 0 of its 3 entries after "+window.String(),
		"the failure has to name the moment as well as the shortfall: one raised before the wait "+
			"is about an append that never landed, which is a different fault and not this one's")
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
