package memcold

// Internal, because moving a watermark takes the transaction the caller is
// already in and this store hands nobody outside the package one.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/aromanovich/waltz/wal"
)

func newWatermarkStore(t *testing.T) *Store {
	t.Helper()
	store, release, err := New("memcold-watermark")
	require.NoError(t, err)
	t.Cleanup(release)
	return store
}

// move is a drain that commits: one transaction, one watermark, nothing else.
func move(t *testing.T, s *Store, shard wal.ShardID, seqno wal.Seqno) {
	t.Helper()
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx)
	require.NoError(t, err)
	require.NoError(t, SetWatermark(ctx, tx, shard, seqno))
	require.NoError(t, tx.Commit())
}

func TestShardWithNoWatermarkReadsAbsent(t *testing.T) {
	seqno, ok, err := newWatermarkStore(t).Watermark(context.Background(), 1)
	require.NoError(t, err)
	require.False(t, ok)
	require.Zero(t, seqno)
}

func TestWatermarkReadsBackWhatTheDrainWrote(t *testing.T) {
	s := newWatermarkStore(t)
	move(t, s, 1, 7)

	seqno, ok, err := s.Watermark(context.Background(), 1)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, wal.Seqno(7), seqno)
}

func TestWatermarkMovedTwiceReadsTheSecond(t *testing.T) {
	s := newWatermarkStore(t)
	move(t, s, 1, 7)
	move(t, s, 1, 12)

	seqno, ok, err := s.Watermark(context.Background(), 1)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, wal.Seqno(12), seqno)
}

func TestShardsDoNotShareAWatermark(t *testing.T) {
	ctx := context.Background()
	s := newWatermarkStore(t)
	move(t, s, 1, 7)
	move(t, s, 2, 3)

	first, ok, err := s.Watermark(ctx, 1)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, wal.Seqno(7), first)

	second, ok, err := s.Watermark(ctx, 2)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, wal.Seqno(3), second)

	_, ok, err = s.Watermark(ctx, 3)
	require.NoError(t, err)
	require.False(t, ok)
}

// The seam's whole point: a watermark that could survive its transaction being
// rolled back would vouch for rows that are not there.
func TestWatermarkOfARolledBackTransactionIsNotThere(t *testing.T) {
	ctx := context.Background()
	s := newWatermarkStore(t)
	move(t, s, 1, 7)

	tx, err := s.db.BeginTx(ctx)
	require.NoError(t, err)
	require.NoError(t, SetWatermark(ctx, tx, 1, 12))
	require.NoError(t, tx.Rollback())

	seqno, ok, err := s.Watermark(ctx, 1)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, wal.Seqno(7), seqno)
}
