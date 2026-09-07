package checker_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/aromanovich/waltz/verify/checker"
	"github.com/aromanovich/waltz/wal"
)

// look is one observation of one shard: the entries it saw, at the epochs it
// saw them, and the watermark it read afterwards.
func look(shard wal.ShardID, watermark wal.Seqno, entries ...checker.Observed) checker.Sample {
	return checker.Sample{
		Shard:           shard,
		Entries:         entries,
		WatermarkBefore: watermark,
		WatermarkAfter:  watermark,
		HasWatermark:    watermark > 0,
	}
}

func at(epoch wal.Epoch, seqnos ...wal.Seqno) []checker.Observed {
	out := make([]checker.Observed, 0, len(seqnos))
	for _, s := range seqnos {
		out = append(out, checker.Observed{Seqno: s, Epoch: epoch})
	}
	return out
}

// A sample is one look and every one of them is partial: the observer loses
// reads and a trim takes prefixes, so what the run reached is the maximum over
// looks and not what the last one happened to hold.
func TestTheAccountIsTheMaximumOverPartialLooks(t *testing.T) {
	account := checker.AccountOf([]checker.Sample{
		look(1, 3, at(7, 1, 2, 3, 4)...),
		// A later look, after a trim: fewer entries, further along.
		look(1, 6, at(7, 5, 6, 7)...),
		// One the observer lost most of.
		look(1, 0),
	})

	log := account[1]
	require.EqualValues(t, 7, log.Tail, "the log reached seqno 7, even though no one look held all of it")
	require.EqualValues(t, 6, log.Watermark, "and the cold store caught up to 6")
	require.Equal(t, 4, log.Longest, "the best single look held four entries")

	blocks := log.Blocks()
	require.Len(t, blocks, 1)
	require.Equal(t, checker.Block{Epoch: 7, Lowest: 1, Highest: 7, Entries: 7}, blocks[0],
		"the epoch's block is the union of what the looks saw of it")
}

func TestTwoLooksAtOneEntryCountItOnce(t *testing.T) {
	account := checker.AccountOf([]checker.Sample{
		look(1, 1, at(4, 1, 2, 3)...),
		look(1, 2, at(4, 2, 3, 4)...),
	})

	block, ok := account[1].Block(4)
	require.True(t, ok)
	require.Equal(t, 4, block.Entries, "the two overlapping looks hold four entries between them")
}

// The fence cuts the log at a seqno, so an owner's block sits wholly above the
// one before it. Entries of two epochs interleaving is I4 broken, and until now
// nothing but a fifteen-minute cluster run could say so.
func TestInterleavedBlocksAreWhatAFenceMakesImpossible(t *testing.T) {
	clean := checker.AccountOf([]checker.Sample{
		look(1, 0, append(at(7, 1, 2, 3), at(8, 4, 5)...)...),
	})
	require.Empty(t, clean[1].Interleaved(), "two owners in acquire order interleave nothing")

	broken := checker.AccountOf([]checker.Sample{
		// The zombie's 4 lands above the successor's 3: two writers held the
		// shard at once.
		look(1, 0, append(at(7, 1, 2, 4), at(8, 3, 5)...)...),
	})
	pairs := broken[1].Interleaved()
	require.Len(t, pairs, 1)
	require.EqualValues(t, 7, pairs[0][0].Epoch)
	require.EqualValues(t, 8, pairs[0][1].Epoch)
}

func TestAnEpochThatNeverAppendedHasNoBlock(t *testing.T) {
	account := checker.AccountOf([]checker.Sample{look(1, 0, at(9, 1, 2)...)})

	_, ok := account[1].Block(8)
	require.False(t, ok, "the claimant that was fenced before its first append wrote nothing")

	block, ok := account[1].Block(9)
	require.True(t, ok)
	require.Equal(t, 2, block.Entries)
}

func TestShardsAreAccountedForApart(t *testing.T) {
	account := checker.AccountOf([]checker.Sample{
		look(1, 5, at(7, 1, 2)...),
		look(2, 9, at(7, 1, 2, 3)...),
	})

	require.Len(t, account, 2)
	require.EqualValues(t, 2, account[1].Tail)
	require.EqualValues(t, 3, account[2].Tail)
	require.EqualValues(t, 9, account[2].Watermark)
	require.Contains(t, account[2].String(), "epoch 7: seqnos 1..3 (3 entries)")
}
