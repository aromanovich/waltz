package checker

import (
	"fmt"
	"slices"
	"strings"

	"github.com/aromanovich/waltz/wal"
)

// Account is the log's own account of a run, folded out of the samples an
// observer kept: per shard, how far the log reached and how far the cold store
// caught up, and per epoch inside it, the block of seqnos that epoch wrote.
//
// Every number in it is a maximum across looks, and that is the whole of what
// this fold knows that a caller writing it inline keeps having to remember: a
// [Sample] is one look at one shard, the observer may lose a read, and a trim
// may have taken a prefix between two of them. So a shard's tail is the highest
// seqno *any* sample saw, and an epoch's block is the union of what every
// sample saw of it.
//
// What it is not is a second journal. It answers questions about the log's
// shape — which epochs wrote, where their blocks sit, how far the run got —
// where the journal answers questions about what was acked.
type Account map[wal.ShardID]ShardLog

// AccountOf folds the samples. Samples of any number of shards may be mixed:
// that is what an observer's [Sampler.SampleAll] produces.
func AccountOf(samples []Sample) Account {
	out := Account{}
	for _, s := range samples {
		log := out[s.Shard]
		if log.blocks == nil {
			log.blocks = map[wal.Epoch]*block{}
		}
		if len(s.Entries) > log.Longest {
			log.Longest = len(s.Entries)
		}
		if s.HasWatermark && s.WatermarkAfter > log.Watermark {
			log.Watermark = s.WatermarkAfter
		}
		for _, e := range s.Entries {
			if e.Seqno > log.Tail {
				log.Tail = e.Seqno
			}
			b := log.blocks[e.Epoch]
			if b == nil {
				b = &block{lowest: e.Seqno, highest: e.Seqno, seqnos: map[wal.Seqno]bool{}}
				log.blocks[e.Epoch] = b
			}
			b.lowest = min(b.lowest, e.Seqno)
			b.highest = max(b.highest, e.Seqno)
			b.seqnos[e.Seqno] = true
		}
		out[s.Shard] = log
	}
	return out
}

// ShardLog is one shard's account.
type ShardLog struct {
	// Tail is the highest seqno any sample saw.
	Tail wal.Seqno
	// Watermark is the highest watermark any sample saw: how far apply got.
	Watermark wal.Seqno
	// Longest is the most entries one sample held — what a journal that
	// remembered nothing would have at its best single look.
	Longest int

	blocks map[wal.Epoch]*block
}

type block struct {
	lowest, highest wal.Seqno
	// seqnos counts entries without double-counting the ones two samples both
	// saw, which every overlapping pair of looks produces.
	seqnos map[wal.Seqno]bool
}

// Block is what one epoch wrote: the seqnos it holds, and the range they sit
// in. A fence cuts the log at a seqno, so blocks are disjoint and in acquire
// order — see [ShardLog.Interleaved].
type Block struct {
	Epoch           wal.Epoch
	Lowest, Highest wal.Seqno
	Entries         int
}

// Blocks are the shard's epoch blocks in acquire order, which is epoch order.
func (l ShardLog) Blocks() []Block {
	out := make([]Block, 0, len(l.blocks))
	for epoch, b := range l.blocks {
		out = append(out, Block{
			Epoch: epoch, Lowest: b.lowest, Highest: b.highest, Entries: len(b.seqnos),
		})
	}
	slices.SortFunc(out, func(a, b Block) int { return int(a.Epoch) - int(b.Epoch) })
	return out
}

// Block is what one epoch wrote, and whether it wrote at all. An epoch with no
// block is an owner that never appended — which for a fenced-off claimant is
// the claim, and for a generation that was supposed to write is a finding.
func (l ShardLog) Block(epoch wal.Epoch) (Block, bool) {
	b, ok := l.blocks[epoch]
	if !ok {
		return Block{Epoch: epoch}, false
	}
	return Block{Epoch: epoch, Lowest: b.lowest, Highest: b.highest, Entries: len(b.seqnos)}, true
}

// Interleaved is the pairs of blocks whose seqno ranges overlap, and it is
// empty on a correct log. Two owners' entries interleaving is I4 broken: the
// fence cuts the log at a seqno and the next owner continues above it, so an
// epoch cannot write inside another's range — whatever either owner believed
// about who held the shard.
func (l ShardLog) Interleaved() [][2]Block {
	blocks := l.Blocks()
	var out [][2]Block
	for i := range blocks {
		for j := i + 1; j < len(blocks); j++ {
			if blocks[i].Highest >= blocks[j].Lowest && blocks[j].Highest >= blocks[i].Lowest {
				out = append(out, [2]Block{blocks[i], blocks[j]})
			}
		}
	}
	return out
}

// String renders the blocks in epoch order, for the line a green run leaves
// behind.
func (l ShardLog) String() string {
	blocks := l.Blocks()
	if len(blocks) == 0 {
		return "no entries observed"
	}
	parts := make([]string, 0, len(blocks))
	for _, b := range blocks {
		parts = append(parts, fmt.Sprintf("epoch %d: seqnos %d..%d (%d entries)",
			b.Epoch, b.Lowest, b.Highest, b.Entries))
	}
	return strings.Join(parts, ", ")
}
