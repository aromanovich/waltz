// Package coldtest is the cold store a drain lands in, in memory.
//
// It is the second adapter here at the two seams the apply cycle reaches the
// cold store through, [cold.Applier] and [cold.Watermarker]. The other is
// cold/memcold, which is a store: it writes the rows a batch carries and answers
// what became of them. This one answers the drain's outcome instead — refused,
// counted, the watermark read back — which is what a package composing a layer
// to test something else needs and what a store has no way to be asked for.
//
// It is a double and not a cold store: it counts the drains that landed and the
// watermark each moved, and interprets nothing. What a folded batch *means* has
// no specification apart from a real store's behaviour, so a memory store that
// answered that question would be a second implementation of the thing under
// test rather than a fixture for it.
package coldtest

import (
	"context"
	"sync"

	"github.com/aromanovich/waltz/fold"
	"github.com/aromanovich/waltz/wal"
)

// Cold is one cold store: an applier and the watermark reader beside it. One
// value serves as both, which is the point — a composition whose drains land
// somewhere the watermark does not read back is a shard that replays what it
// already applied.
//
// It satisfies cold.Applier and cold.Watermarker — and so cold.Store — by
// shape, which is the whole of what it stands in for.
type Cold struct {
	mu      sync.Mutex
	refusal error
	drains  int
	applied map[wal.ShardID]wal.Seqno
}

// New is a cold store no drain has written to: every shard reads as having no
// watermark, which is the state a replay starts from.
func New() *Cold { return &Cold{applied: map[wal.ShardID]wal.Seqno{}} }

// Refusing is a cold store every drain fails against, for a test whose point is
// that no drain belongs in it. The refusal is returned unwrapped, so what a
// caller classifies is what it passed in.
func Refusing(err error) *Cold {
	c := New()
	c.refusal = err
	return c
}

// Apply records the drain and moves the shard's watermark to what the batch
// carried.
func (c *Cold) Apply(_ context.Context, shard wal.ShardID, _ wal.Epoch, batch fold.Batch) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.refusal != nil {
		return c.refusal
	}
	c.drains++
	c.applied[shard] = batch.Watermark()
	return nil
}

// Watermark is what this store's own drains moved, so a shard nothing drained
// reads as absent rather than as zero — the two answers the seam distinguishes,
// and the ones a real store may not collapse even though the cycle floors both
// at [wal.FirstSeqno] − 1.
func (c *Cold) Watermark(_ context.Context, shard wal.ShardID) (wal.Seqno, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	seqno, ok := c.applied[shard]
	return seqno, ok, nil
}

// Drains is how many landed here.
func (c *Cold) Drains() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.drains
}
