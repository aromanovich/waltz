// Package coldtest is an in-memory test double for the cold store, at the
// [cold.Applier] and [cold.Watermarker] seams.
//
// Unlike cold/memcold, which stores the rows a batch carries, it records only
// the drain's outcome: refused or not, how many landed, and the watermark each
// moved. It never interprets a batch; doing so would be a second implementation
// of the store under test.
package coldtest

import (
	"context"
	"sync"

	"github.com/aromanovich/waltz/fold"
	"github.com/aromanovich/waltz/wal"
)

// Cold is both the applier and the watermark reader, so the watermark always
// reads back what the drains wrote; otherwise a shard would replay what it had
// already applied. It satisfies cold.Store.
type Cold struct {
	mu      sync.Mutex
	refusal error
	drains  int
	applied map[wal.ShardID]wal.Seqno
}

// New returns an empty store: every shard reads as having no watermark.
func New() *Cold { return &Cold{applied: map[wal.ShardID]wal.Seqno{}} }

// Refusing returns a store on which every Apply fails with err, unwrapped.
func Refusing(err error) *Cold {
	c := New()
	c.refusal = err
	return c
}

// Apply counts the drain and sets the shard's watermark to the batch's.
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

// Watermark returns the last drained seqno. A shard never drained reads as
// absent (ok false), not zero: the seam distinguishes the two, even though the
// cycle floors both at [wal.FirstSeqno] − 1.
func (c *Cold) Watermark(_ context.Context, shard wal.ShardID) (wal.Seqno, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	seqno, ok := c.applied[shard]
	return seqno, ok, nil
}

// Drains returns how many drains landed.
func (c *Cold) Drains() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.drains
}
