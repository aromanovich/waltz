package cycle

// The ExecutionStore wrapper's write entry point, and the translation of a
// cycle's answer into errors the history service understands.
//
// Nothing here wraps errors: ContextImpl.handleWriteErrorLocked type-switches
// on concrete types, so a wrapped one becomes an unknown outcome and a
// background re-acquire.

import (
	"context"
	"fmt"

	p "go.temporal.io/server/common/persistence"

	"github.com/aromanovich/waltz/baserow"
	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/wal"
)

// Write acks one intercepted write into its shard's log and reports what the
// drain carrying it did. base carries the cold-store reads the condition
// authority needs for assertions the window does not determine ([baserow.Rows]).
//
// epoch is the caller's rangeID, checked as the plugin's own write would
// (I11); otherwise a fenced-out shard context's write would be accepted under
// this node's epoch. Zero means none (deletes, range-complete); the drain's
// epoch CAS fences those.
//
// With no cycle for the shard the answer is ShardOwnershipLost: writing to the
// store directly would bypass the log and break the next drain's assertions.
func (m *Manager) Write(
	ctx context.Context,
	mut mutation.Mutation,
	epoch wal.Epoch,
	base *baserow.Rows,
) error {
	shard := wal.ShardID(mut.ShardID())
	c := m.Shard(shard)
	if c == nil {
		return lost(shard, "this node holds no apply cycle for it")
	}
	if epoch != 0 && epoch != c.Epoch() {
		return lost(shard, fmt.Sprintf("the write carries epoch %d, this node's cycle holds %d", epoch, c.Epoch()))
	}
	// State is read after the write: the halt [storeError] translates is
	// usually one this write caused.
	err := c.write(ctx, mut, base)
	return storeError(c.State(), shard, err)
}

func lost(shard wal.ShardID, why string) error {
	return &p.ShardOwnershipLostError{
		ShardID: int32(shard),
		Msg:     fmt.Sprintf("shard %d is not this node's: %s", shard, why),
	}
}
