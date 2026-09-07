package cycle

// The boundary the ExecutionStore wrapper writes through: one method, and the
// translation of what a cycle answers into what the history service's write
// path understands. It is here because the wrapper may not import the plugin.
//
// Nothing here wraps: ContextImpl.handleWriteErrorLocked type-switches on
// concrete types with no errors.As, so an unrecognised value becomes an unknown
// outcome and a background re-acquire.

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
// epoch is the rangeID the caller wrote under, checked as the plugin's own
// AssertShard(rangeID) would (I11): without it a shard context already fenced
// out has its write re-stamped with this node's epoch and accepted. Zero means
// no rangeID, as on the deletes, which the drain's epoch CAS fences instead.
//
// A shard this node holds no cycle for is answered with ShardOwnershipLost.
// Falling through to the store below would be a write around the log, and the
// next drain would assert a base version that write already moved.
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
	// Two statements, because the state is read after the write: the halt
	// [storeError] translates is usually the one this write discovered.
	err := c.write(ctx, mut, base)
	return storeError(c.State(), shard, err)
}

func lost(shard wal.ShardID, why string) error {
	return &p.ShardOwnershipLostError{
		ShardID: int32(shard),
		Msg:     fmt.Sprintf("shard %d is not this node's to write: %s", shard, why),
	}
}
