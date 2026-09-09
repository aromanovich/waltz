package acceptance

// The one caller obligation on wal.Log.Trim, enforced instead of trusted.
//
// upTo may never exceed the seqno the cold store has committed: above it the log
// is the only copy, and a trim is the only operation in the log contract that
// neither refuses nor halts nor can be undone. No backend can check it — a log
// knows nothing about a cold store — so the check has to live where both are in
// reach, which is here.
//
// The layer keeps the obligation in three prose sites and no enforced one:
// Drained is reached only after a settlesForward (cycle.go), Settle moves
// applied only under MoveWatermark (tailstate.go), and Drained is handed
// Applied rather than Commit. Break any of the three and the resulting Trim is
// perfectly contract-legal while deleting an acked entry no store holds, and
// nothing in wal/, in wal/waltest or in the handbook's contract table would
// report it. This guard is what reports it.
//
// It samples nothing: every trim the run makes goes through Trim below.

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/aromanovich/waltz/cold"
	"github.com/aromanovich/waltz/wal"
)

// trimGuard is a wal.Log that judges what its caller asks it to delete, against
// the watermark the cold store has actually committed.
type trimGuard struct {
	wal.Log
	mark cold.Watermarker

	trims  atomic.Int64
	broken atomic.Pointer[string]
}

func (g *trimGuard) Trim(ctx context.Context, shard wal.ShardID, upTo wal.Seqno) error {
	g.trims.Add(1)
	// Read before the delete, not after: a drain committing between the two
	// moves the watermark up, which can only make the comparison stricter than
	// the moment it is about.
	committed, found, err := g.mark.Watermark(ctx, shard)
	switch {
	case err != nil:
		g.fail(fmt.Sprintf("shard %d: the watermark could not be read while a trim to %d was in flight: %v",
			shard, upTo, err))
	case !found:
		g.fail(fmt.Sprintf("shard %d: a trim to %d, and no drain has ever committed for this shard: "+
			"every entry it deletes is one only the log holds", shard, upTo))
	case upTo > committed:
		g.fail(fmt.Sprintf("shard %d: a trim to %d, past the committed watermark %d: "+
			"%d acked entries the cold store does not hold would be deleted",
			shard, upTo, committed, upTo-committed))
	}
	return g.Log.Trim(ctx, shard, upTo)
}

// fail records the first violation, since a later trim would otherwise overwrite
// the one that matters.
func (g *trimGuard) fail(why string) {
	g.broken.CompareAndSwap(nil, &why)
}

// violation is what the guard saw, or the empty string.
func (g *trimGuard) violation() string {
	if s := g.broken.Load(); s != nil {
		return *s
	}
	return ""
}
