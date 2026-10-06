package waltest

// Another way a backend breaks guarantee 5 while every call succeeds: a read
// returns fewer entries than it holds, and readers treat a short page as the
// end of the log.

import (
	"context"

	"github.com/aromanovich/waltz/wal"
)

// Truncating is a [wal.Log] that answers every read with at most Cap entries,
// whatever the limit. A backend paging by response size rather than row count
// behaves like this, with no error and a normal-looking page.
//
// Like [Expiring] and [Unfenced], it breaks the contract on purpose. It shows
// that a caller reading a whole log checks it reached the end rather than
// trusting a short page.
type Truncating struct {
	wal.Log

	// Cap is the most entries a read returns; zero or less means no cap.
	Cap int
}

func (t Truncating) ReadFrom(
	ctx context.Context, shard wal.ShardID, from wal.Seqno, limit int,
) ([]wal.Entry, error) {
	if t.Cap > 0 && limit > t.Cap {
		limit = t.Cap
	}
	return t.Log.ReadFrom(ctx, shard, from, limit)
}
