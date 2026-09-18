package waltest

// The other way a backend breaks guarantee 5 with every call succeeding: it
// answers a read with fewer entries than it holds, and a page short of its limit
// is the end of the log to everything that reads one.

import (
	"context"

	"github.com/aromanovich/waltz/wal"
)

// Truncating is a [wal.Log] answering every read with at most Cap entries,
// however many the caller asked for and however many are there. That is how a
// backend pages when its real limit is a response size rather than a row count —
// a message size, a query response, a driver's row buffer — and it is legal-
// looking from outside: no error, ascending seqnos, a page like any other.
//
// It is not a log a backend may be, which is what tells it from [Faulty]:
// a faulting log refuses calls, as a correct backend does under load, while this
// one succeeds and answers less than it holds. [Expiring] is the other of its
// kind. It is here so that a caller reading a whole log can be shown to check
// that it reached the end rather than to trust a short page for it.
type Truncating struct {
	wal.Log

	// Cap is the most entries a read answers with. Zero or less caps nothing.
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
