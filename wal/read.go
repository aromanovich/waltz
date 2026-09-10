package wal

import (
	"context"
	"fmt"
	"iter"
)

// Entries reads a shard's log from a seqno in pages, yielding every entry in
// seqno order. It is a free function rather than a method on [Log] so that
// paging costs a backend author nothing: what it needs is [Log.ReadFrom].
//
// The loop ends where the log does. A page shorter than the one asked for is
// the log's end, which is the whole of the termination rule and the one fact
// about [Log.ReadFrom] that every caller reading a whole log would otherwise
// have to hold.
//
// A failed read yields the zero entry with the error and stops. Stopping is
// what makes ignoring that error a truncated log rather than a wrong one, but
// the log is then short of whatever the failed page held: a caller that has to
// tell "the log ends here" from "the read failed" must look at it.
//
// A full page that ends below the seqno it was asked for is refused rather than
// read again. A backend that ignores from never returns a short page, so the
// loop would otherwise spin until the context gave out minutes later — which
// reads as a slow cluster and is a backend bug.
// EntryAt reads back the one entry a seqno holds, and whether it holds one. It
// is a free function for [Entries]' reason — what it needs is [Log.ReadFrom] —
// and it exists because "does the log hold seqno n" is a question no caller can
// put to that method as it stands: a read is *at or above* from, so the first
// entry it answers with may be a higher one, which is legal after a trim and is
// no answer about the seqno asked for. Every caller settling one position would
// otherwise re-derive that, and the one that got it wrong would read a later
// entry as the one it was asking about.
//
// A caller reading a seqno it may itself have written wants this rather than a
// page: the answer is one entry or none, and the error is the log's own.
func EntryAt(ctx context.Context, log Log, shard ShardID, seqno Seqno) (Entry, bool, error) {
	entries, err := log.ReadFrom(ctx, shard, seqno, 1)
	switch {
	case err != nil:
		return Entry{}, false, fmt.Errorf("wal: reading shard %d at seqno %d: %w", shard, seqno, err)
	case len(entries) == 0:
		return Entry{}, false, nil
	case entries[0].Seqno != seqno:
		return Entry{}, false, fmt.Errorf(
			"wal: shard %d answered a read from seqno %d with seqno %d, so it holds nothing at the "+
				"one asked about", shard, seqno, entries[0].Seqno)
	}
	return entries[0], true, nil
}

func Entries(ctx context.Context, log Log, shard ShardID, from Seqno, page int) iter.Seq2[Entry, error] {
	return func(yield func(Entry, error) bool) {
		for {
			entries, err := log.ReadFrom(ctx, shard, from, page)
			if err != nil {
				yield(Entry{}, fmt.Errorf("wal: reading shard %d from seqno %d: %w", shard, from, err))
				return
			}
			for _, e := range entries {
				if !yield(e, nil) {
					return
				}
			}
			if len(entries) < page {
				return
			}
			last := entries[len(entries)-1].Seqno
			if last < from {
				yield(Entry{}, fmt.Errorf(
					"wal: shard %d returned a full page ending at seqno %d for a read from %d: "+
						"the reads are not advancing", shard, last, from))
				return
			}
			from = last + 1
		}
	}
}
