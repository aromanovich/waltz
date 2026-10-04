package wal

import (
	"context"
	"fmt"
	"iter"
)

// EntryAt reads the entry at seqno, reporting whether it exists. Use it
// rather than [Log.ReadFrom] to ask about one seqno: ReadFrom returns entries
// at or above from, so after a trim the first entry may be a later one.
//
// It returns the entry, or none, or an error: the log's, or a refusal when
// the log answered with a higher seqno.
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

// Entries reads a shard's log from a seqno in pages, yielding every entry in
// seqno order. It is a free function so backends need only [Log.ReadFrom].
//
// A page shorter than requested is the end of the log.
//
// A failed read yields the zero entry with the error and stops. A caller
// that ignores the error sees a truncated log, so one that must tell the end
// of the log from a failure has to check it.
//
// A full page ending below from is refused as an error. A backend that
// ignores from never returns a short page, so the loop would otherwise spin
// until the context expired.
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
