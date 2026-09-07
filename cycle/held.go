package cycle

import (
	"sync"

	"github.com/aromanovich/waltz/wal"
)

// held is the cycles this node holds and the counters of the ones it has
// retired, with the one mutex over both.
//
// A type rather than three fields on [Manager], for the reason the lock exists
// and the reason it is dangerous: every read path resolves through
// [Manager.Shard], which takes this mutex, so any code that calls into a
// cycle's goroutine while holding it stops every shard on the node — one cycle
// blocked inside a base read and the whole registry waits behind it.
//
// Each method below takes the lock, finishes its map arithmetic and returns.
// None of them hands out the lock and none of them calls into a `*Cycle`: where
// one is handed back it is for the caller to question outside the lock, which
// is what [held.totals] says of its second result. So there is no lock on
// [Manager] a caller could hold across a call into a cycle — which is a
// property of these six bodies and not of the type, since `held` is a package
// neighbour of `Manager` and its mutex is reachable from there. A method added
// here that asks a cycle anything puts the inversion straight back.
type held struct {
	mu      sync.Mutex
	shards  map[wal.ShardID]*Cycle
	retired Totals
}

func newHeld() *held { return &held{shards: make(map[wal.ShardID]*Cycle)} }

// get is the shard's current cycle, or nil where this node holds none.
func (h *held) get(shard wal.ShardID) *Cycle {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.shards[shard]
}

// install puts fresh in the shard's slot and hands back whatever it displaced,
// so the caller can count and retire it with no lock held.
func (h *held) install(shard wal.ShardID, fresh *Cycle) (previous *Cycle) {
	h.mu.Lock()
	defer h.mu.Unlock()
	previous = h.shards[shard]
	h.shards[shard] = fresh
	return previous
}

// takeAll empties the map and returns what was in it: shutdown's one step, so
// nothing is left for a second Close to drain.
func (h *held) takeAll() []*Cycle {
	h.mu.Lock()
	defer h.mu.Unlock()
	all := h.list()
	h.shards = make(map[wal.ShardID]*Cycle)
	return all
}

// retire adds a superseded cycle's counters to the node's running total. Its
// seqnos stay out: a fresh cycle inherits the log's, so summing them
// double-counts.
func (h *held) retire(counted Counters) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.retired.Counters.add(counted)
	h.retired.Epochs++
}

// totals hands back the retired block and the live cycles together, since a
// reading that mixed two moments could count one cycle's counters twice — in
// the retired block it had just joined and in the snapshot it had not yet left.
// The cycles are returned unasked: [Manager.Totals] questions them outside the
// lock.
func (h *held) totals() (Totals, []*Cycle) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.retired, h.list()
}

// list is the map's values. Callers hold the lock.
func (h *held) list() []*Cycle {
	all := make([]*Cycle, 0, len(h.shards))
	for _, c := range h.shards {
		all = append(all, c)
	}
	return all
}
