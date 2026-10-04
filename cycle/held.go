package cycle

import (
	"maps"
	"slices"
	"sync"

	"github.com/aromanovich/waltz/wal"
)

// held is the cycles this node holds plus the counters of superseded ones,
// under one mutex.
//
// Every read path resolves through [Manager.Shard], which takes this mutex,
// so calling into a cycle while holding it can stall every shard on the node.
// Each method therefore takes the lock, does only map arithmetic and returns;
// none calls into a *Cycle. Cycles handed back are for the caller to question
// outside the lock. Nothing in the compiler enforces this (Manager can reach
// mu): a method here that asks a cycle anything brings the deadlock back.
type held struct {
	mu      sync.Mutex
	shards  map[wal.ShardID]*Cycle
	retired Totals
	// closed is set by [held.takeAll], so shutdown is distinguishable from
	// the empty map before the first acquire.
	closed bool
}

func newHeld() *held { return &held{shards: make(map[wal.ShardID]*Cycle)} }

// get is the shard's current cycle, or nil where this node holds none.
func (h *held) get(shard wal.ShardID) *Cycle {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.shards[shard]
}

// install puts fresh in the shard's slot and returns what it displaced, for
// the caller to retire with no lock held. took is false after [held.takeAll];
// the caller must then retire fresh, since nothing would drain it.
func (h *held) install(shard wal.ShardID, fresh *Cycle) (previous *Cycle, took bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, false
	}
	previous = h.shards[shard]
	h.shards[shard] = fresh
	return previous, true
}

// takeAll empties the map and refuses further installs, so a second Close
// finds nothing and no acquire lands after shutdown.
func (h *held) takeAll() []*Cycle {
	h.mu.Lock()
	defer h.mu.Unlock()
	all := h.list()
	h.shards = make(map[wal.ShardID]*Cycle)
	h.closed = true
	return all
}

// retire adds a superseded cycle's counters to the node's total. Seqnos stay
// out: the next cycle inherits them, so summing double-counts.
func (h *held) retire(counted Counters) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.retired.Counters.add(counted)
	h.retired.Epochs++
}

// totals returns the retired block and the live cycles from one moment, so no
// cycle is counted both as retired and as live. [Manager.Totals] questions the
// cycles outside the lock.
func (h *held) totals() (Totals, []*Cycle) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.retired, h.list()
}

// list is the map's values. Callers hold the lock.
func (h *held) list() []*Cycle { return slices.Collect(maps.Values(h.shards)) }
