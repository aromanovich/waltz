package guard

// The two lines that say the cycle and the wrapper were built for each other.
//
// wrapper.ShardObserver has exactly one method because exactly one persistence
// call reports a shard changing hands (#44), and the cycle is its first
// implementation (#55): an acquire fences the log at the new epoch and installs
// the cycle for it, in that order, and nothing else in the API says a shard was
// ever given back.
//
// wrapper.ShardLayer is the other direction and intercept mode's whole seam
// (#57, #78, #80, #81): the ExecutionStore wrapper hands one mutable-state write
// down and asks the same value for the three reads — the two mutable-state ones
// and the task range; the registry routes each to the shard's cycle and
// translates what came back into what the history service type-switches on. It
// had a face of its own until #142 — the layer being told what its queues had
// deleted — and it is gone with the compensation it existed for: a range delete
// is one of the writes now.
//
// ShardLayer **is** the observer as well, and the line above it is therefore no
// longer a separate wiring claim but the first of its three: the option holds
// one value, so a layer that answered writes and reads while nobody had told it
// about the acquire is a state the type no longer has. The halves stay named
// separately below because they were built phases apart and each remains a
// claim of its own — a `cycle.Manager` that lost any one of them fails here by
// the name of the half it lost, rather than at the composed interface where
// three claims read as one.
//
// wrapper.MetricsSink is the third, and it is the only one that carries
// something *into* the layer (#59): the server's metrics handler exists only
// once it builds a data store factory, which is after the main has composed the
// registry, so the registry is told afterwards. It is a face of ShardLayer now,
// so waltz.go enforces it by compiling and the line below is the named-failure
// half — before that it was structural typing between two packages that never
// mention each other, and a renamed method left the layer silently unmetered.
//
// They live here rather than in either package because neither may depend on
// the other for them: wrapper is defined over upstream's interface and knows
// nothing about the WAL layer's insides, and the cycle knows nothing about
// persistence stores. Putting the two together is the composition's business.

import (
	"github.com/aromanovich/waltz/cycle"
	"github.com/aromanovich/waltz/wrapper"
)

var (
	_ wrapper.ShardObserver = (*cycle.Manager)(nil)
	_ wrapper.ShardWriter   = (*cycle.Manager)(nil)
	_ wrapper.ShardReader   = (*cycle.Manager)(nil)
	_ wrapper.ShardLayer    = (*cycle.Manager)(nil)
	_ wrapper.MetricsSink   = (*cycle.Manager)(nil)
)
