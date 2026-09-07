// Package foldrun drives mutations through one accumulator the way a cycle
// does — window by window, with fold's refusal recovery — and counts what the
// folding did.
//
// It owns the loop and nothing above it. Where the mutations come from is the
// caller's (a generator, a WAL read back), and so is what a drained batch is
// for — applying it to a cold store, holding on to it, or only counting it.
// That is why the drain is a callback and not something this package performs.
//
// It counts only what every caller counts the same way. Anything read off a
// [fold.Batch] stays with the caller, deliberately: "tombstone" means
// KindDelete to one of them and KindDelete-or-KindDeleteCurrent to another, and
// a shared counter would have to pick one and silently change the other.
package foldrun

import (
	"github.com/aromanovich/waltz/fold"
	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/wal"
)

// Run is what one drive folded. Every field is a count of the loop's own
// events, so two runs of the same stream at the same window size report the
// same numbers whatever their callers did with the batches.
type Run struct {
	// Mutations is what was handed to [Driver.Add] and accepted.
	Mutations int
	// FoldedIn sums the drained windows' MutationsIn: what fold took in over
	// the whole run, which is Mutations minus whatever is still in the window
	// when the run ends.
	FoldedIn int
	// Emitted sums the merged requests the drains produced.
	Emitted int
	// Drains counts calls of the drain callback, empty batches included. A
	// caller that means "windows that emitted something" counts that itself,
	// off the batch.
	Drains int
	// Refusals counts fold.ErrRefused recoveries: the documented drain-and-retry
	// loop actually running, rather than a run that never met one.
	Refusals int
}

// CollapseRatio is what the folding delivered — mutations in over merged
// requests out. Meaningless without the generator knobs that produced the
// stream, so callers print it with them.
func (r Run) CollapseRatio() float64 {
	if r.Emitted == 0 {
		return 0
	}
	return float64(r.FoldedIn) / float64(r.Emitted)
}

// Driver folds a stream into one accumulator, draining at a window size and
// wherever fold refuses.
type Driver struct {
	acc      *fold.Accumulator
	window   int
	on       func(fold.Batch) error
	inWindow int
	run      Run
}

// New drives shard's accumulator, draining every window mutations. on receives
// each drained batch, including an empty one — an empty drain is a fact about
// the window and some callers assert on it.
//
// A window of 0 or less drains at every mutation, which is sync mode's shape.
func New(shard wal.ShardID, window int, on func(fold.Batch) error) *Driver {
	return &Driver{acc: fold.New(shard), window: window, on: on}
}

// Run is the counts so far. Safe to read mid-run; the final numbers need
// [Driver.Flush] first, or the last window goes uncounted.
func (d *Driver) Run() Run { return d.run }

// Add folds one mutation in, draining first if fold refuses it and again if the
// window is full afterwards. An error is fold's own refusal surviving its
// recovery, or the drain callback's — both mean the run cannot continue.
func (d *Driver) Add(seqno wal.Seqno, m mutation.Mutation) error {
	refusal, err := d.acc.AddOrDrain(seqno, m, d.drain)
	if err != nil {
		return err
	}
	if refusal.Drained {
		d.run.Refusals++
	}
	d.run.Mutations++
	d.inWindow++
	if d.inWindow >= d.window {
		return d.drain()
	}
	return nil
}

// Flush drains what is left, so the last window counts like every other one.
func (d *Driver) Flush() error { return d.drain() }

func (d *Driver) drain() error {
	batch := d.acc.Drain()
	d.inWindow = 0
	d.run.Drains++
	d.run.FoldedIn += batch.Stats().MutationsIn
	d.run.Emitted += batch.Len()
	if d.on == nil {
		return nil
	}
	return d.on(batch)
}
