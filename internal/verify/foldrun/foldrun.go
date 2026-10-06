// Package foldrun drives mutations through one accumulator as a cycle does,
// window by window with fold's refusal recovery, and counts what happened.
//
// The caller supplies the mutations and decides what a drained batch is for,
// so the drain is a callback. Only the loop's own events are counted; anything
// read off a [fold.Batch] (such as what counts as a tombstone) is the caller's
// to define and count.
package foldrun

import (
	"github.com/aromanovich/waltz/fold"
	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/wal"
)

// Run holds the loop's counts. They depend only on the stream and window size,
// not on what callers did with the batches.
type Run struct {
	// Mutations is how many [Driver.Add] accepted.
	Mutations int
	// FoldedIn sums the drained batches' MutationsIn: Mutations minus what is
	// still in the open window.
	FoldedIn int
	// Emitted sums the merged requests the drains produced.
	Emitted int
	// Drains counts every drain, empty ones included.
	Drains int
	// Refusals counts fold.ErrRefused drain-and-retry recoveries.
	Refusals int
}

// CollapseRatio is mutations folded in per merged request out, or 0 if none
// were emitted. It means little without the generator settings, so print it
// with them.
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
// every drained batch, empty ones included; a nil on discards them. A window of
// 0 or less drains after every mutation, as sync mode does.
func New(shard wal.ShardID, window int, on func(fold.Batch) error) *Driver {
	return &Driver{acc: fold.New(shard), window: window, on: on}
}

// Run returns the counts so far. Call [Driver.Flush] first for final numbers,
// or the last window goes uncounted.
func (d *Driver) Run() Run { return d.run }

// Add folds one mutation in, draining first if fold refuses it and again if the
// window is then full. Any error, from fold or the drain callback, ends the
// run.
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

// Flush drains the open window.
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
