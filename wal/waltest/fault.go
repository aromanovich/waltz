package waltest

import (
	"context"
	"slices"
	"sync"

	"github.com/aromanovich/waltz/wal"
)

// Fault decides whether a call fails instead of reaching the log. call is how
// many calls of that method the decorator has seen since the fault was set,
// this one included, so [Once] is "the next one" wherever it is set.
//
// A fault that blocks holds the call it was asked about and nothing else.
type Fault func(call int) error

// Once fails the next call with err and admits every one after it.
func Once(err error) Fault {
	return func(call int) error {
		if call == 1 {
			return err
		}
		return nil
	}
}

// Always fails every call with err.
func Always(err error) Fault {
	return func(int) error { return err }
}

// Faulty is a [wal.Log] that fails on demand: it asks a fault before each call
// and delegates everything the fault admits, so what a call that goes through
// does is the backend's answer and not this type's. Anything above the log can
// therefore be driven against a backend that keeps the contract and still be
// shown a failure at the seam.
//
// It implements none of the four methods' semantics and holds no log state.
// Whatever it wraps is what a test is driving, and a fault refusing a call is
// the only difference from driving that backend directly.
//
// Every method is safe for concurrent use, and so is setting a fault while the
// log is in use. Importing this package carries [testing] with it, since the
// conformance suite beside this file needs it, so a fault belongs in a test.
type Faulty struct {
	log wal.Log

	mu     sync.Mutex
	seams  [trimCall + 1]seam
	fences []wal.Epoch
	trims  []wal.Seqno
}

var _ wal.Log = (*Faulty)(nil)

// NewFaulty wraps a log. With no fault set it is the log it wraps.
func NewFaulty(log wal.Log) *Faulty { return &Faulty{log: log} }

// seam is one method's fault and the calls it has been asked about.
type seam struct {
	fault Fault
	calls int
}

type method int

const (
	fenceCall method = iota
	appendCall
	// landedCall is [Faulty.AfterAppend]'s seam, asked once the append has
	// reached the log. It is a method of its own here because it is a second
	// fault on one call, with its own count.
	landedCall
	readCall
	trimCall
)

// OnFence sets the fault the next [Faulty.Fence] calls are asked about,
// replacing any fault already there and restarting its count. So do OnAppend,
// OnRead and OnTrim, each for its own method.
func (f *Faulty) OnFence(fault Fault) { f.set(fenceCall, fault) }

func (f *Faulty) OnAppend(fault Fault) { f.set(appendCall, fault) }

// AfterAppend sets the fault an append is asked about once it has reached the
// log, so the call fails having done its work. That is the ambiguous append —
// durable and reported failed — and it is the one outcome [wal]'s three
// refusals cannot express, each of them saying the write is whole one way or
// the other. A caller driving what happens after one has nothing else to reach
// for: refusing the call ([Faulty.OnAppend]) stages the opposite case.
//
// It composes with OnAppend and is asked second, so a call that fault refused
// never reaches this one.
func (f *Faulty) AfterAppend(fault Fault) { f.set(landedCall, fault) }

func (f *Faulty) OnRead(fault Fault) { f.set(readCall, fault) }

func (f *Faulty) OnTrim(fault Fault) { f.set(trimCall, fault) }

func (f *Faulty) set(m method, fault Fault) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seams[m] = seam{fault: fault}
}

// Fences are the epochs [Faulty.Fence] was called at, in order, the refused
// calls included. Trims is the same for [Faulty.Trim]'s watermarks.
//
// Those two calls are recorded because the log cannot be asked about them
// afterwards: fencing twice at one epoch leaves a log fenced once, and a trim
// states where the log should start, so a cadence that ran twice leaves what
// one that ran once leaves. An append's work is the log's own contents and a
// read leaves nothing, so neither is recorded here.
func (f *Faulty) Fences() []wal.Epoch {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.fences)
}

func (f *Faulty) Trims() []wal.Seqno {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.trims)
}

func (f *Faulty) Fence(ctx context.Context, shard wal.ShardID, epoch wal.Epoch) error {
	if err := f.call(fenceCall, func() { f.fences = append(f.fences, epoch) }); err != nil {
		return err
	}
	return f.log.Fence(ctx, shard, epoch)
}

func (f *Faulty) Append(
	ctx context.Context, shard wal.ShardID, epoch wal.Epoch, seqno wal.Seqno, payload []byte,
) error {
	if err := f.call(appendCall, nil); err != nil {
		return err
	}
	if err := f.log.Append(ctx, shard, epoch, seqno, payload); err != nil {
		return err
	}
	// Asked only for an append that landed, so its count is ambiguous appends
	// staged rather than calls made.
	return f.call(landedCall, nil)
}

func (f *Faulty) ReadFrom(
	ctx context.Context, shard wal.ShardID, from wal.Seqno, limit int,
) ([]wal.Entry, error) {
	if err := f.call(readCall, nil); err != nil {
		return nil, err
	}
	return f.log.ReadFrom(ctx, shard, from, limit)
}

func (f *Faulty) Trim(ctx context.Context, shard wal.ShardID, upTo wal.Seqno) error {
	if err := f.call(trimCall, func() { f.trims = append(f.trims, upTo) }); err != nil {
		return err
	}
	return f.log.Trim(ctx, shard, upTo)
}

// Close is the wrapped backend's, with no seam of its own: what a decorated log
// holds to release is a fact about that backend rather than about this
// decorator.
func (f *Faulty) Close() { f.log.Close() }

// call asks the method's fault and then records the call, so a fault that
// blocks holds the record with the call it is holding — a trim waiting at the
// seam has not been made yet — while a call the fault refused is still a call
// that was made.
//
// The fault runs outside the mutex: it may block, and holding the decorator
// while it does would stop every other shard's calls with it.
func (f *Faulty) call(m method, record func()) error {
	f.mu.Lock()
	seam := &f.seams[m]
	seam.calls++
	fault, call := seam.fault, seam.calls
	f.mu.Unlock()

	var err error
	if fault != nil {
		err = fault(call)
	}
	if record != nil {
		f.mu.Lock()
		record()
		f.mu.Unlock()
	}
	return err
}
