package waltest

import (
	"context"
	"slices"
	"sync"

	"github.com/aromanovich/waltz/wal"
)

// Fault decides whether a call fails instead of reaching the log. call counts
// calls of that method since the fault was set, starting at 1. A fault that
// blocks holds only its own call.
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

// Faulty is a [wal.Log] decorator that fails on demand: it asks a fault
// before each call and passes admitted calls to the wrapped backend. It holds
// no log state, so a test drives a real backend and sees failures at the seam.
//
// All methods, including setting a fault, are safe for concurrent use. This
// package imports [testing], so use Faulty only in tests.
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
	// landedCall is [Faulty.AfterAppend]'s seam: a second fault on Append,
	// with its own count.
	landedCall
	readCall
	trimCall
)

// OnFence sets the fault for [Faulty.Fence], replacing any previous one and
// restarting its count. OnAppend, OnRead and OnTrim do the same for their
// methods.
func (f *Faulty) OnFence(fault Fault) { f.set(fenceCall, fault) }

func (f *Faulty) OnAppend(fault Fault) { f.set(appendCall, fault) }

// AfterAppend sets a fault asked after an append has reached the log, so the
// call fails having written its entry: durable but reported failed. None of
// [wal]'s refusals can express this; [Faulty.OnAppend] stages the opposite.
//
// It is asked after OnAppend's fault, so a call OnAppend refused never
// reaches it.
func (f *Faulty) AfterAppend(fault Fault) { f.set(landedCall, fault) }

func (f *Faulty) OnRead(fault Fault) { f.set(readCall, fault) }

func (f *Faulty) OnTrim(fault Fault) { f.set(trimCall, fault) }

func (f *Faulty) set(m method, fault Fault) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seams[m] = seam{fault: fault}
}

// Fences returns the epochs [Faulty.Fence] was called at, in order, refused
// calls included. Trims does the same for [Faulty.Trim]'s upTo.
//
// Only these two are recorded because they are idempotent: the log cannot
// show whether they ran once or twice. Appends are visible in the log itself.
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
	// Counts only appends that landed.
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

// Close closes the wrapped backend; it has no fault.
func (f *Faulty) Close() { f.log.Close() }

// call asks the method's fault, then records the call. So a call blocked in
// its fault is not yet recorded, while a refused call is.
//
// The fault runs outside the mutex because it may block, which would stall
// every other shard's calls.
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
