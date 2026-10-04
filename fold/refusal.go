package fold

// Recovery from [ErrRefused]: drain the window, then retry the mutation at the
// head of a fresh one. One retry suffices because both refusals (the fold's
// and the condition authority's) depend on what the window holds, and an
// accumulator emptied by [Accumulator.Drain] refuses nothing. A second refusal
// is an invariant violation.
//
// The drain is a callback because consumers do different things with a
// drained window: a transaction, a kept batch, a cold-store write.

import (
	"errors"
	"fmt"

	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/wal"
)

// Refusal reports what one call had to do to get its mutation in, and where its
// error came from. The zero value means the window took the mutation and
// nothing was drained.
type Refusal struct {
	// Drained means the window refused and was drained to make room; the main
	// drain trigger outside policy, so drain counters want it.
	Drained bool
	// DrainFailed means the returned error is the drain callback's, unwrapped,
	// and nothing was retried. That error is already classified (a fenced
	// shard, an ambiguous transaction); do not treat it as a fold invariant
	// violation.
	DrainFailed bool
}

// AddOrDrain is [Accumulator.Add] with the recovery applied, so it never
// returns [ErrRefused] for the first attempt. drain is required.
func (a *Accumulator) AddOrDrain(seqno wal.Seqno, m mutation.Mutation, drain func() error) (Refusal, error) {
	return a.recover(func() error { return a.Add(seqno, m) }, drain)
}

// CheckOrDrain is [Accumulator.Check] with the same recovery: in an empty
// window every assertion heads it, so none can be refused.
func (a *Accumulator) CheckOrDrain(m mutation.Mutation, drain func() error) (Delegated, Refusal, error) {
	del, r, _, err := a.checkOrDrain(m, drain)
	return del, r, err
}

// checkOrDrain is [Accumulator.CheckOrDrain] with the counters beside its
// answer, for the same reader [Accumulator.check] has.
func (a *Accumulator) checkOrDrain(m mutation.Mutation, drain func() error) (Delegated, Refusal, coverage, error) {
	var del Delegated
	var cov coverage
	r, err := a.recover(func() error {
		var cerr error
		del, cov, cerr = a.check(m)
		return cerr
	}, drain)
	// Only the retry's coverage: counting the refused attempt too would count
	// the same assertions twice.
	return del, r, cov, err
}

// recover retries exactly once: a second refusal means the invariant above
// broke, and draining again would not help.
func (a *Accumulator) recover(op func() error, drain func() error) (Refusal, error) {
	err := op()
	if !errors.Is(err, ErrRefused) {
		return Refusal{}, err
	}
	if drain == nil {
		return Refusal{}, fmt.Errorf("%w: and the caller offered no drain to recover with", err)
	}
	if derr := drain(); derr != nil {
		return Refusal{Drained: true, DrainFailed: true}, derr
	}
	return Refusal{Drained: true}, op()
}
