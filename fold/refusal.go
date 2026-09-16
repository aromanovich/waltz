package fold

// [ErrRefused]'s recovery, written once: drain the window, retry the mutation
// at the head of a fresh one. It terminates because both refusals — the fold's
// and the condition authority's — turn on what the window already holds and
// [Accumulator.Drain] empties it, so a drained accumulator refuses nothing.
// That is a property of the merge rules rather than of anything here, and a
// second refusal is an invariant violation.
//
// The drain is a callback because each consumer does something different with a
// drained window: a transaction, a kept batch, a write to a cold store.

import (
	"errors"
	"fmt"

	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/wal"
)

// Refusal is what one call had to do to get its mutation in, and where its
// error came from. The zero value is the ordinary case: the window took the
// mutation and nothing was drained.
type Refusal struct {
	// Drained reports that the window refused and was drained to make room. It
	// is the dominant non-policy drain trigger, so a consumer counting drains
	// wants it.
	Drained bool
	// DrainFailed reports that the returned error is the drain callback's own,
	// unwrapped, with nothing retried after it. Such an error has already
	// classified itself (a fenced shard, an ambiguous transaction), so a caller
	// that type-switches must not read it as a fold invariant violation.
	DrainFailed bool
}

// AddOrDrain is [Accumulator.Add] with the refusal's recovery applied, so its
// error is never [ErrRefused] on the first attempt's account. drain is what
// this consumer does with a window it was forced to close, and is required.
func (a *Accumulator) AddOrDrain(seqno wal.Seqno, m mutation.Mutation, drain func() error) (Refusal, error) {
	return a.recover(func() error { return a.Add(seqno, m) }, drain)
}

// CheckOrDrain is [Accumulator.Check] with the same recovery, which works for
// the same reason: an empty window discards nothing, so every assertion heads it
// and none can be refused.
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
	// The retry's coverage and not the refused attempt's: the refused assertion
	// is one this window could not determine, and after the drain it heads an
	// empty window and is recorded. Counting both would report the same
	// assertion set twice and call the second reading a wider one.
	return del, r, cov, err
}

// recover retries exactly once: a second refusal is the property above having
// stopped holding, not a case to keep draining at.
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
