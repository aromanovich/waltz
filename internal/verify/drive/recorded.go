package drive

import (
	"context"
	"time"

	p "go.temporal.io/server/common/persistence"

	"github.com/aromanovich/waltz/internal/verify/checker"
	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/wal"
)

// Recorder drives one shard's calls and writes down what happened to each: two
// fsynced lines with a deadline between them, and the gap between the lines is
// the third outcome class.
//
// Three things about it are the checker's contract rather than convenience:
//
//   - the call line is durable before the store is touched: a process killed
//     inside the call must leave a call with no outcome, not an entry nothing
//     accounts for;
//   - the outcome line is always written, whatever the store said, because a
//     call with no outcome means one thing and must not mean two;
//   - a fenced refusal is noted beside its outcome, so a case reading the
//     record can tell "the shard was taken from us" from every other refusal.
//
// What it does not own is the stop rule. A driver stops its shard at its first
// non-acked call and a probe retries until it gets a definite answer; both are
// policies over the same unit, and they are their callers'.
type Recorder struct {
	// Store is what the call goes to, and Record what it is written down in.
	Store  p.ExecutionStore
	Record *checker.Record

	// Shard and Epoch are what every line carries. The epoch is stamped onto
	// the request too, the mutation not carrying one (I11).
	Shard wal.ShardID
	Epoch wal.Epoch

	// Timeout bounds one call and is released per call rather than at the end
	// of the run. Zero or less means the caller's context is the whole of the
	// bound — which for a driver holding no deadline of its own is no bound at
	// all, so the third outcome class then arrives only from a kill or from an
	// error checker.Classify has never seen.
	Timeout time.Duration
}

// Outcome is what one recorded call came to: how the checker reads it, and what
// the store actually said, which the caller needs for the note it writes next.
type Outcome struct {
	Outcome checker.Outcome
	Err     error
}

// Acked reports the one outcome a driver may carry on from.
func (o Outcome) Acked() bool { return o.Outcome == checker.Acked }

// Fenced reports the refusal that means the shard is no longer this node's.
func (o Outcome) Fenced() bool { return checker.Fenced(o.Err) }

// Detail is what the store said, in the words the record holds.
func (o Outcome) Detail() string { return checker.Detail(o.Err) }

// Call drives one mutation and records it. The error it returns is the
// *record's*, never the store's: a record that cannot be written is a run that
// proves nothing, so a caller stops rather than driving blind, where the
// store's own refusal is the subject matter and comes back in the outcome.
func (r Recorder) Call(ctx context.Context, m mutation.Mutation) (Outcome, error) {
	id, err := r.Record.Call(r.Shard, r.Epoch, m)
	if err != nil {
		return Outcome{}, err
	}

	callCtx, cancel := r.bound(ctx)
	callErr := Apply(callCtx, r.Store, int64(r.Epoch), m)
	cancel()

	out := Outcome{Outcome: checker.Classify(callErr), Err: callErr}
	if err := r.Record.Outcome(id, out.Outcome, out.Detail()); err != nil {
		return out, err
	}
	// Beside the outcome and before whatever the caller says about it: the note
	// belongs to the call, and a case that reads the record for a fence must
	// find one wherever the caller's own commentary went.
	if out.Fenced() {
		if err := r.Record.Fenced(r.Shard, r.Epoch, out.Detail()); err != nil {
			return out, err
		}
	}
	return out, nil
}

func (r Recorder) bound(ctx context.Context) (context.Context, context.CancelFunc) {
	if r.Timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, r.Timeout)
}
