package drive

import (
	"context"
	"time"

	p "go.temporal.io/server/common/persistence"

	"github.com/aromanovich/waltz/internal/verify/checker"
	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/wal"
)

// Recorder drives one shard's calls and records each as two fsynced lines, a
// call and an outcome. A call with no outcome line is the third outcome class.
//
// The checker relies on three rules:
//
//   - the call line is durable before the store is touched, so a kill inside
//     the call leaves a call with no outcome, never an unaccounted entry;
//   - the outcome line is always written, whatever the store said, so a
//     missing outcome has only one meaning;
//   - a fenced refusal is also recorded as a fence, so a reader can tell "the
//     shard was taken from us" from other refusals.
//
// When to stop is the caller's policy, not the Recorder's.
type Recorder struct {
	Store  p.ExecutionStore
	Record *checker.Record

	// Shard and Epoch go on the call and fence lines; Epoch is also stamped on
	// the request, since the mutation carries none (I11).
	Shard wal.ShardID
	Epoch wal.Epoch

	// Timeout bounds each call separately. Zero or less means only ctx bounds
	// it; with no deadline there, a call with no outcome then arises only from
	// a kill or an error checker.Classify does not know.
	Timeout time.Duration
}

// Outcome is one call's result: the checker's classification and the store's
// raw error.
type Outcome struct {
	Outcome checker.Outcome
	Err     error
}

// Acked reports whether the store acknowledged the call.
func (o Outcome) Acked() bool { return o.Outcome == checker.Acked }

// Fenced reports the refusal that means the shard is no longer this node's.
func (o Outcome) Fenced() bool { return checker.Fenced(o.Err) }

// Detail is the store's error as the record writes it.
func (o Outcome) Detail() string { return checker.Detail(o.Err) }

// Call drives one mutation and records it. The store's error is in the
// Outcome; the returned error is only a failure to write the record, and the
// caller must stop, since an unrecorded run proves nothing.
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
	// Written here, right after the outcome, so a fence is recorded whatever
	// the caller writes next.
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
