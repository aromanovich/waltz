package drive

import (
	"fmt"

	"go.temporal.io/server/service/history/tasks"

	"github.com/aromanovich/waltz/internal/verify/mutgen"
	"github.com/aromanovich/waltz/mutation"
)

// Delivery is one generated mutation on its way into whatever writes it: the
// bytes a log would carry for it, and what comes back out of them.
type Delivery struct {
	// Index counts from 0 over the whole stream rather than over one
	// [Stream.Drive] call, so it is the seqno a consumer would assign minus one.
	Index int
	// Mutation is the decoded form and never the generated one: every task is
	// rebuilt through the registry, and what the payload does not carry — the
	// rangeID (I11), the new events — is gone, so a codec loss reaches a
	// consumer here rather than being covered by the value the generator kept.
	Mutation mutation.Mutation
	// Payload is the encoded form, for a consumer that keeps the bytes — a
	// second path that must not be handed a request the first has touched, or a
	// measurement of what the log carries.
	Payload []byte
}

// Stream is a generated mutation stream with the codec in front of it: the verb
// for driving one at anything that folds or writes what a log carried. Every
// mutation is encoded and decoded before a consumer sees it, because what a
// replay hands fold and the store came back out of the log rather than out of
// the generator — the hot path folds the caller's own request, so the codec's
// losses show only on the value a replay rebuilds.
//
// That round trip is the whole of it and there is no way past it, which is what
// keeps a caller out: a driver modelling the *client* of a store wants the
// request as the generator built it, so it holds a [mutgen.Generator] of its
// own. The two are not interchangeable: what a client sent — its rangeID, its
// new events — and what came back out of the log are different values, and a
// driver that confuses them changes what its own calls mean.
//
// What shape of stream it is comes from the [mutgen.Config] — [mutgen.Default],
// [mutgen.WorkflowRunsOnly], [mutgen.UnbrokenChains] — and not from knobs a
// caller zeroes.
//
// It is resumable, which is why it is a value and not a function: a caller
// driving one stream in two phases must not restart it, since mutgen's task ids
// and version chains only move forward and a second generator on the same seed
// re-emits creates the store already holds.
type Stream struct {
	g        *mutgen.Generator
	seed     int64
	registry tasks.TaskCategoryRegistry
	index    int
}

// NewStream returns the stream cfg describes. The error is a bad config.
func NewStream(cfg mutgen.Config) (*Stream, error) {
	g, err := mutgen.New(cfg)
	if err != nil {
		return nil, err
	}
	return &Stream{g: g, seed: cfg.Seed, registry: tasks.NewDefaultTaskCategoryRegistry()}, nil
}

// Next generates one mutation and puts it through the codec. Every error names
// the seed, which is what reproduces it.
func (s *Stream) Next() (Delivery, error) {
	m, err := s.g.Next()
	if err != nil {
		return Delivery{}, fmt.Errorf("generating mutation %d of seed %d: %w", s.index, s.seed, err)
	}
	payload, err := mutation.Encode(m)
	if err != nil {
		return Delivery{}, fmt.Errorf("encoding mutation %d (%s) of seed %d: %w", s.index, m.Kind(), s.seed, err)
	}
	decoded, err := mutation.Decode(payload, s.registry)
	if err != nil {
		return Delivery{}, fmt.Errorf("decoding mutation %d (%s) of seed %d: %w", s.index, m.Kind(), s.seed, err)
	}
	d := Delivery{Index: s.index, Mutation: decoded, Payload: payload}
	s.index++
	return d, nil
}

// Drive hands the next n mutations to sink, stopping at the first error. The
// sink's own error is returned as it is: it is the consumer's failure, and
// wrapping it here would put this package's vocabulary in front of it.
//
// A caller whose stop rule is a measured quantity rather than a count — bytes
// folded, a counter it is watching — writes its loop over [Stream.Next].
func (s *Stream) Drive(n int, sink func(Delivery) error) error {
	for range n {
		d, err := s.Next()
		if err != nil {
			return err
		}
		if err := sink(d); err != nil {
			return err
		}
	}
	return nil
}

// Report is what the stream has turned out to be so far.
func (s *Stream) Report() mutgen.Report { return s.g.Report() }
