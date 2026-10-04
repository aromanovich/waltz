package drive

import (
	"fmt"

	"go.temporal.io/server/service/history/tasks"

	"github.com/aromanovich/waltz/internal/verify/mutgen"
	"github.com/aromanovich/waltz/mutation"
)

// Delivery is one generated mutation after a codec round trip.
type Delivery struct {
	// Index counts from 0 over the whole stream, not one [Stream.Drive] call:
	// it is the consumer's seqno minus one.
	Index int
	// Mutation is the decoded form, never the generated one, so codec losses
	// (such as the dropped rangeID, I11) reach the consumer.
	Mutation mutation.Mutation
	// Payload is the encoded form, for a consumer that keeps the bytes or
	// needs a second copy untouched by the first.
	Payload []byte
}

// Stream is a generated mutation stream whose every mutation is encoded and
// decoded before a consumer sees it. That is what a replay sees: the hot path
// folds the caller's own request, so codec losses show only on replay.
//
// A driver modelling a store's client needs the request as generated (with its
// rangeID), so it uses its own [mutgen.Generator] instead; the two values are
// not interchangeable.
//
// The stream's shape comes from the [mutgen.Config] ([mutgen.Default],
// [mutgen.WorkflowRunsOnly], [mutgen.UnbrokenChains]).
//
// A Stream is resumable. A caller driving in phases must keep one Stream: task
// ids and version chains only move forward, and a fresh generator on the same
// seed would re-emit creates the store already holds.
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

// Next generates one mutation and round-trips it through the codec. Errors
// name the seed, which reproduces them.
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

// Drive hands the next n mutations to sink, stopping at the first error. A
// sink error is returned unwrapped. To stop on something other than a count,
// loop over [Stream.Next].
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

// Report summarises what the stream has generated so far.
func (s *Stream) Report() mutgen.Report { return s.g.Report() }
