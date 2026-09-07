package checker

import (
	"context"
	"hash/fnv"
	"sync"
	"time"

	"go.temporal.io/server/service/history/tasks"

	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/wal"
)

// The sampling loop. It is what makes the checker a journal rather than a look,
// and its coverage is the retention window divided by the sample interval — so
// an observer slower than the trim has holes, and the holes are silent. A run
// that wants A6 declares [Policy.WholeLog], which removes the retention window
// from that fraction altogether.

// WatermarkReader is the applied position the cold store carries for a shard.
// It is a function rather than a type from apply, because a checker that
// imported apply would reach the plugin — and the whole claim is that this
// instrument judges the layer from outside it, through the contract.
type WatermarkReader func(context.Context, wal.ShardID) (wal.Seqno, bool, error)

// Sampler reads the world: the log through the contract's own [wal.Log.ReadFrom]
// and the watermark through the reader it is given. It never fences — a fence
// would take the shard away from the nodes under observation, which is the
// observer changing the run.
type Sampler struct {
	// Log is any WAL backend. Backend-independence is not decoration here
	// (ADR 0002): an instrument with SQL of its own would make every assertion
	// a statement about backend #1 instead of about the contract.
	Log wal.Log
	// Watermark reads the applied position.
	Watermark WatermarkReader
	// Registry decodes an entry's payload far enough to say who it is about,
	// which is A9's input. It must be the registry the run was written with —
	// an unknown category id is fatal to a decode by design.
	Registry tasks.TaskCategoryRegistry
	// Page bounds one ReadFrom. Zero means [DefaultPage].
	Page int

	mu sync.Mutex
	// subjects caches the decode per (shard, seqno). An entry never changes —
	// that is I4, and A3 is what checks it — so the *subject* may be cached
	// while the digest may not: caching the digest too would make A3 compare a
	// remembered value with itself.
	subjects  map[wal.ShardID]map[wal.Seqno]Workflow
	failures  int
	undecoded int
}

// DefaultPage is how many entries one ReadFrom asks for.
const DefaultPage = 512

// Sample reads one shard once.
//
// The watermark is read before the log and again after it, and both go into the
// [Sample]. Neither read alone is sound for both assertions that use it: an
// entry appended between the log read and a later watermark read would make A6
// report apply running ahead of the ack, and a trim between an earlier
// watermark read and the log read would make A5 report a log that starts above
// the recovery point. Two reads cost one round trip and remove both.
func (s *Sampler) Sample(ctx context.Context, shard wal.ShardID) (Sample, error) {
	out := Sample{Shard: shard}

	before, hadBefore, err := s.Watermark(ctx, shard)
	if err != nil {
		return out, err
	}

	page := s.Page
	if page <= 0 {
		page = DefaultPage
	}
	for e, err := range wal.Entries(ctx, s.Log, shard, wal.FirstSeqno, page) {
		if err != nil {
			return out, err
		}
		out.Entries = append(out.Entries, s.observe(shard, e))
	}

	after, hadAfter, err := s.Watermark(ctx, shard)
	if err != nil {
		return out, err
	}

	out.HasWatermark = hadBefore || hadAfter
	out.WatermarkBefore, out.WatermarkAfter = before, after
	if !hadBefore {
		// No watermark row yet means nothing has been applied, which is a
		// lower bound of zero and not a missing reading.
		out.WatermarkBefore = 0
	}
	if !hadAfter {
		out.WatermarkAfter = out.WatermarkBefore
	}
	return out, nil
}

// observe turns one entry into what the journal remembers.
//
// An entry whose payload will not decode is kept with an empty subject rather
// than dropped: A1 through A6 are about seqnos and epochs and still hold, and
// A9 is a `≤` that an entry nobody can identify simply does not contribute to.
// The count is reported by [Sampler.Undecoded], because an instrument silently
// unable to read the log it is judging is the failure mode to notice.
func (s *Sampler) observe(shard wal.ShardID, e wal.Entry) Observed {
	digest := fnv.New64a()
	_, _ = digest.Write(e.Payload)
	o := Observed{Seqno: e.Seqno, Epoch: e.Epoch, Digest: digest.Sum64()}

	s.mu.Lock()
	defer s.mu.Unlock()
	if cached, ok := s.subjects[shard][e.Seqno]; ok {
		o.Subject = cached
		return o
	}
	m, _, err := mutation.DecodeEntry(e.Payload, s.Registry)
	if err != nil {
		s.undecoded++
		return o
	}
	subject, _, err := Identify(m)
	if err != nil {
		s.undecoded++
		return o
	}
	if s.subjects == nil {
		s.subjects = map[wal.ShardID]map[wal.Seqno]Workflow{}
	}
	if s.subjects[shard] == nil {
		s.subjects[shard] = map[wal.Seqno]Workflow{}
	}
	s.subjects[shard][e.Seqno] = subject
	o.Subject = subject
	return o
}

// Observer is what an observation is folded into. [Journal] is one.
//
// It is an interface rather than a *Journal because a harness sometimes needs
// the samples themselves as well — re-judging one run's evidence with a piece of
// the wiring deliberately broken is how the instrument is shown to work, and a
// journal keeps digests rather than samples on purpose. A tee is five lines and
// needs no second sampling loop, which is the thing worth not having two of.
type Observer interface{ Observe(Sample) }

// Watch samples every shard on a cadence until ctx is done.
//
// A read that fails is skipped rather than fatal — a partition is one of the
// cases this instrument exists for, and an observer that died with the node
// would report nothing about exactly the run it came to watch. What it is not
// allowed to be is silent: [Sampler.Failures] is the count, and a run whose
// journal is thin because its reads were failing is a run that judged less than
// it looks like it did.
func (s *Sampler) Watch(ctx context.Context, obs Observer, shards []wal.ShardID, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		s.SampleAll(ctx, obs, shards)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// SampleAll takes one observation of every shard and folds it in.
func (s *Sampler) SampleAll(ctx context.Context, obs Observer, shards []wal.ShardID) {
	for _, shard := range shards {
		sample, err := s.Sample(ctx, shard)
		if err != nil {
			s.mu.Lock()
			s.failures++
			s.mu.Unlock()
			continue
		}
		obs.Observe(sample)
	}
}

// Failures is how many observations could not be taken.
func (s *Sampler) Failures() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.failures
}

// Undecoded is how many entries the sampler could not identify. Anything above
// zero means A9 was judging less of the log than it looks like.
func (s *Sampler) Undecoded() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.undecoded
}
