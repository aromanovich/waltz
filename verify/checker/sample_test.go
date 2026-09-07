package checker

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/service/history/tasks"

	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/wal"
	"github.com/aromanovich/waltz/wal/memwal"
)

// The sampler against a real backend. It is backend #2 (memwal), which is a
// backend and not a double — so what ReadFrom does here is what ReadFrom does —
// and the watermark is a function, because the checker may not import apply any
// more than it may import a store.

func filled(t *testing.T, n int) (*memwal.Backend, []mutation.Mutation) {
	t.Helper()
	log := memwal.New()
	ctx := context.Background()
	require.NoError(t, log.Fence(ctx, 1, 2))

	ms := stream(t, n)
	for i, m := range ms {
		payload, err := mutation.Encode(m)
		require.NoError(t, err)
		require.NoError(t, log.Append(ctx, 1, 2, wal.FirstSeqno+wal.Seqno(i), payload))
	}
	return log, ms
}

func TestTheSamplerReadsTheLogThroughTheContract(t *testing.T) {
	const entries = 700 // more than DefaultPage, so the paging is exercised
	log, ms := filled(t, entries)

	s := &Sampler{
		Log:       log,
		Watermark: func(context.Context, wal.ShardID) (wal.Seqno, bool, error) { return 5, true, nil },
		Registry:  tasks.NewDefaultTaskCategoryRegistry(),
	}
	sample, err := s.Sample(context.Background(), 1)
	require.NoError(t, err)
	require.Len(t, sample.Entries, entries)
	require.Zero(t, s.Undecoded(), "the instrument could not read the log it is judging")

	// Every entry knows who it is about, which is A9's input, and it is the
	// same answer the driver would have stamped on its call line.
	for i, e := range sample.Entries {
		require.EqualValues(t, i+1, e.Seqno)
		want, _, err := Identify(ms[i])
		require.NoError(t, err)
		require.Equal(t, want, e.Subject)
		require.NotZero(t, e.Digest)
	}
}

// TestTheWatermarkIsReadTwiceAndTheTwoAreNotInterchangeable pins the ordering
// A5 and A6 disagree about.
//
// A6 wants a lower bound — a watermark read after the log may name an entry
// appended since, which is not apply running ahead of the ack. A5 wants an upper
// one — a trim may have fired between the two reads, and the log the sample
// holds was trimmed to a watermark no higher than the later one. One read cannot
// be both, and a checker that used one would report a finding per race.
func TestTheWatermarkIsReadTwiceAndTheTwoAreNotInterchangeable(t *testing.T) {
	log, _ := filled(t, 4)

	var reads atomic.Int64
	s := &Sampler{
		Log: log,
		Watermark: func(context.Context, wal.ShardID) (wal.Seqno, bool, error) {
			return wal.Seqno(reads.Add(1)), true, nil
		},
		Registry: tasks.NewDefaultTaskCategoryRegistry(),
	}
	sample, err := s.Sample(context.Background(), 1)
	require.NoError(t, err)
	require.EqualValues(t, 2, reads.Load(), "the watermark is read before the log and after it")
	require.EqualValues(t, 1, sample.WatermarkBefore)
	require.EqualValues(t, 2, sample.WatermarkAfter)
}

// TestAShardWithNoWatermarkYetIsNotAFinding: a shard nothing has applied to has
// no watermark row, which is a lower bound of zero rather than a missing
// reading — and reading it as missing would take A5 and A6 out of a run that is
// simply young.
func TestAShardWithNoWatermarkYetIsNotAFinding(t *testing.T) {
	log, _ := filled(t, 3)

	s := &Sampler{
		Log:       log,
		Watermark: func(context.Context, wal.ShardID) (wal.Seqno, bool, error) { return 0, false, nil },
		Registry:  tasks.NewDefaultTaskCategoryRegistry(),
	}
	sample, err := s.Sample(context.Background(), 1)
	require.NoError(t, err)

	j := New(Chaos())
	j.Observe(sample)
	require.Empty(t, j.Findings())
}

// TestAFailedObservationIsSkippedAndCounted: a partition is one of the cases
// this instrument exists for, so a read that failed may not be fatal — but a
// journal that is thin because its reads were failing has judged less than it
// looks like, and silence about that is how a run is green for the wrong reason.
func TestAFailedObservationIsSkippedAndCounted(t *testing.T) {
	log, _ := filled(t, 3)

	s := &Sampler{
		Log:       log,
		Watermark: func(context.Context, wal.ShardID) (wal.Seqno, bool, error) { return 0, false, context.DeadlineExceeded },
		Registry:  tasks.NewDefaultTaskCategoryRegistry(),
	}
	j := New(Chaos())
	s.SampleAll(context.Background(), j, []wal.ShardID{1})

	require.Equal(t, 1, s.Failures())
	require.Zero(t, j.Census().Samples, "a failed read is not an observation")
	require.Empty(t, j.Findings())
}

// TestTheSamplerNeverFences is the observer's own rule: a fence would take the
// shard away from the nodes under observation, which is the instrument changing
// the run it came to watch. memwal refuses an append at an epoch it is not
// fenced at, so a sampler that fenced would show up here as a writer that can
// no longer write.
func TestTheSamplerNeverFences(t *testing.T) {
	log, _ := filled(t, 3)
	ctx := context.Background()

	s := &Sampler{
		Log:       log,
		Watermark: func(context.Context, wal.ShardID) (wal.Seqno, bool, error) { return 3, true, nil },
		Registry:  tasks.NewDefaultTaskCategoryRegistry(),
	}
	_, err := s.Sample(ctx, 1)
	require.NoError(t, err)

	payload, err := mutation.Encode(stream(t, 1)[0])
	require.NoError(t, err)
	require.NoError(t, log.Append(ctx, 1, 2, 4, payload),
		"the writer lost its log to the observer")
}
