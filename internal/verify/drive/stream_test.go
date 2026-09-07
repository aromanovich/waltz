package drive_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/aromanovich/waltz/internal/verify/drive"
	"github.com/aromanovich/waltz/internal/verify/mutgen"
	"github.com/aromanovich/waltz/mutation"
)

func testStream(t *testing.T) *drive.Stream {
	t.Helper()
	cfg := mutgen.Default()
	cfg.Seed = 20260814
	cfg.ShardID = 1
	s, err := drive.NewStream(cfg)
	require.NoError(t, err)
	return s
}

// The two halves of a delivery are one mutation: the bytes a log would carry,
// and what a replay of them produces. A consumer that keeps the payload and a
// consumer that takes the mutation are looking at the same thing.
func TestADeliveryIsWhatCameBackOutOfItsOwnPayload(t *testing.T) {
	require.NoError(t, testStream(t).Drive(16, func(m drive.Delivery) error {
		again, err := mutation.Encode(m.Mutation)
		require.NoError(t, err)
		require.Equal(t, m.Payload, again)
		return nil
	}))
}

// The stop rule is the caller's: a sink that fails ends the drive there, and the
// mutations behind it are never generated.
func TestASinksErrorStopsTheDriveWhereItWasRaised(t *testing.T) {
	s := testStream(t)
	boom := errors.New("boom")

	seen := 0
	err := s.Drive(16, func(drive.Delivery) error {
		seen++
		if seen == 3 {
			return boom
		}
		return nil
	})
	require.ErrorIs(t, err, boom)
	require.Equal(t, 3, seen)

	next, err := s.Next()
	require.NoError(t, err)
	require.Equal(t, 3, next.Index)
}

// One stream in two phases is one stream: the second phase must not re-emit what
// the first already drove, since a create the store holds is not a create it
// accepts.
func TestTheIndexIsTheStreamsAndNotOneDrivesOwn(t *testing.T) {
	s := testStream(t)

	var indices []int
	sink := func(m drive.Delivery) error {
		indices = append(indices, m.Index)
		return nil
	}
	require.NoError(t, s.Drive(3, sink))
	require.NoError(t, s.Drive(3, sink))
	require.Equal(t, []int{0, 1, 2, 3, 4, 5}, indices)
}
