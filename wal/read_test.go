package wal_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/aromanovich/waltz/wal"
)

// pagedLog is a log that answers reads out of one shard's entries. Its three
// knobs are the three ways a page can come back: short, failed, and — the one
// that is a backend bug rather than a shape a log has — ignoring the seqno it
// was asked for.
type pagedLog struct {
	entries    []wal.Entry
	ignoreFrom bool
	failOn     int
	// asked is the from of every read, so its length is the read count.
	asked []wal.Seqno
}

func (l *pagedLog) Fence(context.Context, wal.ShardID, wal.Epoch) error { return nil }

func (l *pagedLog) Append(context.Context, wal.ShardID, wal.Epoch, wal.Seqno, []byte) error {
	return nil
}

func (l *pagedLog) Trim(context.Context, wal.ShardID, wal.Seqno) error { return nil }

func (l *pagedLog) Close() {}

func (l *pagedLog) ReadFrom(_ context.Context, _ wal.ShardID, from wal.Seqno, limit int) ([]wal.Entry, error) {
	l.asked = append(l.asked, from)
	if l.failOn == len(l.asked) {
		return nil, errors.New("the datashard is unavailable")
	}
	if l.ignoreFrom {
		from = wal.FirstSeqno
	}
	var page []wal.Entry
	for _, e := range l.entries {
		if e.Seqno < from {
			continue
		}
		if len(page) == limit {
			break
		}
		page = append(page, e)
	}
	return page, nil
}

func logOf(n int) *pagedLog {
	l := &pagedLog{}
	for i := range n {
		l.entries = append(l.entries, wal.Entry{
			Seqno:   wal.FirstSeqno + wal.Seqno(i),
			Epoch:   7,
			Payload: []byte{byte(i)},
		})
	}
	return l
}

func collect(t *testing.T, log wal.Log, from wal.Seqno, page int) ([]wal.Seqno, error) {
	t.Helper()
	var (
		seqnos []wal.Seqno
		failed error
	)
	for e, err := range wal.Entries(t.Context(), log, 3, from, page) {
		if err != nil {
			failed = err
			break
		}
		seqnos = append(seqnos, e.Seqno)
	}
	return seqnos, failed
}

func TestAWholeLogIsReadInPagesUntilAShortOne(t *testing.T) {
	log := logOf(130)

	seqnos, err := collect(t, log, wal.FirstSeqno, 64)

	require.NoError(t, err)
	require.Len(t, seqnos, 130, "every entry of the log is yielded")
	require.Equal(t, wal.FirstSeqno, seqnos[0])
	require.Equal(t, wal.Seqno(130), seqnos[len(seqnos)-1])
	require.Equal(t, []wal.Seqno{1, 65, 129}, log.asked,
		"each page resumes just above the last entry of the one before it")
}

// TestAFullPageThatEndsWhereItBeganStillAdvances is the livelock guard's own
// boundary. At a page of one every page is full and its last seqno is exactly
// the one the read began at, so a guard refusing `last == from` beside
// `last < from` would refuse the first page — and with it every recovery a
// sync-mode node makes, its window being one by construction. Every other case
// in this file pages at 64, where a full page always ends far above its start,
// and until this one the boundary was reached only through a caller whose page
// size happened to equal that window.
func TestAFullPageThatEndsWhereItBeganStillAdvances(t *testing.T) {
	log := logOf(3)

	seqnos, err := collect(t, log, wal.FirstSeqno, 1)

	require.NoError(t, err)
	require.Equal(t, []wal.Seqno{1, 2, 3}, seqnos, "every entry is yielded, one page each")
	require.Equal(t, []wal.Seqno{1, 2, 3, 4}, log.asked,
		"a page of one is full at every entry, so the read ends on the empty page above the log")
}

func TestALogThatEndsOnAPageBoundaryCostsOneMoreRead(t *testing.T) {
	log := logOf(64)

	seqnos, err := collect(t, log, wal.FirstSeqno, 64)

	require.NoError(t, err)
	require.Len(t, seqnos, 64)
	require.Len(t, log.asked, 2,
		"a full page is not the end of the log: only a short one is, so the empty page after it is the terminator")
}

func TestReadingAboveTheLogYieldsNothing(t *testing.T) {
	log := logOf(10)

	seqnos, err := collect(t, log, 11, 64)

	require.NoError(t, err)
	require.Empty(t, seqnos)
}

func TestAFailedPageIsYieldedAsAnErrorAndEndsTheRead(t *testing.T) {
	log := logOf(130)
	log.failOn = 2

	seqnos, err := collect(t, log, wal.FirstSeqno, 64)

	require.Len(t, seqnos, 64, "the entries read before the failure are still yielded")
	require.ErrorContains(t, err, "the datashard is unavailable")
	require.ErrorContains(t, err, "from seqno 65", "the error says where the read stopped")
	require.Len(t, log.asked, 2, "the iteration stops at the failure rather than retrying it")
}

func TestABackendThatIgnoresTheSeqnoIsRefusedRatherThanReadForever(t *testing.T) {
	log := logOf(130)
	log.ignoreFrom = true

	seqnos, err := collect(t, log, wal.FirstSeqno, 64)

	require.Len(t, seqnos, 128, "the two pages it did answer are yielded")
	require.ErrorContains(t, err, "the reads are not advancing")
	require.Len(t, log.asked, 2, "the second page is what proves it, and there is no third")
}

func TestBreakingOutOfTheLoopStopsTheReads(t *testing.T) {
	log := logOf(130)

	var read int
	for range wal.Entries(t.Context(), log, 3, wal.FirstSeqno, 64) {
		read++
		if read == 10 {
			break
		}
	}

	require.Len(t, log.asked, 1, "no page is read for entries the caller has stopped taking")
}
