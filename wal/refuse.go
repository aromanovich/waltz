package wal

import "fmt"

// Argument checks a backend runs before doing anything, and the refusal it
// returns for what it found. The backend establishes the facts; these
// functions choose the error and its precedence, so every backend answers the
// contract the same way. None of them touches a log.
//
// The context obligations are stated on [Log], not here, and checked by
// wal/waltest's ACancelledContextChangesNothing.

// CheckFence refuses a fence whose arguments the contract does not admit.
func CheckFence(shard ShardID, epoch Epoch) error {
	if epoch == 0 {
		return fmt.Errorf("fencing shard %d: %w", shard, ErrZeroEpoch)
	}
	return nil
}

// FenceRefusal checks a fence against the epoch held, returning nil if it may
// proceed. Equal epochs proceed as a no-op, so a restarted process that still
// owns the shard can replay its acquire.
func FenceRefusal(shard ShardID, held, epoch Epoch) error {
	if held > epoch {
		return fmt.Errorf("%w: shard %d is fenced at epoch %d, cannot fence at %d",
			ErrFenced, shard, held, epoch)
	}
	return nil
}

// CheckAppend refuses an append whose arguments the contract does not admit.
func CheckAppend(shard ShardID, epoch Epoch, seqno Seqno, payload []byte) error {
	switch {
	case epoch == 0:
		return fmt.Errorf("appending to shard %d: %w", shard, ErrZeroEpoch)
	case seqno < FirstSeqno:
		return fmt.Errorf("appending to shard %d: seqno %d is below the first seqno %d",
			shard, seqno, FirstSeqno)
	case payload == nil:
		return fmt.Errorf("appending to shard %d at seqno %d: the payload is nil", shard, seqno)
	}
	return nil
}

// AppendState is what a backend found out before an append: one field per
// refusal the contract defines. It and [AppendRefusal] are exported for
// backends outside this module.
type AppendState struct {
	// Owner is the epoch the log is fenced at; zero means never fenced.
	Owner Epoch
	// Taken reports whether the seqno is already written.
	Taken bool
	// HasPredecessor reports whether the entry below the seqno exists. It is
	// true for an append at [FirstSeqno] (or the backend reads its own
	// bookkeeping below the first entry, which always exists).
	HasPredecessor bool
}

// RefuseAtNext is [AppendRefusal] for a backend that tracks next, the seqno
// its log continues at: seqnos below next are spent, next is this entry's,
// and anything above is a gap. It reports HasPredecessor true for taken
// seqnos too, which is correct only because [AppendRefusal] checks Taken
// first; that is why it lives here and not in each backend.
func RefuseAtNext(shard ShardID, epoch Epoch, seqno Seqno, owner Epoch, next Seqno) error {
	return AppendRefusal(shard, epoch, seqno, AppendState{
		Owner:          owner,
		Taken:          seqno < next,
		HasPredecessor: seqno <= next,
	})
}

// AppendRefusal returns the refusal for what the backend found, or nil if the
// append may proceed.
//
// The order is part of the contract. [ErrFenced] comes first because
// [ErrAlreadyWritten] counts as an ack: a displaced writer told
// ErrAlreadyWritten would treat the new owner's entry as its own commit.
func AppendRefusal(shard ShardID, epoch Epoch, seqno Seqno, found AppendState) error {
	switch {
	case found.Owner == 0:
		return fmt.Errorf("%w: shard %d is not fenced, cannot append at epoch %d",
			ErrFenced, shard, epoch)
	case found.Owner != epoch:
		return fmt.Errorf("%w: shard %d is fenced at epoch %d, cannot append at %d",
			ErrFenced, shard, found.Owner, epoch)
	case found.Taken:
		return fmt.Errorf("%w: seqno %d of shard %d is taken",
			ErrAlreadyWritten, seqno, shard)
	case !found.HasPredecessor:
		return fmt.Errorf("%w: shard %d has no seqno %d, cannot append at %d",
			ErrGap, shard, seqno-1, seqno)
	}
	return nil
}

// CheckTrim reports whether a trim reaches any entries. Below [FirstSeqno]
// lies only backend bookkeeping, such as a fence row at seqno 0, and a trim
// must never delete it: losing the ownership record silently hands the shard
// to whichever epoch fences next.
func CheckTrim(upTo Seqno) (proceed bool) {
	return upTo >= FirstSeqno
}

// CheckRead refuses invalid read arguments and returns the seqno to start at.
// A from below [FirstSeqno] is raised to it, since lower seqnos are not
// entries.
func CheckRead(shard ShardID, from Seqno, limit int) (Seqno, error) {
	if limit <= 0 {
		return 0, fmt.Errorf("reading shard %d from %d: limit %d is not positive", shard, from, limit)
	}
	return max(from, FirstSeqno), nil
}
