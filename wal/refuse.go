package wal

import "fmt"

// The checks a backend performs before it does anything, and the diagnosis it
// gives once it knows what it found. A backend decides *what is true* — the
// whole of the difference between a chained CAS and a mutex — and the functions
// here decide what to say about it and in what order, so that two backends
// cannot answer one contract differently and a third inherits both.
//
// Nothing here touches a log: they take the arguments, and the shard wherever
// there is an error string for it to appear in.
//
// The context obligations are not here and are not derivable from what is: what
// a cancelled call may leave behind, and that these checks run before it, are
// stated on [Log] and driven by wal/waltest's ACancelledContextChangesNothing.

// CheckFence refuses a fence whose arguments the contract does not admit.
func CheckFence(shard ShardID, epoch Epoch) error {
	if epoch == 0 {
		return fmt.Errorf("fencing shard %d: %w", shard, ErrZeroEpoch)
	}
	return nil
}

// FenceRefusal diagnoses a fence against the epoch the log is already fenced
// at, and returns nil when the fence may proceed. Equal epochs proceed: fencing
// at the epoch the log already carries is a no-op rather than a failure, so a
// process restart without a change of ownership can replay its acquire.
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

// AppendState is what a backend has found out about an append it has not
// performed: the three questions the contract's three refusals are answers to.
//
// It is exported, with [AppendRefusal], for backends written outside this
// module; the only caller here is [RefuseAtNext].
type AppendState struct {
	// Owner is the epoch the log is fenced at. Zero means nobody has fenced it.
	Owner Epoch
	// Taken reports whether the seqno is already written.
	Taken bool
	// HasPredecessor reports whether the entry below the seqno is there. An
	// append at [FirstSeqno] has none to miss, so a backend answers it true —
	// or, if its own bookkeeping sits below the first entry, reads that, which
	// is always there.
	HasPredecessor bool
}

// RefuseAtNext is [AppendRefusal] for a backend that keeps the seqno its log
// continues at rather than answering the two questions separately, which is
// every backend whose state is in this process or in a replicated command.
// next is exclusive: the seqnos below it are spent, the one at it is this
// entry's, and everything above it is a hole.
//
// It is here rather than derived per backend because the derivation leans on
// the diagnosis order: it answers HasPredecessor true for the taken seqnos
// below next as well, which is only right because [AppendRefusal] reports Taken
// first.
func RefuseAtNext(shard ShardID, epoch Epoch, seqno Seqno, owner Epoch, next Seqno) error {
	return AppendRefusal(shard, epoch, seqno, AppendState{
		Owner:          owner,
		Taken:          seqno < next,
		HasPredecessor: seqno <= next,
	})
}

// AppendRefusal diagnoses what the backend found, and returns nil when the
// append may proceed.
//
// The order is the contract's: [ErrFenced] outranks the rest because
// [ErrAlreadyWritten] is an ack, and a zombie handed one would take the word of
// the writer that took the shard from it as its own commitSeqno. A backend that
// diagnoses in its own order answers a different contract.
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

// CheckTrim answers whether a trim has entries to reach. An upTo below
// [FirstSeqno] reaches none: what lives down there is the backend's own
// bookkeeping — a fence row at seqno 0, say — and a trim may never take it.
// Deleting an ownership record hands the shard to whichever epoch asks next,
// with nothing anywhere to report the loss, which is why the bound is the
// contract's to state rather than each backend's to re-derive.
func CheckTrim(upTo Seqno) (proceed bool) {
	return upTo >= FirstSeqno
}

// CheckRead refuses a read whose arguments the contract does not admit and
// returns the seqno it starts at: a from below [FirstSeqno] reads from
// [FirstSeqno], which is a clamp rather than a refusal because seqnos below it
// are not entries.
func CheckRead(shard ShardID, from Seqno, limit int) (Seqno, error) {
	if limit <= 0 {
		return 0, fmt.Errorf("reading shard %d from %d: limit %d is not positive", shard, from, limit)
	}
	return max(from, FirstSeqno), nil
}
