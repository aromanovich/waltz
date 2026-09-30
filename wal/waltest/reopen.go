package waltest

// The second obligation [RunContractSuite] cannot express, and the one the
// contract's third guarantee is entirely about: a completed append is durable,
// which is a claim about *storage* and not about the value that acked it. Every
// case in the suite drives one log for the lifetime of one process, so a backend
// that acknowledges into memory it never gets out of that process passes all of
// them — and what it loses is acked data the cold store does not hold, which is
// the whole reason it was in the log.
//
// The fence has the same shape one guarantee over, and [RunContractSuite]'s own
// doc is where that blind spot is named: a backend recording the owning epoch in
// a process-local field passes every fencing case here, the contention one
// included, and in a deployment it is two writers acking at one seqno, each told
// its entries are durable, with no error anywhere.
//
// That doc sends the author to a test between two processes, and the whole of it
// does need two. Half of it does not: **open the storage a second time and ask
// the fresh value what it holds and who owns the shard.** A backend whose appends
// never left the process answers with nothing; one whose epoch never left it
// admits a fence below the one that is supposed to have cut it off. Neither needs
// a kill, a second process or a judge outside both.
//
// [CheckReopen] is that check. It is a function rather than a case in the suite
// because what it takes is not a log but a way of opening one, which `memwal` —
// the backend that ships here — cannot be: it is a map in this process and there
// is nothing of it to reopen. `wal/memwal`'s own tests are where the check is
// proved in both directions, against [Unfenced] for the ownership half and
// against an opener that hands back a fresh backend for the other.

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/aromanovich/waltz/wal"
)

// reopenShortfall is what a run short of its entries means where an open came
// between: guarantee 3 is that a completed append is durable, and the causes are
// the ones that keep an ack inside the process rather than the ones a clock
// reaches — which is [retentionShortfall]'s list and a different check.
const reopenShortfall = "guarantee 3 is that a completed append means every entry up to that seqno is " +
	"durable, which is a claim about storage and not about the value that acked. An ack into memory, " +
	"into a transaction nothing committed, or into a buffer nothing flushed each break it — and a " +
	"reopen is the only thing in this package that can ask, every other case reading back through " +
	"the value that appended"

// continuedShortfall is the shortfall of the last read, where no open came between:
// the value that appended is the value being read, so what it means is the
// readback guarantee and not the durability one. Separate from the two above
// because the whole reason a shortfall carries a diagnosis is that it points at
// what to go and look at.
const continuedShortfall = "this is the same value that appended, with no open in between, so what it " +
	"says is guarantee 5: ReadFrom returns every entry a completed append acked and no trim has " +
	"removed. RunContractSuite's first case is the narrower form of it"

// reopenEntries is how many entries the check writes before closing. More than
// one, so that a backend which persists the entry it is still holding — a buffer
// flushed on close, a transaction committed there — is still caught by the ones
// before it.
const reopenEntries = 3

// CheckReopen appends a short run to shard, closes the log, opens the storage
// again and requires the second value to answer for everything the first one
// acked: the same entries, the same ownership, and the same position to continue
// at.
//
// open must return a log over storage that persists — the same cluster, the same
// table, the same directory — and must be callable more than once; the check
// closes what it opens. shard must be one nothing else writes and epoch one
// nothing else has fenced it at, and epoch must be above the first, since the
// check fences below it to ask who owns the shard.
//
// What a failure means, in the order they are checked:
//
//   - the entries are not all there. Guarantee 3: a completed [wal.Log.Append]
//     means every entry up to that seqno is durable. A backend that acked into
//     memory, or into a transaction nothing committed, or into a buffer nothing
//     flushed, fails here — and nothing else in this package can fail it,
//     because every other case reads back through the value that did the
//     appending;
//   - a fence below epoch is admitted. Guarantee 2: a fence cuts off all lower
//     epochs, which is a statement about the log rather than about one writer's
//     memory of it. A backend failing here fences nobody in a failover: the
//     displaced owner goes on appending, and two writers ack at one seqno;
//   - the log will not continue at the next seqno. [wal.Log.Close]'s own promise
//     is that the entries stay and ownership outlives the call, so the position
//     the log continues at has to as well. A backend failing here has a
//     successor that cannot write at all, or one that starts over at
//     [wal.FirstSeqno] and overwrites what it inherited.
//
// What it does not establish. It is one process throughout, so it says nothing
// about a fence *racing* a displaced owner's append — that needs two writers
// sharing no memory and is the test [RunContractSuite] sends the author to. And
// it does not run the clock, so a retention window shorter than the gap between
// the two opens is [CheckRetention]'s to catch and not this one's.
func CheckReopen(
	ctx context.Context, open func() (wal.Log, error), shard wal.ShardID, epoch wal.Epoch,
) error {
	if epoch < 2 {
		return fmt.Errorf("waltest: epoch %d has nothing below it to fence at, so the "+
			"ownership half of this check cannot be asked", epoch)
	}

	want, err := writeThenClose(ctx, open, shard, epoch)
	if err != nil {
		return err
	}

	again, err := open()
	if err != nil {
		return fmt.Errorf("waltest: opening the log a second time: %w", err)
	}
	defer again.Close()

	if err := requireRun(ctx, again, shard, want, "after the log was reopened", reopenShortfall); err != nil {
		return err
	}

	// Before the re-fence below, which is what would put back an ownership the
	// backend let lapse on its own clock: what is being asked here is whether the
	// epoch is in the storage, and a lapsed lease does not make a lower one the
	// owner.
	switch err := again.Fence(ctx, shard, epoch-1); {
	case err == nil:
		return fmt.Errorf("waltest: shard %d admitted a fence at epoch %d after being reopened at "+
			"epoch %d, so the owning epoch did not reach storage. In a failover that is a displaced "+
			"owner still appending: two writers acking at one seqno, each told its entries are "+
			"durable, with no error anywhere", shard, epoch-1, epoch)
	case !errors.Is(err, wal.ErrFenced):
		return fmt.Errorf("waltest: shard %d answered a fence at the stale epoch %d with %w, where "+
			"the contract's answer is wal.ErrFenced: a caller separates being fenced from a failure "+
			"and retries one of the two", shard, epoch-1, err)
	}

	// Idempotent at the same epoch, so this only restores an ownership the
	// backend may have let lapse between the two opens — the same allowance
	// [CheckRetention] makes, and for the same reason: [wal.Log.Close] admits
	// ownership expiring, and what this is about is the entries and the epoch.
	if err := again.Fence(ctx, shard, epoch); err != nil {
		return fmt.Errorf("waltest: re-fencing shard %d at its own epoch %d after the reopen: %w", shard, epoch, err)
	}

	next := wal.FirstSeqno + reopenEntries
	if err := again.Append(ctx, shard, epoch, next, []byte("reopen check, after the reopen")); err != nil {
		return fmt.Errorf("waltest: shard %d does not append at seqno %d after being reopened, so what "+
			"the log continues at did not survive the close: a successor either cannot write at all "+
			"or starts over at the first seqno and overwrites what it inherited: %w", shard, next, err)
	}
	want = append(want, wal.Entry{Seqno: next, Epoch: epoch, Payload: []byte("reopen check, after the reopen")})
	return requireRun(ctx, again, shard, want, "after the reopened log was appended to", continuedShortfall)
}

// writeThenClose is the first half: one fence, a short run, a readback that makes
// a later failure about the reopen rather than about an append that never landed,
// and the close whose promises the second half is about.
func writeThenClose(
	ctx context.Context, open func() (wal.Log, error), shard wal.ShardID, epoch wal.Epoch,
) ([]wal.Entry, error) {
	log, err := open()
	if err != nil {
		return nil, fmt.Errorf("waltest: opening the log: %w", err)
	}
	// Closed on every path out: a check that left the first value open would ask
	// the second one about storage the first still holds a lease or a transaction
	// on, and a backend that then refuses would be failed for this check's own
	// mistake.
	defer log.Close()

	if err := log.Fence(ctx, shard, epoch); err != nil {
		return nil, fmt.Errorf("waltest: fencing shard %d at epoch %d: %w", shard, epoch, err)
	}

	// The payloads name what wrote them rather than reusing the suite's
	// payloadFor, for [CheckRetention]'s reason: these are rows somebody reads
	// out of a real deployment's log while wondering what put them there.
	want := make([]wal.Entry, 0, reopenEntries+1)
	for i := range reopenEntries {
		seqno := wal.FirstSeqno + wal.Seqno(i)
		payload := fmt.Appendf(nil, "reopen check, seqno %d", seqno)
		if err := log.Append(ctx, shard, epoch, seqno, payload); err != nil {
			return nil, fmt.Errorf("waltest: appending seqno %d: %w", seqno, err)
		}
		want = append(want, wal.Entry{Seqno: seqno, Epoch: epoch, Payload: payload})
	}
	if err := requireRun(ctx, log, shard, want, "before the log was closed", reopenShortfall); err != nil {
		return nil, err
	}
	return want, nil
}

// Unfenced is a [wal.Log] that keeps the owning epoch in this process instead of
// in storage: its [wal.Log.Fence] answers out of a map of its own and never asks
// the log below, so a fresh value over the same storage believes the shard is
// unowned and admits a fence beneath the epoch that owns it.
//
// It is what [CheckReopen]'s ownership half is proved against, and — like
// [Expiring] and [Truncating] — it is not a log a backend may be. What tells the
// three of them from [Faulty] is that a faulting log refuses calls, which a
// correct backend does under load, while these three succeed and answer wrongly.
//
// Its entries are the wrapped log's and are exactly as durable as that log's,
// which is the whole point: it isolates the epoch and changes nothing else. The
// wrapped log is fenced at whatever epoch an append carries, so the only thing
// that admits or refuses anything here is this value's own map — a log holding the
// epoch in storage, wrapped, would otherwise go on answering correctly underneath
// and there would be nothing to prove the check against.
func Unfenced(log wal.Log) wal.Log {
	return &unfenced{log: log, owner: map[wal.ShardID]wal.Epoch{}}
}

type unfenced struct {
	log wal.Log

	mu    sync.Mutex
	owner map[wal.ShardID]wal.Epoch
}

var _ wal.Log = (*unfenced)(nil)

// Fence is the defect, stated: this process's memory of who owns the shard is the
// whole of the answer. A value that remembers an owner refuses a lower epoch, and
// one that remembers nobody — every freshly opened one — admits it.
func (u *unfenced) Fence(_ context.Context, shard wal.ShardID, epoch wal.Epoch) error {
	if epoch == 0 {
		return wal.ErrZeroEpoch
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if held, known := u.owner[shard]; known && epoch < held {
		return wal.ErrFenced
	}
	u.owner[shard] = epoch
	return nil
}

// Append fences the log below at the epoch it carries, so that what the wrapped
// log holds never answers an ownership question — [Unfenced]'s own map is the only
// authority, which is what makes the epoch process-local rather than merely
// duplicated.
func (u *unfenced) Append(
	ctx context.Context, shard wal.ShardID, epoch wal.Epoch, seqno wal.Seqno, payload []byte,
) error {
	if err := u.log.Fence(ctx, shard, epoch); err != nil {
		return err
	}
	return u.log.Append(ctx, shard, epoch, seqno, payload)
}

func (u *unfenced) ReadFrom(
	ctx context.Context, shard wal.ShardID, from wal.Seqno, limit int,
) ([]wal.Entry, error) {
	return u.log.ReadFrom(ctx, shard, from, limit)
}

func (u *unfenced) Trim(ctx context.Context, shard wal.ShardID, upTo wal.Seqno) error {
	return u.log.Trim(ctx, shard, upTo)
}

// Close drops what this process remembered about ownership and leaves the wrapped
// log's entries alone, which is the shape being modelled: the epoch was never
// anywhere else.
func (u *unfenced) Close() {
	u.mu.Lock()
	u.owner = map[wal.ShardID]wal.Epoch{}
	u.mu.Unlock()
	u.log.Close()
}
