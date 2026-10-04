package waltest

// Durability (guarantee 3) and fencing (guarantee 2) are claims about storage,
// not about the value that acked. Every suite case uses one log value for its
// whole life, so a backend that keeps acked entries or the owning epoch only
// in process memory passes them all. In a deployment that loses acked data, or
// lets two writers ack at one seqno with no error.
//
// [CheckReopen] catches both without a second process: it opens the storage
// again and asks the fresh value what it holds and who owns the shard. It is a
// function rather than a suite case because it takes an opener, and memwal
// cannot be reopened. wal/memwal's tests prove it against [Unfenced] and an
// opener that returns a fresh backend.

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/aromanovich/waltz/wal"
)

// reopenShortfall explains missing entries after a reopen: a durability
// failure (guarantee 3), unlike [retentionShortfall]'s time-based causes.
const reopenShortfall = "guarantee 3 is that a completed append means every entry up to that seqno is " +
	"durable, which is a claim about storage and not about the value that acked. An ack into memory, " +
	"into a transaction nothing committed, or into a buffer nothing flushed each break it — and a " +
	"reopen is the only thing in this package that can ask, every other case reading back through " +
	"the value that appended"

// continuedShortfall explains missing entries on the final read, made through
// the value that appended: a readback failure (guarantee 5), not durability.
// Each shortfall gets its own text so it points at the right cause.
const continuedShortfall = "this is the same value that appended, with no open in between, so what it " +
	"says is guarantee 5: ReadFrom returns every entry a completed append acked and no trim has " +
	"removed. RunContractSuite's first case is the narrower form of it"

// reopenEntries is how many entries the check writes before closing. More
// than one, so a backend that persists only the last entry (say, flushed on
// close) is still caught by the earlier ones.
const reopenEntries = 3

// CheckReopen appends a short run to shard, closes the log, opens the storage
// again and requires the new value to have the same entries, the same owner
// and the same next seqno.
//
// open must return a log over persistent storage and be callable repeatedly;
// the check closes what it opens. Nothing else may write shard or fence it at
// epoch, and epoch must be at least 2, since the check fences below it.
//
// Failures, in the order checked:
//
//   - entries are missing. Guarantee 3: a completed [wal.Log.Append] is
//     durable. Acking into memory, an uncommitted transaction or an unflushed
//     buffer fails here, and nowhere else in this package;
//   - a fence below epoch is admitted. Guarantee 2: a fence cuts off all lower
//     epochs in the log, not in one writer's memory. Otherwise a displaced
//     owner keeps appending after failover;
//   - the log will not continue at the next seqno. [wal.Log.Close] keeps
//     entries and ownership, so the position must survive too; otherwise a
//     successor cannot write, or restarts at [wal.FirstSeqno] and overwrites.
//
// It does not test a fence racing a displaced owner's append (that needs two
// processes; see [RunContractSuite]), nor retention over time
// ([CheckRetention]).
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

	// Asked before the re-fence below, which would restore a lapsed ownership.
	// A lapsed lease does not make a lower epoch the owner.
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

	// Idempotent at the same epoch; it only restores ownership that lapsed
	// between the opens, which [wal.Log.Close] allows (as in [CheckRetention]).
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

// writeThenClose fences, appends a short run, reads it back (so a later
// failure is about the reopen, not the appends) and closes the log.
func writeThenClose(
	ctx context.Context, open func() (wal.Log, error), shard wal.ShardID, epoch wal.Epoch,
) ([]wal.Entry, error) {
	log, err := open()
	if err != nil {
		return nil, fmt.Errorf("waltest: opening the log: %w", err)
	}
	// Closed on every path, so the second open does not meet a lease or
	// transaction this value still holds.
	defer log.Close()

	if err := log.Fence(ctx, shard, epoch); err != nil {
		return nil, fmt.Errorf("waltest: fencing shard %d at epoch %d: %w", shard, epoch, err)
	}

	// Payloads name the check, since they land in a real deployment's log.
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

// Unfenced is a [wal.Log] that keeps the owning epoch in process memory, not
// in storage: Fence answers from its own map, so a fresh value over the same
// storage admits a fence below the real owner.
//
// [CheckReopen]'s ownership half is proved against it. Like [Expiring] and
// [Truncating], it violates the contract on purpose: unlike [Faulty], whose
// refusals a correct backend may also return, these succeed and answer wrongly.
//
// Entries are the wrapped log's, so only the epoch is affected. Appends fence
// the wrapped log at their own epoch so it never refuses anything itself.
func Unfenced(log wal.Log) wal.Log {
	return &unfenced{log: log, owner: map[wal.ShardID]wal.Epoch{}}
}

type unfenced struct {
	log wal.Log

	mu    sync.Mutex
	owner map[wal.ShardID]wal.Epoch
}

var _ wal.Log = (*unfenced)(nil)

// Fence is the defect: it consults only this value's memory, so a freshly
// opened value admits any epoch.
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

// Append first fences the wrapped log at its epoch, so the wrapped log never
// decides ownership; only [Unfenced]'s map does.
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

// Close forgets ownership and closes the wrapped log, whose entries remain.
func (u *unfenced) Close() {
	u.mu.Lock()
	u.owner = map[wal.ShardID]wal.Epoch{}
	u.mu.Unlock()
	u.log.Close()
}
