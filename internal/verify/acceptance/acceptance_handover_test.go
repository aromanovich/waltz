package acceptance

// Two owners alive at once over one log and one database, where the one that
// lost the shard goes on to drain. acceptance_twonode_test.go builds that same
// pair for a different question — whether a drain's witness is owner-scoped —
// and its parked applier answers without passing the batch down, so the store
// below is never asked to refuse anything. This is where a fenced owner's batch
// reaches the database.
//
// The rest of this package changes hands through a single Manager, where
// ShardAcquired retires the predecessor and stops its goroutine, so the
// predecessor never acts again. A node whose lease went elsewhere keeps its
// cycle, its window and its timers, and finds out by acting.
//
// There are two fences, each stopping one of the two things such a node can
// still do, which is why either one looks redundant from where the other stands:
//
//   - the log's stops the appends, and it is in place before the range id moves
//     (the order wrapper.ShardStore.UpdateShard imposes), so a write by the old
//     owner is refused while the database still names him owner;
//   - the cold store's epoch CAS stops the drains, which need neither an append
//     nor a caller — a shutdown drain and the age timer both fire out of a full
//     window on their own.
//
// [TestAShardThatLosesItsEpochMidRun] leaves the log unfenced deliberately, so it
// exercises the second alone from inside one process. These are that pair with
// both fences real and two live cycles between them, and then the one case that
// isolates the second fence — which takes some care, for a reason worth writing
// down.
//
// A stale window carrying run rows is refused twice over: the epoch has moved,
// and so has every db_record_version the batch asserts, since the live owner
// applied those same entries. So a run that drives ordinary traffic stays green
// with the epoch check deleted, and it is not evidence about the epoch check —
// the third case here is, over the one window nothing else in the transaction
// can refuse. Task work asserts nothing. A range completion is a category and
// two keys, and a stale owner draining one takes away rows a live owner acked.

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/aromanovich/waltz/cycle"
	"github.com/aromanovich/waltz/internal/verify/drive"
	"github.com/aromanovich/waltz/internal/verify/mutbuild"
	"github.com/aromanovich/waltz/internal/verify/mutgen"
	"github.com/aromanovich/waltz/wal"
)

// TestTheLogFenceStopsAnOwnerBeforeTheDatabaseChangesHands is the first fence on
// its own: the successor has fenced the log and taken the range id, and the
// predecessor — told nothing, holding a window — writes.
func TestTheLogFenceStopsAnOwnerBeforeTheDatabaseChangesHands(t *testing.T) {
	s := newSeams(t, 20260912)
	require.NoError(t, s.drive(t, seamsBeforeLoss))

	predecessor, stale, acked := s.mgr, s.epoch, s.acked
	before, _, err := s.store.Watermark(s.ctx, seamsShard)
	require.NoError(t, err)
	require.Greater(t, wal.Seqno(acked), before,
		"the predecessor applied everything it acked, so it has no tail for the successor to inherit")

	s.mgr = s.anotherNode(t)
	s.handOver(t)

	// The predecessor writes. The log refuses it, and the shard is declared lost
	// to the caller — which is the answer that re-acquires, where the log's own
	// error would fall to a background retry.
	err = s.alienWrite(t, predecessor, stale)
	require.Error(t, err, "a fenced owner appended into a log it no longer holds")
	require.ErrorAs(t, err, new(*p.ShardOwnershipLostError))
	require.Equal(t, cycle.StateHaltedLost, predecessor.Shard(seamsShard).State())

	// Nothing below the log moved, which is what makes this the first fence and
	// not the second: the write stopped at the append, so the drain's own CAS was
	// never reached and the entry consumed no seqno.
	after, _, err := s.store.Watermark(s.ctx, seamsShard)
	require.NoError(t, err)
	require.Equal(t, before, after, "a refused append moved the watermark")
	_, last := s.logEnds(t)
	require.EqualValues(t, acked, last, "the log grew by the entry it refused")

	// And the tail the halted owner leaves is the successor's, whole.
	require.NoError(t, s.drive(t, seamsBeforeLoss/4))
	totals := s.mgr.Totals()
	require.EqualValues(t, wal.Seqno(acked)-before, totals.Replayed,
		"the successor replayed something other than the entries between the watermark %d and the last ack %d",
		before, acked)
	require.Empty(t, totals.Halted, "the successor halted on the tail it inherited")
	require.Empty(t, s.trims.violation())
}

// TestTheShutdownDrainOfALostShardCommitsNothing is the second fence, against
// the one drain no watermark asks for. The predecessor is not refused a write,
// because it makes none: it simply shuts down, some thousands of mutations after
// the database stopped honouring its epoch, and drains the window it has been
// holding all along.
//
// By then the successor has replayed that very window and gone on writing, so
// what the drain carries is rows at versions the database has left behind. Two
// things refuse it and this run does not say which, deliberately: what it holds
// is that the shard is whole afterwards, whichever of them spoke.
func TestTheShutdownDrainOfALostShardCommitsNothing(t *testing.T) {
	const seed = 20260913

	control := newSeams(t, seed)
	require.NoError(t, control.drive(t, seamsMutations))
	control.mgr.Close(control.ctx)
	expected := control.ledger.snapshot()

	s := newSeams(t, seed)
	require.NoError(t, s.drive(t, seamsBeforeLoss))
	predecessor := s.mgr
	held := predecessor.Totals()
	require.Greater(t, held.Acked, held.Applied,
		"the predecessor holds no window, so its shutdown drain would carry nothing")

	s.mgr = s.anotherNode(t)
	s.handOver(t)

	// The successor inherits that window through the log and goes on past it.
	require.NoError(t, s.drive(t, seamsBeforeLoss/4))
	applied, _, err := s.store.Watermark(s.ctx, seamsShard)
	require.NoError(t, err)
	require.Greater(t, applied, held.Applied,
		"the successor committed nothing, so the predecessor's window is not yet rows and this drain is not stale")
	landed := s.ledger.snapshot()
	refused := s.ledger.refusedDrains

	predecessor.Close(s.ctx)

	require.Greater(t, s.ledger.refusedDrains, refused,
		"the shutdown drain never reached the database, so nothing here was fenced")
	after, _, err := s.store.Watermark(s.ctx, seamsShard)
	require.NoError(t, err)
	require.Equal(t, applied, after,
		"a node that had lost the shard moved the watermark, and it moved it backwards")
	// Every row at the version the live owner left it: a window applied a second
	// time takes the ones it names back to where it found them.
	s.holds(t, landed)

	// And the shard is whole afterwards, which is the claim the assertions above
	// cannot make between them: the successor finishes the stream and the database
	// is what one uninterrupted owner of it would have left.
	require.NoError(t, s.drive(t, seamsMutations-s.acked))
	totals := s.mgr.Totals()
	s.mgr.Close(s.ctx)

	require.Empty(t, totals.Halted, "the successor halted after the predecessor's drain was refused")
	seqno, ok, err := s.store.Watermark(s.ctx, seamsShard)
	require.NoError(t, err)
	require.True(t, ok)
	require.EqualValues(t, seamsMutations, seqno,
		"the watermark is short of the stream: entries were acked and never applied by anybody")
	require.Empty(t, s.trims.violation())

	runs := union(expected.runs, s.ledger.runs, byRun)
	require.NotEmpty(t, runs)
	for _, key := range runs {
		want, got := control.runRow(t, key), s.runRow(t, key)
		if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
			t.Fatalf("run %s of workflow %s is not what one uninterrupted owner left (-uninterrupted +handed over):\n%s",
				key.runID, key.workflowID, diff)
		}
	}
	for _, key := range union(expected.current, s.ledger.current, byWorkflow) {
		require.Equal(t, control.currentRowOf(t, key), s.currentRowOf(t, key),
			"workflow %s names a different run after the handover", key.workflowID)
	}
	t.Logf("%d entries replayed by the successor, %d drains refused, watermark %d over %d runs",
		totals.Replayed, s.ledger.refusedDrains-refused, seqno, len(runs))
}

// TestAStaleRangeCompletionCannotTakeTheSuccessorsTasks is the drain's fence on
// its own, over a window nothing else in the transaction has any grounds to
// refuse: one range completion, which names a category and two keys and asserts
// nothing about anything. The rows it would take away belong to the owner that
// took the shard, and were acked to callers of his.
func TestAStaleRangeCompletionCannotTakeTheSuccessorsTasks(t *testing.T) {
	s := newSeams(t, 20260915)
	build := mutbuild.For(seamsShard)
	lowest, highest := tasks.NewImmediateKey(100), tasks.NewImmediateKey(200)

	// The predecessor's queue processor completes a range: acked into the log,
	// folded, and drained by nobody.
	predecessor, stale := s.mgr, s.epoch
	require.NoError(t, predecessor.Write(
		s.ctx, build.RangeComplete(taskCategory, lowest, highest), stale, s.rows))
	held := predecessor.Totals()
	require.Greater(t, held.Acked, held.Applied,
		"the completion was applied where it was written, so nothing stale is left to drain")

	// The shard changes hands. The successor replays that completion — acked, so
	// its to apply — and then writes tasks of its own inside the range it covered.
	s.mgr = s.anotherNode(t)
	s.handOver(t)
	written := []p.InternalHistoryTask{immediateTask(120), immediateTask(150), immediateTask(180)}
	require.NoError(t, s.mgr.Write(s.ctx, build.AddTasks(taskCategory, written...), s.epoch, s.rows))
	s.mgr.Close(s.ctx)

	present := s.taskIDs(t, lowest, highest)
	require.Len(t, present, len(written),
		"the successor's tasks are not in the database, so this run has nothing to lose")
	applied, _, err := s.store.Watermark(s.ctx, seamsShard)
	require.NoError(t, err)

	// And the predecessor shuts down, holding a completion whose range is now
	// full of another owner's acked rows.
	predecessor.Close(s.ctx)

	require.Equal(t, present, s.taskIDs(t, lowest, highest),
		"a range completion drained under a lost epoch deleted the tasks the shard's owner had acked")
	after, _, err := s.store.Watermark(s.ctx, seamsShard)
	require.NoError(t, err)
	require.Equal(t, applied, after,
		"the stale drain moved the watermark, and to its own seqno below the successor's: an owner after this one replays from under a log trimmed to the higher one")
}

// taskCategory is the queue this file's task traffic belongs to — an immediate
// one, so a range is two task ids and a page is ordered by them alone.
var taskCategory = tasks.CategoryTransfer

// immediateTask is one row of that queue at a given task id.
func immediateTask(id int64) p.InternalHistoryTask {
	return p.InternalHistoryTask{
		Key:  tasks.NewImmediateKey(id),
		Blob: &commonpb.DataBlob{Data: []byte("task"), EncodingType: enumspb.ENCODING_TYPE_PROTO3},
	}
}

// taskIDs is the ids the database holds in [lowest, highest) of that queue. It
// pages deliberately small: a comparison over whatever came back in one page is
// a comparison of first pages.
func (s *seams) taskIDs(t *testing.T, lowest, highest tasks.Key) []int64 {
	return s.taskIDsOf(t, taskCategory, lowest, highest)
}

// taskIDsOf pages one category's rows back out of the store, in key order.
func (s *seams) taskIDsOf(t *testing.T, category tasks.Category, lowest, highest tasks.Key) []int64 {
	t.Helper()
	var out []int64
	var token []byte
	for {
		resp, err := s.store.GetHistoryTasks(s.ctx, &p.GetHistoryTasksRequest{
			ShardID:             seamsShard,
			TaskCategory:        category,
			InclusiveMinTaskKey: lowest,
			ExclusiveMaxTaskKey: highest,
			BatchSize:           2,
			NextPageToken:       token,
		})
		require.NoError(t, err)
		for _, task := range resp.Tasks {
			out = append(out, task.Key.TaskID)
		}
		if token = resp.NextPageToken; len(token) == 0 {
			return out
		}
	}
}

// anotherNode is a second registry over the same log and the same database, on
// the seams this run's [seams] already holds: the predecessor is fenced by this
// node's own acquire rather than by a test reaching around it, and both nodes'
// drains land in the one ledger, which is the record of what the database holds
// whoever wrote it.
func (s *seams) anotherNode(t *testing.T) *cycle.Manager {
	t.Helper()
	mgr, err := cycle.NewManager(cycle.Deps{
		Log:       s.trims,
		Writer:    s.stage,
		Recoverer: s.stage,
		Registry:  tasks.NewDefaultTaskCategoryRegistry(),
	}, cycle.Fixed(seamsPolicy()))
	require.NoError(t, err)
	t.Cleanup(func() { mgr.Close(s.ctx) })
	return mgr
}

// handOver is an acquire in the order a real one has: the log is fenced at the
// new epoch first and the range id lands second, so the log's epoch never lags
// the database's. [seams.takeShard] does it the other way round, which is the
// order the epoch-loss case needs and the opposite of what these two are about.
func (s *seams) handOver(t *testing.T) {
	t.Helper()
	epoch := s.epoch + 1
	require.NoError(t, s.mgr.ShardAcquired(s.ctx, seamsShard, epoch))
	require.EqualValues(t, epoch, s.bumpRange(t),
		"the range id did not land on the epoch the log was just fenced at")
	s.epoch = epoch
}

// alienWrite writes one mutation of a stream of its own through mgr at epoch,
// and answers with what the layer said.
//
// A mutation of the run's own stream would cost more than the refusal is worth:
// the generator counts one as it hands it over and carries the version it would
// have left, so a write that never reached the log leaves that model a step ahead
// of the database for every later mutation of the same run — which then fails its
// assertion, thousands of mutations from here, on a condition that was nobody's
// defect. This stream draws its own namespace from its own seed, so nothing the
// write names is anything the shard's traffic will name again.
func (s *seams) alienWrite(t *testing.T, mgr *cycle.Manager, epoch wal.Epoch) error {
	t.Helper()
	cfg := mutgen.Default()
	cfg.Seed = 20260914
	cfg.ShardID = seamsShard
	alien, err := drive.NewStream(cfg)
	require.NoError(t, err)

	var refusal error
	require.NoError(t, alien.Drive(1, func(d drive.Delivery) error {
		refusal = mgr.Write(s.ctx, d.Mutation, epoch, s.rows)
		// The refusal is this function's answer, not the drive's: returning it
		// here would report a refused write as a stream that ended.
		return nil
	}))
	return refusal
}
