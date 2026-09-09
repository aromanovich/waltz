package acceptance

// Two nodes over one log and one store, which is the whole of what "two nodes"
// means to this layer: nothing in it speaks to another node, so every
// interaction between two owners of a shard already goes through the log and the
// epoch. A second process would add an address space and no schedule.
//
// What that buys is the one question a single cycle cannot be asked: whether a
// drain's witness is owner-scoped. [cycle.Cycle.resolve] reads the shard's
// watermark and compares it against its own drain's seqno, consulting neither
// its epoch nor the log — so a watermark another owner moved answers a question
// it was never asked.

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"

	"github.com/aromanovich/waltz/baserow"
	"github.com/aromanovich/waltz/cold"
	"github.com/aromanovich/waltz/cold/memcold"
	"github.com/aromanovich/waltz/cycle"
	"github.com/aromanovich/waltz/fold"
	"github.com/aromanovich/waltz/internal/verify/drive"
	"github.com/aromanovich/waltz/internal/verify/mutgen"
	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/wal"
	"github.com/aromanovich/waltz/wal/memwal"
)

const twoNodeShard = wal.ShardID(11)

// errAmbiguous is what a transport that gave out mid-transaction leaves behind:
// no class the applier recognises, so [apply.Classify] calls it an unknown
// outcome, which is the one arm that reaches resolve.
var errAmbiguous = errors.New("the connection went away mid-transaction")

// parkedApplier holds the first drain inside Apply until a test releases it, and
// answers that one with errAmbiguous rather than passing it to the store. That
// is a drain whose rows were never written and whose outcome its caller cannot
// read — the state the watermark exists to resolve. Every later drain passes
// through.
type parkedApplier struct {
	inner   cold.Applier
	release chan struct{}
	held    chan struct{}
	once    sync.Once
	freed   sync.Once
}

// free releases the parked drain, and is safe to call more than once.
func (a *parkedApplier) free() { a.freed.Do(func() { close(a.release) }) }

func (a *parkedApplier) Apply(ctx context.Context, shard wal.ShardID, epoch wal.Epoch, batch fold.Batch) error {
	first := false
	a.once.Do(func() { first = true; close(a.held) })
	if !first {
		return a.inner.Apply(ctx, shard, epoch, batch)
	}
	<-a.release
	return errAmbiguous
}

// TestASyncWriterIsNotToldItSucceededByAnotherNodesWatermark is the second
// candidate of the data-loss audit, staged rather than argued.
//
// Node A runs sync mode, where the drain is what answers the caller and the ack
// is therefore provisional: the condition has not been verified when the entry
// becomes durable. A's caller writes a mutation whose condition is false. A
// appends it, enters its drain, and the drain's outcome becomes unreadable.
//
// While A is in there, node B takes the shard, replays that entry, meets the
// same false condition and drops it — which is exactly right, and moves no
// watermark. B then writes and drains work of its own, which does move it, to a
// seqno above A's.
//
// A now asks the only witness it has. The seqno it names is its own; the
// watermark it reads is B's. Answering "at or above, so my drain committed"
// would return nil to a caller whose write was refused and whose rows exist
// nowhere — an acknowledgement of a state transition that did not happen, which
// is the acked-is-never-lost rule broken from the far side.
func TestASyncWriterIsNotToldItSucceededByAnotherNodesWatermark(t *testing.T) {
	ctx := context.Background()

	store, release, err := memcold.New("twonode")
	require.NoError(t, err)
	t.Cleanup(release)
	rows, err := baserow.Of(store)
	require.NoError(t, err)
	log := memwal.New()
	registry := tasks.NewDefaultTaskCategoryRegistry()

	parked := &parkedApplier{inner: store, release: make(chan struct{}), held: make(chan struct{})}

	sync := cycle.Defaults()
	sync.Sync = true
	nodeA, err := cycle.NewManager(cycle.Deps{
		Log: log, Writer: parked, Recoverer: store, Registry: registry,
	}, cycle.Fixed(sync))
	require.NoError(t, err)
	t.Cleanup(func() { nodeA.Close(ctx) })

	nodeB, err := cycle.NewManager(cycle.Deps{
		Log: log, Writer: store, Recoverer: store, Registry: registry,
	}, cycle.Fixed(sync))
	require.NoError(t, err)
	t.Cleanup(func() { nodeB.Close(ctx) })
	// Registered last so it runs first: a Close on the parked node would
	// otherwise wait for a drain nothing is going to release.
	t.Cleanup(parked.free)

	// A takes the shard, and its caller writes a mutation asserting a version no
	// row has. Sync mode takes no pre-window read, so nothing refuses it before
	// the append: that is what the provisional bit is for.
	epochA := takeShard(t, store)
	require.NoError(t, nodeA.ShardAcquired(ctx, twoNodeShard, epochA))

	// A create and an update of one run, from the generator, so both are
	// requests the store below will actually execute. A writes only the update,
	// against a run no create has landed for: its version assertion cannot hold.
	create, update := twoUnrelatedRuns(t)
	doomed := update

	answered := make(chan error, 1)
	go func() { answered <- nodeA.Write(ctx, doomed, epochA, rows) }()
	<-parked.held // A is inside its drain, with the entry durable behind it

	// B takes the shard and is asked for work of its own. Its first request
	// replays A's tail: the entry is provisional and its condition is false, so
	// it is dropped and the watermark stays where it was. B's own write then
	// drains, and that one commits.
	epochB := takeShard(t, store)
	require.Greater(t, epochB, epochA)
	require.NoError(t, nodeB.ShardAcquired(ctx, twoNodeShard, epochB))

	seeded := create
	if err := nodeB.Write(ctx, seeded, epochB, rows); err != nil {
		t.Fatalf("node B could not take the shard on: %v", err)
	}

	mark, found, err := store.Watermark(ctx, twoNodeShard)
	require.NoError(t, err)
	require.True(t, found, "B's own drain has to have committed, or there is no foreign watermark to be fooled by")
	require.GreaterOrEqual(t, mark, wal.FirstSeqno,
		"and it must sit at or above A's seqno, which is the comparison resolve makes")
	require.EqualValues(t, 1, nodeB.Totals().Dropped,
		"B must have met the false condition and dropped the entry, not applied it")
	t.Logf("A acked at seqno 1; B dropped it and its own drain left the watermark at %d", mark)

	// A's drain finally comes back, unreadable. It reads the watermark B moved.
	parked.free()
	got := <-answered

	require.IsType(t, &p.ShardOwnershipLostError{}, got,
		"A's caller must be told the shard is gone, not that a write another node dropped succeeded; got %v", got)
	require.NotErrorIs(t, got, errAmbiguous, "and not handed back the ambiguity either")

	// Whatever A answered, the truth is that the update left no row behind: its
	// condition never held, on either node.
	mut := doomed.Update.UpdateWorkflowMutation
	_, err = store.GetWorkflowExecution(ctx, &p.GetWorkflowExecutionRequest{
		ShardID: int32(twoNodeShard), NamespaceID: mut.NamespaceID,
		WorkflowID: mut.WorkflowID, RunID: mut.RunID,
	})
	require.Error(t, err, "the doomed run must exist nowhere: its condition never held")
}

// twoUnrelatedRuns is a create of one run and an update of another, both from
// the generator so that each is a request the cold store can execute. The
// update's own run is never created, which is what makes its version assertion
// fail wherever it is applied.
func twoUnrelatedRuns(t *testing.T) (create, update mutation.Mutation) {
	t.Helper()
	cfg := mutgen.Default()
	cfg.Seed = 20260909
	cfg.ShardID = int32(twoNodeShard)
	cfg.Workflows = 4
	stream, err := drive.NewStream(cfg)
	require.NoError(t, err)

	require.NoError(t, stream.Drive(256, func(m drive.Delivery) error {
		switch {
		case create.Kind() == mutation.KindInvalid && m.Mutation.Kind() == mutation.KindCreate:
			create = m.Mutation
		case create.Kind() != mutation.KindInvalid && update.Kind() == mutation.KindInvalid &&
			m.Mutation.Kind() == mutation.KindUpdate &&
			m.Mutation.Update.UpdateWorkflowMutation.RunID != create.Create.NewWorkflowSnapshot.RunID:
			update = m.Mutation
		}
		return nil
	}))
	require.NotEqual(t, mutation.KindInvalid, create.Kind(), "the generator produced no create")
	require.NotEqual(t, mutation.KindInvalid, update.Kind(),
		"the generator produced no update of a run other than that create's")
	return create, update
}

// takeShard moves the shard row's range id, which is what an acquire is from
// underneath, and hands back the epoch that acquire is fenced at (I11).
func takeShard(t *testing.T, store *memcold.Store) wal.Epoch {
	t.Helper()
	rangeID, err := drive.TakeShardBumpingRangeID(context.Background(), store.ShardStore(), int32(twoNodeShard))
	require.NoError(t, err)
	return wal.Epoch(rangeID)
}
