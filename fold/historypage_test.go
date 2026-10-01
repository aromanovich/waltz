package fold

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"

	"github.com/aromanovich/waltz/mutation"
)

// The order the merge has to hold is the store's own, and it is not the obvious
// one: the store keeps the transaction id negated and sorts ascending, so a
// forward page is node ascending and transaction *descending*. Getting it
// backwards puts two writes of one node the wrong way round, and the reader
// takes the older one.
func TestAForwardPageOrdersNodesUpAndTransactionsDown(t *testing.T) {
	acc := New(7)
	windowHolds(acc, "tree", "b", []p.InternalHistoryNode{
		node(5, 100), node(4, 200), node(5, 101),
	})

	page := readWholeBranch(t, acc, request(20), noBaseRows)
	require.Equal(t, []historyKey{{4, 200}, {5, 101}, {5, 100}}, keysOf(page))
}

func TestAReversePageInvertsBothHalvesOfTheKey(t *testing.T) {
	acc := New(7)
	windowHolds(acc, "tree", "b", []p.InternalHistoryNode{
		node(5, 100), node(4, 200), node(5, 101),
	})

	req := request(20)
	req.ReverseOrder = true
	page := readWholeBranch(t, acc, req, noBaseRows)
	require.Equal(t, []historyKey{{5, 100}, {5, 101}, {4, 200}}, keysOf(page))
}

// The whole point of the merge: a history row acked into the window is a row
// the cold store does not have yet, and a reader that missed it would rebuild a
// workflow short the events its own caller was told were durable.
func TestAPageInterleavesTheWindowWithTheColdStore(t *testing.T) {
	acc := New(7)
	windowHolds(acc, "tree", "b", []p.InternalHistoryNode{node(2, 200), node(4, 400)})

	page := readWholeBranch(t, acc, request(20), rows(node(1, 100), node(3, 300)))
	require.Equal(t, []historyKey{{1, 100}, {2, 200}, {3, 300}, {4, 400}}, keysOf(page))
}

// A node the drain wrote between the window's take and the base read is in both
// halves. The cold row is the one that stays, being the one the store keeps.
func TestANodeInBothHalvesIsEmittedOnce(t *testing.T) {
	acc := New(7)
	windowHolds(acc, "tree", "b", []p.InternalHistoryNode{node(1, 100), node(2, 200)})

	page := readWholeBranch(t, acc, request(20), rows(node(1, 100)))
	require.Equal(t, []historyKey{{1, 100}, {2, 200}}, keysOf(page))
}

// The pagination rule, which is taskpage.go's: a page is never larger than the
// size asked for, and no base page is ever half-emitted — so the store is never
// asked to answer one token twice, and a caller's own token is never parsed
// here. Driven across every page size that cuts the stream somewhere different.
func TestPagingNeverCutsInsideABasePageAndNeverOverruns(t *testing.T) {
	for size := 1; size <= 9; size++ {
		t.Run(fmt.Sprintf("page size %d", size), func(t *testing.T) {
			acc := New(7)
			windowHolds(acc, "tree", "b", []p.InternalHistoryNode{
				node(2, 200), node(4, 400), node(6, 600), node(8, 800),
			})
			base := countingBase(rows(node(1, 100), node(3, 300), node(5, 500), node(7, 700)))

			req := request((size))
			var got []historyKey
			var token []byte
			for range 40 {
				req.NextPageToken = token
				resp, err := acc.HistoryPage(req, "tree", base.page)
				require.NoError(t, err)
				require.LessOrEqual(t, len(resp.Nodes), size, "a page may not exceed the size asked for")
				got = append(got, keysOf(resp.Nodes)...)
				if token = resp.NextPageToken; len(token) == 0 {
					break
				}
			}
			require.Equal(t, []historyKey{
				{1, 100}, {2, 200}, {3, 300}, {4, 400}, {5, 500}, {6, 600}, {7, 700}, {8, 800},
			}, got, "every node, once, in order, whichever page size cuts the stream")
			if size > len(acc.historyWrites) {
				require.Zero(t, base.reAsked,
					"a pagination the window does not overflow never re-asks the base a cursor")
			}
		})
	}
}

// The cut rule's other half, which is what a base token handed back untouched
// means: where the window alone fills the page, the base page just read is
// emitted nowhere and its cursor stays exactly where it was. Half-emitting it
// would lose rows on one side and duplicate them on the other.
func TestAPageTheWindowFillsLeavesTheBasesCursorWhereItWas(t *testing.T) {
	acc := New(7)
	windowHolds(acc, "tree", "b", []p.InternalHistoryNode{
		node(2, 200), node(3, 300), node(4, 400),
	})
	base := countingBase(rows(node(5, 500), node(6, 600)))

	req := request(2)
	req.NextPageToken = encodeHistoryToken(&historyPageToken{Base: []byte{0}})
	resp, err := acc.HistoryPage(req, "tree", base.page)
	require.NoError(t, err)
	require.Equal(t, []historyKey{{2, 200}, {3, 300}}, keysOf(resp.Nodes))
	require.Equal(t, []byte{0}, BaseHistoryToken(resp.NextPageToken),
		"nothing of the base page was emitted, so its cursor may not move")
}

func TestAPageDropsWindowNodesOutsideTheRange(t *testing.T) {
	acc := New(7)
	windowHolds(acc, "tree", "b", []p.InternalHistoryNode{node(1, 100), node(5, 500), node(9, 900)})

	req := request(20)
	req.MinNodeID, req.MaxNodeID = 5, 9
	page := readWholeBranch(t, acc, req, noBaseRows)
	require.Equal(t, []historyKey{{5, 500}}, keysOf(page))
}

// TestAWindowOverflowingThePageWithNothingUnderItPaginates drives the one branch
// where the base page is empty and the window alone is longer than the page.
// Without the guard that keeps the branch from reading basePage[0] it panics,
// inside a history read, on the shard's own goroutine.
//
// It is reachable rather than defensive: the base answers an empty page for a
// branch whose nodes are all still in the window, which is every branch of a
// workflow whose events this shard has acked and not yet drained.
func TestAWindowOverflowingThePageWithNothingUnderItPaginates(t *testing.T) {
	acc := New(7)
	windowHolds(acc, "tree", "b", []p.InternalHistoryNode{
		node(1, 100), node(2, 200), node(3, 300),
	})

	req := request(2)
	var got []historyKey
	var token []byte
	for range 10 {
		req.NextPageToken = token
		resp, err := acc.HistoryPage(req, "tree", noBaseRows)
		require.NoError(t, err)
		require.LessOrEqual(t, len(resp.Nodes), 2)
		got = append(got, keysOf(resp.Nodes)...)
		if token = resp.NextPageToken; len(token) == 0 {
			break
		}
	}
	require.Equal(t, []historyKey{{1, 100}, {2, 200}, {3, 300}}, got,
		"every window node, once, in order, with no base page to rest the cut on")
}

func TestMetadataOnlyStripsTheWindowsBlobsToo(t *testing.T) {
	acc := New(7)
	windowHolds(acc, "tree", "b", []p.InternalHistoryNode{node(1, 100)})

	req := request(20)
	req.MetadataOnly = true
	page := readWholeBranch(t, acc, req, noBaseRows)
	require.Len(t, page, 1)
	require.Nil(t, page[0].Events, "a metadata-only read asked for no blobs, and the window's are blobs")
}

// The window is keyed by tree and branch, and a branch id is unique only inside
// a tree: a page that keyed on the branch alone would hand one tree's nodes to
// another's reader.
func TestAPageSeesOnlyItsOwnBranchOfItsOwnTree(t *testing.T) {
	acc := New(7)
	windowHolds(acc, "tree", "b", []p.InternalHistoryNode{node(1, 100)})
	windowHolds(acc, "tree", "other", []p.InternalHistoryNode{node(2, 200)})
	windowHolds(acc, "other-tree", "b", []p.InternalHistoryNode{node(3, 300)})

	page := readWholeBranch(t, acc, request(20), noBaseRows)
	require.Equal(t, []historyKey{{1, 100}}, keysOf(page))
}

// A store that breaks one of the three things this merge's arithmetic rests on
// is refused rather than carried: what it costs is spent in somebody else's
// reader, which cannot name the store that did it.
func TestABasePageTheMergeCannotRestOnIsRefused(t *testing.T) {
	cases := map[string]struct {
		rows  []p.InternalHistoryNode
		token []byte
		want  error
	}{
		"empty beside a token": {nil, []byte("more"), ErrBasePageEmptyBesideAToken},
		"not ascending":        {[]p.InternalHistoryNode{node(3, 300), node(1, 100)}, nil, ErrBasePageNotAscending},
		// Ascending means strictly. What an equal pair costs is a node the merge
		// emits twice — mergeHistoryNodes deduplicates *between* the two sources
		// and walks each of them as a strictly ascending run.
		"a key twice": {[]p.InternalHistoryNode{node(2, 200), node(2, 200)}, nil, ErrBasePageNotAscending},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			acc := New(7)
			_, err := acc.HistoryPage(request(20), "tree",
				func(int, []byte) ([]p.InternalHistoryNode, []byte, error) { return c.rows, c.token, nil })
			require.ErrorIs(t, err, c.want)
		})
	}

	t.Run("larger than asked for", func(t *testing.T) {
		acc := New(7)
		_, err := acc.HistoryPage(request(1), "tree",
			func(int, []byte) ([]p.InternalHistoryNode, []byte, error) {
				return []p.InternalHistoryNode{node(1, 100), node(2, 200)}, nil, nil
			})
		require.ErrorIs(t, err, ErrBasePageTooLarge)
	})
}

// A token of ours reaching a plugin's own parser is a pagination that fails
// mid-read, so every caller about to answer without this window unwraps first.
func TestTheBaseTokenComesBackOutOfOneOfOurs(t *testing.T) {
	require.Nil(t, BaseHistoryToken(nil))
	require.Equal(t, []byte("theirs"), BaseHistoryToken([]byte("theirs")),
		"a token we did not write is the base's own and travels unchanged")

	ours := encodeHistoryToken(&historyPageToken{Base: []byte("theirs"), AfterNode: 4, After: true})
	require.Equal(t, []byte("theirs"), BaseHistoryToken(ours))

	done := encodeHistoryToken(&historyPageToken{Base: []byte("spent"), BaseDone: true, AfterNode: 4, After: true})
	require.Nil(t, BaseHistoryToken(done),
		"a cursor the base already said it was done with names rows behind the reader: handing it back "+
			"would restart the pagination there, and an empty token repeats rows the caller has seen instead")
}

// ---------------------------------------------------------------- fixtures

func node(nodeID, txnID int64) p.InternalHistoryNode {
	return p.InternalHistoryNode{
		NodeID:            nodeID,
		TransactionID:     txnID,
		PrevTransactionID: txnID - 1,
		Events:            &commonpb.DataBlob{Data: []byte{byte(nodeID)}},
	}
}

func request(pageSize int) *p.InternalReadHistoryBranchRequest {
	return &p.InternalReadHistoryBranchRequest{
		ShardID:   7,
		BranchID:  "b",
		MinNodeID: 1,
		MaxNodeID: 1 << 30,
		PageSize:  pageSize,
	}
}

func keysOf(nodes []p.InternalHistoryNode) []historyKey {
	out := make([]historyKey, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, keyOf(n))
	}
	return out
}

func noBaseRows(int, []byte) ([]p.InternalHistoryNode, []byte, error) { return nil, nil, nil }

// rows answers the whole set in one page, which is what a base with fewer rows
// than the ask does.
func rows(ns ...p.InternalHistoryNode) HistoryBasePage {
	return func(ask int, _ []byte) ([]p.InternalHistoryNode, []byte, error) {
		return ns[:min(ask, len(ns))], nil, nil
	}
}

// base pages through its rows one ask at a time and counts the tokens it is
// handed twice.
type base struct {
	all []p.InternalHistoryNode
	// seen counts each cursor handed over, and reAsked is how many were handed
	// over twice — the store obligation the cut rule bounds but does not remove.
	seen    map[string]int
	reAsked int
}

func countingBase(one HistoryBasePage) *base {
	all, _, _ := one(1<<30, nil)
	return &base{all: all, seen: map[string]int{}}
}

func (b *base) page(ask int, token []byte) ([]p.InternalHistoryNode, []byte, error) {
	if len(token) > 0 {
		b.seen[string(token)]++
		if b.seen[string(token)] > 1 {
			b.reAsked++
		}
	}
	from := 0
	if len(token) > 0 {
		from = int(token[0])
	}
	to := min(from+ask, len(b.all))
	if from >= len(b.all) {
		return nil, nil, nil
	}
	var next []byte
	if to < len(b.all) {
		next = []byte{byte(to)}
	}
	return b.all[from:to], next, nil
}

func readWholeBranch(
	t *testing.T, acc *Accumulator, req *p.InternalReadHistoryBranchRequest, base HistoryBasePage,
) []p.InternalHistoryNode {
	t.Helper()
	resp, err := acc.HistoryPage(req, "tree", base)
	require.NoError(t, err)
	require.Empty(t, resp.NextPageToken, "the fixture's page holds everything")
	return resp.Nodes
}

// What the drain gets: every batch the window folded, in WAL order, and a
// watermark at or above the entry that carried the last one. A batch left
// behind is a mutable state published over history rows nobody wrote.
func TestTheBatchCarriesEveryFoldedBatchInLogOrder(t *testing.T) {
	acc := New(shardID)
	require.NoError(t, acc.Add(11, carrying(create(p.CreateWorkflowModeBrandNew), 4, 5)))
	require.NoError(t, acc.Add(12, carrying(update(p.UpdateWorkflowModeUpdateCurrent), 6)))

	batch := acc.Drain()
	require.Equal(t, []int64{4, 5, 6}, nodeIDs(batch.History()))
	require.GreaterOrEqual(t, int(batch.Watermark()), 12,
		"a watermark below the entry that carried a batch would have the drain publish rows the log still owns")

	require.Empty(t, acc.Drain().History(), "a drained window may not hand the same batches to a second transaction")
}

func nodeIDs(rs []*p.InternalAppendHistoryNodesRequest) []int64 {
	out := make([]int64, 0, len(rs))
	for _, r := range rs {
		out = append(out, r.Node.NodeID)
	}
	return out
}

// carrying puts one batch per node id on a create's or an update's own slot, the
// way a record that carried them decodes.
func carrying[R *p.InternalCreateWorkflowExecutionRequest | *p.InternalUpdateWorkflowExecutionRequest](
	req R, nodeIDs ...int64,
) mutation.Mutation {
	batches := make([]*p.InternalAppendHistoryNodesRequest, 0, len(nodeIDs))
	for _, id := range nodeIDs {
		batches = append(batches, &p.InternalAppendHistoryNodesRequest{
			ShardID:    int32(shardID),
			BranchInfo: &persistencespb.HistoryBranch{TreeId: "tree", BranchId: "b"},
			Node:       node(id, 100+id),
		})
	}
	switch r := any(req).(type) {
	case *p.InternalCreateWorkflowExecutionRequest:
		r.NewWorkflowNewEvents = batches
		return mutation.Mutation{Create: r}
	default:
		r.(*p.InternalUpdateWorkflowExecutionRequest).UpdateWorkflowNewEvents = batches
		return mutation.Mutation{Update: r.(*p.InternalUpdateWorkflowExecutionRequest)}
	}
}

// windowHolds fills the accumulator's history the way a folded record does: one
// append request per node, all on one branch of one tree.
func windowHolds(acc *Accumulator, treeID, branchID string, nodes []p.InternalHistoryNode) {
	for _, n := range nodes {
		acc.historyWrites = append(acc.historyWrites, &p.InternalAppendHistoryNodesRequest{
			ShardID:    int32(acc.shard),
			BranchInfo: &persistencespb.HistoryBranch{TreeId: treeID, BranchId: branchID},
			Node:       n,
		})
	}
}
