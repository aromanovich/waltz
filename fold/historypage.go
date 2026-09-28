package fold

// One page of a history branch, answered from the cold store's rows and the
// window's undrained nodes.
//
// The pagination rule is [taskpage.go]'s, and it is the same rule for the same
// reason: the base's token is the plugin's own format, which this layer may not
// parse or synthesise, so the cut is at the end of a base page or below its
// first row, never inside one. Either the whole base page is emitted and its
// token advances, or none of it is and the incoming token is handed back
// untouched. That is what lets a merged page be exactly the size asked for, and
// it bounds re-reading the base to the one branch where the window alone fills a
// page — see the fourth requirement on [HistoryBasePage].
//
// What differs from a task page is the order and the absence of deletes. A
// history page has a direction: forward is node ascending and transaction
// descending, which is the order the store reads in because it stores the
// transaction id negated and sorts ascending; reverse inverts both. And nothing
// is subtracted — a window node is a row that is not in the store yet, never one
// that has been taken out of it.

import (
	"cmp"
	"encoding/json"
	"fmt"
	"slices"

	p "go.temporal.io/server/common/persistence"
)

// HistoryBasePage is one page of the cold store's own answer for the branch and
// range the request names. A page size and a token rather than a request, since
// those are the only two things the merge decides.
//
// It is held to three of the four requirements [BasePage] states — a page no
// larger than the ask, rows in the store's own order, and no empty page beside a
// token — and not to the range one: a history read filters by node id on both
// sides, so a row outside the range is dropped here rather than reaching a
// caller. A zero-length returned token means the base is exhausted.
//
// There is a fourth, which [BasePage] has too and does not say: **a token may be
// handed back, and must answer the same rows.** It follows from the cut rule
// rather than from this merge — a base page the cut emits nothing from is left
// unread, and the only way to reach it again is its own token. The rule bounds
// where that happens to the one branch where the window alone fills a page, so a
// pagination the window does not overflow never re-asks anything; it does not
// remove it. Temporal's SQL plugins satisfy it outright, their history token
// being the last row's key rather than a session, and Cassandra's driver paging
// state is re-usable by the same argument. A store whose token is a server-side
// cursor consumed by reading is the one that cannot serve this seam.
type HistoryBasePage func(pageSize int, token []byte) ([]p.InternalHistoryNode, []byte, error)

// historyKey orders one node. The pair is the store's own row key, and a window
// node whose key a cold row already has is that same row, mid-drain: the cold
// row wins, because it is the one the store will keep.
type historyKey struct {
	nodeID int64
	txnID  int64
}

func keyOf(n p.InternalHistoryNode) historyKey {
	return historyKey{nodeID: n.NodeID, txnID: n.TransactionID}
}

// compareHistory orders two keys the way the store returns them. Forward is
// node ascending, transaction descending; reverse is both inverted.
func compareHistory(a, b historyKey, reverse bool) int {
	if reverse {
		if c := cmp.Compare(b.nodeID, a.nodeID); c != 0 {
			return c
		}
		return cmp.Compare(a.txnID, b.txnID)
	}
	if c := cmp.Compare(a.nodeID, b.nodeID); c != 0 {
		return c
	}
	return cmp.Compare(b.txnID, a.txnID)
}

// historyPageToken is what a merged read hands back: the base store's own token
// and the last key emitted, which is where the window's half resumes.
type historyPageToken struct {
	// Base is the cold store's token for this branch, as the cold store wrote
	// it. Empty beside BaseDone false means the base has not been asked yet.
	Base []byte `json:"base,omitempty"`
	// BaseDone says the base is exhausted, so no further call is made to it.
	BaseDone bool `json:"baseDone,omitempty"`

	AfterNode int64 `json:"afterNode,omitempty"`
	AfterTxn  int64 `json:"afterTxn,omitempty"`
	After     bool  `json:"after,omitempty"`
}

func (t *historyPageToken) after() (historyKey, bool) {
	if !t.After {
		return historyKey{}, false
	}
	return historyKey{nodeID: t.AfterNode, txnID: t.AfterTxn}, true
}

func (t *historyPageToken) setAfter(k historyKey) {
	t.After, t.AfterNode, t.AfterTxn = true, k.nodeID, k.txnID
}

// historyTokenMagic frames this layer's token so a store's own can be told
// apart. Same rule as the task page's: what a prefix answers is whether the
// token is one of ours at all.
var historyTokenMagic = [4]byte{'w', 'h', 's', '1'}

// encodeHistoryToken frames one of ours. A nil token is the pagination being
// over, which is an absent token rather than an empty one.
func encodeHistoryToken(t *historyPageToken) []byte {
	if t == nil {
		return nil
	}
	body, err := json.Marshal(t)
	if err != nil {
		panic(fmt.Sprintf("fold: encoding a history page token: %v", err))
	}
	return append(historyTokenMagic[:], body...)
}

// decodeHistoryToken reads one of ours back. A token that is not ours is
// adopted as the base's rather than refused, which is the one place this differs
// from the task page: history reads have callers that legitimately paginate
// across a node with no cycle, so a page answered from the cold store alone
// hands back the store's own token and the next page may arrive here with it.
func decodeHistoryToken(raw []byte) *historyPageToken {
	var t historyPageToken
	if len(raw) >= len(historyTokenMagic) && [4]byte(raw[:4]) == historyTokenMagic &&
		json.Unmarshal(raw[len(historyTokenMagic):], &t) == nil {
		return &t
	}
	// The zero value is what "no cursor either side" means, so an empty argument
	// needs no case of its own and no caller needs a nil check.
	return &historyPageToken{Base: raw}
}

// BaseHistoryToken is the cold store's own token inside one this layer wrote, or
// the argument unchanged where it wrote none. It is what a caller hands the base
// store when it is about to answer a page without this window in it: a token of
// ours reaching a plugin's parser is a pagination that fails mid-read.
func BaseHistoryToken(raw []byte) []byte {
	if len(raw) == 0 {
		return nil
	}
	t := decodeHistoryToken(raw)
	if t.BaseDone {
		// The base had no more to give when we stopped asking it, and what is
		// left of the pagination is window nodes this caller cannot reach.
		// An empty token restarts the base rather than ending it, so the rows it
		// repeats are ones the caller has seen and none are skipped.
		return nil
	}
	return t.Base
}

// HistoryPage answers one page of the branch the request names from this
// window's nodes and the cold store's, under the rule at the top of this file.
// treeID is parsed from the branch token by the caller, this layer holding no
// branch-token codec of its own.
func (a *Accumulator) HistoryPage(
	req *p.InternalReadHistoryBranchRequest,
	treeID string,
	base HistoryBasePage,
) (*p.InternalReadHistoryBranchResponse, error) {
	token := decodeHistoryToken(req.NextPageToken)
	window := a.branchNodes(treeID, req.BranchID)
	page, next, err := mergeHistoryPage(req, base, window, token)
	if err != nil {
		return nil, err
	}
	return &p.InternalReadHistoryBranchResponse{
		Nodes:         page,
		NextPageToken: encodeHistoryToken(next),
	}, nil
}

func mergeHistoryPage(
	req *p.InternalReadHistoryBranchRequest,
	base HistoryBasePage,
	window []p.InternalHistoryNode,
	token *historyPageToken,
) ([]p.InternalHistoryNode, *historyPageToken, error) {
	// A page of zero rows would make the pagination endless.
	pageSize := max(req.PageSize, 1)
	after, hasAfter := token.after()

	// What the window still owes this pagination, in the store's own order.
	var tail []p.InternalHistoryNode
	for _, node := range window {
		if node.NodeID < req.MinNodeID || node.NodeID >= req.MaxNodeID {
			continue
		}
		if hasAfter && compareHistory(keyOf(node), after, req.ReverseOrder) <= 0 {
			continue
		}
		if req.MetadataOnly {
			node.Events = nil
		}
		tail = append(tail, node)
	}
	slices.SortFunc(tail, func(x, y p.InternalHistoryNode) int {
		return compareHistory(keyOf(x), keyOf(y), req.ReverseOrder)
	})

	var basePage []p.InternalHistoryNode
	var nextBase []byte
	baseDone := token.BaseDone
	if !baseDone {
		// The base is asked for what the window does not already fill, floored at
		// one: asking for nothing would end the pagination with rows left in the
		// store.
		ask := max(pageSize-len(tail), 1)
		var err error
		basePage, nextBase, err = base(ask, token.Base)
		if err != nil {
			return nil, nil, err
		}
		if err := refuseHistoryPage(basePage, nextBase, ask, req.ReverseOrder); err != nil {
			return nil, nil, err
		}
	}

	// How far this page may reach. A base page with a token says nothing above
	// its last key, so a window node beyond it may have unread cold rows before
	// it; without a token the base is exhausted and every window node is in
	// reach.
	bounded := len(nextBase) > 0 && len(basePage) > 0
	inReach := tail
	var upTo historyKey
	if bounded {
		upTo = keyOf(basePage[len(basePage)-1])
		inReach = nil
		for _, node := range tail {
			if compareHistory(keyOf(node), upTo, req.ReverseOrder) <= 0 {
				inReach = append(inReach, node)
			}
		}
	}

	merged := mergeHistoryNodes(basePage, inReach, req.ReverseOrder)
	if len(merged) <= pageSize {
		if !bounded {
			// The base is exhausted and every remaining window node is in this
			// page: the pagination is over.
			return merged, nil, nil
		}
		next := &historyPageToken{Base: nextBase}
		next.setAfter(upTo)
		return merged, next, nil
	}

	// The window alone overflows the page. Emit window nodes strictly below the
	// base page's first key and leave that page unread: its token is untouched,
	// so the same rows come back next time. This branch implies len(tail) >=
	// pageSize, hence an ask of one, so at most a single base row is held back.
	cut := len(inReach)
	if len(basePage) > 0 {
		first := keyOf(basePage[0])
		cut = 0
		for _, node := range inReach {
			if compareHistory(keyOf(node), first, req.ReverseOrder) >= 0 {
				break
			}
			cut++
		}
	}
	next := &historyPageToken{BaseDone: baseDone || len(basePage) == 0}
	if cut == 0 {
		// The base's first row is at or below the window's first, so nothing is
		// strictly below it and cutting there would emit an empty page for ever.
		// That row is merged[0] — a tie deduplicates to it — and this branch
		// holds at most one base row, so emitting that one entry emits the base
		// page whole, which the cut rule allows.
		next.Base, next.BaseDone = nextBase, !bounded
		next.setAfter(keyOf(merged[0]))
		return merged[:1], next, nil
	}
	only := inReach[:min(cut, pageSize)]
	// Nothing was emitted from the base, so its cursor stays where it was.
	next.Base = token.Base
	next.setAfter(keyOf(only[len(only)-1]))
	return only, next, nil
}

// refuseHistoryPage holds the store to what this merge's own arithmetic rests
// on: a page no larger than it asked for, rows in the store's own order, and no
// empty page beside a token saying it holds more. The first two bound the page
// and the window's half of it; the third would be read here as the end of a
// pagination, so rows the store still held are never shown.
func refuseHistoryPage(page []p.InternalHistoryNode, token []byte, ask int, reverse bool) error {
	if len(page) == 0 {
		if len(token) > 0 {
			return fmt.Errorf("%w: a token of %d bytes", ErrBasePageEmptyBesideAToken, len(token))
		}
		return nil
	}
	if len(page) > ask {
		return fmt.Errorf("%w: asked for %d, got %d", ErrBasePageTooLarge, ask, len(page))
	}
	for i := 1; i < len(page); i++ {
		if compareHistory(keyOf(page[i-1]), keyOf(page[i]), reverse) >= 0 {
			return fmt.Errorf("%w: node %d follows node %d",
				ErrBasePageNotAscending, page[i].NodeID, page[i-1].NodeID)
		}
	}
	return nil
}

// mergeHistoryNodes merges two pages already in the store's order. A key both
// sides hold is one the drain has written since the window took it, and the
// cold row is the one that survives.
func mergeHistoryNodes(base, tail []p.InternalHistoryNode, reverse bool) []p.InternalHistoryNode {
	out := make([]p.InternalHistoryNode, 0, len(base)+len(tail))
	i, j := 0, 0
	for i < len(base) && j < len(tail) {
		switch c := compareHistory(keyOf(base[i]), keyOf(tail[j]), reverse); {
		case c == 0:
			out = append(out, base[i])
			i, j = i+1, j+1
		case c < 0:
			out = append(out, base[i])
			i++
		default:
			out = append(out, tail[j])
			j++
		}
	}
	out = append(out, base[i:]...)
	return append(out, tail[j:]...)
}
