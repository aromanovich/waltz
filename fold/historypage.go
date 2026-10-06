package fold

// One page of a history branch, built from the cold store's rows and the
// window's undrained nodes.
//
// The pagination rule is taskpage.go's, for the same reason: the base's token
// is the plugin's format, so a page is cut at the end of a base page or below
// its first row, never inside it. Either the whole base page is emitted and
// its token advances, or none of it is and the incoming token is returned
// untouched. The base is re-read only when the window alone fills a page (see
// the fourth requirement on [HistoryBasePage]).
//
// Two differences from a task page. Order: forward is node ascending,
// transaction descending (the store keeps the transaction id negated); reverse
// inverts both. No deletes: a window node is a row not yet in the store, never
// one removed from it.

import (
	"cmp"
	"encoding/json"
	"fmt"
	"slices"

	p "go.temporal.io/server/common/persistence"
)

// HistoryBasePage fetches one page of the cold store's answer for the requested
// branch and range, given only a page size and a token. An empty returned
// token means the base is exhausted.
//
// It must meet three of [BasePage]'s requirements: no more rows than asked,
// rows in the store's order, and no empty page beside a token. The range
// requirement is not checked: the store filters by node id, as the merge does
// for the window.
//
// Fourth, also needed by [BasePage]: a token may be replayed and must return
// the same rows. A base page the cut emits nothing from is left unread and
// reached again only through its own token; this happens only when the window
// alone fills a page. Temporal's SQL plugins satisfy it (the token is the last
// row's key), and so does Cassandra's paging state. A store whose token is a
// server-side cursor consumed by reading cannot serve this seam.
type HistoryBasePage func(pageSize int, token []byte) ([]p.InternalHistoryNode, []byte, error)

// historyKey is the store's row key for one node. A window node with the same
// key as a cold row is that row mid-drain; the cold row wins, as the one the
// store keeps.
type historyKey struct {
	nodeID int64
	txnID  int64
}

func keyOf(n p.InternalHistoryNode) historyKey {
	return historyKey{nodeID: n.NodeID, txnID: n.TransactionID}
}

// compareHistory orders two keys the way the store returns them.
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

// historyPageToken is a merged read's token: the base's token and the last key
// emitted, where the window resumes.
type historyPageToken struct {
	// Base is the cold store's token. Empty with BaseDone false means read the
	// base from its start.
	Base []byte `json:"base,omitempty"`
	// BaseDone means the base is exhausted and is not called again.
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

// historyTokenMagic prefixes this layer's token so it can be told from a
// store's own.
var historyTokenMagic = [4]byte{'w', 'h', 's', '1'}

// encodeHistoryToken encodes our token. A nil token (pagination over) encodes
// as an absent token, not an empty one.
func encodeHistoryToken(t *historyPageToken) []byte {
	if t == nil {
		return nil
	}
	body, err := json.Marshal(t)
	if err != nil {
		// Unreachable: the struct is four scalars and a byte slice.
		panic(fmt.Sprintf("fold: encoding a history page token: %v", err))
	}
	return append(historyTokenMagic[:], body...)
}

// decodeHistoryToken decodes our token. Unlike the task page, a foreign token
// is taken as the base's rather than refused: a page answered from the cold
// store alone (no cycle on the node) hands back the store's token, and the
// next page may arrive here with it.
func decodeHistoryToken(raw []byte) *historyPageToken {
	var t historyPageToken
	if len(raw) >= len(historyTokenMagic) && [4]byte(raw[:4]) == historyTokenMagic &&
		json.Unmarshal(raw[len(historyTokenMagic):], &t) == nil {
		return &t
	}
	// Covers an empty argument too: the zero value means no cursor on either
	// side, and callers need no nil check.
	return &historyPageToken{Base: raw}
}

// BaseHistoryToken returns the cold store's token inside one this layer wrote,
// or the argument unchanged if it is not ours. A caller answering a page from
// the base store alone must pass the store this, since our token would fail
// the plugin's parser mid-pagination.
func BaseHistoryToken(raw []byte) []byte {
	if len(raw) == 0 {
		return nil
	}
	t := decodeHistoryToken(raw)
	if t.BaseDone {
		// The base was exhausted and only window nodes, unreachable here,
		// remained. An empty token restarts the base rather than ending it, so
		// rows may repeat but none are skipped.
		return nil
	}
	return t.Base
}

// HistoryPage answers one page of the requested branch from the window's nodes
// and the cold store's, under the rule at the top of this file. The caller
// parses treeID from the branch token; this layer has no branch-token codec.
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
		// Ask for what the window does not fill, at least one: asking for nothing
		// would end the pagination with rows left in the store.
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

	// How far this page may reach. With a token, unread cold rows may lie past
	// the base page's last key, so window nodes beyond it wait; without one the
	// base is exhausted and every window node is in reach.
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
			// Base exhausted and every remaining window node is here: done.
			return merged, nil, nil
		}
		next := &historyPageToken{Base: nextBase}
		next.setAfter(upTo)
		return merged, next, nil
	}

	// The window alone overflows the page. Emit window nodes strictly below the
	// base page's first key and leave the base page unread, token untouched, so
	// it comes back next time. Here len(tail) >= pageSize, so the ask was one
	// and at most one base row is held back.
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
		// The base's first row is at or below the window's first, so cutting
		// below it would emit empty pages forever. That row is merged[0] (a tie
		// deduplicates to it) and is the whole base page here, so emitting it
		// alone obeys the cut rule.
		next.Base, next.BaseDone = nextBase, !bounded
		next.setAfter(keyOf(merged[0]))
		return merged[:1], next, nil
	}
	only := inReach[:min(cut, pageSize)]
	// Nothing emitted from the base: its cursor stays.
	next.Base = token.Base
	next.setAfter(keyOf(only[len(only)-1]))
	return only, next, nil
}

// refuseHistoryPage checks the three [HistoryBasePage] requirements the merge
// rests on. An empty page beside a token would otherwise end the pagination
// with rows still unread.
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

// mergeHistoryNodes merges two pages already in the store's order. On a shared
// key (written by a drain since) the cold row wins.
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
