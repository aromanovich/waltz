package cycle

// What a *Cycle exposes, held against the reason each name is on it.

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/aromanovich/waltz/wal"
)

// doors is every exported method on a *Cycle and what it is for. The bar is
// I11's — Manager.Shard hands the handle to whoever asks, so nothing on it may
// be a way to write around Manager.Write — and not "somebody outside calls it":
// State and Retire are what internal/verify/ drives a cycle by, Stats and Retire
// what waltz.Layer.ShardStats and RetireShard answer off, and Shard, Epoch and
// Close are reached only from inside this package today. All six either read or
// stop.
// Each row carries the call as well, so the two tests below range over one
// table: a door added to a second list and not the first is how a name gets
// judged and never driven.
var doors = map[string]struct {
	why  string
	call func(*env)
}{
	"Shard": {"what the cycle owns; a handle out of Manager.Shard names itself by this and Epoch",
		func(e *env) { e.c.Shard() }},
	"Epoch": {"the same, and the number that says a shard has changed hands",
		func(e *env) { e.c.Epoch() }},
	"Stats": {"one shard's counters, which Manager.Totals sums and a witness reads per shard",
		func(e *env) { e.c.Stats() }},
	"State": {"whether the shard is still this node's to write, answerable after the goroutine is gone",
		func(e *env) { e.c.State() }},
	"Retire": {"stop without draining, which is what a fenced-out epoch's tail requires",
		func(e *env) { e.c.Retire() }},
	"Close": {"drain and stop, Manager.Close's half of shutdown",
		func(e *env) { _ = e.c.Close(context.Background()) }},
}

// TestEveryDoorOnACycleIsInTheTable reads both ways: an exported method the
// table does not name, and a row naming a method that no longer exists. That
// every one of them is a read or a stop is the test below, which drives the
// same rows.
func TestEveryDoorOnACycleIsInTheTable(t *testing.T) {
	ct := reflect.TypeFor[*Cycle]()

	for method := range ct.Methods() {
		name := method.Name
		require.Contains(t, doors, name,
			"Cycle.%s is exported, and Manager.Shard hands a *Cycle to whoever asks: it is a "+
				"door onto the loop, and one that writes skips Manager.Write, where I11's "+
				"epoch check lives. Either unexport it, or add it to doors with what it is "+
				"for — and it may not be a write", name)
	}

	for name, door := range doors {
		_, ok := ct.MethodByName(name)
		require.True(t, ok, "doors claims Cycle.%s is exported (%s), and it is not: a row "+
			"nothing checks is the prose this test replaced", name, door.why)
	}
}

// TestNoDoorOnACycleAppends is the half the table above cannot state. The set is
// names only, so a door that starts writing keeps its name and keeps that test
// green — which is the direction that matters, [Manager.Write] being where
// I11's epoch check lives. So every door is called, and the log is held to what
// it already held.
//
// The bar is that nothing is *appended*, not that the log is untouched: Retire
// and Close legitimately drain and trim, and a trim takes entries out from
// below.
func TestNoDoorOnACycleAppends(t *testing.T) {
	highest := func(t *testing.T, e *env) wal.Seqno {
		t.Helper()
		var top wal.Seqno
		for _, entry := range e.entries(t) {
			top = max(top, entry.Seqno)
		}
		return top
	}

	for name, door := range doors {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t, nil)
			// A window with something in it, so the two doors that drain have a
			// transaction to run rather than nothing to do.
			ns, wf, run := ids()
			require.NoError(t, e.add(t, mkCreate(ns, wf, run)))
			acked := highest(t, e)
			require.NotZero(t, acked, "nothing was acked, so an appending door would have nothing to exceed")

			door.call(e)

			for _, entry := range e.entries(t) {
				require.LessOrEqualf(t, entry.Seqno, acked,
					"Cycle.%s put seqno %d in the log. A *Cycle is handed to whoever asks through "+
						"Manager.Shard, so a door that appends is a write path around Manager.Write, "+
						"where I11's epoch check lives", name, entry.Seqno)
			}
		})
	}
}
