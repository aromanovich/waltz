package cycle

// What a *Cycle exposes, held against the reason each name is on it.

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

// doors is every exported method on a *Cycle and what it is for. The bar is
// I11's — Manager.Shard hands the handle to whoever asks, so nothing on it may
// be a way to write around Manager.Write — and not "somebody outside calls it":
// Epoch, Stats and Retire are what verify/ drives a cycle by, State, Shard and
// Close are reached only from inside this package today, and all six either read
// or stop.
var doors = map[string]string{
	"Shard":  "what the cycle owns; a handle out of Manager.Shard names itself by this and Epoch",
	"Epoch":  "the same, and the number that says a shard has changed hands",
	"Stats":  "one shard's counters, which Manager.Totals sums and a witness reads per shard",
	"State":  "whether the shard is still this node's to write, answerable after the goroutine is gone",
	"Retire": "stop without draining, which is what a fenced-out epoch's tail requires",
	"Close":  "drain and stop, Manager.Close's half of shutdown",
}

// TestOnlyTheRegistryCanWriteToACycle reads both ways: an exported method the
// table does not name, and a row naming a method that no longer exists.
func TestOnlyTheRegistryCanWriteToACycle(t *testing.T) {
	ct := reflect.TypeFor[*Cycle]()

	for method := range ct.Methods() {
		name := method.Name
		require.Contains(t, doors, name,
			"Cycle.%s is exported, and Manager.Shard hands a *Cycle to whoever asks: it is a "+
				"door onto the loop, and one that writes skips Manager.Write, where I11's "+
				"epoch check lives. Either unexport it, or add it to doors with what it is "+
				"for — and it may not be a write", name)
	}

	for name, why := range doors {
		_, ok := ct.MethodByName(name)
		require.True(t, ok, "doors claims Cycle.%s is exported (%s), and it is not: a row "+
			"nothing checks is the prose this test replaced", name, why)
	}
}
