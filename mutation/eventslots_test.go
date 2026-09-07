package mutation

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	p "go.temporal.io/server/common/persistence"
)

// [Mutation.EventSlots] is the only enumeration of the event batches a request
// carries, and the payload carries none of them (D3), so a slot nobody names is
// a mutation acked over history nodes nobody wrote — durable, correct, and
// invisible to every functional suite. Cases name a slot's contents, in the
// order the store receives them.
func TestEveryRequestShapesEventSlotsAreNamed(t *testing.T) {
	cases := []struct {
		name   string
		m      Mutation
		events []string
	}{
		{
			name: "create",
			m: Mutation{Create: &p.InternalCreateWorkflowExecutionRequest{
				NewWorkflowNewEvents: oneEvent("create-new-run"),
			}},
			events: []string{"create-new-run"},
		},
		{
			name: "update",
			m: Mutation{Update: &p.InternalUpdateWorkflowExecutionRequest{
				UpdateWorkflowNewEvents: oneEvent("update-mutation"),
			}},
			events: []string{"update-mutation"},
		},
		{
			// Continue-as-new: a second slot, not a second request.
			name: "update carrying a new run",
			m: Mutation{Update: &p.InternalUpdateWorkflowExecutionRequest{
				UpdateWorkflowNewEvents: oneEvent("update-mutation"),
				NewWorkflowNewEvents:    oneEvent("update-new-run"),
			}},
			events: []string{"update-mutation", "update-new-run"},
		},
		{
			name: "conflict-resolve",
			m: Mutation{ConflictResolve: &p.InternalConflictResolveWorkflowExecutionRequest{
				ResetWorkflowEventsNewEvents: oneEvent("resolve-reset"),
			}},
			events: []string{"resolve-reset"},
		},
		{
			// The widest request there is: three runs, three slots.
			name: "conflict-resolve carrying all three",
			m: Mutation{ConflictResolve: &p.InternalConflictResolveWorkflowExecutionRequest{
				CurrentWorkflowEventsNewEvents: oneEvent("resolve-current"),
				ResetWorkflowEventsNewEvents:   oneEvent("resolve-reset"),
				NewWorkflowEventsNewEvents:     oneEvent("resolve-new-run"),
			}},
			events: []string{"resolve-current", "resolve-reset", "resolve-new-run"},
		},
		{
			// The request type has no such field at all.
			name:   "set",
			m:      Mutation{Set: &p.InternalSetWorkflowExecutionRequest{}},
			events: nil,
		},
		{
			name:   "delete",
			m:      Mutation{Delete: &p.DeleteWorkflowExecutionRequest{RunID: "run"}},
			events: nil,
		},
		{
			name:   "delete-current",
			m:      Mutation{DeleteCurrent: &p.DeleteCurrentWorkflowExecutionRequest{RunID: "run"}},
			events: nil,
		},
		{
			name:   "add-tasks",
			m:      Mutation{AddTasks: &p.InternalAddHistoryTasksRequest{}},
			events: nil,
		},
		{
			name:   "range-complete-tasks",
			m:      Mutation{RangeCompleteTasks: &p.RangeCompleteHistoryTasksRequest{}},
			events: nil,
		},
	}

	covered := map[Kind]bool{}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.events, eventNames(c.m))
		})
		covered[c.m.Kind()] = true
	}

	for k := KindInvalid + 1; int(k) < KindCount; k++ {
		require.True(t, covered[k], "kind %s has no case saying which event slots it carries", k)
	}

	require.Nil(t, Mutation{}.EventSlots(), "a mutation holding no request carries no events")
}

// One row accounts for each event-bearing request field: a
// field holding new events is one fieldset_test records as dropped for D3, and
// one [Mutation.EventSlots] hands to whoever writes the mutation. A field
// Temporal adds to a request is dropped from the payload by construction, so
// this is what makes it a slot rather than lost state.
func TestEveryFieldOfNewEventsIsASlotAndIsRecordedDropped(t *testing.T) {
	batches := reflect.TypeFor[[]*p.InternalAppendHistoryNodesRequest]()

	for k := KindInvalid + 1; int(k) < KindCount; k++ {
		t.Run(k.String(), func(t *testing.T) {
			var m Mutation
			slot := reflect.ValueOf(kinds[k].slot(&m)).Elem()
			request := reflect.New(slot.Type().Elem())
			slot.Set(request)

			var marks, fields []string
			for i := range request.Elem().NumField() {
				f := request.Elem().Type().Field(i)
				if f.Type != batches {
					continue
				}
				marks = append(marks, f.Name)
				fields = append(fields, fmt.Sprintf("%s %s", f.Name, f.Type))
				request.Elem().Field(i).Set(reflect.ValueOf(oneEvent(f.Name)))
			}

			require.ElementsMatch(t, marks, eventNames(m),
				"kind %s carries new events in %v, and EventSlots hands over %v: a field missing "+
					"from the row is a batch nobody writes, and the mutation is acked over history "+
					"nodes that are not there", k, marks, eventNames(m))
			require.ElementsMatch(t, fields, recordedAsHistory(t, slot.Type().Elem()),
				"kind %s: the fields holding new events and the fields fieldset_test records as "+
					"dropped for D3 must be the same fields", k)
		})
	}
}

// recordedAsHistory reports which fields of a request mirroredStructs records as
// dropped because event history stays out of the WAL.
func recordedAsHistory(t *testing.T, request reflect.Type) []string {
	t.Helper()

	for _, s := range mirroredStructs {
		if s.typ != request {
			continue
		}
		var out []string
		for _, f := range s.fields {
			if f.how == dropped && f.why == whyHistory {
				out = append(out, f.field)
			}
		}
		return out
	}
	t.Fatalf("%s is walked by no row of mirroredStructs", request)
	return nil
}

// oneEvent builds a batch of one append request whose node carries the name.
func oneEvent(name string) []*p.InternalAppendHistoryNodesRequest {
	return []*p.InternalAppendHistoryNodesRequest{
		{Node: p.InternalHistoryNode{NodeID: 1, Events: blob(name)}},
	}
}

// eventNames reads the names back out of every slot, in enumeration order.
func eventNames(m Mutation) []string {
	var out []string
	for _, slot := range m.EventSlots() {
		for _, request := range slot {
			out = append(out, string(request.Node.Events.Data))
		}
	}
	return out
}
