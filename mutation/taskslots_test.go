package mutation

import (
	"testing"

	"github.com/stretchr/testify/require"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
)

// [Mutation.TaskSlots] is the only enumeration of the task maps the layer
// reads, sweeps on a range delete, counts and hands the store, so a shape whose
// slots go unnamed there is a task that is visible and unwritten, or written
// and invisible. Cases name a slot's contents, in the order callers concatenate
// them in.
func TestEveryRequestShapesTaskSlotsAreNamed(t *testing.T) {
	cases := []struct {
		name  string
		m     Mutation
		slots []string
	}{
		{
			name: "create",
			m: Mutation{Create: &p.InternalCreateWorkflowExecutionRequest{
				NewWorkflowSnapshot: p.InternalWorkflowSnapshot{Tasks: markedTasks("create-snapshot")},
			}},
			slots: []string{"create-snapshot"},
		},
		{
			name: "update",
			m: Mutation{Update: &p.InternalUpdateWorkflowExecutionRequest{
				UpdateWorkflowMutation: p.InternalWorkflowMutation{Tasks: markedTasks("update-mutation")},
			}},
			slots: []string{"update-mutation"},
		},
		{
			// Continue-as-new: a second slot, not a second request.
			name: "update carrying a new run",
			m: Mutation{Update: &p.InternalUpdateWorkflowExecutionRequest{
				UpdateWorkflowMutation: p.InternalWorkflowMutation{Tasks: markedTasks("update-mutation")},
				NewWorkflowSnapshot:    new(p.InternalWorkflowSnapshot{Tasks: markedTasks("update-new-run")}),
			}},
			slots: []string{"update-new-run", "update-mutation"},
		},
		{
			name: "conflict-resolve",
			m: Mutation{ConflictResolve: &p.InternalConflictResolveWorkflowExecutionRequest{
				ResetWorkflowSnapshot: p.InternalWorkflowSnapshot{Tasks: markedTasks("resolve-reset")},
			}},
			slots: []string{"resolve-reset"},
		},
		{
			// The widest request there is: three runs, three slots.
			name: "conflict-resolve carrying all three",
			m: Mutation{ConflictResolve: &p.InternalConflictResolveWorkflowExecutionRequest{
				ResetWorkflowSnapshot:   p.InternalWorkflowSnapshot{Tasks: markedTasks("resolve-reset")},
				NewWorkflowSnapshot:     new(p.InternalWorkflowSnapshot{Tasks: markedTasks("resolve-new-run")}),
				CurrentWorkflowMutation: new(p.InternalWorkflowMutation{Tasks: markedTasks("resolve-current")}),
			}},
			slots: []string{"resolve-reset", "resolve-new-run", "resolve-current"},
		},
		{
			name: "set",
			m: Mutation{Set: &p.InternalSetWorkflowExecutionRequest{
				SetWorkflowSnapshot: p.InternalWorkflowSnapshot{Tasks: markedTasks("set-snapshot")},
			}},
			slots: []string{"set-snapshot"},
		},
		{
			// A Delete's tasks live in the window, not in the request.
			name:  "delete",
			m:     Mutation{Delete: &p.DeleteWorkflowExecutionRequest{RunID: "run"}},
			slots: nil,
		},
		{
			name:  "delete-current",
			m:     Mutation{DeleteCurrent: &p.DeleteCurrentWorkflowExecutionRequest{RunID: "run"}},
			slots: nil,
		},
		{
			// None, with rows present to say so: an AddTasks' rows belong to
			// no workflow, and enumerating them here would write them twice.
			name:  "add-tasks",
			m:     Mutation{AddTasks: &p.InternalAddHistoryTasksRequest{Tasks: markedTasks("add-tasks")}},
			slots: nil,
		},
		{
			name:  "range-complete-tasks",
			m:     Mutation{RangeCompleteTasks: &p.RangeCompleteHistoryTasksRequest{TaskCategory: tasks.CategoryTransfer}},
			slots: nil,
		},
	}

	covered := map[Kind]bool{}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.slots, slotNames(c.m))
		})
		covered[c.m.Kind()] = true
	}

	// Every kind must appear above, so a new one fails here by name.
	for k := KindInvalid + 1; int(k) < KindCount; k++ {
		require.True(t, covered[k], "kind %s has no case saying which task slots it carries", k)
	}
}

// A range delete folding into the window replaces the map a pending request
// writes from, so a slot must address the request's own field, not a copy.
func TestATaskSlotIsTheRequestsOwnMap(t *testing.T) {
	m := Mutation{Update: &p.InternalUpdateWorkflowExecutionRequest{
		UpdateWorkflowMutation: p.InternalWorkflowMutation{Tasks: markedTasks("before")},
	}}

	*m.TaskSlot(PartMutation) = markedTasks("after")

	require.Equal(t, markedTasks("after"), m.Update.UpdateWorkflowMutation.Tasks)
}

// markedTasks builds one task row whose blob carries the given name.
func markedTasks(name string) map[tasks.Category][]p.InternalHistoryTask {
	return map[tasks.Category][]p.InternalHistoryTask{
		tasks.CategoryTransfer: {{Key: tasks.NewImmediateKey(1), Blob: blob(name)}},
	}
}

// slotNames reads the marks back out of every slot, in enumeration order.
func slotNames(m Mutation) []string {
	var out []string
	for _, slot := range m.TaskSlots() {
		for _, list := range *slot {
			for _, task := range list {
				out = append(out, string(task.Blob.Data))
			}
		}
	}
	return out
}
