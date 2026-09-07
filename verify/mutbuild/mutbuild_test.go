package mutbuild_test

// What this package promises, driven: a mutation it builds is one the store
// would admit, and one the layer above the fold could serve.

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/tasks"

	"github.com/aromanovich/waltz/verify/mutbuild"
)

const shard = 7

var build = mutbuild.For(shard)

// TestAnInvalidStateIsRefusedRatherThanBuilt is the check, broken on purpose.
// COMPLETED with a RUNNING status is the pair
// ValidateCreateWorkflowStateStatus exists to catch — the store refuses it, so
// a fixture carrying it drives a request Temporal never sends. Before this
// package, three test files could each build one and nothing would say so.
func TestAnInvalidStateIsRefusedRatherThanBuilt(t *testing.T) {
	refusal := refuses(t, func() {
		build.Create("ns", "wf", "run", mutbuild.WithState(
			enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		))
	})
	require.Contains(t, refusal, "built an invalid create")
	require.Contains(t, refusal, "invalid state", "the validator's own words, not ours")
}

// TestAModeIsCheckedAgainstTheStateItCarries is the *second* validator, and the
// pair is chosen so that only it can fire: ZOMBIE with a RUNNING status
// satisfies the state/status rule above, and is exactly what a create taking
// the current row may not carry. So a fixture that passed the first check and
// not this one is caught, which is the half a single validator would miss.
func TestAModeIsCheckedAgainstTheStateItCarries(t *testing.T) {
	zombie := mutbuild.WithState(
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	)
	refusal := refuses(t, func() { build.Create("ns", "wf", "run", zombie) })
	require.Contains(t, refusal, "create mode",
		"the mode validator, not the state/status one this pair satisfies")

	// And on the mode CreateOver moves to, which is why that constructor
	// re-checks rather than trusting the create it started from.
	require.Contains(t,
		refuses(t, func() { build.CreateOver("ns", "wf", "run", "previous", 1, zombie) }),
		"create mode")
}

// refuses drives fn and reports what the refusal said, failing if it built the
// fixture instead.
func refuses(t *testing.T, fn func()) string {
	t.Helper()
	var refusal string
	func() {
		defer func() {
			got, ok := recover().(string)
			require.True(t, ok, "mutbuild refuses with a string, or did not refuse at all")
			refusal = got
		}()
		fn()
	}()
	return refusal
}

// TestEveryRunCarriesItsStateAsABlob is the defect this package was written to
// close: cycle's create carried the blob because the read path
// deserialises it, apply's did not, and the two names were the same.
// Only the blob is recorded, so a mutation carrying the struct alone survives a
// fold and vanishes on replay.
func TestEveryRunCarriesItsStateAsABlob(t *testing.T) {
	ns, wf, run := "ns", "wf", "run-1"

	create := build.Create(ns, wf, run)
	requireStateBlob(t, create.Create.NewWorkflowSnapshot.ExecutionStateBlob, run)

	update := build.Update(ns, wf, run, 2)
	requireStateBlob(t, update.Update.UpdateWorkflowMutation.ExecutionStateBlob, run)

	set := build.Set(ns, wf, run, 3)
	requireStateBlob(t, set.Set.SetWorkflowSnapshot.ExecutionStateBlob, run)
}

// TestAnAddTasksWithNoRowsCarriesNoMap: the one mutation that folds to nothing
// is a request with a nil task map, not one with an empty category in it.
func TestAnAddTasksWithNoRowsCarriesNoMap(t *testing.T) {
	require.Nil(t, build.AddTasks(tasks.CategoryTransfer).AddTasks.Tasks)
	require.Len(t, build.AddTasks(tasks.CategoryTransfer, mutbuild.Task("t")).AddTasks.Tasks, 1)
}

// requireStateBlob reads the blob back the way the read path does: what a
// fixture carries is only real if it deserialises.
func requireStateBlob(t *testing.T, blob *commonpb.DataBlob, run string) {
	t.Helper()
	require.NotNil(t, blob, "a run with no state blob is one no read path could render")
	state, err := serialization.WorkflowExecutionStateFromBlob(blob)
	require.NoError(t, err)
	require.Equal(t, run, state.RunId)
}
