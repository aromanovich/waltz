package fold

// assert.go derives the assertion set; both halves of this package then apply
// it differently — fold.go records it through adopt, which keeps a run's
// existing head, and Emitted assembles what is left, while check.go evaluates
// or delegates. So the two can still disagree about what a mutation asserts,
// and the disagreement is a stale write acked. The differential oracle cannot
// see the authority's half of it, check.go having no effect on what a drain
// writes.
//
// On an EMPTY window the two sets are comparable: every run and every
// current-row assertion is a head, so add* records all of them into
// Emitted.RunAssertions and decide* delegates all of them, before the
// recorded/discarded partition can make the two legitimately differ. A
// non-empty window is deliberately never used here — there decide evaluates
// instead of delegating and adopt keeps the run's existing head assertion and
// drops the fallback, so a comparison would be of the partition rather than of
// the derivation, which is check_test.go's and check_corpus_test.go's job.
//
// Two empty derivations compare equal, so the equality alone would pass on a
// pair of handlers that derived nothing. The runs/current columns of the shape
// table are the arity claim that stops that, and
// TestEveryKindIsInTheAssertionShapeTable is the other half; deleting either as
// redundant leaves the file green and inert.
//
// What is not compared, and stays elsewhere:
//
//   - ordering. Delegated.Runs is a slice in the store's registration order and
//     that order is load-bearing (Check's own doc comment), while
//     Assertions.Runs is a map — so this comparison is set-valued and the two
//     paths genuinely register a conflict-resolve's runs in different orders.
//     TestWhatTheWindowDoesNotHoldIsDelegated is where order is claimed;
//   - field aliasing. Where a faithful request holds one value in two fields, a
//     swap between them is invisible here: assertConflictResolve reads the
//     current run from ExecutionState.RunId while the run assertion beside it
//     uses RunID, and in any request the store would accept those are equal.
//
// The current row's other fact — the write — has one derivation and one
// consumer, so there is nothing to compare it against. The table carries it as
// arity: which run each kind and mode hands the row to, and which of them assert
// the row without writing it.
//
// In package fold rather than fold_test because DelegatedCurrent.want and
// DelegatedRun.want are unexported, which is what costs this file its own
// request builders.

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"

	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/wal"
)

const (
	shardID wal.ShardID = 7
	nsID                = "ns-1"
	wfID                = "wf-1"
	runA                = "run-a"
	runB                = "run-b"
	runC                = "run-c"
)

// snapshot and workflowMutation carry an execution state on every part: fold
// and check both dereference it rather than checking it, so a part without one
// is a fixture the store would have refused.

func snapshot(run string, version int64) p.InternalWorkflowSnapshot {
	return p.InternalWorkflowSnapshot{
		NamespaceID:        nsID,
		WorkflowID:         wfID,
		RunID:              run,
		DBRecordVersion:    version,
		LastWriteVersion:   100 + version,
		ExecutionState:     &persistencespb.WorkflowExecutionState{RunId: run},
		ExecutionStateBlob: &commonpb.DataBlob{Data: []byte("state-" + run)},
	}
}

func workflowMutation(run string, version int64) p.InternalWorkflowMutation {
	return p.InternalWorkflowMutation{
		NamespaceID:        nsID,
		WorkflowID:         wfID,
		RunID:              run,
		DBRecordVersion:    version,
		LastWriteVersion:   100 + version,
		ExecutionState:     &persistencespb.WorkflowExecutionState{RunId: run},
		ExecutionStateBlob: &commonpb.DataBlob{Data: []byte("state-" + run)},
	}
}

// derivedRun, derivedCurrent and derived are one assertion set flattened out of
// whichever path produced it. The want travels whole rather than spread into
// fields: RunAssertion and CurrentAssertion are comparable, and comparing them
// whole is what reaches a field added to either after this file was written.
type derivedRun struct {
	namespaceID, workflowID, runID string
	want                           RunAssertion
}

type derivedCurrent struct {
	namespaceID, workflowID string
	want                    CurrentAssertion
}

type derived struct {
	runs    []derivedRun
	current *derivedCurrent
	// write is the current row's tail fact, which only the fold derives.
	write *CurrentWrite
}

// writeRun is the run a derivation hands the current row to, empty where it
// writes the row not at all.
func writeRun(d derived) string {
	if d.write == nil {
		return ""
	}
	return d.write.RunID
}

// recordedBy is what the fold records for m: the assertions apply's transaction
// will carry into the cold store.
func recordedBy(t *testing.T, m mutation.Mutation) derived {
	t.Helper()

	a := New(shardID)
	require.NoError(t, a.Add(wal.FirstSeqno, m))
	batch := a.Drain()
	require.LessOrEqual(t, batch.Len(), 1, "one mutation on a fresh accumulator emits at "+
		"most one request; more than one and the set below is spread over requests this "+
		"comparison flattens together")

	var d derived
	for e := range batch.Each() {
		for run, want := range e.RunAssertions() {
			d.runs = append(d.runs, derivedRun{
				namespaceID: e.NamespaceID, workflowID: e.WorkflowID, runID: run, want: want,
			})
		}
		if cur := e.Workflow().Current; cur != nil {
			d.current = &derivedCurrent{
				namespaceID: e.NamespaceID, workflowID: e.WorkflowID, want: *cur,
			}
		}
		d.write = e.Workflow().CurrentWrite
	}
	return d
}

// decidedBy is what the authority derives for m against an empty window. The
// three claims before the collection are what make the result a derivation
// rather than half of a partition.
func decidedBy(t *testing.T, m mutation.Mutation) derived {
	t.Helper()

	del, cov, err := New(shardID).check(m)
	require.NoError(t, err, "an empty window determines nothing, so it refuses nothing")
	require.Zero(t, cov.evaluated, "an empty window discards nothing, so nothing can have been "+
		"evaluated against it: a non-zero count here means the comparison is reading the "+
		"recorded/discarded partition and not the derivation")
	require.Equal(t, cov.asserted, cov.recorded, "every assertion this mutation carries heads an "+
		"empty window, so every one must be recorded")

	var d derived
	seen := map[string]bool{}
	for _, r := range del.Runs {
		require.Falsef(t, seen[r.RunID], "the authority delegates two assertions for run %s: the "+
			"fold's are a map keyed by run id, so the second would be invisible to this "+
			"comparison", r.RunID)
		seen[r.RunID] = true
		d.runs = append(d.runs, derivedRun{
			namespaceID: r.NamespaceID, workflowID: r.WorkflowID, runID: r.RunID, want: r.want,
		})
	}
	if c := del.Current; c != nil {
		d.current = &derivedCurrent{namespaceID: c.NamespaceID, workflowID: c.WorkflowID, want: c.want}
	}
	return d
}

func requireSameDerivation(t *testing.T, byFold, byCheck derived) {
	t.Helper()

	foldRuns := map[string]derivedRun{}
	for _, r := range byFold.runs {
		foldRuns[r.runID] = r
	}
	checkRuns := map[string]derivedRun{}
	for _, r := range byCheck.runs {
		checkRuns[r.runID] = r
	}

	for run, f := range foldRuns {
		c, ok := checkRuns[run]
		if !ok {
			t.Fatalf("the fold records a run assertion on %s that the authority derives nothing "+
				"for: %+v — apply would assert a condition the authority never answered", run, f.want)
		}
		require.Equalf(t, f.namespaceID, c.namespaceID, "run %s: the assertion's namespace id", run)
		require.Equalf(t, f.workflowID, c.workflowID, "run %s: the assertion's workflow id", run)
		require.Equalf(t, f.want.MustNotExist, c.want.MustNotExist,
			"run %s: the assertion's MustNotExist", run)
		require.Equalf(t, f.want.BaseVersion, c.want.BaseVersion,
			"run %s: the assertion's BaseVersion", run)
		// The named requires are the message; this is the comparison. A field
		// added to RunAssertion is derived twice like the two above it and is
		// named by nothing here, so without this it would be the one part of the
		// assertion nothing holds the two paths to.
		require.Equalf(t, f.want, c.want, "run %s: the two derivations differ in a field this "+
			"comparison does not name — name it above, so the next drift in it says which", run)
	}
	for run, c := range checkRuns {
		if _, ok := foldRuns[run]; !ok {
			t.Fatalf("the authority derives a run assertion on %s the fold records none for: %+v — "+
				"the authority would answer for an assertion apply never registers", run, c.want)
		}
	}

	switch {
	case byFold.current != nil && byCheck.current == nil:
		t.Fatalf("the fold records a current-row assertion the authority derives none for: %+v — "+
			"apply would assert a condition the authority never answered", byFold.current.want)
	case byFold.current == nil && byCheck.current != nil:
		t.Fatalf("the authority derives a current-row assertion the fold records none for: %+v — "+
			"the authority would answer for an assertion apply never registers", byCheck.current.want)
	case byFold.current == nil:
		return
	}

	f, c := *byFold.current, *byCheck.current
	require.Equal(t, f.namespaceID, c.namespaceID, "the current row: the assertion's namespace id")
	require.Equal(t, f.workflowID, c.workflowID, "the current row: the assertion's workflow id")
	// As a number: CurrentKind has no String, and check.go already anticipates a
	// fifth one.
	require.Equal(t, int(f.want.Kind), int(c.want.Kind), "the current row: the assertion's Kind")
	require.Equal(t, f.want.RunID, c.want.RunID, "the current row: the assertion's RunID")
	require.Equal(t, f.want.LastWriteVersion, c.want.LastWriteVersion,
		"the current row: the assertion's LastWriteVersion")
	require.Equal(t, f.want, c.want, "the current row: the two derivations differ in a field "+
		"this comparison does not name — name it above, so the next drift in it says which")
}

// shape is one request the two paths are driven with. runs and current are
// arity and not a third copy of the rule: they are what stops the equality
// passing on two sets that are both empty because a derivation was deleted from
// both files.
type shape struct {
	name  string
	kind  mutation.Kind
	build func() mutation.Mutation

	// runs is how many run assertions this request must produce, current
	// whether it must produce the workflow's current-row assertion, and write
	// the run its current-row write names — empty where it writes no row.
	runs    int
	current bool
	write   string
}

func create(mode p.CreateWorkflowMode) *p.InternalCreateWorkflowExecutionRequest {
	return &p.InternalCreateWorkflowExecutionRequest{
		ShardID: int32(shardID), Mode: mode, NewWorkflowSnapshot: snapshot(runA, 1),
	}
}

func update(mode p.UpdateWorkflowMode) *p.InternalUpdateWorkflowExecutionRequest {
	return &p.InternalUpdateWorkflowExecutionRequest{
		ShardID: int32(shardID), Mode: mode, UpdateWorkflowMutation: workflowMutation(runA, 4),
	}
}

func conflictResolve(mode p.ConflictResolveWorkflowMode) *p.InternalConflictResolveWorkflowExecutionRequest {
	return &p.InternalConflictResolveWorkflowExecutionRequest{
		ShardID: int32(shardID), Mode: mode, ResetWorkflowSnapshot: snapshot(runA, 4),
	}
}

// assertionShapes is every kind crossed with every mode of the three requests
// that have one. The mode axis is hand-written: upstream's CreateWorkflowMode,
// UpdateWorkflowMode and ConflictResolveWorkflowMode have no count constant, so
// unlike the kind axis a mode a temporal bump adds gets no row and no failure.
var assertionShapes = []shape{
	{
		name: "create/brand-new", kind: mutation.KindCreate, runs: 1, current: true, write: runA,
		build: func() mutation.Mutation {
			return mutation.Mutation{Create: create(p.CreateWorkflowModeBrandNew)}
		},
	},
	{
		name: "create/update-current", kind: mutation.KindCreate, runs: 1, current: true, write: runA,
		build: func() mutation.Mutation {
			req := create(p.CreateWorkflowModeUpdateCurrent)
			req.PreviousRunID, req.PreviousLastWriteVersion = runB, 11
			return mutation.Mutation{Create: req}
		},
	},
	{
		name: "create/bypass-current", kind: mutation.KindCreate, runs: 1, current: false,
		build: func() mutation.Mutation {
			return mutation.Mutation{Create: create(p.CreateWorkflowModeBypassCurrent)}
		},
	},
	{
		name: "update/update-current", kind: mutation.KindUpdate, runs: 1, current: true, write: runA,
		build: func() mutation.Mutation {
			return mutation.Mutation{Update: update(p.UpdateWorkflowModeUpdateCurrent)}
		},
	},
	{
		name: "update/bypass-current", kind: mutation.KindUpdate, runs: 1, current: true,
		build: func() mutation.Mutation {
			return mutation.Mutation{Update: update(p.UpdateWorkflowModeBypassCurrent)}
		},
	},
	{
		name: "update/ignore-current", kind: mutation.KindUpdate, runs: 1, current: false,
		build: func() mutation.Mutation {
			return mutation.Mutation{Update: update(p.UpdateWorkflowModeIgnoreCurrent)}
		},
	},
	{
		name: "update/update-current+continue-as-new", kind: mutation.KindUpdate, runs: 2, current: true, write: runB,
		build: func() mutation.Mutation {
			req := update(p.UpdateWorkflowModeUpdateCurrent)
			ns := snapshot(runB, 1)
			req.NewWorkflowSnapshot = &ns
			return mutation.Mutation{Update: req}
		},
	},
	{
		name: "update/bypass-current+continue-as-new", kind: mutation.KindUpdate, runs: 2, current: true,
		build: func() mutation.Mutation {
			req := update(p.UpdateWorkflowModeBypassCurrent)
			ns := snapshot(runB, 1)
			req.NewWorkflowSnapshot = &ns
			return mutation.Mutation{Update: req}
		},
	},
	{
		name: "set", kind: mutation.KindSet, runs: 1, current: false,
		build: func() mutation.Mutation {
			return mutation.Mutation{Set: &p.InternalSetWorkflowExecutionRequest{
				ShardID: int32(shardID), SetWorkflowSnapshot: snapshot(runA, 4),
			}}
		},
	},
	{
		name: "conflict-resolve/update-current", kind: mutation.KindConflictResolve, runs: 1, current: true, write: runA,
		build: func() mutation.Mutation {
			return mutation.Mutation{ConflictResolve: conflictResolve(p.ConflictResolveWorkflowModeUpdateCurrent)}
		},
	},
	{
		// The current-row assertion is derived from the mutated current run
		// rather than from the reset one, on both sides: the pair most likely
		// to drift, and why this row and the one below are separate.
		name: "conflict-resolve/update-current+current-mutation", kind: mutation.KindConflictResolve,
		runs: 2, current: true, write: runA,
		build: func() mutation.Mutation {
			req := conflictResolve(p.ConflictResolveWorkflowModeUpdateCurrent)
			cur := workflowMutation(runB, 6)
			req.CurrentWorkflowMutation = &cur
			return mutation.Mutation{ConflictResolve: req}
		},
	},
	{
		name: "conflict-resolve/update-current+new-run", kind: mutation.KindConflictResolve,
		runs: 2, current: true, write: runB,
		build: func() mutation.Mutation {
			req := conflictResolve(p.ConflictResolveWorkflowModeUpdateCurrent)
			ns := snapshot(runB, 1)
			req.NewWorkflowSnapshot = &ns
			return mutation.Mutation{ConflictResolve: req}
		},
	},
	{
		name: "conflict-resolve/update-current+all-three-parts", kind: mutation.KindConflictResolve,
		runs: 3, current: true, write: runC,
		build: func() mutation.Mutation {
			req := conflictResolve(p.ConflictResolveWorkflowModeUpdateCurrent)
			cur := workflowMutation(runB, 6)
			ns := snapshot(runC, 1)
			req.CurrentWorkflowMutation, req.NewWorkflowSnapshot = &cur, &ns
			return mutation.Mutation{ConflictResolve: req}
		},
	},
	{
		name: "conflict-resolve/bypass-current", kind: mutation.KindConflictResolve, runs: 1, current: true,
		build: func() mutation.Mutation {
			return mutation.Mutation{ConflictResolve: conflictResolve(p.ConflictResolveWorkflowModeBypassCurrent)}
		},
	},
	{
		name: "conflict-resolve/bypass-current+all-three-parts", kind: mutation.KindConflictResolve,
		runs: 3, current: true,
		build: func() mutation.Mutation {
			req := conflictResolve(p.ConflictResolveWorkflowModeBypassCurrent)
			cur := workflowMutation(runB, 6)
			ns := snapshot(runC, 1)
			req.CurrentWorkflowMutation, req.NewWorkflowSnapshot = &cur, &ns
			return mutation.Mutation{ConflictResolve: req}
		},
	},
	{
		name: "delete", kind: mutation.KindDelete, runs: 0, current: false,
		build: func() mutation.Mutation {
			return mutation.Mutation{Delete: &p.DeleteWorkflowExecutionRequest{
				ShardID: int32(shardID), NamespaceID: nsID, WorkflowID: wfID, RunID: runA,
			}}
		},
	},
	{
		name: "delete-current", kind: mutation.KindDeleteCurrent, runs: 0, current: false,
		build: func() mutation.Mutation {
			return mutation.Mutation{DeleteCurrent: &p.DeleteCurrentWorkflowExecutionRequest{
				ShardID: int32(shardID), NamespaceID: nsID, WorkflowID: wfID, RunID: runA,
			}}
		},
	},
	{
		name: "add-tasks", kind: mutation.KindAddTasks, runs: 0, current: false,
		build: func() mutation.Mutation {
			return mutation.Mutation{AddTasks: &p.InternalAddHistoryTasksRequest{
				ShardID: int32(shardID), NamespaceID: nsID, WorkflowID: wfID,
				Tasks: map[tasks.Category][]p.InternalHistoryTask{
					tasks.CategoryTransfer: {{Key: tasks.NewImmediateKey(1), Blob: &commonpb.DataBlob{Data: []byte("t")}}},
				},
			}}
		},
	},
	{
		name: "range-complete-tasks", kind: mutation.KindRangeCompleteTasks, runs: 0, current: false,
		build: func() mutation.Mutation {
			return mutation.Mutation{RangeCompleteTasks: &p.RangeCompleteHistoryTasksRequest{
				ShardID:             int32(shardID),
				TaskCategory:        tasks.CategoryTransfer,
				InclusiveMinTaskKey: tasks.NewImmediateKey(1),
				ExclusiveMaxTaskKey: tasks.NewImmediateKey(9),
			}}
		},
	},
}

// TestTheAuthorityDerivesWhatTheFoldRecords drives one mutation down both
// derivations and compares them field by field.
func TestTheAuthorityDerivesWhatTheFoldRecords(t *testing.T) {
	for _, s := range assertionShapes {
		t.Run(s.name, func(t *testing.T) {
			require.Equal(t, s.kind, s.build().Kind(), "the row's kind column is not what it builds")

			// Built twice: Add merges requests in place and takes ownership of
			// what it is handed, so the decided side must not be given a request
			// the fold has already consumed.
			byFold := recordedBy(t, s.build())
			byCheck := decidedBy(t, s.build())

			require.Lenf(t, byFold.runs, s.runs, "this shape must record %d run assertion(s), and "+
				"an equality between two sets of the wrong size is what the arity column exists "+
				"to catch: %+v", s.runs, byFold.runs)
			require.Equalf(t, s.current, byFold.current != nil, "this shape must record a "+
				"current-row assertion: %v", s.current)
			require.Equalf(t, s.write, writeRun(byFold), "this shape must hand the current row "+
				"to %q: the assertion and the write are separate facts, and a mode that asserts "+
				"the row without writing it is where they part", s.write)

			requireSameDerivation(t, byFold, byCheck)
		})
	}
}

// TestEveryKindIsInTheAssertionShapeTable is what makes a ninth kind fail by
// name rather than pass unjudged, the two derivations being compared for every
// kind but that one.
func TestEveryKindIsInTheAssertionShapeTable(t *testing.T) {
	covered := map[mutation.Kind]bool{}
	for _, s := range assertionShapes {
		covered[s.kind] = true
	}
	for k := mutation.KindInvalid + 1; int(k) < mutation.KindCount; k++ {
		require.Truef(t, covered[k], "no shape covers %s: the two derivations are compared for "+
			"every other kind and not for this one", k)
	}
}
