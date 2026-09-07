package mutgen_test

// What these assert is the four properties a corpus is worth anything for:
// it is reproducible from its seed, it collapses (and says so together with the
// knob that made it), it contains the shapes that make a fold do work — chains,
// tasks in every category, deletes of keys that were really there, tombstones
// and re-creations — and it survives the WAL's own codec.
//
// None of them needs a cluster. Whether the stream is one a real store accepts
// is a different claim and needs one.

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/aromanovich/waltz/fold"
	"github.com/aromanovich/waltz/internal/verify/mutgen"
	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/wal"
)

const shard int32 = 3

func config(seed int64) mutgen.Config {
	cfg := mutgen.Default()
	cfg.Seed = seed
	cfg.ShardID = shard
	return cfg
}

func take(t *testing.T, cfg mutgen.Config, n int) ([]mutation.Mutation, mutgen.Report) {
	t.Helper()
	g, err := mutgen.New(cfg)
	require.NoError(t, err)
	stream, err := g.Take(n)
	require.NoError(t, err)
	return stream, g.Report()
}

func encodeAll(t *testing.T, stream []mutation.Mutation) [][]byte {
	t.Helper()
	out := make([][]byte, 0, len(stream))
	for i, m := range stream {
		payload, err := mutation.Encode(m)
		require.NoError(t, err, "mutation %d", i)
		out = append(out, payload)
	}
	return out
}

// TestSameSeedSameStream is the seed's whole promise, and it is asserted on
// encoded bytes rather than on structs: what a later run has to reproduce is the
// WAL payload, and a difference the structs share — a map's iteration order, a
// clock reading — passes a struct comparison and still changes the bytes.
func TestSameSeedSameStream(t *testing.T) {
	first, firstReport := take(t, config(1), 300)
	second, secondReport := take(t, config(1), 300)

	require.Equal(t, encodeAll(t, first), encodeAll(t, second),
		"the same seed must produce the same stream, byte for byte")
	require.Equal(t, firstReport, secondReport)

	other, _ := take(t, config(2), 300)
	require.NotEqual(t, encodeAll(t, first), encodeAll(t, other),
		"a different seed must produce a different stream, or the seed is not the source of anything")
}

// TestEveryMutationIsOneRequestForOneShard: the two invariants everything above
// this package relies on — one mutation is one request (I1), and a stream is one
// shard's log, because one shard is one accumulator.
func TestEveryMutationIsOneRequestForOneShard(t *testing.T) {
	stream, _ := take(t, config(3), 200)
	for i, m := range stream {
		require.NotEqual(t, mutation.KindInvalid, m.Kind(), "mutation %d holds no single request", i)
		require.Equal(t, shard, m.ShardID(), "mutation %d belongs to another shard", i)
	}
}

// TestReportDescribesWhatWasHandedOut: a deletion is two mutations, so a caller
// that stops between them must not be told about a delete it never received.
// The report would otherwise overcount by one for every second stream length,
// and an acceptance run's numbers are as good as this.
func TestReportDescribesWhatWasHandedOut(t *testing.T) {
	g, err := mutgen.New(config(16))
	require.NoError(t, err)

	counted := map[mutation.Kind]int{}
	for i := range 400 {
		m, err := g.Next()
		require.NoError(t, err)
		counted[m.Kind()]++

		report := g.Report()
		require.Equal(t, i+1, report.Mutations)
		require.Equal(t, counted[mutation.KindCreate], report.Creates, "after %d mutations", i+1)
		require.Equal(t, counted[mutation.KindUpdate], report.Updates, "after %d mutations", i+1)
		require.Equal(t, counted[mutation.KindSet], report.Sets, "after %d mutations", i+1)
		require.Equal(t, counted[mutation.KindDelete], report.Deletes, "after %d mutations", i+1)
		require.Equal(t, counted[mutation.KindDeleteCurrent], report.DeleteCurrent, "after %d mutations", i+1)
		require.Equal(t, counted[mutation.KindConflictResolve], report.ConflictResolves, "after %d mutations", i+1)
	}
	require.NotZero(t, counted[mutation.KindDelete], "the stream contained no deletion to stop inside")
}

// TestAFullStreamIsMissingNothing is the coverage claim in one assertion: a
// stream configured for every shape carries every shape.
func TestAFullStreamIsMissingNothing(t *testing.T) {
	corpus, err := mutgen.Corpus(config(12), 300)
	require.NoError(t, err)

	require.Empty(t, corpus.Report.Missing(),
		"a default stream carries every shape the generator was configured for: %s", corpus.Report)
	require.Len(t, corpus.Mutations, 300)
	require.Len(t, corpus.Payloads, 300, "every mutation is carried in the shape the log holds")
}

// TestAThinStreamNamesWhatItLacks: the point of the predicate is the failure
// message, so what it says has to name the shape and not merely a count.
func TestAThinStreamNamesWhatItLacks(t *testing.T) {
	thin := config(12)
	thin.WorkflowReuse = 0
	thin.SnapshotRate, thin.ContinueAsNewRate, thin.BufferedRate, thin.TombstoneRate = 0.5, 0.5, 0.5, 0.5
	corpus, err := mutgen.Corpus(thin, 3)
	require.NoError(t, err)

	missing := corpus.Report.Missing()
	require.NotEmpty(t, missing)
	require.Contains(t, missing, "a collapse ratio above 1: the stream asks fold nothing")
	require.Contains(t, missing, "a snapshot barrier of the Set shape")
	require.Contains(t, missing, "a tombstone")
}

// TestAShapeNobodyAskedForIsNotMissing: a suite that turns the task rates off —
// three of them do, because a task record is neither a create nor an update —
// must not be told its stream lacks history tasks.
func TestAShapeNobodyAskedForIsNotMissing(t *testing.T) {
	quiet := config(12)
	quiet.TaskDensity, quiet.AddTasksRate, quiet.RangeCompleteRate = 0, 0, 0
	quiet.BufferedRate, quiet.TombstoneRate = 0, 0
	corpus, err := mutgen.Corpus(quiet, 300)
	require.NoError(t, err)

	require.Empty(t, corpus.Report.Missing(),
		"the shapes this stream was configured without are not shapes it is missing: %s", corpus.Report)
	require.Zero(t, corpus.Report.Tasks, "and it really did produce none")
}

// TestRatioOneIsRecognisable: a corpus generated at WorkflowReuse 0 never
// touches a workflow twice, so its collapse ratio is exactly 1.00 — and 1.00 is
// the number a broken fold would also report. What makes the difference visible
// is that the ratio never travels without its knob, and that Collapses()
// answers no.
func TestRatioOneIsRecognisable(t *testing.T) {
	flat := config(4)
	flat.WorkflowReuse = 0
	_, report := take(t, flat, 200)

	require.Equal(t, 1.0, report.CollapseRatio, "no workflow is ever touched twice")
	require.False(t, report.Collapses(), "a stream at ratio 1.0 asks nothing of fold")
	require.Contains(t, report.String(), "WorkflowReuse 0.00",
		"the ratio may not be reported without the knob it was measured at")

	deep := config(4)
	deep.WorkflowReuse = 0.95
	_, deepReport := take(t, deep, 200)
	require.Greater(t, deepReport.CollapseRatio, 1.5, "reuse is the dial that sets the ratio")
	require.True(t, deepReport.Collapses())
	require.Contains(t, deepReport.String(), "WorkflowReuse 0.95")
}

// TestKeyReuseSetsThePerKeyCollapse: the second dial. At KeyReuse 0 every
// upsert names a key of its own — which is what upstream's generator does, and
// why a chain out of it collapses by nothing.
func TestKeyReuseSetsThePerKeyCollapse(t *testing.T) {
	flat := config(5)
	flat.KeyReuse = 0
	_, flatReport := take(t, flat, 200)
	require.Equal(t, 1.0, flatReport.KeyCollapse, "every upsert under a key of its own")
	require.Contains(t, flatReport.String(), "KeyReuse 0.00")

	reused := config(5)
	reused.KeyReuse = 0.9
	_, reusedReport := take(t, reused, 200)
	require.Greater(t, reusedReport.KeyCollapse, flatReport.KeyCollapse)
}

// TestChainsFold is the corpus meeting its consumer: a generated stream folds,
// and it collapses.
//
// It also pins what a consumer has to implement. A default stream contains
// windows this accumulator refuses — a continue-as-new out of a run whose window
// state is already a snapshot — and the refusal is not a defect on either side:
// it is a valid stream fold cannot express as merged requests, recovered by
// draining and letting the refused mutation head a fresh window. Every consumer
// of a generated stream needs that loop, so the corpus's own test is where it
// is written down.
func TestChainsFold(t *testing.T) {
	stream, report := take(t, config(6), 400)

	a := fold.New(wal.ShardID(shard))
	refusals, batches, foldedIn, emittedOut, deletes := 0, 0, 0, 0, 0
	drain := func() {
		batch := a.Drain()
		out, stats := slices.Collect(batch.Each()), batch.Stats()
		foldedIn += stats.MutationsIn
		emittedOut += len(out)
		batches++
		for _, e := range out {
			if e.Request.Kind() == mutation.KindDelete {
				deletes++
			}
		}
	}

	for i, m := range stream {
		seqno := wal.Seqno(i + 1)
		refusal, err := a.AddOrDrain(seqno, m, func() error { drain(); return nil })
		// Anything the refusal's own recovery did not absorb means the stream
		// itself is corrupt: a mutation on a tombstoned run, or one that cannot
		// follow the window in any acked stream. Those are corpus bugs, not
		// fold's business.
		require.NoError(t, err, "a valid stream must fold: mutation %d (%s)", i, m.Kind())
		if refusal.Drained {
			refusals++
		}
	}
	drain()

	require.NotZero(t, refusals,
		"the default stream no longer exercises fold's refusal path, which every consumer has to implement")
	require.Equal(t, len(stream), foldedIn, "every mutation must land in exactly one window")
	require.Greater(t, float64(foldedIn)/float64(emittedOut), 1.5,
		"the windows must collapse, or the corpus is asking fold nothing")
	require.Less(t, emittedOut, len(stream), "one merged request per dirty workflow, not per mutation")
	require.NotZero(t, deletes, "the stream contained deletions and the fold emitted none")
	t.Logf("%d mutations -> %d merged requests over %d windows (%d refusals); generator says ratio %.2f",
		foldedIn, emittedOut, batches, refusals, report.CollapseRatio)
}

// TestContinueAsNewCarriesBothRuns: the one request that writes two runs. The
// store's rule is that the run being updated may not be left created or running
// (ValidateUpdateWorkflowModeState case 2), so a continue-as-new closes it with
// status CONTINUED_AS_NEW, and the new run starts its own chain while taking over
// the current-execution row.
func TestContinueAsNewCarriesBothRuns(t *testing.T) {
	stream, report := take(t, config(17), 500)
	require.NotZero(t, report.ContinueAsNews, "no run continued as new")
	require.LessOrEqual(t, report.ContinueAsNews, report.Updates,
		"a continue-as-new is one request and is counted inside the updates")

	seen := 0
	for i, m := range stream {
		if m.Kind() != mutation.KindUpdate || m.Update.NewWorkflowSnapshot == nil {
			continue
		}
		seen++
		mut := m.Update.UpdateWorkflowMutation
		snap := m.Update.NewWorkflowSnapshot

		require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, mut.ExecutionState.State,
			"mutation %d: the store refuses a continue-as-new out of a live run", i)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW, mut.ExecutionState.Status,
			"mutation %d", i)
		require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_CREATED, snap.ExecutionState.State,
			"mutation %d: the new run is a fresh one", i)
		require.Equal(t, mut.NamespaceID, snap.NamespaceID,
			"mutation %d: the store refuses a continue-as-new into another namespace", i)
		require.Equal(t, mut.WorkflowID, snap.WorkflowID, "mutation %d", i)
		require.NotEqual(t, mut.RunID, snap.RunID, "mutation %d: the new run is a different run", i)
		require.Equal(t, p.UpdateWorkflowModeUpdateCurrent, m.Update.Mode,
			"mutation %d: the current row has to move to the new run", i)
	}
	require.Equal(t, report.ContinueAsNews, seen)
}

// TestConflictResolveResetsTheCurrentRun: the second snapshot barrier. Only the
// reset-only shape is generated, and in mode UpdateCurrent, which is what holds
// when the run being reset is the current one.
func TestConflictResolveResetsTheCurrentRun(t *testing.T) {
	stream, report := take(t, config(18), 500)
	require.NotZero(t, report.ConflictResolves, "no conflict-resolve was generated")
	require.NotZero(t, report.Sets, "the two snapshot shapes share a knob, so both must appear")

	seen := 0
	for i, m := range stream {
		if m.Kind() != mutation.KindConflictResolve {
			continue
		}
		seen++
		req := m.ConflictResolve
		require.Equal(t, p.ConflictResolveWorkflowModeUpdateCurrent, req.Mode, "mutation %d", i)
		require.Nil(t, req.NewWorkflowSnapshot, "mutation %d: reset-only is the generated shape", i)
		require.Nil(t, req.CurrentWorkflowMutation, "mutation %d: reset-only is the generated shape", i)
		require.NotNil(t, req.ResetWorkflowSnapshot.ExecutionStateBlob, "mutation %d", i)
	}
	require.Equal(t, report.ConflictResolves, seen)
}

// TestBufferedEventsComeInBatchesAndAreCleared: one slot per mutation is the
// rule fold has to keep, so what the corpus owes it is a chain of mutations
// each carrying a batch — and a clear, which only makes sense once something is
// buffered.
func TestBufferedEventsComeInBatchesAndAreCleared(t *testing.T) {
	stream, report := take(t, config(19), 500)
	require.NotZero(t, report.BufferedBatches, "no buffered events at all")
	require.NotZero(t, report.BufferedClears, "nothing ever cleared its buffer")
	require.Contains(t, report.String(), "BufferedRate 0.25",
		"the batch count is only interpretable next to the knob")

	// Two mutations of one run carrying a batch each is what a fold must not
	// merge; without that pair the corpus asks nothing of the rule.
	perRun := map[string]int{}
	batches, clears := 0, 0
	for _, m := range stream {
		if m.Kind() != mutation.KindUpdate {
			continue
		}
		mut := m.Update.UpdateWorkflowMutation
		if mut.NewBufferedEvents != nil {
			require.NotEmpty(t, mut.NewBufferedEvents.Data, "a batch must carry serialized events")
			perRun[mut.RunID]++
			batches++
		}
		if mut.ClearBufferedEvents {
			clears++
		}
	}
	require.Equal(t, report.BufferedBatches, batches)
	require.Equal(t, report.BufferedClears, clears)

	chained := 0
	for _, n := range perRun {
		if n > 1 {
			chained++
		}
	}
	require.NotZero(t, chained, "no run carried two batches, so nothing exercises \"batches do not merge\"")
}

// TestTasksCoverAllFourCategories: upstream's own generator maps every category
// to an empty slice (`tests/util.go`), so tasks in all four categories are this
// package's job.
func TestTasksCoverAllFourCategories(t *testing.T) {
	_, report := take(t, config(7), 300)
	require.NotZero(t, report.Tasks)
	for _, category := range []tasks.Category{
		tasks.CategoryTransfer,
		tasks.CategoryTimer,
		tasks.CategoryVisibility,
		tasks.CategoryReplication,
	} {
		require.NotZero(t, report.TasksByCat[int32(category.ID())],
			"no task in category %s", category.Name())
	}
	require.NotZero(t, config(7).TaskDensity, "the density knob must be on for this to mean anything")
}

// TestTaskDensityZeroMeansNoTasks: the knob has to be able to turn tasks off,
// for a consumer that wants a stream isolating mutable state from queues.
func TestTaskDensityZeroMeansNoTasks(t *testing.T) {
	cfg := config(8)
	cfg.TaskDensity = 0
	_, report := take(t, cfg, 100)
	require.Zero(t, report.Tasks)
}

// TestDeletesNameKeysThatWereThere: a delete of an id nothing ever upserted
// exercises none of fold's upsert-vs-delete resolution, so every delete here
// names a key an earlier mutation of the same run upserted — and no mutation
// ever holds one key in both sets, which is the shape the store resolves
// wrongly and fold would then be blamed for.
func TestDeletesNameKeysThatWereThere(t *testing.T) {
	stream, report := take(t, config(9), 400)
	require.NotZero(t, report.SubDeletes, "the delete-after-upsert knob produced nothing")

	upserted := map[runKey]map[string]bool{}
	seen := 0

	for i, m := range stream {
		// A snapshot re-states the whole run and a create starts it, so their
		// key sets are recorded too — including the new run of a
		// continue-as-new, which is an update carrying a snapshot.
		recordSnapshotKeys(upserted, m)
		if m.Kind() != mutation.KindUpdate {
			continue
		}
		mut := m.Update.UpdateWorkflowMutation
		key := runKey{mut.NamespaceID, mut.WorkflowID, mut.RunID}

		for id := range mut.DeleteActivityInfos {
			name := fmt.Sprintf("activity-%d", id)
			require.True(t, upserted[key][name],
				"mutation %d deletes activity %d, which was never upserted", i, id)
			require.NotContains(t, mut.UpsertActivityInfos, id,
				"mutation %d has activity %d in both sets, which no Temporal diff produces", i, id)
			seen++
		}
		for id := range mut.DeleteTimerInfos {
			require.True(t, upserted[key]["timer:"+id],
				"mutation %d deletes timer %q, which was never upserted", i, id)
			require.NotContains(t, mut.UpsertTimerInfos, id,
				"mutation %d has timer %q in both sets", i, id)
			seen++
		}

		if upserted[key] == nil {
			upserted[key] = map[string]bool{}
		}
		for id := range mut.UpsertActivityInfos {
			upserted[key][fmt.Sprintf("activity-%d", id)] = true
		}
		for id := range mut.UpsertTimerInfos {
			upserted[key]["timer:"+id] = true
		}
	}
	require.Equal(t, report.SubDeletes, seen, "the report and the stream disagree about deletes")
}

// runKey identifies one run's key set while walking a stream.
type runKey struct{ ns, wf, run string }

// recordSnapshotKeys folds a create's or set's key set into the same map, so a
// delete of a key that arrived in a snapshot is not reported as a delete of a
// key that was never there.
func recordSnapshotKeys(upserted map[runKey]map[string]bool, m mutation.Mutation) {
	var snapshots []*p.InternalWorkflowSnapshot
	switch m.Kind() {
	case mutation.KindCreate:
		snapshots = append(snapshots, &m.Create.NewWorkflowSnapshot)
	case mutation.KindSet:
		snapshots = append(snapshots, &m.Set.SetWorkflowSnapshot)
	case mutation.KindConflictResolve:
		snapshots = append(snapshots, &m.ConflictResolve.ResetWorkflowSnapshot)
	case mutation.KindUpdate:
		// A continue-as-new starts its new run with keys of its own.
		if m.Update.NewWorkflowSnapshot != nil {
			snapshots = append(snapshots, m.Update.NewWorkflowSnapshot)
		}
	}
	for _, s := range snapshots {
		key := runKey{s.NamespaceID, s.WorkflowID, s.RunID}
		if upserted[key] == nil {
			upserted[key] = map[string]bool{}
		}
		for id := range s.ActivityInfos {
			upserted[key][fmt.Sprintf("activity-%d", id)] = true
		}
		for id := range s.TimerInfos {
			upserted[key]["timer:"+id] = true
		}
	}
}

// TestTombstonesAndRecreation: both delete requests, and a workflow re-created
// after deletion. The pair's order is the one Temporal's delete flow uses,
// current pointer first.
func TestTombstonesAndRecreation(t *testing.T) {
	stream, report := take(t, config(10), 600)
	require.NotZero(t, report.Deletes, "no workflow was deleted")
	require.NotZero(t, report.Recreations, "no workflow id was ever used by a second run")

	pairs, recreatedAfterDelete := 0, 0
	deletedRuns := map[string]bool{}
	deletedWorkflows := map[string]bool{}

	for i, m := range stream {
		switch m.Kind() {
		case mutation.KindDeleteCurrent:
			require.Less(t, i+1, len(stream), "a delete-current must be followed by its delete")
			next := stream[i+1]
			require.Equal(t, mutation.KindDelete, next.Kind(),
				"the current pointer is deleted first and the mutable state second (shard/context_impl.go)")
			require.Equal(t, m.DeleteCurrent.RunID, next.Delete.RunID, "the pair must name one run")
			pairs++
		case mutation.KindDelete:
			deletedRuns[m.Delete.RunID] = true
			deletedWorkflows[m.Delete.WorkflowID] = true
		case mutation.KindCreate:
			snap := m.Create.NewWorkflowSnapshot
			if deletedWorkflows[snap.WorkflowID] && !deletedRuns[snap.RunID] {
				require.Equal(t, p.CreateWorkflowModeBrandNew, m.Create.Mode,
					"a workflow re-created after deletion has no current row to assert")
				recreatedAfterDelete++
			}
		}
	}
	require.Equal(t, report.Deletes, pairs)
	require.NotZero(t, recreatedAfterDelete, "no workflow was re-created after deletion")
}

// TestRecreationOverAClosedRun is the other re-use of a workflow id: the run
// completed and was not deleted, so the create has to assert the current row —
// which is where PreviousRunID and PreviousLastWriteVersion come from, and the
// only shape in the corpus that exercises fold's current-equals-with-version
// assertion.
func TestRecreationOverAClosedRun(t *testing.T) {
	cfg := config(11)
	cfg.TombstoneRate = 0 // never delete: every reuse is a create over a closed run
	stream, report := take(t, cfg, 400)
	require.NotZero(t, report.Recreations)
	require.Zero(t, report.Deletes)

	lastWriteVersions := map[string]int64{}
	checked := 0
	for i, m := range stream {
		switch m.Kind() {
		case mutation.KindCreate:
			if m.Create.Mode == p.CreateWorkflowModeUpdateCurrent {
				require.NotEmpty(t, m.Create.PreviousRunID, "mutation %d", i)
				require.Equal(t, lastWriteVersions[m.Create.PreviousRunID],
					m.Create.PreviousLastWriteVersion,
					"mutation %d asserts a last-write-version the previous run never wrote", i)
				checked++
			}
			lastWriteVersions[m.Create.NewWorkflowSnapshot.RunID] = m.Create.NewWorkflowSnapshot.LastWriteVersion
		case mutation.KindUpdate:
			mut := m.Update.UpdateWorkflowMutation
			lastWriteVersions[mut.RunID] = mut.LastWriteVersion
		}
	}
	require.Equal(t, report.Recreations, checked)
}

// TestVersionChainIsTheOneTheStoreAsserts: a mutable-state store asserts
// DBRecordVersion-1 on the run's row, so a chain that skips or repeats a version
// is a stream path A rejects — and a corpus that only fold ever reads would not
// notice.
func TestVersionChainIsTheOneTheStoreAsserts(t *testing.T) {
	stream, _ := take(t, config(12), 500)

	version := map[string]int64{}
	for i, m := range stream {
		switch m.Kind() {
		case mutation.KindCreate:
			snap := m.Create.NewWorkflowSnapshot
			require.Equal(t, int64(1), snap.DBRecordVersion, "mutation %d: a create starts the chain at 1", i)
			version[snap.RunID] = snap.DBRecordVersion
		case mutation.KindUpdate:
			mut := m.Update.UpdateWorkflowMutation
			require.Equal(t, version[mut.RunID]+1, mut.DBRecordVersion,
				"mutation %d: the store asserts the previous version, so the chain may not skip", i)
			version[mut.RunID] = mut.DBRecordVersion
			if snap := m.Update.NewWorkflowSnapshot; snap != nil {
				require.Equal(t, int64(1), snap.DBRecordVersion,
					"mutation %d: a continued-as-new run starts its own chain at 1", i)
				version[snap.RunID] = snap.DBRecordVersion
			}
		case mutation.KindSet:
			snap := m.Set.SetWorkflowSnapshot
			require.Equal(t, version[snap.RunID]+1, snap.DBRecordVersion, "mutation %d", i)
			version[snap.RunID] = snap.DBRecordVersion
		case mutation.KindConflictResolve:
			snap := m.ConflictResolve.ResetWorkflowSnapshot
			require.Equal(t, version[snap.RunID]+1, snap.DBRecordVersion, "mutation %d", i)
			version[snap.RunID] = snap.DBRecordVersion
		}
	}
}

// TestStreamSurvivesTheCodec: the corpus exists to travel through the WAL, so a
// mutation this package builds that the codec cannot carry is a corpus bug, not
// a codec bug — and it is cheaper to find here than in a folder comparison.
func TestStreamSurvivesTheCodec(t *testing.T) {
	stream, _ := take(t, config(13), 150)
	registry := tasks.NewDefaultTaskCategoryRegistry()
	compare := []cmp.Option{
		protocmp.Transform(),
		cmp.Comparer(func(a, b time.Time) bool { return a.Equal(b) && a.IsZero() == b.IsZero() }),
		// tasks.Category is comparable and has unexported fields, which cmp
		// refuses to walk. A range delete carries one.
		cmpopts.EquateComparable(tasks.Category{}),
	}

	for i, m := range stream {
		payload, err := mutation.Encode(m)
		require.NoError(t, err, "mutation %d", i)
		decoded, err := mutation.Decode(payload, registry)
		require.NoError(t, err, "mutation %d", i)
		if diff := cmp.Diff(m, decoded, compare...); diff != "" {
			t.Fatalf("mutation %d did not survive the codec:\n%s", i, diff)
		}
	}
}

// TestConfigIsChecked: a knob out of range is a stream that quietly exercises
// something other than what was asked for.
func TestConfigIsChecked(t *testing.T) {
	for name, mangle := range map[string]func(*mutgen.Config){
		"WorkflowReuse above 1": func(c *mutgen.Config) { c.WorkflowReuse = 1.5 },
		"KeyReuse below 0":      func(c *mutgen.Config) { c.KeyReuse = -0.1 },
		"no chain at all":       func(c *mutgen.Config) { c.MaxChainLength = 0 },
		"negative task density": func(c *mutgen.Config) { c.TaskDensity = -1 },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := config(14)
			mangle(&cfg)
			_, err := mutgen.New(cfg)
			require.Error(t, err)
		})
	}
}

// TestWorkflowsCapsTheKeySpace: the key space is a cap rather than a
// suggestion, because a stream that spreads over unboundedly many workflows is
// one whose collapse ratio is set by its length instead of by the knob.
func TestWorkflowsCapsTheKeySpace(t *testing.T) {
	cfg := config(15)
	cfg.Workflows = 5
	_, report := take(t, cfg, 300)
	require.LessOrEqual(t, report.Workflows, 5)
	require.Greater(t, report.CollapseRatio, 10.0, "a small key space is a deep collapse")
}
