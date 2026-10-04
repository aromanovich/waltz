// Package mutgen generates valid, deterministic mutation streams. A seed must
// reproduce the same encoded bytes, so generation must not depend on time,
// UUIDs, Go map iteration or protobuf maps.
package mutgen

import (
	"fmt"
	"maps"
	"math/rand/v2"

	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/tasks"

	"github.com/aromanovich/waltz/mutation"
)

// Config is the shape of the stream. Zero values are not defaulted: start
// from [Default] and change the knobs you need.
type Config struct {
	// Seed fixes the stream: same config and seed, same bytes.
	Seed int64

	// ShardID is the shard every mutation belongs to; a stream is one shard's
	// log.
	ShardID int32

	// Workflows caps the workflow key space. 0 means unbounded.
	Workflows int

	// WorkflowReuse is P(a step touches an existing workflow rather than a new
	// one). It sets the collapse ratio: at 0 fold has nothing to merge.
	WorkflowReuse float64

	// KeyReuse is P(a sub-entity upsert names a key the run holds or deleted,
	// rather than a fresh one). It decides whether upserts merge per key and
	// whether upsert-vs-delete resolution is exercised.
	KeyReuse float64

	// MaxChainLength is how many updates a run takes before the last closes
	// it. It caps the collapse ratio.
	MaxChainLength int

	// Upserts is how many sub-entity upserts a mutation carries: activity
	// infos (int64 keys) and timer infos (string keys).
	Upserts int

	// DeleteAfterUpsert is P(a mutation also deletes a key the run upserted
	// earlier). Only such deletes exercise upsert-vs-delete resolution.
	DeleteAfterUpsert float64

	// TaskDensity is the mean history tasks per mutation, over four categories.
	TaskDensity float64

	// SnapshotRate is P(a step on a live run is a snapshot-bearing request, the
	// barrier that resets the accumulator, I8). Half are SetWorkflowExecution,
	// half reset-only ConflictResolve; fold treats them alike.
	SnapshotRate float64

	// ContinueAsNewRate is P(a run at the end of its chain continues as new
	// rather than completing). It is the one request carrying two runs, which
	// fold merges and sometimes refuses.
	ContinueAsNewRate float64

	// BufferedRate is P(an update carries buffered events), and also P(an
	// update clears existing buffered events, as completing a workflow task
	// does). Batches do not merge, so only a chain of them tests that.
	BufferedRate float64

	// TombstoneRate is P(a closed run is deleted rather than superseded). The
	// key stays in the space, so a later step re-creates it after deletion.
	TombstoneRate float64

	// AddTasksRate is P(a step is a standalone AddHistoryTasks on an existing
	// run): tasks with no mutable state, as queue processors and admin paths
	// send.
	AddTasksRate float64

	// RangeCompleteRate is P(a step is a range delete of one category's tasks).
	// Ranges come from keys already emitted, never at random, since a random
	// range would cover nothing and pass vacuously. [Report.TasksCovered]
	// shows whether they covered anything.
	RangeCompleteRate float64
}

// Default exercises every fold rule: mergeable chains, reused keys, tasks in
// every category, snapshots inside chains, and deletions. Zeros are not
// defaulted because 0 is meaningful for most knobs (WorkflowReuse 0 is the
// worthless corpus an acceptance must recognise).
func Default() Config {
	return Config{
		WorkflowReuse:     0.8,
		KeyReuse:          0.5,
		MaxChainLength:    8,
		Upserts:           2,
		DeleteAfterUpsert: 0.3,
		TaskDensity:       1.5,
		SnapshotRate:      0.1,
		ContinueAsNewRate: 0.3,
		BufferedRate:      0.25,
		TombstoneRate:     0.5,
		AddTasksRate:      0.1,
		RangeCompleteRate: 0.05,
	}
}

// WorkflowRunsOnly is [Default] without standalone AddTasks and RangeComplete,
// so every mutation names a run. Use it for per-run claims, or when counting
// your own range deletes. Tasks inside mutations ([Config.TaskDensity]) stay.
func WorkflowRunsOnly() Config {
	cfg := Default()
	cfg.AddTasksRate, cfg.RangeCompleteRate = 0, 0
	return cfg
}

// UnbrokenChains is [WorkflowRunsOnly] without snapshots, continue-as-new or
// tombstones: each run is a create then up to [Config.MaxChainLength] updates,
// and nothing forces fold to drain mid-window.
func UnbrokenChains() Config {
	cfg := WorkflowRunsOnly()
	cfg.SnapshotRate, cfg.ContinueAsNewRate, cfg.TombstoneRate = 0, 0, 0
	return cfg
}

// validate rejects out-of-range knobs, so a typo fails instead of yielding a
// stream that exercises nothing.
func (c Config) validate() error {
	for _, knob := range []struct {
		name  string
		value float64
	}{
		{"WorkflowReuse", c.WorkflowReuse},
		{"KeyReuse", c.KeyReuse},
		{"DeleteAfterUpsert", c.DeleteAfterUpsert},
		{"SnapshotRate", c.SnapshotRate},
		{"ContinueAsNewRate", c.ContinueAsNewRate},
		{"BufferedRate", c.BufferedRate},
		{"TombstoneRate", c.TombstoneRate},
		{"AddTasksRate", c.AddTasksRate},
		{"RangeCompleteRate", c.RangeCompleteRate},
	} {
		if knob.value < 0 || knob.value > 1 {
			return fmt.Errorf("mutgen: %s is a probability, got %v", knob.name, knob.value)
		}
	}
	if c.MaxChainLength < 1 {
		return fmt.Errorf("mutgen: MaxChainLength must be at least 1, got %d", c.MaxChainLength)
	}
	if c.Upserts < 0 {
		return fmt.Errorf("mutgen: Upserts cannot be negative, got %d", c.Upserts)
	}
	if c.TaskDensity < 0 {
		return fmt.Errorf("mutgen: TaskDensity cannot be negative, got %v", c.TaskDensity)
	}
	if c.Workflows < 0 {
		return fmt.Errorf("mutgen: Workflows cannot be negative, got %d", c.Workflows)
	}
	return nil
}

// Report describes a generated stream. Each collapse ratio sits beside the
// knob that set it, since a bare ratio of 1.0 looks like a pass but means no
// workflow was touched twice.
type Report struct {
	Seed      int64
	Mutations int
	Workflows int // distinct workflow keys the stream touched
	// Runs counts runs created, including by continue-as-new, so it can
	// exceed Creates.
	Runs          int
	WorkflowReuse float64
	// CollapseRatio is workflow mutations over distinct workflows: the upper
	// bound on what fold can merge. Standalone task requests are excluded:
	// they name no workflow and would inflate it with nothing to merge.
	CollapseRatio float64

	Upserts      int
	DistinctKeys int
	KeyReuse     float64
	KeyCollapse  float64 // upserts over distinct sub-entity keys
	SubDeletes   int
	Tasks        int
	TasksByCat   map[int32]int
	Creates      int
	Updates      int
	// ContinueAsNews counts updates that also carried a new run; they are
	// included in Updates.
	ContinueAsNews   int
	ConflictResolves int
	Sets             int
	// Deletes counts DeleteWorkflowExecution requests, which equals the runs
	// tombstoned: each run is deleted at most once, as the second of a pair.
	// A workflow id can be tombstoned repeatedly.
	Deletes       int
	DeleteCurrent int
	Recreations   int // runs created under a workflow id that already had one

	// BufferedBatches counts mutations carrying buffered events, and
	// BufferedClears those that cleared them. Batches do not merge.
	BufferedBatches int
	BufferedClears  int
	BufferedRate    float64

	// TaskAdds counts standalone AddHistoryTasks, TaskRanges range deletes.
	TaskAdds   int
	TaskRanges int
	// TasksCovered is how many tasks the ranges actually cover; zero with
	// ranges present means the deletion rule was never exercised.
	TasksCovered int

	// cfg lets [Report.Missing] ignore shapes the config disabled.
	cfg Config
}

// Missing names shapes the config enables but the stream did not produce. A
// corpus run gates on it being empty, so a generator that silently stopped
// producing, say, tombstones fails instead of leaving suites vacuously green.
func (r Report) Missing() []string {
	var missing []string
	want := func(absent bool, what string) {
		if absent {
			missing = append(missing, what)
		}
	}

	want(!r.Collapses(), "a collapse ratio above 1: the stream asks fold nothing")
	want(r.Updates == 0, "an update chain")
	if r.cfg.SnapshotRate > 0 {
		want(r.Sets == 0, "a snapshot barrier of the Set shape")
		want(r.ConflictResolves == 0, "a snapshot barrier of the conflict-resolve shape")
	}
	if r.cfg.ContinueAsNewRate > 0 {
		want(r.ContinueAsNews == 0, "a request carrying two runs")
	}
	if r.cfg.BufferedRate > 0 {
		want(r.BufferedBatches == 0, "buffered events")
		want(r.BufferedClears == 0, "a cleared buffer")
	}
	if r.cfg.TombstoneRate > 0 {
		want(r.Deletes == 0, "a tombstone")
		want(r.Recreations == 0, "a workflow id reused by a second run")
	}
	if r.cfg.DeleteAfterUpsert > 0 {
		want(r.SubDeletes == 0, "a delete of a sub-entity key that was there")
	}
	if r.cfg.TaskDensity > 0 {
		want(len(r.TasksByCat) < 4,
			fmt.Sprintf("history tasks in all four categories, and it carries %v", r.TasksByCat))
	}
	return missing
}

// Collapses reports whether the stream gives fold anything to merge (ratio
// above 1). Gate on this, not on the printed ratio.
func (r Report) Collapses() bool {
	return r.CollapseRatio > 1
}

func (r Report) String() string {
	return fmt.Sprintf(
		"seed %d: %d mutations over %d workflows (%d runs) — collapse ratio %.2f at WorkflowReuse %.2f\n"+
			"  %d sub-entity upserts over %d distinct keys — key collapse %.2f at KeyReuse %.2f, %d deletes of live keys\n"+
			"  %d history tasks; create %d (recreate %d), update %d (continue-as-new %d), set %d, "+
			"conflict-resolve %d, tombstone %d (delete %d + delete-current %d)\n"+
			"  buffered events: %d batches at BufferedRate %.2f, %d cleared\n"+
			"  history tasks: %d standalone adds, %d range deletes covering %d tasks",
		r.Seed, r.Mutations, r.Workflows, r.Runs, r.CollapseRatio, r.WorkflowReuse,
		r.Upserts, r.DistinctKeys, r.KeyCollapse, r.KeyReuse, r.SubDeletes,
		r.Tasks, r.Creates, r.Recreations, r.Updates, r.ContinueAsNews, r.Sets,
		r.ConflictResolves, r.Deletes, r.Deletes, r.DeleteCurrent,
		r.BufferedBatches, r.BufferedRate, r.BufferedClears,
		r.TaskAdds, r.TaskRanges, r.TasksCovered)
}

// Generator is an endless stream of valid mutations for one shard. It is not
// safe for concurrent use.
type Generator struct {
	cfg        Config
	rng        *rand.Rand
	serializer serialization.Serializer

	namespaceID string
	// pool holds every workflow key touched, in creation order (a slice: maps
	// may not be iterated).
	pool   []*workflowState
	nextID int64 // next task id, monotone per shard as the real allocator is
	// queue holds the rest of a multi-mutation step, such as a deletion pair.
	queue []mutation.Mutation

	// emitted records, per category, the task keys emitted and how far range
	// deletes have reached; ranges are drawn from it. cats lists the same
	// categories in fixed order, since maps may not be iterated.
	emitted map[int32]*emittedTasks
	cats    []tasks.Category

	// rep accumulates counters; [Generator.Report] fills the rest.
	rep Report
}

// workflowState is one workflow key. At most one of run and closed is set:
// run is live, closed is completed but still current, and both nil means the
// key is free (never created, or deleted).
type workflowState struct {
	workflowID string
	run        *runState
	closed     *runState
	created    int
}

// runState is the store's view of one run, as far as validity needs it.
type runState struct {
	runID            string
	createRequestID  string
	version          int64 // db_record_version currently in the store
	nextEventID      int64
	lastWriteVersion int64
	state            enumsspb.WorkflowExecutionState
	status           enumspb.WorkflowExecutionStatus
	updates          int

	// Sub-entity keys (slices: maps may not be iterated). live are held, gone
	// were deleted; an upsert may name either, exercising upsert-after-delete.
	liveActivities, goneActivities []int64
	liveTimers, goneTimers         []string
	nextActivity                   int64
	nextTimer                      int
	// buffered counts unflushed buffered batches; a clear needs one.
	buffered int
}

// New returns a generator for cfg; it fails only on a bad config.
func New(cfg Config) (*Generator, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	g := &Generator{
		cfg: cfg,
		// PCG needs two words; the second is derived so the seed is enough.
		rng:        rand.New(rand.NewPCG(uint64(cfg.Seed), uint64(cfg.Seed)^0x9e3779b97f4a7c15)),
		serializer: serialization.NewSerializer(),
		nextID:     1,
	}
	g.rep.TasksByCat = map[int32]int{}
	g.emitted = map[int32]*emittedTasks{}
	g.namespaceID = g.newUUID()
	return g, nil
}

// Next returns the stream's next mutation. The stream never ends.
func (g *Generator) Next() (mutation.Mutation, error) {
	if len(g.queue) == 0 {
		if err := g.step(); err != nil {
			return mutation.Mutation{}, err
		}
	}
	m := g.queue[0]
	g.queue = g.queue[1:]

	// Counted on hand-out, not when built, so the [Report] matches what the
	// caller received even if it stops inside a queued deletion pair.
	g.rep.Mutations++
	switch m.Kind() {
	case mutation.KindCreate:
		g.rep.Creates++
	case mutation.KindUpdate:
		g.rep.Updates++
		mut := m.Update.UpdateWorkflowMutation
		// Read from the request so the report cannot drift from it.
		if m.Update.NewWorkflowSnapshot != nil {
			g.rep.ContinueAsNews++
		}
		if mut.NewBufferedEvents != nil {
			g.rep.BufferedBatches++
		}
		if mut.ClearBufferedEvents {
			g.rep.BufferedClears++
		}
	case mutation.KindConflictResolve:
		g.rep.ConflictResolves++
	case mutation.KindSet:
		g.rep.Sets++
	case mutation.KindDeleteCurrent:
		g.rep.DeleteCurrent++
	case mutation.KindDelete:
		g.rep.Deletes++
	case mutation.KindAddTasks:
		g.rep.TaskAdds++
	case mutation.KindRangeCompleteTasks:
		g.rep.TaskRanges++
	}
	return m, nil
}

// Take returns the next n mutations. For 10^5 or more use [Generator.Next],
// as [Corpus] explains.
func (g *Generator) Take(n int) ([]mutation.Mutation, error) {
	out := make([]mutation.Mutation, 0, n)
	for range n {
		m, err := g.Next()
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, nil
}

// Report describes the stream so far. The returned value owns its map, so
// later generation does not change it.
func (g *Generator) Report() Report {
	r := g.rep
	r.cfg = g.cfg
	r.Seed = g.cfg.Seed
	r.Workflows = len(g.pool)
	r.WorkflowReuse = g.cfg.WorkflowReuse
	r.KeyReuse = g.cfg.KeyReuse
	r.BufferedRate = g.cfg.BufferedRate
	r.TasksByCat = maps.Clone(g.rep.TasksByCat)
	if r.Workflows > 0 {
		r.CollapseRatio = float64(r.Mutations-r.TaskAdds-r.TaskRanges) / float64(r.Workflows)
	}
	if r.DistinctKeys > 0 {
		r.KeyCollapse = float64(r.Upserts) / float64(r.DistinctKeys)
	}
	return r
}
