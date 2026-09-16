// Package mutgen generates valid, deterministic mutation streams. A seed must
// reproduce the same encoded bytes, so that a failure found at one is
// reproducible from it alone; generation therefore must not depend on time,
// UUIDs, Go map iteration, or protobuf maps.
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

// Config is the shape of the stream. There is no zero-value defaulting: start
// from [Default] and change the knobs the experiment is about.
type Config struct {
	// Seed fixes the stream. Two generators with the same config and seed
	// produce the same mutations, byte for byte.
	Seed int64

	// ShardID is the shard every mutation belongs to. One stream is one
	// shard's log, because one shard is one accumulator and one apply
	// transaction.
	ShardID int32

	// Workflows caps the workflow key space. 0 means unbounded — a fresh key
	// whenever WorkflowReuse says so, which is what a low-reuse stream needs.
	Workflows int

	// WorkflowReuse is P(a step touches a workflow already in the stream)
	// rather than starting a new one. This is the dial that sets the collapse
	// ratio: at 0 every mutation is a workflow of its own and a fold has
	// nothing to merge. See [Default].
	WorkflowReuse float64

	// KeyReuse is P(a sub-entity upsert names a key the run already holds, or
	// one it deleted, rather than a fresh one). This is the second collapse:
	// it decides whether a window's upserts merge per key or only pile up, and
	// whether the accumulator's upsert-vs-delete resolution is exercised at
	// all. See [Default].
	KeyReuse float64

	// MaxChainLength is how many updates a run takes before the last one
	// closes it. It bounds the collapse ratio from above.
	MaxChainLength int

	// Upserts is how many sub-entity upserts a mutation carries — activity
	// infos under int64 keys and timer infos under string keys, so both key
	// shapes travel.
	Upserts int

	// DeleteAfterUpsert is P(a mutation also deletes a key the run upserted in
	// an earlier mutation). Only a delete that names a key the run really holds
	// exercises the accumulator's upsert-vs-delete resolution; a delete of an
	// id that was never there resolves against nothing.
	DeleteAfterUpsert float64

	// TaskDensity is the mean number of history tasks per mutation, spread over
	// all four categories.
	TaskDensity float64

	// SnapshotRate is P(a step on a live run is a snapshot-bearing request
	// rather than an update) — the barrier that resets the accumulator
	// (invariant I8). Half of them are a SetWorkflowExecution and half a
	// reset-only ConflictResolveWorkflowExecution: the accumulator treats the
	// two the same way, so a knob each would be a knob nobody has a reason to
	// turn.
	SnapshotRate float64

	// ContinueAsNewRate is P(a run that has reached the end of its chain
	// continues as new rather than simply completing). A continue-as-new is the
	// one request that carries two runs, which is what makes it worth
	// generating: fold merges it into a single request owning both, and refuses
	// some windows because of it.
	ContinueAsNewRate float64

	// BufferedRate is P(an update carries a batch of buffered events), and also
	// P(an update with something already buffered clears it, as completing a
	// workflow task does). Batches do not merge — one slot per mutation — so a
	// chain of them is the only thing that exercises the rule.
	BufferedRate float64

	// TombstoneRate is P(a closed run is deleted rather than superseded by a
	// new run under the same workflow id). A deleted workflow's key stays in
	// the space, so a later step re-creates it: that is the re-created-after-
	// deletion case.
	TombstoneRate float64

	// AddTasksRate is P(a step is a standalone AddHistoryTasks on a run the
	// stream already holds, rather than a workflow step). It is the request
	// kind that carries tasks with no mutable state beside them, which is what
	// the four queue processors and the admin paths do.
	AddTasksRate float64

	// RangeCompleteRate is P(a step is a range delete of one category's tasks).
	//
	// The range is derived from keys the stream has already emitted, never
	// drawn at random, and that is the trap this knob exists to avoid: a random
	// range covers nothing, the rule under test is exercised in name only, and
	// the run is green. [Report.TasksCovered] is the number that tells the two
	// apart, as [Report.Collapses] does for the collapse ratio.
	RangeCompleteRate float64
}

// Default is a stream with something in it for every rule fold has: chains long
// enough to merge, keys reused often enough for the per-key resolution to fire,
// tasks in every category, snapshots inside chains, and deletions.
//
// It is a function rather than zero-value defaulting because 0 is a meaningful
// value for four of the knobs — WorkflowReuse 0 is precisely the corpus an
// acceptance must be able to recognise as worthless — and a Config that filled
// zeros in would make that stream unaskable-for.
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

// WorkflowRunsOnly is [Default] with the two history-task rates off, so every
// mutation of the stream names a workflow run. A task record names none, so a
// caller whose claims are stated run by run cannot state one about it — and a
// caller that drives its own range delete and counts what it covered gets the
// wrong number from a stream that completed ranges of its own.
//
// The tasks a mutation carries are unaffected: that is [Config.TaskDensity].
func WorkflowRunsOnly() Config {
	cfg := Default()
	cfg.AddTasksRate, cfg.RangeCompleteRate = 0, 0
	return cfg
}

// UnbrokenChains is [WorkflowRunsOnly] with the three knobs that end a chain off
// as well: a run is a create and then updates up to [Config.MaxChainLength], and
// nothing in the stream makes fold drain of its own accord. That is what a
// caller asking about a *window* needs — a snapshot barrier, a continue-as-new's
// refusal or a tombstone empties the window it was about.
func UnbrokenChains() Config {
	cfg := WorkflowRunsOnly()
	cfg.SnapshotRate, cfg.ContinueAsNewRate, cfg.TombstoneRate = 0, 0, 0
	return cfg
}

// validate rejects a config that cannot mean anything, so a typo in a knob is
// not a stream that looks fine and exercises nothing.
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

// Report is what a stream turned out to be: the two collapse ratios and, beside
// each, the knob that set it. The pairing is the point — a bare ratio of 1.0
// reads as a pass and means the corpus never re-touched a workflow.
type Report struct {
	Seed      int64
	Mutations int
	Workflows int // distinct workflow keys the stream touched
	// Runs counts runs the stream created, which a continue-as-new raises above
	// Creates: it starts a run without being a create.
	Runs          int
	WorkflowReuse float64
	// CollapseRatio is workflow mutations over distinct workflows: the upper
	// bound on what fold can merge, and the number an acceptance gates on.
	//
	// The history-task requests are excluded from the numerator on purpose.
	// They name no workflow, so counting them would raise the ratio on a stream
	// that gave the accumulator nothing more to collapse — the same failure
	// mode WorkflowReuse 0 has, arriving from the other direction.
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
	// ContinueAsNews counts the updates that also carried a new run. They are
	// inside Updates too: a continue-as-new is one request.
	ContinueAsNews   int
	ConflictResolves int
	Sets             int
	// Deletes counts DeleteWorkflowExecution requests, which is also the number
	// of runs the stream tombstoned: a run is deleted at most once, and always
	// as the second half of a pair. One workflow id can be tombstoned more than
	// once, the key being free to be created again after each.
	Deletes       int
	DeleteCurrent int
	Recreations   int // runs created under a workflow id that already had one

	// BufferedBatches counts mutations carrying a batch of buffered events, and
	// BufferedClears the ones that cleared what was buffered. Batches do not
	// merge, so the count is also how many hand-offs an apply has to make.
	BufferedBatches int
	BufferedClears  int
	BufferedRate    float64

	// TaskAdds counts standalone AddHistoryTasks requests and TaskRanges the
	// range deletes beside them.
	TaskAdds   int
	TaskRanges int
	// TasksCovered is how many tasks the stream's own ranges actually cover —
	// the number that says the deletion rule was exercised rather than merely
	// present. A stream with ranges and no coverage is the corpus trap
	// [Config.RangeCompleteRate] warns about.
	TasksCovered int

	// cfg is what the stream was asked for, so [Report.Missing] can tell a
	// shape the generator failed to produce from one it was never configured
	// to.
	cfg Config
}

// Missing names the shapes this stream was configured to produce and did not.
// A corpus run gates on it being empty, and that is the claim rather than the
// mutation count: a generator regression that quietly stopped producing
// tombstones leaves every suite over it judging fold on creates, green.
//
// It asks only about what the config could produce, so a stream with the task
// rates turned off — three suites need one — is not missing history tasks.
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

// Collapses reports whether the stream gives fold anything to do. An acceptance
// run should gate on this rather than on the ratio it prints: a stream at
// ratio 1.0 is not a fold that failed to collapse, it is a corpus that asked
// nothing of it.
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
// safe for concurrent use, and it is not meant to be: the whole point is that
// the sequence is a function of the seed.
type Generator struct {
	cfg        Config
	rng        *rand.Rand
	serializer serialization.Serializer

	namespaceID string
	// pool holds every workflow key the stream has touched, in creation order.
	// A slice rather than a map because generation may not iterate a map.
	pool   []*workflowState
	nextID int64 // next task id, monotone per shard as the real allocator is
	// queue holds the mutations of a step that produced more than one — a
	// deletion is always a pair.
	queue []mutation.Mutation

	// emitted is the per-category ledger the range deletes are drawn from:
	// which keys the stream has put into the store, and how far its own deletes
	// have already reached. cats is the same set in a fixed order, because
	// generation may not iterate a map.
	emitted map[int32]*emittedTasks
	cats    []tasks.Category

	counters
}

type counters struct {
	mutations                                      int
	runs                                           int
	upserts, distinctKeys, subDeletes, tasks       int
	creates, updates, sets, deletes, deleteCurrent int
	conflictResolves, continueAsNews               int
	bufferedBatches, bufferedClears                int
	recreations                                    int
	tasksByCat                                     map[int32]int
	taskAdds, taskRanges, tasksCovered             int
}

// workflowState is one workflow key's place in the stream. At most one of run
// and closed is set: run is a live run taking updates, closed is a completed
// run still holding the current-execution row, and both nil means the key is
// free — never created, or deleted and awaiting re-creation.
type workflowState struct {
	workflowID string
	run        *runState
	closed     *runState
	created    int
}

// runState is what the store holds for one run, as far as validity needs it.
type runState struct {
	runID            string
	createRequestID  string
	version          int64 // db_record_version currently in the store
	nextEventID      int64
	lastWriteVersion int64
	state            enumsspb.WorkflowExecutionState
	status           enumspb.WorkflowExecutionStatus
	updates          int

	// The run's sub-entity keys. Slices, not sets: generation may not iterate
	// a map. live are keys the store holds, gone are keys it deleted — an
	// upsert may name either, and naming a deleted one is the upsert-after-
	// delete half of fold's per-key rule.
	liveActivities, goneActivities []int64
	liveTimers, goneTimers         []string
	nextActivity                   int64
	nextTimer                      int
	// buffered counts the run's unflushed buffered-event batches, so that a
	// clear is only generated when there is something to clear.
	buffered int
}

// New returns a generator for cfg. The error is a bad config, never a bad
// stream.
func New(cfg Config) (*Generator, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	g := &Generator{
		cfg: cfg,
		// Two words of state from one seed: PCG wants both, and deriving the
		// second keeps the seed the only knob a caller has to record.
		rng:        rand.New(rand.NewPCG(uint64(cfg.Seed), uint64(cfg.Seed)^0x9e3779b97f4a7c15)),
		serializer: serialization.NewSerializer(),
		nextID:     1,
	}
	g.tasksByCat = map[int32]int{}
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

	// Counted here rather than where the request was built, so that a [Report]
	// describes the stream the caller has actually seen. It matters for exactly
	// one shape: a deletion is queued as a pair, so a caller that stops between
	// the two would otherwise be told about a delete it was never handed.
	g.mutations++
	switch m.Kind() {
	case mutation.KindCreate:
		g.creates++
	case mutation.KindUpdate:
		g.updates++
		mut := m.Update.UpdateWorkflowMutation
		// Read off the request rather than remembered from the step, so the
		// report cannot drift from what the request actually says.
		if m.Update.NewWorkflowSnapshot != nil {
			g.continueAsNews++
		}
		if mut.NewBufferedEvents != nil {
			g.bufferedBatches++
		}
		if mut.ClearBufferedEvents {
			g.bufferedClears++
		}
	case mutation.KindConflictResolve:
		g.conflictResolves++
	case mutation.KindSet:
		g.sets++
	case mutation.KindDeleteCurrent:
		g.deleteCurrent++
	case mutation.KindDelete:
		g.deletes++
	case mutation.KindAddTasks:
		g.taskAdds++
	case mutation.KindRangeCompleteTasks:
		g.taskRanges++
	}
	return m, nil
}

// Take returns the next n mutations. It is a convenience for tests and for
// short corpora; a 10^6-mutation run must use [Generator.Next], since a
// materialised stream of that size is gigabytes of payload.
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

// Report describes the stream produced so far.
func (g *Generator) Report() Report {
	r := Report{
		cfg:              g.cfg,
		Seed:             g.cfg.Seed,
		Mutations:        g.mutations,
		Workflows:        len(g.pool),
		Runs:             g.runs,
		WorkflowReuse:    g.cfg.WorkflowReuse,
		Upserts:          g.upserts,
		DistinctKeys:     g.distinctKeys,
		KeyReuse:         g.cfg.KeyReuse,
		BufferedRate:     g.cfg.BufferedRate,
		SubDeletes:       g.subDeletes,
		Tasks:            g.tasks,
		TasksByCat:       make(map[int32]int, len(g.tasksByCat)),
		Creates:          g.creates,
		Updates:          g.updates,
		ContinueAsNews:   g.continueAsNews,
		ConflictResolves: g.conflictResolves,
		BufferedBatches:  g.bufferedBatches,
		BufferedClears:   g.bufferedClears,
		Sets:             g.sets,
		Deletes:          g.deletes,
		DeleteCurrent:    g.deleteCurrent,
		Recreations:      g.recreations,
		TaskAdds:         g.taskAdds,
		TaskRanges:       g.taskRanges,
		TasksCovered:     g.tasksCovered,
	}
	maps.Copy(r.TasksByCat, g.tasksByCat)
	if r.Workflows > 0 {
		r.CollapseRatio = float64(r.Mutations-r.TaskAdds-r.TaskRanges) / float64(r.Workflows)
	}
	if r.DistinctKeys > 0 {
		r.KeyCollapse = float64(r.Upserts) / float64(r.DistinctKeys)
	}
	return r
}
