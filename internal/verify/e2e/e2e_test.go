package e2e

// One run, two compositions. The workflow, the worker, the server and the
// database are identical in both; what differs is the one value handed to
// temporal.WithCustomDataStoreFactory — the store bare, or the store with the
// layer in front of it. So a difference between the two arms is the layer's,
// and there is nowhere else for it to have come from.
//
// The green workflow is the weaker half of what this file says. A server whose
// layer fell out of the path completes the same workflow just as fast, which is
// what verify/witness exists for: the intercept arm states what it was supposed
// to be and hands over what the layer's own counters saw, and the control arm
// states that the layer was empty — a claim that goes red if this "passthrough"
// run quietly still had a layer in it.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/log"
	persistenceclient "go.temporal.io/server/common/persistence/client"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/aromanovich/waltz"
	"github.com/aromanovich/waltz/cold/memcold"
	"github.com/aromanovich/waltz/cycle"
	"github.com/aromanovich/waltz/internal/verify/witness"
	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/wal"
	"github.com/aromanovich/waltz/wal/memwal"
	"github.com/aromanovich/waltz/wrapper"
)

// budget bounds the whole of one arm — boot, namespace, workflow, drain and
// shutdown — so a server that comes up and never serves fails rather than hangs.
const budget = 3 * time.Minute

// drainWait is what the run gives the age watermark once the workflow is over.
// The shipped age is 5s and every writer is idle by then, so this is generous
// against a slow machine rather than against the policy.
const drainWait = 60 * time.Second

// taskQueue is the one queue the worker polls.
const taskQueue = "waltz-e2e"

// upper is the activity, and greet the workflow that calls it. Between them
// they are the smallest run that is still a real one: a workflow task and an
// activity task, so the history service writes a create, several updates and
// the transfer and timer tasks that carry them.
func upper(_ context.Context, name string) (string, error) {
	return "hello " + strings.ToUpper(name), nil
}

func greet(ctx workflow.Context, name string) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	})
	var out string
	if err := workflow.ExecuteActivity(ctx, upper, name).Get(ctx, &out); err != nil {
		return "", err
	}
	return out, nil
}

// arm is one composition of the two seams under one server.
type arm struct {
	store   *memcold.Store
	layer   *waltz.Layer
	cluster *Cluster
	client  sdkclient.Client
}

// start composes the layer, builds the factory the server is given, and brings
// the server up with a namespace registered on it.
//
// intercept is the only branch: both arms compose a layer, and the control arm
// simply does not hand it to the factory. That is what gives the control
// something to be judged by — [witness.NoLayer] reads the layer's counters and
// says they must all be zero, which nothing could say about a run that composed
// no layer at all.
func start(ctx context.Context, t *testing.T, intercept bool) *arm {
	t.Helper()

	store, release, err := memcold.New(ClusterName)
	require.NoError(t, err)
	t.Cleanup(release)

	logger := log.NewZapLogger(log.BuildZapLogger(log.Config{Stdout: true, Level: "error"}))

	// Composed before the server is built and shut down after it has stopped:
	// the budget assertion is a reason not to start, and the shutdown drain
	// needs a store that is still there.
	layer, err := waltz.Compose(
		waltz.Backends{Log: memwal.New(), Cold: store},
		cycle.Fixed(policy()),
		waltz.DefaultTaskCategories(),
		logger,
		nil,
	)
	require.NoError(t, err)

	var base persistenceclient.AbstractDataStoreFactory = memcold.NewAbstractDataStoreFactory(store)
	if intercept {
		base = layer.AbstractFactory(base)
	} else {
		base = waltz.AbstractFactory(base, wrapper.Options{})
	}

	c, err := Start(base)
	require.NoError(t, err)

	a := &arm{store: store, layer: layer, cluster: c}
	namespace := "e2e-" + uuid.NewString()
	a.register(ctx, t, namespace)

	a.client, err = c.Client(ctx, namespace)
	require.NoError(t, err)
	return a
}

// policy is the shipped configuration, and the window in it is what makes an
// intercept run mean anything: at a window of one every drain carries one
// mutation, nothing is folded, and the run is green with the fold path deleted.
// The age watermark is what fires here — one workflow is nowhere near 256
// mutations or 256 KiB — which is the drain a run with no writers left can
// still expect.
func policy() cycle.Config { return cycle.Defaults() }

// register creates the namespace and waits for the frontend to describe it.
// Registration is asynchronous behind the frontend's own namespace cache, so a
// workflow started the instant it returns is a NamespaceNotFound.
func (a *arm) register(ctx context.Context, t *testing.T, namespace string) {
	t.Helper()

	admin, err := a.cluster.Client(ctx, "")
	require.NoError(t, err)
	defer admin.Close()

	_, err = admin.WorkflowService().RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
	})
	require.NoError(t, err)

	until(ctx, t, "the namespace never became visible to the frontend", func() error {
		_, err := admin.WorkflowService().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
			Namespace: namespace,
		})
		return err
	})
}

// until retries f until it succeeds, and fails with the last error it saw
// rather than with a timeout that names nothing. Bounded by ctx, which is the
// arm's whole budget: everything it is used for is a service catching up with
// something already committed, so there is no shorter honest deadline.
func until(ctx context.Context, t *testing.T, what string, f func() error) {
	t.Helper()
	for {
		err := f()
		if err == nil {
			return
		}
		if ctx.Err() != nil {
			t.Fatalf("%s: %v", what, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// run drives one workflow to completion through the frontend and returns what
// it answered.
func (a *arm) run(ctx context.Context, t *testing.T) string {
	t.Helper()

	w := worker.New(a.client, taskQueue, worker.Options{})
	w.RegisterWorkflow(greet)
	w.RegisterActivity(upper)
	require.NoError(t, w.Start())
	defer w.Stop()

	// The workflow start races the namespace reaching the history and matching
	// services, which have caches of their own behind the frontend's.
	var handle sdkclient.WorkflowRun
	until(ctx, t, "no workflow could be started", func() error {
		var err error
		handle, err = a.client.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
			ID:                       "e2e-" + uuid.NewString(),
			TaskQueue:                taskQueue,
			WorkflowExecutionTimeout: time.Minute,
		}, greet, "waltz")
		return err
	})

	var out string
	require.NoError(t, handle.Get(ctx, &out))
	return out
}

// stop takes the arm down in the order the layer's lifecycle requires: the
// server first, so its writers are gone, then the drain that lands whatever
// they left in the window.
func (a *arm) stop(ctx context.Context, t *testing.T) {
	t.Helper()
	a.client.Close()
	require.NoError(t, a.cluster.Stop())
	a.layer.Shutdown(ctx, 30*time.Second)
}

// watermarks is what the cold store holds for every shard of the cluster: the
// applied position each drain committed inside its own transaction. It is read
// from the database rather than from the layer, so a run that says a drain
// committed and a store that says nothing was applied cannot both be believed.
func (a *arm) watermarks(ctx context.Context, t *testing.T) map[wal.ShardID]wal.Seqno {
	t.Helper()
	found := map[wal.ShardID]wal.Seqno{}
	for shard := range wal.ShardID(Shards) {
		seqno, ok, err := a.store.Watermark(ctx, shard+1)
		require.NoError(t, err)
		if ok {
			found[shard+1] = seqno
		}
	}
	return found
}

// TestAWorkflowRunsThroughTheLayer is the intercept arm: a real server whose
// history shards write into the log, and a workflow that completes over it.
func TestAWorkflowRunsThroughTheLayer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	a := start(ctx, t, true)
	require.Equal(t, "hello WALTZ", a.run(ctx, t))

	// Sampled with the server still up, and after a wait rather than after a
	// drain of our own: what it proves is that the age watermark fires on its
	// own with every writer idle, which is the only drain a quiet shard gets.
	totals := a.waitForDrain(ctx, t)
	t.Log(witness.Describe(witness.Observed{Totals: totals}))

	require.NotZero(t, totals.Acked, "no mutation reached the log: the server ran on the cold store alone")
	require.NotZero(t, totals.Drains, "nothing was drained: the log holds a window nobody applied")
	require.NotZero(t, totals.Applied, "no drain moved the applied position")
	require.NotZero(t, totals.WrittenTasks, "no history task rode the log: the task path went around the accumulator")

	marks := a.watermarks(ctx, t)
	require.NotEmpty(t, marks, "no shard's watermark was ever written: no drain committed in the database")
	t.Logf("watermarks: %v", marks)

	requireWitness(t, witness.Expect{
		Window:         witness.Windowed,
		ShardsAcquired: true,
		MutableState:   true,
		HistoryTasks:   true,
		// The queues poll while the window holds what they are looking for, so
		// a page merged out of the window is a page this run must have read.
		// Ranges is not claimed beside it: a queue checkpoints on a 30s timer
		// and a run this short honestly completes none.
		TaskReads: true,
		Kinds: []witness.KindClaim{
			{Kind: mutation.KindCreate, Why: "the workflow was started, so a create was written"},
			{Kind: mutation.KindUpdate, Why: "the workflow ran a task and an activity, so its mutable state was updated"},
		},
	}, witness.Observed{Totals: totals})

	a.stop(ctx, t)
}

// TestAWorkflowRunsWithTheLayerOutOfThePath is the control. The same server,
// the same store, the same workflow — and a layer that must have seen none of
// it, which is what says the intercept arm's numbers came from the layer being
// in the path rather than from the run happening at all.
func TestAWorkflowRunsWithTheLayerOutOfThePath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	a := start(ctx, t, false)
	require.Equal(t, "hello WALTZ", a.run(ctx, t))

	totals := a.layer.Totals()
	t.Log(witness.Describe(witness.Observed{Totals: totals}))
	requireWitness(t, witness.Expect{Window: witness.NoLayer}, witness.Observed{Totals: totals})

	require.Empty(t, a.watermarks(ctx, t),
		"a shard's watermark was written by a run with no layer: something drained")

	a.stop(ctx, t)
}

// waitForDrain gives the age watermark time to fire and returns the totals as
// soon as a drain is visible.
func (a *arm) waitForDrain(ctx context.Context, t *testing.T) cycle.Totals {
	t.Helper()
	deadline := time.Now().Add(drainWait)
	for {
		totals := a.layer.Totals()
		if totals.Drains > 0 {
			return totals
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			t.Fatalf("no drain committed within %s: %s", drainWait,
				witness.Describe(witness.Observed{Totals: totals}))
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// requireWitness fails with every claim the run broke rather than the first.
func requireWitness(t *testing.T, expect witness.Expect, observed witness.Observed) {
	t.Helper()
	errs := expect.Check(observed)
	if len(errs) == 0 {
		return
	}
	var lines []string
	for _, err := range errs {
		lines = append(lines, "  "+err.Error())
	}
	t.Fatalf("the run was not the run it claims to be:\n%s", strings.Join(lines, "\n"))
}
