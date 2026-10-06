package cycle

// The two mutable-state reads: answered from the window where it holds the
// run, otherwise from the cold store.
//
// Nothing here wraps errors: ContextImpl.handleReadError type-switches on one
// concrete type.
//
// Reads run on the cycle's goroutine, where the drain runs, so none lands
// between a drain emptying the window and its commit, when a mutation is in
// neither source. The cost: a shard's reads and writes serialise.
//
// The base row arrives as a thunk because neither the cycle nor the wrapper
// may name a store.

import (
	"context"

	"go.temporal.io/api/serviceerror"
	p "go.temporal.io/server/common/persistence"

	"github.com/aromanovich/waltz/fold"
	"github.com/aromanovich/waltz/wal"
)

// GetWorkflowExecution answers a mutable-state read for the shard the request
// names, through that shard's overlay. A shard this node holds no cycle for is
// [noCycleRoute]'s.
func (m *Manager) GetWorkflowExecution(
	ctx context.Context,
	req *p.GetWorkflowExecutionRequest,
	base func(context.Context) (*p.InternalGetWorkflowExecutionResponse, error),
) (*p.InternalGetWorkflowExecutionResponse, error) {
	shard := wal.ShardID(req.ShardID)
	c := m.Shard(shard)
	if c == nil {
		route, refusal := noCycleRoute(mutableStateRead, shard)
		return offLoop(ctx, route, refusal, base)
	}
	return c.getWorkflowExecution(ctx, req, base)
}

// GetCurrentExecution answers a current-execution read, under the same rule.
func (m *Manager) GetCurrentExecution(
	ctx context.Context,
	req *p.GetCurrentExecutionRequest,
	base func(context.Context) (*p.InternalGetCurrentExecutionResponse, error),
) (*p.InternalGetCurrentExecutionResponse, error) {
	shard := wal.ShardID(req.ShardID)
	c := m.Shard(shard)
	if c == nil {
		route, refusal := noCycleRoute(mutableStateRead, shard)
		return offLoop(ctx, route, refusal, base)
	}
	return c.getCurrentExecution(ctx, req, base)
}

// offLoop answers a read no loop served (no cycle, or a stopped one). Only
// [passThrough] reaches base; every other route returns the rule's non-nil
// refusal.
func offLoop[T any](
	ctx context.Context,
	route readRoute,
	refusal error,
	base func(context.Context) (T, error),
) (T, error) {
	if route == passThrough {
		return base(ctx)
	}
	var zero T
	return zero, refusal
}

// getWorkflowExecution asks this cycle's goroutine for the run's state, under
// [Manager.GetWorkflowExecution].
func (c *Cycle) getWorkflowExecution(
	ctx context.Context,
	req *p.GetWorkflowExecutionRequest,
	base func(context.Context) (*p.InternalGetWorkflowExecutionResponse, error),
) (*p.InternalGetWorkflowExecutionResponse, error) {
	resp, stopped, err := ask(ctx, c, func(s *state) (*p.InternalGetWorkflowExecutionResponse, error) {
		return c.readExecution(ctx, s, req, base)
	})
	if stopped {
		route, refusal := c.stoppedRead(mutableStateRead, err)
		return offLoop(ctx, route, refusal, base)
	}
	return resp, err
}

// getCurrentExecution asks this cycle's goroutine for the workflow's current
// run, under [Manager.GetCurrentExecution].
func (c *Cycle) getCurrentExecution(
	ctx context.Context,
	req *p.GetCurrentExecutionRequest,
	base func(context.Context) (*p.InternalGetCurrentExecutionResponse, error),
) (*p.InternalGetCurrentExecutionResponse, error) {
	resp, stopped, err := ask(ctx, c, func(s *state) (*p.InternalGetCurrentExecutionResponse, error) {
		return c.readCurrent(ctx, s, req, base)
	})
	if stopped {
		route, refusal := c.stoppedRead(mutableStateRead, err)
		return offLoop(ctx, route, refusal, base)
	}
	return resp, err
}

// stoppedRead routes a read on a cycle whose goroutine is gone
// ([stoppedRoute]), using the mirrored tail since no loop is left to ask. halt
// is the refusal [ask] returned.
func (c *Cycle) stoppedRead(who reader, halt error) (readRoute, error) {
	return stoppedRoute(c.State(), c.mirror.Empty(), who, c.shard, halt)
}

// prelude is the order all four reads share: readiness gate, count, routing
// rule, then the DrainOnRead drain. pass means serve from the cold store.
//
// takeView builds the read's view of the window and reports whether it held
// what the read wants; nil means the read takes no view. It runs after the
// gate (replay resets the accumulator), before the routing rule (a passed-
// through read still counts), and again, uncounted, after a drain empties the
// window.
//
// Each position matters. Routing before the gate would miss a fence only the
// replay finds and merge a window the replay then resets; counting after
// routing loses passed-through reads; draining before the count shows counters
// for a window the read never saw.
func (c *Cycle) prelude(ctx context.Context, s *state, who reader, takeView func(*state) bool) (bool, error) {
	// A read triggers replay; otherwise an inherited tail would be answered
	// from a cold store the log is ahead of.
	if err := c.startForRead(ctx, s); err != nil {
		return false, err
	}
	if takeView != nil {
		c.countRead(s, takeView(s))
	}

	switch pass, err := c.routeRead(s, who); {
	case err != nil:
		return false, err
	case pass:
		return true, nil
	}

	drained, err := c.drainForRead(ctx, s)
	if err != nil {
		return false, err
	}
	if drained && takeView != nil {
		takeView(s)
	}
	return false, nil
}

// windowView is a read's view of the window, taken before the base row: whether
// the window holds the target, whether the cold store's row is still needed,
// and the merge of the two. [fold.CurrentView] satisfies it; [fold.RunView]
// does through [runView].
type windowView[Resp any] interface {
	Held() bool
	NeedsBase() bool
	Render(base Resp) (Resp, bool, error)
}

// runView is [fold.RunView] as a [windowView]. Rendering a run cannot fail,
// where rendering the current row deserialises the state blob it holds.
type runView struct{ fold.RunView }

func (v runView) Render(
	base *p.InternalGetWorkflowExecutionResponse,
) (*p.InternalGetWorkflowExecutionResponse, bool, error) {
	resp, found := v.RunView.Render(base)
	return resp, found, nil
}

// readOverlay is the loop's half of both mutable-state reads: [Cycle.prelude],
// the base row if the view needs it, then the render. takeView runs on the loop
// and builds the view from s; notFound builds the absence error.
func readOverlay[Resp any](
	ctx context.Context,
	c *Cycle,
	s *state,
	base func(context.Context) (Resp, error),
	takeView func(*state) windowView[Resp],
	notFound func() error,
) (Resp, error) {
	var zero Resp

	var view windowView[Resp]
	switch pass, err := c.prelude(ctx, s, mutableStateRead, func(s *state) bool {
		view = takeView(s)
		return view.Held()
	}); {
	case err != nil:
		return zero, err
	case pass:
		return base(ctx)
	}

	var row Resp
	if view.NeedsBase() {
		var err error
		// Unwrapped, NotFound included: for a row the window does not hold, the
		// base's answer is the answer.
		if row, err = base(ctx); err != nil {
			return zero, err
		}
	}
	resp, found, err := view.Render(row)
	if err != nil {
		return zero, err
	}
	if !found {
		return zero, notFound()
	}
	return resp, nil
}

// readExecution is the loop's half.
func (c *Cycle) readExecution(
	ctx context.Context,
	s *state,
	req *p.GetWorkflowExecutionRequest,
	base func(context.Context) (*p.InternalGetWorkflowExecutionResponse, error),
) (*p.InternalGetWorkflowExecutionResponse, error) {
	return readOverlay(ctx, c, s, base,
		func(s *state) windowView[*p.InternalGetWorkflowExecutionResponse] {
			return runView{s.acc.ViewRun(req.NamespaceID, req.WorkflowID, req.RunID)}
		},
		func() error {
			return serviceerror.NewNotFoundf(
				"workflow execution not found for workflow ID %q and run ID %q", req.WorkflowID, req.RunID)
		})
}

// readCurrent is the same, for the current-execution row.
func (c *Cycle) readCurrent(
	ctx context.Context,
	s *state,
	req *p.GetCurrentExecutionRequest,
	base func(context.Context) (*p.InternalGetCurrentExecutionResponse, error),
) (*p.InternalGetCurrentExecutionResponse, error) {
	return readOverlay(ctx, c, s, base,
		func(s *state) windowView[*p.InternalGetCurrentExecutionResponse] {
			return s.acc.ViewCurrent(req.NamespaceID, req.WorkflowID)
		},
		func() error {
			return serviceerror.NewNotFoundf(
				"current workflow execution not found for workflow ID %q", req.WorkflowID)
		})
}

// routeRead asks [loopRoute] with the loop's values; pass is true only for
// [passThrough]. The halt error is built here, so an unconverted refusal is
// this cycle's own.
func (c *Cycle) routeRead(s *state, who reader) (bool, error) {
	unresolved, _ := s.tail.Stalled()
	route, refusal := loopRoute(s.st, s.tail.Empty(), unresolved.Seqno, who, c.shard, c.halted(s))
	return route == passThrough, refusal
}

// startForRead is the readiness gate: a running cycle that has not yet
// replayed its tail does so now, on the caller's context.
// A halt raised by the replay is left to [Cycle.routeRead]; returned here, its
// error is unrecognised at the store boundary and could let a task reader
// complete a range it should have been refused. A failure that leaves the
// cycle running is returned.
func (c *Cycle) startForRead(ctx context.Context, s *state) error {
	if s.st != StateRunning {
		return nil
	}
	err := c.start(ctx, s)
	if err != nil && s.st != StateRunning {
		return nil
	}
	return err
}

// drainForRead implements [Config.DrainOnRead]: apply the window before the
// read. It reports whether it drained, since an earlier view is now stale.
// It runs after the routing rule because a halted cycle may not drain, and a
// stalled one is refused there first. A failed drain fails the read.
func (c *Cycle) drainForRead(ctx context.Context, s *state) (bool, error) {
	if !c.policy().DrainOnRead {
		return false, nil
	}
	return true, c.drain(ctx, s, drainRead)
}

// countRead counts the read and, separately, whether the window held anything
// for it (see [Counters.ReadsHeld]).
func (c *Cycle) countRead(s *state, held bool) {
	s.Reads++
	if held {
		s.ReadsHeld++
	}
}
