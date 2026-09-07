package cycle

// The two mutable-state reads: answered from the window where it holds the run,
// from the cold store where it does not.
//
// Nothing here wraps. What comes out is what ContextImpl.handleReadError
// type-switches on, and it matches one concrete type with no errors.As.
//
// A read is a job on the cycle's own goroutine because the drain runs
// there: a read served anywhere else could land in the interval where the window
// has been emptied and the transaction has not yet committed, the one interval
// in which a mutation is in neither source. The cost is that a shard's reads and
// writes serialise.
//
// The base row arrives as a thunk because the cycle may not name a store and the
// wrapper may not import anything that reaches one.

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

// offLoop answers a read no loop served — the registry held no cycle, or the
// one it held has stopped. Only [passThrough] is answered here; every other
// route this side of the loop refuses, and the rules build a non-nil refusal
// for each of them.
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

// stoppedRead hands what a cycle whose goroutine is gone has left to
// [stoppedRoute]: the state its loop stopped in, and the mirrored tail rather
// than [tailstate.Tail.Empty] — nothing is left to ask, so the answer is the
// last one the loop published. halt is the standing refusal [ask] returned in
// place of an answer.
func (c *Cycle) stoppedRead(who reader, halt error) (readRoute, error) {
	return stoppedRoute(c.State(), c.mirror.Empty(), who, c.shard, halt)
}

// prelude is the order the three reads share: the readiness gate, the count, the
// routing rule, and the drain the mode may ask for. pass is that rule's answer —
// the read is to be served from the cold store.
//
// takeView builds the read's view of the window and reports whether the window held
// what the read is for. It runs after the gate, because a replay resets the
// accumulator it reads; before the routing rule, because a read that rule passes
// through is still a read this shard routed; and a second time after a drain,
// which empties the window the first view was taken of. That second call does
// not count again. A read holding no view passes nil.
//
// The order is not an arrangement. The rule consulted before the gate sees a
// running cycle with a stall the replay is about to clear, says nothing, and
// lets the read merge a window that replay has since reset; a count after it
// loses the reads it passes through; a drain before the count shows the counters
// a window the read never saw.
func (c *Cycle) prelude(ctx context.Context, s *state, who reader, takeView func(*state) bool) (bool, error) {
	// A read needs the window, so it triggers replay: a read on an inherited
	// tail would otherwise be answered from a cold store the log is ahead of.
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

// windowView is a read's half of the window, taken before the base row is read:
// whether the window holds what the read is for, whether the answer still needs
// the cold store's row, and the merge of the two. [fold.CurrentView] satisfies
// it as it stands; [fold.RunView] does through [runView].
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

// readOverlay is the loop's half of both mutable-state reads: the shared order
// through [Cycle.prelude], the base row where the view needs one, and the
// render. takeView is called on the loop and must build the view from s; notFound
// names what was looked for.
//
// The two reads differ in what they take and in what they call an absence, and
// in nothing else — so the order here has one body, and a third read of this
// shape is a view and a message.
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

// routeRead hands the loop's own values to [loopRoute] and reads the route back
// as [Cycle.prelude]'s pass: only [passThrough] is served from the cold store.
// The halt's own error is built here, so what the rule hands back unconverted is
// a value this cycle had.
func (c *Cycle) routeRead(s *state, who reader) (bool, error) {
	unresolved, _ := s.tail.Stalled()
	route, refusal := loopRoute(s.st, s.tail.Empty(), unresolved.Seqno, who, c.shard, c.halted(s))
	return route == passThrough, refusal
}

// startForRead is the read path's readiness gate: a running cycle that has not
// read its floor and replayed its tail does so now, on this caller's context.
//
// A halt raised by the replay is swallowed and left to [Cycle.routeRead].
// Otherwise the read that discovers the fence answers with the replay's own
// words, which nothing at the store boundary recognises, and for a task read
// that answer lets a caller complete a range it should have been refused. A
// failure that leaves the cycle running is returned instead.
//
// Its place in the read path is [Cycle.prelude]'s.
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

// drainForRead is [Config.DrainOnRead]: the window applied before the read it
// would have merged into. It reports whether the mode is on, because a view
// taken before it is a view of a window it has emptied.
//
// A halted cycle may not drain, which is why [Cycle.prelude] runs it after the
// routing rule — and a stalled one is refused there, so the resolve every drain
// begins with is not this instrument's to reach. What goes to zero here is
// task-reads-merged, the honest witness that the merge did not run. A drain that
// fails fails the read.
func (c *Cycle) drainForRead(ctx context.Context, s *state) (bool, error) {
	if !c.policy().DrainOnRead {
		return false, nil
	}
	return true, c.drain(ctx, s, drainRead)
}

// countRead counts the read, and separately whether the window held anything for
// it. A mode-level witness needs the second: reads that never crossed a held
// workflow is what a layer that came out empty looks like.
func (c *Cycle) countRead(s *state, held bool) {
	s.Reads++
	if held {
		s.ReadsHeld++
	}
}
