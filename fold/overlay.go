package fold

// The overlay: what a read must be told while the window still holds writes the
// cold store has not seen. Two rules the whole file obeys:
//
//   - the merge is [applyMutationToSnapshot] and no other function, so a read
//     answers with what the drain will write;
//   - nothing here writes: the accumulator's state and the caller's base row
//     are both copied into a private snapshot first.
//
// Only the current row decodes; on the mutable-state path the accumulator
// already holds the blobs [p.InternalWorkflowMutableState] wants.

import (
	"fmt"
	"maps"
	"slices"

	commonpb "go.temporal.io/api/common/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
)

// RunShape names what the window holds for one run, and is the whole of what a
// reader branches on.
type RunShape int

const (
	// RunAbsent: the window holds nothing for this run. The base stands,
	// including its NotFound.
	RunAbsent RunShape = iota
	// RunSnapshot: the window holds whole state for the run (a Create, a Set, a
	// conflict-resolve's reset, the new run of a continue-as-new, or a Create
	// behind a tombstone). The base must not be merged in: a snapshot either
	// clears the run's collection tables first (the plugin's reset path) or
	// asserts the run absent, so the cold store's leftovers are rows the drain
	// is about to delete or rows of a run that is not this one.
	RunSnapshot
	// RunDelta: the window holds a delta for the run (an Update, or a
	// conflict-resolve's current mutation). The answer is base ⊕ delta.
	RunDelta
	// RunTombstone: the window deleted the run. The answer is "no such
	// execution", whatever the cold store still holds.
	RunTombstone
)

func (s RunShape) String() string {
	switch s {
	case RunAbsent:
		return "absent"
	case RunSnapshot:
		return "snapshot"
	case RunDelta:
		return "delta"
	case RunTombstone:
		return "tombstone"
	}
	return fmt.Sprintf("RunShape(%d)", int(s))
}

// RunView is one run's place in the window, as a reader sees it. Obtain one
// from [Accumulator.ViewRun]. It is valid until the next mutation folds in, so
// the goroutine holding it must be the one that owns the accumulator.
type RunView struct {
	Shape RunShape

	// rs is the run's window state, nil unless the shape carries one. Unexported
	// so no caller reaches the accumulator through a value it was handed.
	rs *runState
}

// ViewRun returns the window's view of one run. Neither it nor any read
// through the view mutates the accumulator.
func (a *Accumulator) ViewRun(namespaceID, workflowID, runID string) RunView {
	w := a.peek(namespaceID, workflowID)
	if w == nil {
		return RunView{Shape: RunAbsent}
	}
	rs := w.runs[runID]
	switch {
	case rs == nil:
		return RunView{Shape: RunAbsent}
	case rs.tombstoned:
		return RunView{Shape: RunTombstone}
	case rs.owner == nil:
		// Unreachable: only a tombstone clears an owner, and it sets the flag
		// above at the same time. Answering from the cold store invents nothing.
		return RunView{Shape: RunAbsent}
	case rs.part == partMutation:
		return RunView{Shape: RunDelta, rs: rs}
	default:
		return RunView{Shape: RunSnapshot, rs: rs}
	}
}

// NeedsBase reports whether answering needs the cold store's row; false means
// the window answers alone and the caller saves the round trip.
func (v RunView) NeedsBase() bool { return v.Shape == RunAbsent || v.Shape == RunDelta }

// Held reports whether the window holds this run at all, the reading behind the
// cycle's ReadsHeld counter. Here rather than at the counter, so a fifth
// [RunShape] cannot silently change what it counts.
func (v RunView) Held() bool { return v.Shape != RunAbsent }

// Render answers a mutable-state read. base is the cold store's response and
// must be non-nil exactly when [RunView.NeedsBase] said so. found is false when
// the answer is "no such execution"; the caller turns that into the store's own
// NotFound, since fold has no store error vocabulary.
//
// The answer carries the tail's DBRecordVersion, the one the window's merged
// request will write. Handing out the base's would make the server's next
// conditional write assert a version nothing writes. What apply asserts is
// unaffected: that stays the head-of-window [RunAssertion.BaseVersion].
func (v RunView) Render(base *p.InternalGetWorkflowExecutionResponse) (*p.InternalGetWorkflowExecutionResponse, bool) {
	switch v.Shape {
	case RunTombstone:
		return nil, false

	case RunSnapshot:
		// A snapshot replaces the run's rows wholesale, so only the window's own
		// buffered batches survive.
		snap := copySnapshot(v.rs.owner.snapshotPart(v.rs.part))
		return responseOf(mutableStateOf(snap, concatBlobs(nil, v.rs.buffered))), true

	case RunDelta:
		// Deliberately untested, and a sweep has looked: negating this leaves the
		// tree green because no caller here reaches it. The layer's own read takes
		// the base through a store whose absence is an *error*, so the route out
		// returns before this, and a nil arrives only from a direct caller of this
		// exported method or from a store answering a row with no state.
		if base == nil || base.State == nil {
			return nil, false
		}
		mut := v.rs.owner.mutationPart()
		snap := snapshotOfBase(base)
		applyMutationToSnapshot(snap, mut)
		// Buffered events do not merge: the store's batches come first, then the
		// window's, and the store's go when the merged mutation carries a clear.
		held := base.State.BufferedEvents
		if mut.ClearBufferedEvents {
			held = nil
		}
		return responseOf(mutableStateOf(snap, concatBlobs(held, v.rs.buffered))), true

	default:
		return base, base != nil
	}
}

// responseOf wraps a rendered state, filling both version fields from one
// value. The ExecutionManager reads the response's field; the plugin's own read
// leaves the inner one at zero.
func responseOf(state *p.InternalWorkflowMutableState) *p.InternalGetWorkflowExecutionResponse {
	return &p.InternalGetWorkflowExecutionResponse{State: state, DBRecordVersion: state.DBRecordVersion}
}

// CurrentShape names what the window holds for a workflow's current-execution
// row, a separate question from what it holds for a run: the row is written by
// the window's last writer ([CurrentWrite]), not by the merged request.
type CurrentShape int

const (
	// CurrentUnheld: the window says nothing about the row. The base stands.
	CurrentUnheld CurrentShape = iota
	// CurrentWritten: the window wrote the row, and [CurrentWrite] is its
	// content as the sequential path would have left it.
	CurrentWritten
	// CurrentGone: the window wrote the row and then removed it. No current
	// execution, whatever the base holds.
	CurrentGone
	// CurrentGuarded: delete-currents stand over the base with no window write
	// above them. The store removes the row only if it names that run, so the
	// answer is that guard evaluated against the base.
	CurrentGuarded
)

func (s CurrentShape) String() string {
	switch s {
	case CurrentUnheld:
		return "unheld"
	case CurrentWritten:
		return "written"
	case CurrentGone:
		return "gone"
	case CurrentGuarded:
		return "guarded"
	}
	return fmt.Sprintf("CurrentShape(%d)", int(s))
}

// CurrentView is a workflow's current-execution row as a reader sees it.
type CurrentView struct {
	Shape CurrentShape

	// write is the row's content, for [CurrentWritten].
	write *CurrentWrite
	// runs are the runs the surviving delete-currents name, for
	// [CurrentGuarded].
	runs []string
}

// ViewCurrent returns the window's view of a workflow's current-execution row.
func (a *Accumulator) ViewCurrent(namespaceID, workflowID string) CurrentView {
	return a.peek(namespaceID, workflowID).currentView()
}

// currentView is the one derivation of what the window holds for the current
// row. A removal outranks a write, and a write outranks the surviving guards.
//
// Three readers branch on what it answers and none derives it again: the drain
// ([Accumulator.Drain]) turns it into the record apply asserts, the condition
// authority ([Accumulator.decideCurrent]) asks whether the window determines an
// assertion about the row, and the overlay renders it. Nil-safe on the
// receiver, an absent workflow being a window that holds nothing.
func (w *workflowAcc) currentView() CurrentView {
	if w == nil {
		return CurrentView{Shape: CurrentUnheld}
	}
	switch {
	case w.cur.removed:
		return CurrentView{Shape: CurrentGone}
	case w.cur.write != nil:
		return CurrentView{Shape: CurrentWritten, write: w.cur.write}
	}
	// The delete-currents the window still stands on. They live in pending
	// because each is also a request the drain emits; recordCurrentWrite drops
	// the ones a later write made invisible, which is what makes this scan the
	// surviving guards rather than every delete the window took. More than one
	// may survive, and the row goes if the base names any.
	var runs []string
	for _, pr := range w.pending {
		if pr.m.DeleteCurrent != nil {
			runs = append(runs, pr.m.DeleteCurrent.RunID)
		}
	}
	if len(runs) == 0 {
		return CurrentView{Shape: CurrentUnheld}
	}
	return CurrentView{Shape: CurrentGuarded, runs: runs}
}

// NeedsBase reports whether answering needs the cold store's row.
func (v CurrentView) NeedsBase() bool {
	return v.Shape == CurrentUnheld || v.Shape == CurrentGuarded
}

// Held is [RunView.Held] for the current row, and is here for the same reason.
//
// It is not [workflowAcc.assertsCurrent] and the two deliberately disagree on
// [CurrentGuarded]: a tainted DeleteCurrent is something the window has to say
// about the row, so a read is answered from it, and it carries no head
// assertion, so the condition authority delegates. A read question and a
// partition question, and the guarded shape is where they part.
func (v CurrentView) Held() bool { return v.Shape != CurrentUnheld }

// Render answers a current-execution read; found is false for "no current
// execution", which the caller turns into the store's NotFound. The error is a
// decode failure of the window's own [CurrentWrite] blob.
func (v CurrentView) Render(base *p.InternalGetCurrentExecutionResponse) (*p.InternalGetCurrentExecutionResponse, bool, error) {
	switch v.Shape {
	case CurrentGone:
		return nil, false, nil

	case CurrentWritten:
		state, err := serialization.WorkflowExecutionStateFromBlob(v.write.StateBlob)
		if err != nil {
			return nil, false, fmt.Errorf("fold: reading back the current-execution row of run %s: %w", v.write.RunID, err)
		}
		return &p.InternalGetCurrentExecutionResponse{RunID: v.write.RunID, ExecutionState: state}, true, nil

	case CurrentGuarded:
		if base == nil {
			return nil, false, nil
		}
		if slices.Contains(v.runs, base.RunID) {
			return nil, false, nil
		}
		return base, true, nil

	default:
		return base, base != nil, nil
	}
}

// copySnapshot is a private snapshot holding the same blobs: every map is new,
// so a later fold cannot reach an answer already handed out. Sharing the blobs
// is safe because fold replaces map entries and never writes through a
// *commonpb.DataBlob. Tasks are dropped rather than copied: a mutable-state
// read does not answer them, and [applyMutationToSnapshot] appends to that map.
func copySnapshot(src *p.InternalWorkflowSnapshot) *p.InternalWorkflowSnapshot {
	dst := *src
	dst.ActivityInfos = maps.Clone(src.ActivityInfos)
	dst.TimerInfos = maps.Clone(src.TimerInfos)
	dst.ChildExecutionInfos = maps.Clone(src.ChildExecutionInfos)
	dst.RequestCancelInfos = maps.Clone(src.RequestCancelInfos)
	dst.SignalInfos = maps.Clone(src.SignalInfos)
	dst.ChasmNodes = maps.Clone(src.ChasmNodes)
	dst.SignalRequestedIDs = maps.Clone(src.SignalRequestedIDs)
	dst.Tasks = nil
	return &dst
}

// snapshotOfBase converts the cold store's row into the snapshot
// [applyMutationToSnapshot] folds a delta onto; its maps are copies for the
// reason copySnapshot's are. Tasks and BufferedEvents are not carried across,
// the read having its own rule for both, and the version comes off the response
// rather than off the state, where the plugin leaves a zero.
//
// What survives of the base here is the collections: the fold runs next and
// assigns every scalar from the delta, which always carries the whole of one.
func snapshotOfBase(base *p.InternalGetWorkflowExecutionResponse) *p.InternalWorkflowSnapshot {
	state := base.State
	return &p.InternalWorkflowSnapshot{
		ExecutionInfoBlob:   state.ExecutionInfo,
		ExecutionStateBlob:  state.ExecutionState,
		NextEventID:         state.NextEventID,
		DBRecordVersion:     base.DBRecordVersion,
		Checksum:            state.Checksum,
		ActivityInfos:       maps.Clone(state.ActivityInfos),
		TimerInfos:          maps.Clone(state.TimerInfos),
		ChildExecutionInfos: maps.Clone(state.ChildExecutionInfos),
		RequestCancelInfos:  maps.Clone(state.RequestCancelInfos),
		SignalInfos:         maps.Clone(state.SignalInfos),
		ChasmNodes:          maps.Clone(state.ChasmNodes),
		SignalRequestedIDs:  setOf(state.SignalRequestedIDs),
	}
}

// mutableStateOf hands a private snapshot out as the read's answer. It moves
// rather than copies, since every snapshot reaching it came from copySnapshot
// or snapshotOfBase. The signal-requested ids are sorted on the way out, the
// manager reading them as a set.
func mutableStateOf(snap *p.InternalWorkflowSnapshot, buffered []*commonpb.DataBlob) *p.InternalWorkflowMutableState {
	return &p.InternalWorkflowMutableState{
		ActivityInfos:       snap.ActivityInfos,
		TimerInfos:          snap.TimerInfos,
		ChildExecutionInfos: snap.ChildExecutionInfos,
		RequestCancelInfos:  snap.RequestCancelInfos,
		SignalInfos:         snap.SignalInfos,
		ChasmNodes:          snap.ChasmNodes,
		SignalRequestedIDs:  slices.Sorted(maps.Keys(snap.SignalRequestedIDs)),
		ExecutionInfo:       snap.ExecutionInfoBlob,
		ExecutionState:      snap.ExecutionStateBlob,
		NextEventID:         snap.NextEventID,
		BufferedEvents:      buffered,
		Checksum:            snap.Checksum,
		DBRecordVersion:     snap.DBRecordVersion,
	}
}

// setOf turns the mutable state's signal-requested ids into the set shape a
// snapshot uses.
func setOf(ids []string) map[string]struct{} {
	if len(ids) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		set[id] = struct{}{}
	}
	return set
}

// concatBlobs is the buffered-event order: the store's batches, then the
// window's. A fresh slice, so neither source is appended to through the answer.
func concatBlobs(held, window []*commonpb.DataBlob) []*commonpb.DataBlob {
	return slices.Concat(held, window)
}
