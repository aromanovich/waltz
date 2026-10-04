package mutation

import p "go.temporal.io/server/common/persistence"

// kindInfo is one row of the kind enumeration: bookkeeping facts about a
// [Kind]. Behaviour stays in the per-kind switches in fold and the applier.
// [Mutation.Kind] and [Mutation.ShardID] are hand-written; every mutation field
// needs exactly one row agreeing with both. [Mutation.RangeID] and
// [Mutation.EventSlots] read the last two columns, so a kind added without
// them fails the kind guard by name instead of fencing against zero or writing
// no events.
//
// History-task maps are not a column: which [Part]s a request carries depends
// on the instance (continue-as-new adds one), not the kind. Every kind names
// its task slots through [Mutation.TaskSlot].
type kindInfo struct {
	name string // what [Kind.String] returns

	// slot selects the [Mutation] field carrying the kind's request (a selector,
	// so a rename fails to compile). The guard finds the name by address.
	slot func(*Mutation) any

	present func(Mutation) bool  // whether that field is set
	shard   func(Mutation) int32 // the shard the request names, read from that field

	// rangeID is the epoch the caller wrote under; 0 if the request has none.
	rangeID func(Mutation) int64

	// events lists the request's new-history-event slices, in store order.
	// Both writers (wrapper and fold) walk it, so an omitted slice means a
	// mutation acked over history rows nobody wrote (ADR 0014).
	// [Mutation.ClearEvents] and the codec list the same fields by hand.
	events func(Mutation) [][]*p.InternalAppendHistoryNodesRequest
}

// Explicit values for requests without such a field, so a nil column still
// means a missing decision.
func noRangeID(Mutation) int64                                   { return 0 }
func noEvents(Mutation) [][]*p.InternalAppendHistoryNodesRequest { return nil }

// kinds is the enumeration, indexed by [Kind]. [KindInvalid]'s row is zero;
// [Kind.String] handles it separately.
var kinds = [KindCount]kindInfo{
	KindCreate: {
		name: "create", slot: func(m *Mutation) any { return &m.Create },
		present: func(m Mutation) bool { return m.Create != nil },
		shard:   func(m Mutation) int32 { return m.Create.ShardID },
		rangeID: func(m Mutation) int64 { return m.Create.RangeID },
		events: func(m Mutation) [][]*p.InternalAppendHistoryNodesRequest {
			return [][]*p.InternalAppendHistoryNodesRequest{m.Create.NewWorkflowNewEvents}
		},
	},
	KindUpdate: {
		name: "update", slot: func(m *Mutation) any { return &m.Update },
		present: func(m Mutation) bool { return m.Update != nil },
		shard:   func(m Mutation) int32 { return m.Update.ShardID },
		rangeID: func(m Mutation) int64 { return m.Update.RangeID },
		events: func(m Mutation) [][]*p.InternalAppendHistoryNodesRequest {
			return [][]*p.InternalAppendHistoryNodesRequest{
				m.Update.UpdateWorkflowNewEvents,
				m.Update.NewWorkflowNewEvents,
			}
		},
	},
	KindConflictResolve: {
		name: "conflict-resolve", slot: func(m *Mutation) any { return &m.ConflictResolve },
		present: func(m Mutation) bool { return m.ConflictResolve != nil },
		shard:   func(m Mutation) int32 { return m.ConflictResolve.ShardID },
		rangeID: func(m Mutation) int64 { return m.ConflictResolve.RangeID },
		events: func(m Mutation) [][]*p.InternalAppendHistoryNodesRequest {
			return [][]*p.InternalAppendHistoryNodesRequest{
				m.ConflictResolve.CurrentWorkflowEventsNewEvents,
				m.ConflictResolve.ResetWorkflowEventsNewEvents,
				m.ConflictResolve.NewWorkflowEventsNewEvents,
			}
		},
	},
	KindSet: {
		name: "set", slot: func(m *Mutation) any { return &m.Set },
		present: func(m Mutation) bool { return m.Set != nil },
		shard:   func(m Mutation) int32 { return m.Set.ShardID },
		rangeID: func(m Mutation) int64 { return m.Set.RangeID },
		events:  noEvents,
	},
	KindDelete: {
		name: "delete", slot: func(m *Mutation) any { return &m.Delete },
		present: func(m Mutation) bool { return m.Delete != nil },
		shard:   func(m Mutation) int32 { return m.Delete.ShardID },
		rangeID: noRangeID,
		events:  noEvents,
	},
	KindDeleteCurrent: {
		name: "delete-current", slot: func(m *Mutation) any { return &m.DeleteCurrent },
		present: func(m Mutation) bool { return m.DeleteCurrent != nil },
		shard:   func(m Mutation) int32 { return m.DeleteCurrent.ShardID },
		rangeID: noRangeID,
		events:  noEvents,
	},
	KindAddTasks: {
		name: "add-tasks", slot: func(m *Mutation) any { return &m.AddTasks },
		present: func(m Mutation) bool { return m.AddTasks != nil },
		shard:   func(m Mutation) int32 { return m.AddTasks.ShardID },
		rangeID: func(m Mutation) int64 { return m.AddTasks.RangeID },
		events:  noEvents,
	},
	KindRangeCompleteTasks: {
		name: "range-complete-tasks", slot: func(m *Mutation) any { return &m.RangeCompleteTasks },
		present: func(m Mutation) bool { return m.RangeCompleteTasks != nil },
		shard:   func(m Mutation) int32 { return m.RangeCompleteTasks.ShardID },
		rangeID: noRangeID,
		events:  noEvents,
	},
}
