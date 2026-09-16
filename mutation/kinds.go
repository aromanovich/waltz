package mutation

import p "go.temporal.io/server/common/persistence"

// kindInfo is one row of the kind enumeration: the bookkeeping facts about a
// [Kind], not its behaviour, which stays in the per-kind switches in fold,
// check and apply. [Mutation.Kind] and [Mutation.ShardID] stay hand-written
// fan-outs; every mutation field must have exactly one row that agrees with
// both. The last two columns are read at run time —
// [Mutation.RangeID] and [Mutation.EventSlots] are the table — so a kind added
// without them fails by name in the kind guard rather than fencing against zero
// or writing no events.
//
// History-task maps are deliberately not a column beside events: they are keyed
// by [Part] as well as by kind, and which parts a request carries is a fact
// about the instance rather than about the kind — a continue-as-new brings a
// second one — so the column would hold a switch per row instead of a fact per
// row. [Mutation.TaskSlot] keys on part first for that reason, and
// every new kind must name its slots through [Mutation.TaskSlot].
type kindInfo struct {
	name string // what [Kind.String] returns

	// slot is the [Mutation] field the kind's request travels in, as a selector
	// rather than a name so that a renamed field breaks the row at compile
	// time. The guard recovers the name from the pointer, by address.
	slot func(*Mutation) any

	present func(Mutation) bool  // whether that field is set
	shard   func(Mutation) int32 // the shard the request names, read from that field

	// rangeID is the epoch the caller wrote under, read off the request; 0 for
	// the kinds whose request carries no such field.
	rangeID func(Mutation) int64

	// events names the request's slices of new history events, in the order
	// they must reach the store. Nothing carries them into the payload (D3), so
	// this row is the only enumeration of them, and a kind whose row omits one
	// is a mutation acked over history nodes nobody wrote.
	events func(Mutation) [][]*p.InternalAppendHistoryNodesRequest
}

// The rows for a request whose type has no such field. Named rather than
// written per row, so that a nil column stays a missing decision.
func noRangeID(Mutation) int64                                   { return 0 }
func noEvents(Mutation) [][]*p.InternalAppendHistoryNodesRequest { return nil }

// kinds is the enumeration, indexed by [Kind]. [KindInvalid]'s row is the zero
// value; [Kind.String] answers for it out of band.
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
