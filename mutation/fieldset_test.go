package mutation

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
)

// The field-set guard. The codec is a hand-filled mirror, so a field Temporal
// adds is state the WAL silently stops carrying; this fails at the Temporal
// bump instead. Recording the field is never the fix on its own: decide whether
// it is carried, derived or dropped, implement it in encode.go/decode.go, then
// record the decision here.

type disposition int

const (
	// carried: the field travels in the WAL payload.
	carried disposition = iota
	// derived: absent from the payload, reconstructed from a field that is.
	derived
	// dropped: not carried at all.
	dropped
)

type fieldDecision struct {
	field string // "Name Type", exactly as reflect renders it
	how   disposition
	why   string // required unless carried
}

type mirroredStruct struct {
	name   string
	typ    reflect.Type
	fields []fieldDecision
}

const (
	whyEpoch   = "RangeID is the epoch (I11); it travels with the WAL entry, and a copy could disagree with it"
	whyHistory = "event history stays out of the WAL in v1 (D3); it is written by AppendHistoryNodes before the append"
	whyBlob    = "derived from the blob, which is what the store writes verbatim; carrying the proto would make the stored bytes a re-marshal"
	// The map keyed by the same type is carried: its keys travel as ids beside
	// the rows they key, so the map comes back whole. A bare category has no
	// such row, and one payload decoding on one node and failing on another
	// (ErrUnknownCategory) is that difference.
	whyCategory = "the id travels and Decode rebuilds the category from this process's own registry"
)

var mirroredStructs = []mirroredStruct{
	{
		name: "InternalWorkflowMutation",
		typ:  reflect.TypeFor[p.InternalWorkflowMutation](),
		fields: []fieldDecision{
			{"NamespaceID string", carried, ""},
			{"WorkflowID string", carried, ""},
			{"RunID string", carried, ""},
			{"ExecutionInfo *persistence.WorkflowExecutionInfo", derived, whyBlob},
			{"ExecutionInfoBlob *common.DataBlob", carried, ""},
			{"ExecutionState *persistence.WorkflowExecutionState", derived, whyBlob},
			{"ExecutionStateBlob *common.DataBlob", carried, ""},
			{"NextEventID int64", carried, ""},
			{"StartVersion int64", carried, ""},
			{"LastWriteVersion int64", carried, ""},
			{"DBRecordVersion int64", carried, ""},
			{"UpsertActivityInfos map[int64]*common.DataBlob", carried, ""},
			{"DeleteActivityInfos map[int64]struct {}", carried, ""},
			{"UpsertTimerInfos map[string]*common.DataBlob", carried, ""},
			{"DeleteTimerInfos map[string]struct {}", carried, ""},
			{"UpsertChildExecutionInfos map[int64]*common.DataBlob", carried, ""},
			{"DeleteChildExecutionInfos map[int64]struct {}", carried, ""},
			{"UpsertRequestCancelInfos map[int64]*common.DataBlob", carried, ""},
			{"DeleteRequestCancelInfos map[int64]struct {}", carried, ""},
			{"UpsertSignalInfos map[int64]*common.DataBlob", carried, ""},
			{"DeleteSignalInfos map[int64]struct {}", carried, ""},
			{"UpsertChasmNodes map[string]persistence.InternalChasmNode", carried, ""},
			{"DeleteChasmNodes map[string]struct {}", carried, ""},
			{"UpsertSignalRequestedIDs map[string]struct {}", carried, ""},
			{"DeleteSignalRequestedIDs map[string]struct {}", carried, ""},
			{"NewBufferedEvents *common.DataBlob", carried, ""},
			{"ClearBufferedEvents bool", carried, ""},
			{"Tasks map[tasks.Category][]persistence.InternalHistoryTask", carried, ""},
			{"Condition int64", carried, ""},
			{"Checksum *common.DataBlob", carried, ""},
		},
	},
	{
		name: "InternalWorkflowSnapshot",
		typ:  reflect.TypeFor[p.InternalWorkflowSnapshot](),
		fields: []fieldDecision{
			{"NamespaceID string", carried, ""},
			{"WorkflowID string", carried, ""},
			{"RunID string", carried, ""},
			{"ExecutionInfo *persistence.WorkflowExecutionInfo", derived, whyBlob},
			{"ExecutionInfoBlob *common.DataBlob", carried, ""},
			{"ExecutionState *persistence.WorkflowExecutionState", derived, whyBlob},
			{"ExecutionStateBlob *common.DataBlob", carried, ""},
			{"StartVersion int64", carried, ""},
			{"LastWriteVersion int64", carried, ""},
			{"NextEventID int64", carried, ""},
			{"DBRecordVersion int64", carried, ""},
			{"ActivityInfos map[int64]*common.DataBlob", carried, ""},
			{"TimerInfos map[string]*common.DataBlob", carried, ""},
			{"ChildExecutionInfos map[int64]*common.DataBlob", carried, ""},
			{"RequestCancelInfos map[int64]*common.DataBlob", carried, ""},
			{"SignalInfos map[int64]*common.DataBlob", carried, ""},
			{"ChasmNodes map[string]persistence.InternalChasmNode", carried, ""},
			{"SignalRequestedIDs map[string]struct {}", carried, ""},
			{"Tasks map[tasks.Category][]persistence.InternalHistoryTask", carried, ""},
			{"Condition int64", carried, ""},
			{"Checksum *common.DataBlob", carried, ""},
		},
	},
	{
		name: "InternalChasmNode",
		typ:  reflect.TypeFor[p.InternalChasmNode](),
		fields: []fieldDecision{
			{"Metadata *common.DataBlob", carried, ""},
			{"Data *common.DataBlob", carried, ""},
			{"CassandraBlob *common.DataBlob", dropped,
				"set only when Cassandra is the persistence layer, so it cannot occur here — Encode refuses a node that has one rather than dropping state quietly"},
		},
	},
	{
		name: "InternalHistoryTask",
		typ:  reflect.TypeFor[p.InternalHistoryTask](),
		fields: []fieldDecision{
			{"Key tasks.Key", carried, ""},
			{"Blob *common.DataBlob", carried, ""},
		},
	},
	{
		name: "tasks.Key",
		typ:  reflect.TypeFor[tasks.Key](),
		fields: []fieldDecision{
			{"FireTime time.Time", carried, ""},
			{"TaskID int64", carried, ""},
		},
	},
	{
		name: "InternalCreateWorkflowExecutionRequest",
		typ:  reflect.TypeFor[p.InternalCreateWorkflowExecutionRequest](),
		fields: []fieldDecision{
			{"ShardID int32", carried, ""},
			{"RangeID int64", dropped, whyEpoch},
			{"Mode persistence.CreateWorkflowMode", carried, ""},
			{"PreviousRunID string", carried, ""},
			{"PreviousLastWriteVersion int64", carried, ""},
			{"NewWorkflowSnapshot persistence.InternalWorkflowSnapshot", carried, ""},
			{"NewWorkflowNewEvents []*persistence.InternalAppendHistoryNodesRequest", dropped, whyHistory},
		},
	},
	{
		name: "InternalUpdateWorkflowExecutionRequest",
		typ:  reflect.TypeFor[p.InternalUpdateWorkflowExecutionRequest](),
		fields: []fieldDecision{
			{"ShardID int32", carried, ""},
			{"RangeID int64", dropped, whyEpoch},
			{"Mode persistence.UpdateWorkflowMode", carried, ""},
			{"UpdateWorkflowMutation persistence.InternalWorkflowMutation", carried, ""},
			{"UpdateWorkflowNewEvents []*persistence.InternalAppendHistoryNodesRequest", dropped, whyHistory},
			{"NewWorkflowSnapshot *persistence.InternalWorkflowSnapshot", carried, ""},
			{"NewWorkflowNewEvents []*persistence.InternalAppendHistoryNodesRequest", dropped, whyHistory},
		},
	},
	{
		name: "InternalConflictResolveWorkflowExecutionRequest",
		typ:  reflect.TypeFor[p.InternalConflictResolveWorkflowExecutionRequest](),
		fields: []fieldDecision{
			{"ShardID int32", carried, ""},
			{"RangeID int64", dropped, whyEpoch},
			{"Mode persistence.ConflictResolveWorkflowMode", carried, ""},
			{"ResetWorkflowSnapshot persistence.InternalWorkflowSnapshot", carried, ""},
			{"ResetWorkflowEventsNewEvents []*persistence.InternalAppendHistoryNodesRequest", dropped, whyHistory},
			{"NewWorkflowSnapshot *persistence.InternalWorkflowSnapshot", carried, ""},
			{"NewWorkflowEventsNewEvents []*persistence.InternalAppendHistoryNodesRequest", dropped, whyHistory},
			{"CurrentWorkflowMutation *persistence.InternalWorkflowMutation", carried, ""},
			{"CurrentWorkflowEventsNewEvents []*persistence.InternalAppendHistoryNodesRequest", dropped, whyHistory},
		},
	},
	{
		name: "InternalSetWorkflowExecutionRequest",
		typ:  reflect.TypeFor[p.InternalSetWorkflowExecutionRequest](),
		fields: []fieldDecision{
			{"ShardID int32", carried, ""},
			{"RangeID int64", dropped, whyEpoch},
			{"SetWorkflowSnapshot persistence.InternalWorkflowSnapshot", carried, ""},
		},
	},
	{
		name: "DeleteWorkflowExecutionRequest",
		typ:  reflect.TypeFor[p.DeleteWorkflowExecutionRequest](),
		fields: []fieldDecision{
			{"ShardID int32", carried, ""},
			{"NamespaceID string", carried, ""},
			{"WorkflowID string", carried, ""},
			{"RunID string", carried, ""},
		},
	},
	{
		name: "DeleteCurrentWorkflowExecutionRequest",
		typ:  reflect.TypeFor[p.DeleteCurrentWorkflowExecutionRequest](),
		// The same four fields as the request above, and two kinds all the same:
		// a delete of the run and a delete of the current row are not one
		// decision.
		fields: []fieldDecision{
			{"ShardID int32", carried, ""},
			{"NamespaceID string", carried, ""},
			{"WorkflowID string", carried, ""},
			{"RunID string", carried, ""},
		},
	},
	{
		name: "InternalAddHistoryTasksRequest",
		typ:  reflect.TypeFor[p.InternalAddHistoryTasksRequest](),
		fields: []fieldDecision{
			{"ShardID int32", carried, ""},
			{"RangeID int64", dropped, whyEpoch},
			{"NamespaceID string", carried, ""},
			{"WorkflowID string", carried, ""},
			{"Tasks map[tasks.Category][]persistence.InternalHistoryTask", carried, ""},
		},
	},
	{
		name: "RangeCompleteHistoryTasksRequest",
		typ:  reflect.TypeFor[p.RangeCompleteHistoryTasksRequest](),
		fields: []fieldDecision{
			{"ShardID int32", carried, ""},
			{"TaskCategory tasks.Category", derived, whyCategory},
			{"InclusiveMinTaskKey tasks.Key", carried, ""},
			{"ExclusiveMaxTaskKey tasks.Key", carried, ""},
		},
	},
}

func TestFieldSetGuard(t *testing.T) {
	for _, s := range mirroredStructs {
		t.Run(s.name, func(t *testing.T) {
			actual := fieldSet(s.typ)
			recorded := make([]string, len(s.fields))
			for i, f := range s.fields {
				recorded[i] = f.field
			}

			if diff := describeFieldDiff(recorded, actual); diff != "" {
				t.Fatalf(`%s has changed.

%s
This is the guard doing its job: the codec is an explicit mirror, so a field it
does not know about is state the WAL silently stops carrying. Decide what the
change means for a log — carried, derived, or dropped with a reason — implement
it in encode.go/decode.go, then record the decision here.

Recording the field alone is never the fix.`, s.name, diff)
			}
		})
	}
}

// The guard's own guard: Temporal adding a field cannot be staged against a
// pinned dependency, so it is staged against the comparison instead.
func TestTheGuardCatchesAnUpgrade(t *testing.T) {
	recorded := fieldSet(reflect.TypeFor[p.InternalWorkflowMutation]())

	t.Run("a new sub-collection", func(t *testing.T) {
		upgraded := append(slices.Clone(recorded), "UpsertNexusInfos map[int64]*common.DataBlob")
		require.Contains(t, describeFieldDiff(recorded, upgraded), "new field, no decision recorded")
	})

	t.Run("a field that went away", func(t *testing.T) {
		require.Contains(t, describeFieldDiff(recorded, recorded[:len(recorded)-1]),
			"recorded field is gone upstream")
	})

	t.Run("a reordering", func(t *testing.T) {
		shuffled := slices.Clone(recorded)
		shuffled[0], shuffled[1] = shuffled[1], shuffled[0]
		require.Contains(t, describeFieldDiff(recorded, shuffled), "order changed")
	})

	t.Run("no change at all", func(t *testing.T) {
		require.Empty(t, describeFieldDiff(recorded, recorded))
	})
}

// Every field not carried must record why.
func TestEveryNonCarriedFieldSaysWhy(t *testing.T) {
	for _, s := range mirroredStructs {
		for _, f := range s.fields {
			if f.how != carried {
				require.NotEmpty(t, f.why, "%s.%s is not carried and gives no reason", s.name, f.field)
			}
		}
	}
}

// Which structs get walked is not remembered either: every kind's payload type
// is one, and kinds.go already names it through the row's slot. A request with
// no row would silently drop any field added to it. Only the
// forward direction is a claim: several rows are structs nested inside a
// request, which no kind's slot points at.
func TestEveryKindsRequestStructIsWalked(t *testing.T) {
	walked := make(map[reflect.Type]bool, len(mirroredStructs))
	for _, s := range mirroredStructs {
		walked[s.typ] = true
	}

	var probe Mutation
	for k := KindInvalid + 1; int(k) < KindCount; k++ {
		if kinds[k].slot == nil {
			// A kind with no slot is TestEveryKindIsDeclaredOnceAndReachesItsField's
			// failure, and it says which row points at no field. Dereferencing
			// here would panic the binary before that test runs.
			continue
		}
		// The slot is the address of a pointer field, so the first Elem is the
		// pointer and the second is the request.
		slot := reflect.TypeOf(kinds[k].slot(&probe)).Elem()
		require.Equalf(t, reflect.Pointer, slot.Kind(),
			"kind %s's slot is not a pointer to its request", k)
		require.Truef(t, walked[slot.Elem()],
			"kind %s travels in %s, and no row of mirroredStructs walks it: the codec is a "+
				"hand-filled mirror, so a request struct nothing walks is a field Temporal can "+
				"add and this package will silently stop carrying. Record carried, derived or "+
				"dropped for each of its fields.", k, slot.Elem())
	}
}

func fieldSet(t reflect.Type) []string {
	out := make([]string, 0, t.NumField())
	for f := range t.Fields() {
		out = append(out, fmt.Sprintf("%s %s", f.Name, f.Type))
	}
	return out
}

// describeFieldDiff reports which fields appeared, which vanished, and whether
// the order moved; the empty string when the two agree.
func describeFieldDiff(recorded, actual []string) string {
	inRecorded := make(map[string]bool, len(recorded))
	for _, f := range recorded {
		inRecorded[f] = true
	}
	inActual := make(map[string]bool, len(actual))
	for _, f := range actual {
		inActual[f] = true
	}

	var b strings.Builder
	for _, f := range actual {
		if !inRecorded[f] {
			fmt.Fprintf(&b, "  new field, no decision recorded:  %s\n", f)
		}
	}
	for _, f := range recorded {
		if !inActual[f] {
			fmt.Fprintf(&b, "  recorded field is gone upstream:  %s\n", f)
		}
	}
	if b.Len() == 0 && !slices.Equal(recorded, actual) {
		// Same names, different order: the recorded list is the struct's own
		// order, so the two can be read side by side.
		fmt.Fprintf(&b, "  the fields are the same but their order changed\n")
	}
	return b.String()
}
