// Package mutation is the WAL's record format: it turns one ExecutionStore
// write request into the opaque bytes a [wal.Entry] carries, and back.
//
// The [Payload] generated from mutation.proto is the record's specification.
// Encoding fills that mirror field by field rather than reflecting over
// Temporal's structs, so a field upstream adds is one this package silently
// omits; the field-set test is what makes that a named failure instead of a
// lost column.
package mutation

import (
	"errors"
	"fmt"

	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// formatVersion is written into every payload; [Decode] rejects any other
// value.
const formatVersion = 1

// Kind names the eight shapes a mutation comes in. It is derived from which
// request a [Mutation] holds, never stored alongside it.
type Kind int

// The kinds. KindInvalid is what a [Mutation] holding no request, or more than
// one, reports. KindAddTasks and KindRangeCompleteTasks name no run and assert
// no condition, so neither can fail one.
const (
	KindInvalid Kind = iota
	KindCreate
	KindUpdate
	KindConflictResolve
	KindSet
	KindDelete
	KindDeleteCurrent
	KindAddTasks
	KindRangeCompleteTasks
)

// KindCount is one past the last kind, so an array indexed by [Kind] covers
// every one including [KindInvalid].
const KindCount = int(KindRangeCompleteTasks) + 1

// String returns the kind's name from the table in kinds.go. Anything outside
// the enumeration, [KindInvalid] included, is "invalid".
func (k Kind) String() string {
	if k <= KindInvalid || int(k) >= KindCount {
		return "invalid"
	}
	return kinds[k].name
}

// Mutation is one ExecutionStore-level write request and the unit of
// atomicity: one mutation is one WAL entry (I1). Exactly one field is set.
type Mutation struct {
	Create          *p.InternalCreateWorkflowExecutionRequest
	Update          *p.InternalUpdateWorkflowExecutionRequest
	ConflictResolve *p.InternalConflictResolveWorkflowExecutionRequest
	Set             *p.InternalSetWorkflowExecutionRequest
	Delete          *p.DeleteWorkflowExecutionRequest
	DeleteCurrent   *p.DeleteCurrentWorkflowExecutionRequest
	// AddTasks and RangeCompleteTasks travel through the log so that both take
	// effect in the order issued. An immediate range delete beside a deferred
	// add would run before the rows it should have covered existed, losing a
	// scheduled category's timer outright.
	AddTasks           *p.InternalAddHistoryTasksRequest
	RangeCompleteTasks *p.RangeCompleteHistoryTasksRequest
}

// ErrNotExactlyOneRequest is what a caller fanning out over [Kind] returns when
// handed a mutation reporting [KindInvalid]. Every such caller wraps this one
// error, so a single errors.Is matches them all.
var ErrNotExactlyOneRequest = errors.New("a mutation holds exactly one request")

// Kind reports which request the mutation holds, or [KindInvalid] if it holds
// none or several.
func (m Mutation) Kind() Kind {
	// Hand-written rather than a walk of the kinds table because this is the
	// most-called function in the layer.
	kind, n := KindInvalid, 0
	set := func(k Kind, present bool) {
		if present {
			kind, n = k, n+1
		}
	}
	set(KindCreate, m.Create != nil)
	set(KindUpdate, m.Update != nil)
	set(KindConflictResolve, m.ConflictResolve != nil)
	set(KindSet, m.Set != nil)
	set(KindDelete, m.Delete != nil)
	set(KindDeleteCurrent, m.DeleteCurrent != nil)
	set(KindAddTasks, m.AddTasks != nil)
	set(KindRangeCompleteTasks, m.RangeCompleteTasks != nil)
	if n != 1 {
		return KindInvalid
	}
	return kind
}

// ShardID reports the shard the mutation belongs to, or 0 for [KindInvalid].
// One shard is one log and one apply transaction, so this is the routing key.
func (m Mutation) ShardID() int32 {
	switch m.Kind() {
	case KindCreate:
		return m.Create.ShardID
	case KindUpdate:
		return m.Update.ShardID
	case KindConflictResolve:
		return m.ConflictResolve.ShardID
	case KindSet:
		return m.Set.ShardID
	case KindDelete:
		return m.Delete.ShardID
	case KindDeleteCurrent:
		return m.DeleteCurrent.ShardID
	case KindAddTasks:
		return m.AddTasks.ShardID
	case KindRangeCompleteTasks:
		return m.RangeCompleteTasks.ShardID
	default:
		return 0
	}
}

// RangeID reports the epoch the caller wrote the request under, or 0 both for
// [KindInvalid] and for the three kinds whose request carries no rangeID — the
// two tombstones and the range delete, which the drain's own CAS fences
// instead. It is read off the request and never carried in the payload: the
// rangeID is the epoch (I11), it travels with the entry, and a copy inside the
// record could disagree with it.
func (m Mutation) RangeID() int64 {
	kind := m.Kind()
	if kind == KindInvalid {
		return 0
	}
	return kinds[kind].rangeID(m)
}

// Part names which slot of a request carries one run's row state. A part is a
// fact about the request's shape: a conflict-resolve has up to three, the two
// history-task kinds and the two tombstones none.
type Part int

const (
	// PartSnapshot is the request's own snapshot.
	PartSnapshot Part = iota + 1
	// PartNewSnapshot is the continued-as-new or newly created run.
	PartNewSnapshot
	// PartMutation is the update applied to an existing run.
	PartMutation
)

// parts is the order [Mutation.TaskSlots] enumerates in.
var parts = [...]Part{PartSnapshot, PartNewSnapshot, PartMutation}

// TaskSlot returns the history-task map of one part of the request, or nil
// where the request has no such part. The pointer aliases the map's home in the
// request, and callers such as a range delete write through it.
func (m Mutation) TaskSlot(part Part) *map[tasks.Category][]p.InternalHistoryTask {
	switch part {
	case PartSnapshot:
		switch {
		case m.Create != nil:
			return &m.Create.NewWorkflowSnapshot.Tasks
		case m.Set != nil:
			return &m.Set.SetWorkflowSnapshot.Tasks
		case m.ConflictResolve != nil:
			return &m.ConflictResolve.ResetWorkflowSnapshot.Tasks
		}
	case PartNewSnapshot:
		switch {
		case m.Update != nil && m.Update.NewWorkflowSnapshot != nil:
			return &m.Update.NewWorkflowSnapshot.Tasks
		case m.ConflictResolve != nil && m.ConflictResolve.NewWorkflowSnapshot != nil:
			return &m.ConflictResolve.NewWorkflowSnapshot.Tasks
		}
	case PartMutation:
		switch {
		case m.Update != nil:
			return &m.Update.UpdateWorkflowMutation.Tasks
		case m.ConflictResolve != nil && m.ConflictResolve.CurrentWorkflowMutation != nil:
			return &m.ConflictResolve.CurrentWorkflowMutation.Tasks
		}
	}
	return nil
}

// TaskSlots is every history-task map the mutation carries, in [parts] order,
// skipping the parts the request does not have; callers concatenate the slots,
// so the order is part of the contract. A KindAddTasks request's own rows
// belong to no run and are not returned.
func (m Mutation) TaskSlots() []*map[tasks.Category][]p.InternalHistoryTask {
	out := make([]*map[tasks.Category][]p.InternalHistoryTask, 0, len(parts))
	for _, part := range parts {
		if slot := m.TaskSlot(part); slot != nil {
			out = append(out, slot)
		}
	}
	return out
}

// EventSlots is every slice of new history events the request carries, in the
// order they must reach the store, and nil for a kind whose request carries
// none. The payload drops these (D3), so whoever writes a mutation writes them
// first: a mutation acked over history nodes nobody wrote is a mutable state
// the cold store can never be brought to, and no functional suite sees it.
func (m Mutation) EventSlots() [][]*p.InternalAppendHistoryNodesRequest {
	kind := m.Kind()
	if kind == KindInvalid {
		return nil
	}
	return kinds[kind].events(m)
}

// ErrUnknownCategory is what [Decode] returns when an entry names a task
// category this process does not have. The caller must fail the replay rather
// than skip the group, whose tasks would otherwise be dropped silently.
var ErrUnknownCategory = errors.New("mutation: unknown task category id")

// ErrCassandraBlob is what [Encode] returns for a CHASM node carrying a
// Cassandra-encoded blob: the mirror has no home for it, so encoding would lose
// state.
var ErrCassandraBlob = errors.New("mutation: CHASM node carries a Cassandra blob")

// ErrUncarriedProto is what [Encode] returns for a request whose parsed
// execution info or state is set while the blob that proto is derived from is
// absent. Only the blob is carried, so such a request encodes to one [Decode]
// answers with a nil struct — which is not a record of what the caller handed
// over, in either of two ways: the fold dereferences the state, and the applier
// writes the info's blob, so one shape panics every owner that replays the entry
// and the other commits a row with the field missing.
//
// It is refused here because this is the last place that can refuse. Past the
// append the entry is acked and every owner inherits it, so the choice after
// that is a crash loop or a silent hole; before it, refusing writes nothing.
var ErrUncarriedProto = errors.New("mutation: parsed execution info or state with no blob carrying it")

// Encode turns a mutation into the bytes of a WAL entry's payload. The same
// mutation always encodes to the same bytes, across processes as well.
func Encode(m Mutation) ([]byte, error) { return encode(m, false) }

// EncodeProvisional is [Encode] for an entry acked before its condition was
// verified, so that replay can tell an answer somebody already got from a
// divergence. It is the payload's `provisional` flag, and nothing else about
// the record differs.
func EncodeProvisional(m Mutation) ([]byte, error) { return encode(m, true) }

func encode(m Mutation, provisional bool) ([]byte, error) {
	kind := m.Kind()
	if kind == KindInvalid {
		return nil, fmt.Errorf("mutation: encode: %w", ErrNotExactlyOneRequest)
	}

	payload := &Payload{Format: formatVersion, Provisional: provisional}
	var err error
	switch kind {
	case KindCreate:
		var r *CreateRequest
		r, err = encodeCreate(m.Create)
		payload.Request = &Payload_Create{Create: r}
	case KindUpdate:
		var r *UpdateRequest
		r, err = encodeUpdate(m.Update)
		payload.Request = &Payload_Update{Update: r}
	case KindConflictResolve:
		var r *ConflictResolveRequest
		r, err = encodeConflictResolve(m.ConflictResolve)
		payload.Request = &Payload_ConflictResolve{ConflictResolve: r}
	case KindSet:
		var r *SetRequest
		r, err = encodeSet(m.Set)
		payload.Request = &Payload_Set{Set: r}
	case KindDelete:
		payload.Request = &Payload_Delete{Delete: &DeleteRequest{
			ShardId:     m.Delete.ShardID,
			NamespaceId: m.Delete.NamespaceID,
			WorkflowId:  m.Delete.WorkflowID,
			RunId:       m.Delete.RunID,
		}}
	case KindDeleteCurrent:
		payload.Request = &Payload_DeleteCurrent{DeleteCurrent: &DeleteRequest{
			ShardId:     m.DeleteCurrent.ShardID,
			NamespaceId: m.DeleteCurrent.NamespaceID,
			WorkflowId:  m.DeleteCurrent.WorkflowID,
			RunId:       m.DeleteCurrent.RunID,
		}}
	case KindAddTasks:
		payload.Request = &Payload_AddTasks{AddTasks: &AddTasksRequest{
			ShardId:     m.AddTasks.ShardID,
			NamespaceId: m.AddTasks.NamespaceID,
			WorkflowId:  m.AddTasks.WorkflowID,
			Tasks:       encodeTasks(m.AddTasks.Tasks),
		}}
	case KindRangeCompleteTasks:
		r := m.RangeCompleteTasks
		payload.Request = &Payload_RangeCompleteTasks{RangeCompleteTasks: &RangeCompleteTasksRequest{
			ShardId:      r.ShardID,
			CategoryId:   int32(r.TaskCategory.ID()),
			InclusiveMin: encodeTaskKey(r.InclusiveMinTaskKey),
			ExclusiveMax: encodeTaskKey(r.ExclusiveMaxTaskKey),
		}}
	}
	if err != nil {
		return nil, fmt.Errorf("mutation: encode %s: %w", kind, err)
	}
	// A kind with no arm, and an arm that runs and fills nothing, are the same
	// entry: it marshals, it is appended, the caller is acked, and replay finds
	// no request in it. The switch is left without a default so that both fail
	// here rather than only the first.
	if payload.Request == nil {
		return nil, fmt.Errorf("mutation: encode %s: no arm filled the payload: %w", kind, ErrNotExactlyOneRequest)
	}

	// The format has no map fields, so Deterministic costs nothing here and
	// states what any field added later must satisfy.
	return proto.MarshalOptions{Deterministic: true}.Marshal(payload)
}

// Decode is [Encode]'s inverse over what the payload carries: neither the
// rangeID nor the new-events slices are in it, so a decoded mutation reports a
// zero rangeID and carries no events. The registry must be the server's own
// task-category registry: it is the one input that is not a function of the
// bytes, so the same payload decodes on one node and fails on another.
func Decode(payload []byte, registry tasks.TaskCategoryRegistry) (Mutation, error) {
	m, _, err := DecodeEntry(payload, registry)
	return m, err
}

// DecodeEntry is [Decode] plus whether the entry's ack was provisional, which
// tells replay that a condition failure on it is a drop rather than a halt.
func DecodeEntry(payload []byte, registry tasks.TaskCategoryRegistry) (Mutation, bool, error) {
	if registry == nil {
		return Mutation{}, false, errors.New("mutation: decode: no task category registry")
	}

	var pb Payload
	if err := proto.Unmarshal(payload, &pb); err != nil {
		return Mutation{}, false, fmt.Errorf("mutation: decode: %w", err)
	}
	if pb.Format != formatVersion {
		return Mutation{}, false, fmt.Errorf("mutation: decode: format %d, this build writes %d", pb.Format, formatVersion)
	}
	// Protobuf tolerates unknown fields; a log must not. An entry written by a
	// newer codec would otherwise replay silently short a piece.
	if err := rejectUnknownFields(pb.ProtoReflect()); err != nil {
		return Mutation{}, false, err
	}

	prov := pb.Provisional
	switch r := pb.Request.(type) {
	case *Payload_Create:
		req, err := decodeCreate(r.Create, registry)
		return Mutation{Create: req}, prov, err
	case *Payload_Update:
		req, err := decodeUpdate(r.Update, registry)
		return Mutation{Update: req}, prov, err
	case *Payload_ConflictResolve:
		req, err := decodeConflictResolve(r.ConflictResolve, registry)
		return Mutation{ConflictResolve: req}, prov, err
	case *Payload_Set:
		req, err := decodeSet(r.Set, registry)
		return Mutation{Set: req}, prov, err
	case *Payload_Delete:
		return Mutation{Delete: &p.DeleteWorkflowExecutionRequest{
			ShardID:     r.Delete.ShardId,
			NamespaceID: r.Delete.NamespaceId,
			WorkflowID:  r.Delete.WorkflowId,
			RunID:       r.Delete.RunId,
		}}, prov, nil
	case *Payload_DeleteCurrent:
		return Mutation{DeleteCurrent: &p.DeleteCurrentWorkflowExecutionRequest{
			ShardID:     r.DeleteCurrent.ShardId,
			NamespaceID: r.DeleteCurrent.NamespaceId,
			WorkflowID:  r.DeleteCurrent.WorkflowId,
			RunID:       r.DeleteCurrent.RunId,
		}}, prov, nil
	case *Payload_AddTasks:
		groups, err := decodeTasks(r.AddTasks.Tasks, registry)
		if err != nil {
			return Mutation{}, prov, err
		}
		return Mutation{AddTasks: &p.InternalAddHistoryTasksRequest{
			ShardID:     r.AddTasks.ShardId,
			NamespaceID: r.AddTasks.NamespaceId,
			WorkflowID:  r.AddTasks.WorkflowId,
			Tasks:       groups,
		}}, prov, nil
	case *Payload_RangeCompleteTasks:
		req := r.RangeCompleteTasks
		category, ok := registry.GetCategoryByID(int(req.CategoryId))
		if !ok {
			return Mutation{}, prov, fmt.Errorf("%w: %d", ErrUnknownCategory, req.CategoryId)
		}
		return Mutation{RangeCompleteTasks: &p.RangeCompleteHistoryTasksRequest{
			ShardID:             req.ShardId,
			TaskCategory:        category,
			InclusiveMinTaskKey: decodeTaskKey(req.InclusiveMin),
			ExclusiveMaxTaskKey: decodeTaskKey(req.ExclusiveMax),
		}}, prov, nil
	default:
		return Mutation{}, false, errors.New("mutation: decode: payload holds no request")
	}
}

// rejectUnknownFields fails on the first message in the tree carrying a field
// this build does not know.
func rejectUnknownFields(m protoreflect.Message) error {
	if unknown := m.GetUnknown(); len(unknown) > 0 {
		return fmt.Errorf("mutation: decode: %s carries unknown field(s) — written by a newer codec?",
			m.Descriptor().FullName())
	}
	var err error
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		// The format has no map fields; one added later is a hole here.
		if fd.Kind() != protoreflect.MessageKind || fd.IsMap() {
			return true
		}
		if fd.IsList() {
			list := v.List()
			for i := range list.Len() {
				if err = rejectUnknownFields(list.Get(i).Message()); err != nil {
					return false
				}
			}
			return true
		}
		err = rejectUnknownFields(v.Message())
		return err == nil
	})
	return err
}
