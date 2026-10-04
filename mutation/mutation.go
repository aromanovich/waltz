// Package mutation is the WAL's record format: it turns one ExecutionStore
// write request into the opaque bytes a [wal.Entry] carries, and back.
//
// The [Payload] generated from mutation.proto is the record's specification.
// Encoding copies field by field, so a field upstream adds is silently
// omitted; the field-set test turns that into a named failure.
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

// Kind names the eight shapes of a mutation, derived from which request a
// [Mutation] holds, never stored.
type Kind int

// The kinds. KindInvalid means a [Mutation] holds no request or several.
// KindAddTasks and KindRangeCompleteTasks name no run and assert no condition.
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

// KindCount is one past the last kind; an array of this size covers every
// [Kind], including [KindInvalid].
const KindCount = int(KindRangeCompleteTasks) + 1

// String returns the kind's name; anything outside the enumeration, and
// [KindInvalid], is "invalid".
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
	// AddTasks and RangeCompleteTasks both go through the log so they apply in
	// issue order. Otherwise an immediate range delete misses deferred rows, or
	// a deferred one deletes (by fire time) a timer added after it.
	AddTasks           *p.InternalAddHistoryTasksRequest
	RangeCompleteTasks *p.RangeCompleteHistoryTasksRequest
}

// ErrNotExactlyOneRequest is wrapped by every caller that switches on [Kind]
// and is handed a [KindInvalid] mutation.
var ErrNotExactlyOneRequest = errors.New("a mutation holds exactly one request")

// Kind reports which request the mutation holds, or [KindInvalid] if it holds
// none or several.
func (m Mutation) Kind() Kind {
	// Hand-written, not a table walk: this is the layer's hottest function.
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
// It is the routing key: one shard is one log and one apply transaction.
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

// RangeID reports the epoch the caller wrote the request under, or 0 for
// [KindInvalid] and for the two tombstones and the range delete (fenced by the
// drain's CAS instead). It is never in the payload: the epoch travels with the
// entry (I11), and a copy could disagree.
func (m Mutation) RangeID() int64 {
	kind := m.Kind()
	if kind == KindInvalid {
		return 0
	}
	return kinds[kind].rangeID(m)
}

// Part names which slot of a request carries one run's row state. A
// conflict-resolve has up to three; the history-task kinds and tombstones none.
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

// TaskSlot returns a pointer to one part's history-task map in the request
// (callers write through it), or nil if the request has no such part.
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

// TaskSlots returns every history-task map the mutation carries, in [parts]
// order (callers concatenate them, so the order is contract). A KindAddTasks
// request's rows belong to no run and are not returned.
func (m Mutation) TaskSlots() []*map[tasks.Category][]p.InternalHistoryTask {
	out := make([]*map[tasks.Category][]p.InternalHistoryTask, 0, len(parts))
	for _, part := range parts {
		if slot := m.TaskSlot(part); slot != nil {
			out = append(out, slot)
		}
	}
	return out
}

// EventSlots returns the request's slices of new history events, in store
// order; nil if the kind has none.
//
// A mutation acked over history rows nobody wrote can never be applied, and no
// functional suite would notice. So either the wrapper writes them through the
// store first and strips them off, or they stay on the mutation and a
// cold.HistoryApplier writes them in the drain's transaction.
func (m Mutation) EventSlots() [][]*p.InternalAppendHistoryNodesRequest {
	kind := m.Kind()
	if kind == KindInvalid {
		return nil
	}
	return kinds[kind].events(m)
}

// ClearEvents drops the request's event batches in place, once they are
// written through the store, so a mutation carries exactly the unwritten
// batches and [Encode] and fold need no mode. It modifies the caller's
// request, which the layer owns (wrapper.ShardWriter.Write).
func (m Mutation) ClearEvents() {
	switch m.Kind() {
	case KindCreate:
		m.Create.NewWorkflowNewEvents = nil
	case KindUpdate:
		m.Update.UpdateWorkflowNewEvents = nil
		m.Update.NewWorkflowNewEvents = nil
	case KindConflictResolve:
		m.ConflictResolve.CurrentWorkflowEventsNewEvents = nil
		m.ConflictResolve.ResetWorkflowEventsNewEvents = nil
		m.ConflictResolve.NewWorkflowEventsNewEvents = nil
	}
}

// ErrUnknownCategory is returned by [Decode] for a task category this process
// does not have. The caller must fail the replay, not skip the group.
var ErrUnknownCategory = errors.New("mutation: unknown task category id")

// ErrCassandraBlob is returned by [Encode] for a CHASM node with a
// Cassandra-encoded blob, which the mirror cannot carry.
var ErrCassandraBlob = errors.New("mutation: CHASM node carries a Cassandra blob")

// ErrUncarriedProto is returned by [Encode] when parsed execution info or
// state is set but its blob is absent. Only blobs are carried, so [Decode]
// would yield nil: a nil state panics every replaying owner, a nil info
// commits a row missing the field.
//
// Encode is the last chance to refuse; after the append the entry is acked
// and inherited, leaving a crash loop or a silent hole.
var ErrUncarriedProto = errors.New("mutation: parsed execution info or state with no blob carrying it")

// ErrBlobEncoding is returned by [Encode] for an execution info or state blob
// in an encoding [Decode] cannot parse. These are the only blobs the codec
// parses; all others are carried as opaque bytes.
//
// As with [ErrUncarriedProto], it must be refused before the append: an acked
// entry no owner can decode halts every owner on replay, and the shard never
// starts again.
var ErrBlobEncoding = errors.New("mutation: execution info or state blob in an encoding Decode cannot parse")

// Encode turns a mutation into a WAL entry's payload bytes, including any
// event batches it still holds (wrapper.ExecutionStore.appendEvents strips
// those already written). The same mutation always encodes to the same bytes,
// across processes too. It refuses batches with [ErrMalformedHistory].
func Encode(m Mutation) ([]byte, error) { return encode(m, false) }

// EncodeProvisional is [Encode] with the payload's provisional flag set, for an
// entry acked before its condition was verified. Replay uses the flag to tell
// an already-answered failure from a divergence.
func EncodeProvisional(m Mutation) ([]byte, error) { return encode(m, true) }

func encode(m Mutation, provisional bool) ([]byte, error) {
	kind := m.Kind()
	if kind == KindInvalid {
		return nil, fmt.Errorf("mutation: encode: %w", ErrNotExactlyOneRequest)
	}
	if err := validateHistory(m); err != nil {
		return nil, err
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
	// A missing arm and an arm that fills nothing would both be acked as an
	// entry with no request. The switch has no default so this catches both.
	if payload.Request == nil {
		return nil, fmt.Errorf("mutation: encode %s: no arm filled the payload: %w", kind, ErrNotExactlyOneRequest)
	}

	// No map fields yet, so Deterministic is free; it binds future fields.
	return proto.MarshalOptions{Deterministic: true}.Marshal(payload)
}

// Decode is [Encode]'s inverse. A decoded mutation's RangeID is zero (it is
// not in the payload). The registry must be the server's own task-category
// registry; it is the one input besides the bytes, so a payload can decode on
// one node and fail on another.
func Decode(payload []byte, registry tasks.TaskCategoryRegistry) (Mutation, error) {
	m, _, err := DecodeEntry(payload, registry)
	return m, err
}

// DecodeEntry is [Decode] plus whether the ack was provisional; on replay, a
// condition failure on a provisional entry is a drop, not a halt.
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
	// Protobuf tolerates unknown fields; a log must not, or a newer codec's
	// entry would replay silently missing data.
	if err := rejectUnknownFields(pb.ProtoReflect()); err != nil {
		return Mutation{}, false, err
	}

	prov := pb.Provisional
	m, err := decodeRequest(&pb, registry)
	if err != nil {
		return Mutation{}, false, err
	}
	// Validated again, not trusted: fold and the applier dereference them, and
	// the payload may not come from this build.
	if err := validateHistory(m); err != nil {
		return Mutation{}, false, err
	}
	return m, prov, nil
}

func decodeRequest(pb *Payload, registry tasks.TaskCategoryRegistry) (Mutation, error) {
	switch r := pb.Request.(type) {
	case *Payload_Create:
		req, err := decodeCreate(r.Create, registry)
		return Mutation{Create: req}, err
	case *Payload_Update:
		req, err := decodeUpdate(r.Update, registry)
		return Mutation{Update: req}, err
	case *Payload_ConflictResolve:
		req, err := decodeConflictResolve(r.ConflictResolve, registry)
		return Mutation{ConflictResolve: req}, err
	case *Payload_Set:
		req, err := decodeSet(r.Set, registry)
		return Mutation{Set: req}, err
	case *Payload_Delete:
		return Mutation{Delete: &p.DeleteWorkflowExecutionRequest{
			ShardID:     r.Delete.ShardId,
			NamespaceID: r.Delete.NamespaceId,
			WorkflowID:  r.Delete.WorkflowId,
			RunID:       r.Delete.RunId,
		}}, nil
	case *Payload_DeleteCurrent:
		return Mutation{DeleteCurrent: &p.DeleteCurrentWorkflowExecutionRequest{
			ShardID:     r.DeleteCurrent.ShardId,
			NamespaceID: r.DeleteCurrent.NamespaceId,
			WorkflowID:  r.DeleteCurrent.WorkflowId,
			RunID:       r.DeleteCurrent.RunId,
		}}, nil
	case *Payload_AddTasks:
		groups, err := decodeTasks(r.AddTasks.Tasks, registry)
		if err != nil {
			return Mutation{}, err
		}
		return Mutation{AddTasks: &p.InternalAddHistoryTasksRequest{
			ShardID:     r.AddTasks.ShardId,
			NamespaceID: r.AddTasks.NamespaceId,
			WorkflowID:  r.AddTasks.WorkflowId,
			Tasks:       groups,
		}}, nil
	case *Payload_RangeCompleteTasks:
		req := r.RangeCompleteTasks
		category, ok := registry.GetCategoryByID(int(req.CategoryId))
		if !ok {
			return Mutation{}, fmt.Errorf("%w: %d", ErrUnknownCategory, req.CategoryId)
		}
		return Mutation{RangeCompleteTasks: &p.RangeCompleteHistoryTasksRequest{
			ShardID:             req.ShardId,
			TaskCategory:        category,
			InclusiveMinTaskKey: decodeTaskKey(req.InclusiveMin),
			ExclusiveMaxTaskKey: decodeTaskKey(req.ExclusiveMax),
		}}, nil
	default:
		return Mutation{}, errors.New("mutation: decode: payload holds no request")
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
		// Map fields are not walked; adding one leaves a hole here.
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
