package checker

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	"go.temporal.io/api/serviceerror"
	p "go.temporal.io/server/common/persistence"

	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/wal"
)

// The driver's record: what a node asked the store for, and what it learned.
//
// Each call is two fsynced lines. A kill between them leaves a call with no
// outcome, meaning "the driver does not know". Recording only outcomes would
// make a killed call look never issued while its entry sits in the log. It is
// a file because it must survive the process it records dying.

// Line is one entry of the record and defines the file format; only
// [ReadRecord] parses it.
type Line struct {
	Seq         int64  `json:"seq"`
	Node        string `json:"node"`
	Incarnation int64  `json:"incarnation"`
	Event       string `json:"event"`

	Shard     int32  `json:"shard,omitempty"`
	Epoch     int64  `json:"epoch,omitempty"`
	Call      int64  `json:"call,omitempty"`
	Kind      string `json:"kind,omitempty"`
	Namespace string `json:"namespace,omitempty"`
	Workflow  string `json:"workflow,omitempty"`
	Run       string `json:"run,omitempty"`
	Effect    string `json:"effect,omitempty"`
	Outcome   string `json:"outcome,omitempty"`
	Detail    string `json:"detail,omitempty"`
}

// The events a record holds. Only call and outcome carry claims; fenced is
// narration for the harness, and no assertion rests on it.
const (
	EventCall    = "call"
	EventOutcome = "outcome"
	EventFenced  = "fenced"
)

// Record is one node's record file, appended and fsynced per line.
type Record struct {
	mu          sync.Mutex
	f           *os.File
	node        string
	incarnation int64
	seq         int64
}

// NewRecord opens a record for one node incarnation.
//
// Seq restarts at 1 per process, so a restarted node needs a new incarnation
// or its calls collide with the previous run's. Only the node's starter knows
// it is a restart, so it supplies the incarnation; incarnation 0 on a
// non-empty file is refused.
func NewRecord(path, node string, incarnation int64) (*Record, error) {
	if node == "" {
		return nil, errors.New("checker: a record needs the name of the node writing it")
	}
	if incarnation < 0 {
		return nil, fmt.Errorf("checker: an incarnation cannot be negative, got %d", incarnation)
	}
	if incarnation == 0 {
		if info, err := os.Stat(path); err == nil && info.Size() > 0 {
			return nil, fmt.Errorf("checker: %q already holds a run; a second one needs an "+
				"incarnation of its own, or its calls collide with the first's", path)
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &Record{f: f, node: node, incarnation: incarnation}, nil
}

// write appends and fsyncs one line, returning its seq: its id within this
// incarnation, which an outcome points back at.
func (r *Record) write(l Line) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seq++
	l.Seq = r.seq
	l.Node = r.node
	l.Incarnation = r.incarnation
	b, err := json.Marshal(l)
	if err != nil {
		return l.Seq, err
	}
	if _, err := r.f.Write(append(b, '\n')); err != nil {
		return l.Seq, err
	}
	return l.Seq, r.f.Sync()
}

// Call records a call before the driver makes it.
func (r *Record) Call(shard wal.ShardID, epoch wal.Epoch, m mutation.Mutation) (int64, error) {
	subject, effect, err := Identify(m)
	if err != nil {
		return 0, err
	}
	return r.write(Line{
		Event:     EventCall,
		Shard:     int32(shard),
		Epoch:     int64(epoch),
		Kind:      m.Kind().String(),
		Namespace: subject.Namespace,
		Workflow:  subject.Workflow,
		Run:       subject.Run,
		Effect:    effect.String(),
	})
}

// Outcome records what the call returned.
func (r *Record) Outcome(call int64, outcome Outcome, detail string) error {
	_, err := r.write(Line{Event: EventOutcome, Call: call, Outcome: outcome.String(), Detail: detail})
	return err
}

// Fenced records that the node lost the shard.
func (r *Record) Fenced(shard wal.ShardID, epoch wal.Epoch, detail string) error {
	_, err := r.write(Line{Event: EventFenced, Shard: int32(shard), Epoch: int64(epoch), Detail: detail})
	return err
}

func (r *Record) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.f.Close()
}

// ReadRecord reads a record back; a missing file is an empty record. Lines
// that do not parse are silently dropped, so the short last line a kill can
// leave does not make a killed node's record unreadable.
func ReadRecord(path string) ([]Line, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []Line
	for raw := range strings.SplitSeq(string(b), "\n") {
		if strings.TrimSpace(raw) == "" {
			continue
		}
		var l Line
		if err := json.Unmarshal([]byte(raw), &l); err != nil {
			continue
		}
		out = append(out, l)
	}
	return out, nil
}

// Identify returns a mutation's subject and what the cold store must show once
// it is applied. The driver uses it for the call line and a log reader for an
// entry's payload, so both agree.
//
// A request touching two runs (continue-as-new, conflict-resolve with a new
// run) is identified by the run it is about, and claims nothing about the
// other; deciding what it leaves on every run is fold's job.
func Identify(m mutation.Mutation) (Workflow, Effect, error) {
	switch m.Kind() {
	case mutation.KindCreate:
		s := m.Create.NewWorkflowSnapshot
		return Workflow{s.NamespaceID, s.WorkflowID, s.RunID}, Exists, nil
	case mutation.KindUpdate:
		s := m.Update.UpdateWorkflowMutation
		return Workflow{s.NamespaceID, s.WorkflowID, s.RunID}, Exists, nil
	case mutation.KindConflictResolve:
		s := m.ConflictResolve.ResetWorkflowSnapshot
		return Workflow{s.NamespaceID, s.WorkflowID, s.RunID}, Exists, nil
	case mutation.KindSet:
		s := m.Set.SetWorkflowSnapshot
		return Workflow{s.NamespaceID, s.WorkflowID, s.RunID}, Exists, nil
	case mutation.KindDelete:
		r := m.Delete
		return Workflow{r.NamespaceID, r.WorkflowID, r.RunID}, Removed, nil
	case mutation.KindAddTasks:
		// Task rows are keyed by (shard, category, key), not by run, so there
		// is no execution row to check: no run, no claim.
		r := m.AddTasks
		return Workflow{r.NamespaceID, r.WorkflowID, ""}, NoClaim, nil
	case mutation.KindRangeCompleteTasks:
		// Names no workflow at all.
		return Workflow{}, NoClaim, nil
	case mutation.KindDeleteCurrent:
		// Removes only the current-execution row; the run's mutable state
		// remains, so Exists. The paired Delete carries the removal.
		r := m.DeleteCurrent
		return Workflow{r.NamespaceID, r.WorkflowID, r.RunID}, Exists, nil
	default:
		return Workflow{}, Exists, fmt.Errorf("checker: %w, this holds %v", mutation.ErrNotExactlyOneRequest, m.Kind())
	}
}

// Classify maps a call's error to Acked, Refused or Unknown.
//
// [Refused] means only "the store gave a definite answer"; it must not be read
// as "not written". Some refusals happen before the append (backpressure, the
// condition authority, a halted cycle), but a drain error returned through
// cycle.Manager.Write comes after the call's entry is durable, with the same
// Go type. Any unrecognised error is [Unknown].
func Classify(err error) Outcome {
	switch {
	case err == nil:
		return Acked
	case isRefusal(err):
		return Refused
	default:
		return Unknown
	}
}

// isRefusal is the closed set of typed errors meaning the store or layer
// declined the request. Deadlines, broken connections and anything unlisted
// stay unknown, so the checker never accuses a correct layer.
func isRefusal(err error) bool {
	var condition *p.WorkflowConditionFailedError
	var currentCondition *p.CurrentWorkflowConditionFailedError
	var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
	var invalid *serviceerror.InvalidArgument
	var notFound *serviceerror.NotFound
	var exhausted *serviceerror.ResourceExhausted
	return errors.As(err, &condition) ||
		errors.As(err, &currentCondition) ||
		errors.As(err, &alreadyStarted) ||
		errors.As(err, &invalid) ||
		errors.As(err, &notFound) ||
		errors.As(err, &exhausted) ||
		Fenced(err)
}

// Fenced reports that this node no longer holds the shard, the refusal a
// claimant must stop on. The layer returns ShardOwnershipLostError; through a
// history service it arrives as an Unavailable "shard status unknown", which
// only its text distinguishes from other transport failures.
func Fenced(err error) bool {
	if _, ok := errors.AsType[*p.ShardOwnershipLostError](err); ok {
		return true
	}
	if _, ok := errors.AsType[*serviceerror.Unavailable](err); ok {
		return strings.Contains(err.Error(), "shard")
	}
	return false
}

// Detail formats an error for the record as type and text: the type is what
// [Classify] used, the text is for people.
func Detail(err error) string {
	if err == nil {
		return ""
	}
	return fmt.Sprintf("%T: %v", err, err)
}
