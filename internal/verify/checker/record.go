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
// Two lines per call, each fsynced, and the gap between them is the third
// outcome class: a process killed between them leaves a `call` with no
// `outcome`, which is exactly "the driver does not know". Recording the outcome
// only would make every killed call look like one that was never issued, while
// its entry sits in the log with nothing to account for it.
//
// It is a file and not memory because what it is a record of is a process
// dying: an in-memory record goes with the node, and the one call a run turns
// on is then the one that is missing.

// Line is one entry of the record. It is the file format, so a field is added
// here and read back by [ReadRecord] and nothing else parses it.
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

// The events a record holds. Only the first two carry claims; the third is the
// run's own narration, which a harness reads to know what happened and no
// assertion stands on.
const (
	EventCall    = "call"
	EventOutcome = "outcome"
	EventFenced  = "fenced"
)

// Record is one node's memory of what it asked for, appended and fsynced per
// line.
type Record struct {
	mu          sync.Mutex
	f           *os.File
	node        string
	incarnation int64
	seq         int64
}

// NewRecord opens a record for one node incarnation.
//
// The incarnation is not decoration: `seq` is per process, so a node restarted
// onto the same record path numbers a second run from 1 and the two runs' calls
// collide — one call's outcome read as another's. Whoever starts a node is the
// only thing that knows it is a restart, so the incarnation comes from there,
// and an incarnation of 0 appended to a record that already holds a run is
// refused rather than trusted.
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

// write appends one line and fsyncs it. The returned seq is the line's id
// within this incarnation, which is what an outcome points back at.
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

// Call writes down a call the driver is about to make, before it makes it.
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

// Outcome writes down what the call returned.
func (r *Record) Outcome(call int64, outcome Outcome, detail string) error {
	_, err := r.write(Line{Event: EventOutcome, Call: call, Outcome: outcome.String(), Detail: detail})
	return err
}

// Fenced writes down that the node lost the shard.
func (r *Record) Fenced(shard wal.ShardID, epoch wal.Epoch, detail string) error {
	_, err := r.write(Line{Event: EventFenced, Shard: int32(shard), Epoch: int64(epoch), Detail: detail})
	return err
}

func (r *Record) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.f.Close()
}

// ReadRecord reads back what a node wrote down.
//
// A partial trailing line is dropped rather than being an error: the record is
// appended and fsynced per line, so a kill can at worst leave the last one
// short — and a reader that treated a short last line as a broken file could
// not read the record of a killed node, which is the case the record exists
// for.
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

// Identify reads the subject of a mutation and what the cold store must show
// once it has been applied.
//
// One implementation for both ends on purpose: the driver stamps it on the call
// line, and whoever reads a log back reads it off an entry's payload. A request
// carrying two runs — a continue-as-new, a conflict-resolve with a new run
// beside the reset one — is identified by the run it is *about*, so a claim
// made from it is about that run and no other. That is a narrowing and it is
// stated rather than hidden: saying more would mean deciding what each request
// leaves behind on every run it touches, which is fold's job.
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
		// A shard-level record: it names a namespace and a workflow because the
		// request has them, and no run at all, because the store's task rows are
		// keyed by (shard, category, key). There is no execution row to read it
		// back from, so it makes no claim.
		r := m.AddTasks
		return Workflow{r.NamespaceID, r.WorkflowID, ""}, NoClaim, nil
	case mutation.KindRangeCompleteTasks:
		// And this one names not even that.
		return Workflow{}, NoClaim, nil
	case mutation.KindDeleteCurrent:
		// The current-execution row, not the execution row — so the run's
		// mutable state is still there afterwards, and a read of it would answer
		// yes. Saying Exists here is the honest reading of what a record can
		// see, and the delete that follows in the driver's own pair is what
		// carries the removal.
		r := m.DeleteCurrent
		return Workflow{r.NamespaceID, r.WorkflowID, r.RunID}, Exists, nil
	default:
		return Workflow{}, Exists, fmt.Errorf("checker: %w, this holds %v", mutation.ErrNotExactlyOneRequest, m.Kind())
	}
}

// Classify is the driver's reading of what happened to its call.
//
// Three values, and the boundary between the last two is the one that cannot be
// drawn from the error type: some refusals are provably pre-append —
// backpressure, the condition authority, a halted cycle — and a drain error
// returned up through cycle.Manager.Write is not, because that call's own entry
// is already durable. From outside the two arrive as the same Go type. So
// [Refused] is a name for "the store gave a definite answer" and nothing more,
// it may not be read as absence, and anything this list has never seen is
// [Unknown] rather than assumed.
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

// isRefusal is the closed set of definite answers: each is a typed error the
// store or the layer returns instead of doing what was asked. A deadline, a
// broken connection, or an error this list has never seen is deliberately
// unknown, because the direction of that mistake is a checker that accuses a
// correct layer.
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

// Fenced reports the one refusal a claimant must stop on: this node no longer
// holds the shard. The layer answers it as a ShardOwnershipLostError, so that
// arm matches on the type; a driver going through a history service instead
// meets the shard's own "shard status unknown", an Unavailable like every other
// transport failure, so there the text is the only thing that tells it apart.
func Fenced(err error) bool {
	if _, ok := errors.AsType[*p.ShardOwnershipLostError](err); ok {
		return true
	}
	if _, ok := errors.AsType[*serviceerror.Unavailable](err); ok {
		return strings.Contains(err.Error(), "shard")
	}
	return false
}

// Detail is how an error is written down: the type beside the text, since the
// type is what [Classify] turned on and the text is what a human reads.
func Detail(err error) string {
	if err == nil {
		return ""
	}
	return fmt.Sprintf("%T: %v", err, err)
}
