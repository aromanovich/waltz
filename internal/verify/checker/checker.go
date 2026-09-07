// Package checker is what a driver writes down about the calls it made, and the
// vocabulary for reading that record back.
//
// [Record] is the file: two fsynced lines per call, the first before the store
// is touched and the second once it has answered, so a process killed between
// them leaves a call nobody knows the outcome of rather than one that looks
// like it was never issued. [Classify] is how an error becomes one of the three
// [Outcome] classes, and [Identify] names the run a mutation is about and what
// the cold store must show once it has been applied — one implementation for
// whoever writes a record and whoever reads a log.
//
// It judges nothing: [ReadRecord] hands the lines back and stops there, and
// whether the run they describe was correct is the caller's question.
package checker

// Workflow is what a call and an entry are both about: the subject of one
// mutation. Identity is the driver's problem and not the layer's — the payload
// format carries namespace/workflow/run verbatim, so a driver whose runs are
// its own has an identity for free.
type Workflow struct {
	Namespace string
	Workflow  string
	Run       string
}

func (w Workflow) String() string { return w.Namespace + "/" + w.Workflow + "/" + w.Run }

// Effect is what the cold store must show for a mutation that was applied.
type Effect int

const (
	// Exists: the mutation leaves the run's mutable state behind.
	Exists Effect = iota
	// Removed: the mutation is the deletion of it.
	Removed
	// NoClaim: the mutation is not about a run's mutable state, so nothing
	// reading the record can check it against an execution row. Both
	// history-task records are such mutations — they name a shard, a category
	// and a range, and no run.
	//
	// It is a value rather than an error because the alternative readings are
	// both wrong: refusing the mutation would stop a driver that is behaving,
	// and counting it as undecoded would report the record as damaged.
	NoClaim
)

func (e Effect) String() string {
	switch e {
	case Removed:
		return "removed"
	case NoClaim:
		return "no-claim"
	default:
		return "exists"
	}
}

// Outcome is what the driver learned about one call it made. Three classes, and
// the third is the whole reason the record has to exist: after a kill -9 there
// are calls whose outcome nobody knows, and a reader that takes them for either
// of the other two reports findings that are not.
type Outcome int

const (
	// Unknown: the call was in flight when the world changed under it, its
	// deadline expired, or its error was ambiguous. Nothing is promised about
	// it in either direction.
	Unknown Outcome = iota
	// Acked: the call returned success, so its mutation is in the log (I2).
	Acked
	// Refused: the call returned a definite error — a condition failure, a
	// backpressure refusal, an ownership-lost.
	//
	// A refusal bounds nothing about the cold store. Some refusals are provably
	// pre-append — backpressure, the condition authority, a halted cycle — but a
	// drain error returned up through cycle.Manager.Write arrives after that
	// call's own entry is already durable, and from outside the two are the same
	// Go type. So the class means "the store gave a definite answer" and never
	// that the mutation is absent.
	Refused
)

func (o Outcome) String() string {
	switch o {
	case Acked:
		return "acked"
	case Refused:
		return "refused"
	default:
		return "unknown"
	}
}
