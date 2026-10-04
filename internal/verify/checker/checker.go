// Package checker records the calls a driver made and reads that record back.
//
// [Record] writes two fsynced lines per call: one before the store is touched,
// one after it answers. A process killed in between leaves a call with unknown
// outcome, not one that looks never issued. [Classify] maps an error to an
// [Outcome]; [Identify] names a mutation's run and the [Effect] the cold store
// must show. It judges nothing: deciding whether a run was correct is the
// caller's job.
package checker

// Workflow is the run one mutation is about. The payload carries
// namespace/workflow/run verbatim, so a driver gets identity for free.
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
	// NoClaim: the mutation names no run (e.g. both history-task records, keyed
	// by shard, category and task key), so it cannot be checked against an
	// execution row. Not an error: the record is not damaged.
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

// Outcome is what the driver learned about one call. Unknown is why the record
// exists: after a kill -9, treating an in-flight call as acked or refused
// yields false findings.
type Outcome int

const (
	// Unknown: the call was interrupted, timed out, or failed ambiguously.
	// Nothing is promised either way.
	Unknown Outcome = iota
	// Acked: the call returned success, so its mutation is in the log (I2).
	Acked
	// Refused: the call returned a definite error (condition failure,
	// backpressure, ownership lost). It never means the mutation is absent: a
	// drain error from cycle.Manager.Write arrives after the entry is durable,
	// with the same Go type as a pre-append refusal.
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
