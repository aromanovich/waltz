package cycle

import "github.com/aromanovich/waltz/mutation"

// Counters is what a cycle counted, embedded in both [Stats] and [Totals].
// Every field must be summable (an int or an array of ints) and included in
// [Counters.add]. No positions: [Stats.CommitSeqno] and [Totals.Acked] index a
// log that outlives the cycle, so summing them counts entries once per acquire.
type Counters struct {
	// Drains counts committed drains, Trims the trims started (cadenced or
	// forced by pressure), Refusals the force-drains: a window the accumulator
	// cannot express, or an assertion the condition authority cannot determine.
	Drains   int
	Trims    int
	Refusals int
	// TrimsCommitted is the subset of Trims that reached the log. It is counted
	// when the outcome is known, so it lags Trims while a trim is in flight; on
	// a retired cycle the gap is the trims that failed.
	TrimsCommitted int
	// Replayed counts entries read from an inherited tail, Dropped the
	// provisional ones whose condition failed. A replay attempt abandoned for a
	// retry contributes nothing.
	Replayed int
	Dropped  int
	// Reads counts overlay reads routed, ReadsHeld those the window had
	// something for. ReadsHeld is the witness: zero means the layer was empty.
	Reads     int
	ReadsHeld int
	// TaskReads counts task pages routed, TaskReadsMerged those carrying a
	// window task. TaskCollisions counts keys both sources held, which should
	// never happen; its metric has no shard tag, so only this field names the
	// shard.
	TaskReads       int
	TaskReadsMerged int
	TaskCollisions  int
	// AckedRanges counts range deletes folded, DroppedTasks and WrittenTasks
	// what drains did with them. Zero AckedRanges means the queue path was not
	// exercised.
	AckedRanges  int
	DroppedTasks int
	WrittenTasks int
	// Kinds counts accepted entries by [mutation.Kind]: whether a kind
	// appeared at all, never a rate.
	Kinds [mutation.KindCount]int
}

// add merges another cycle's counters into these. Unexported so no Add is
// promoted onto [Totals], which is read-only.
func (c *Counters) add(o Counters) {
	c.Drains += o.Drains
	c.Trims += o.Trims
	c.Refusals += o.Refusals
	c.TrimsCommitted += o.TrimsCommitted
	c.Replayed += o.Replayed
	c.Dropped += o.Dropped
	c.Reads += o.Reads
	c.ReadsHeld += o.ReadsHeld
	c.TaskReads += o.TaskReads
	c.TaskReadsMerged += o.TaskReadsMerged
	c.TaskCollisions += o.TaskCollisions
	c.AckedRanges += o.AckedRanges
	c.DroppedTasks += o.DroppedTasks
	c.WrittenTasks += o.WrittenTasks
	for k, n := range o.Kinds {
		c.Kinds[k] += n
	}
}
