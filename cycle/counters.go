package cycle

import "github.com/aromanovich/waltz/mutation"

// Counters is what a cycle counted, embedded by both [Stats] and [Totals] so a
// counter added here appears in every witness.
//
// Every field must be summable: an int, or an array of ints. A position may not
// go in — [Stats.CommitSeqno] and [Totals.Acked] are places in a log that
// outlives the cycle, and summing them counts the same entries once per
// acquire. [Counters.add] must include every field.
type Counters struct {
	// Drains counts committed drains, Trims the trims started (cadenced or
	// forced by pressure), and Refusals the force-drains: a window the
	// accumulator cannot express, or an assertion the condition authority
	// cannot determine.
	Drains   int
	Trims    int
	Refusals int
	// TrimsCommitted is the subset of Trims that reached the log. Counted off
	// the loop where the outcome is known, so a reading taken with a trim in
	// flight has Trims above this; a retired cycle's count is taken after its
	// trims finish, so there the gap is the trims that failed.
	TrimsCommitted int
	// Replayed counts entries read out of an inherited tail, Dropped the
	// provisional ones whose condition did not hold. Both describe the replay
	// that finished: an abandoned attempt takes what it counted with it.
	Replayed int
	Dropped  int
	// Reads counts overlay reads routed, ReadsHeld the subset the window had
	// something for. A witness rests on the second: reads that never crossed a
	// held workflow is what an empty layer looks like.
	Reads     int
	ReadsHeld int
	// TaskReads counts task pages routed, TaskReadsMerged those that carried a
	// task out of the window. TaskCollisions counts keys both sources held,
	// which their disjointness should never produce; the metric emitted beside
	// it carries no shard tag, so only this field says which shard.
	TaskReads       int
	TaskReadsMerged int
	TaskCollisions  int
	// AckedRanges counts range deletes folded, DroppedTasks and WrittenTasks
	// what the drains did with them. A zero AckedRanges means no queue ever
	// completed a range, so it does not witness that queue path.
	AckedRanges  int
	DroppedTasks int
	WrittenTasks int
	// Kinds counts accepted entries by [mutation.Kind]. Read it as "did this
	// kind appear at all", never as a rate.
	Kinds [mutation.KindCount]int
}

// add merges another cycle's counters into these. Unexported so no Add is
// promoted onto [Totals], which is a witness's reading and nobody's control
// surface.
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
