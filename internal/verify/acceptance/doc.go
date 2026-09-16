// Package acceptance is the volume half of the evidence: a generated stream
// long enough that what the layer did to it is a measurement rather than an
// anecdote, driven two ways.
//
// One drives the fold alone, window by window, with no log and no store under
// it — the run that says what the folding collapsed and at which generator
// knobs. The other drives both seams at once, wal/memwal under cold/memcold
// with a cycle.Manager between them, so the claim stops being what the layer
// handed down and becomes what the database holds afterwards: the epoch lost
// mid-run, the crash on top of a drain and the second owner taking the shard
// are that same drive with something done to it partway through.
//
// That second way is driven twice over as well — one seed at the shipped window
// and at a window of one, into two databases required to come out identical —
// which is the fold judged against not folding rather than against a record of
// what its own drains carried.
package acceptance
