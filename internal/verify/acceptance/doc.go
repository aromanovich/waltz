// Package acceptance is the volume half of the evidence: a generated stream
// long enough that what the layer did to it is a measurement rather than an
// anecdote, driven two ways.
//
// One drives the fold alone, window by window, with no log and no store under
// it — the run that says what the folding collapsed and at which generator
// knobs. The other drives both seams at once, wal/memwal under cold/memcold
// with a cycle.Manager between them, so the claim stops being what the layer
// handed down and becomes what the database holds afterwards.
package acceptance
