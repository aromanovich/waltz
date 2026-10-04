// Package acceptance drives a long generated stream through the layer in two
// ways.
//
// The first drives the fold alone, window by window, with no log or store, and
// reports what folding collapsed at which generator settings.
//
// The second drives wal/memwal and cold/memcold with a cycle.Manager between
// them, and checks what the database holds afterwards. Variants interrupt the
// run: an epoch lost mid-run, a crash during a drain, a second owner taking the
// shard. One seed is also run at the shipped window and at a window of one; the
// two databases must come out identical, which judges folding against not
// folding.
package acceptance
