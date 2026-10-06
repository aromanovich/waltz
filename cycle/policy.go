package cycle

import "time"

// Policy is where a cycle reads its configuration. It is called at each
// decision, not at the acquire, so a moved setting changes the next drain
// rather than the next epoch. Do not cache its [Config] on the cycle.
//
// Every call must return a complete [Config] (clock, age and the four bounds)
// because nothing downstream fills defaults. Build one with [Fixed] or [Live]:
// a hand-written source gives a nil clock (panics in [New]), a zero hard max
// (refuses every write) and a zero age (the timer spins a CPU per shard).
type Policy func() Config

// Moving is the part of the policy re-read at each decision: the drain
// triggers and the trim cadence, read at [Cycle.add], the age tick,
// [Cycle.drain] and [Cycle.replay]. Sync and DrainOnRead are excluded because
// changing the mode mid-write would break what the caller was promised; the
// four bounds because [Config.CheckBudget] checks them once, before boot.
//
// A nil getter keeps the static value. A getter returning zero means zero:
// zero mutations drains every write, a zero trim cadence trims every drain.
// Age has no zero meaning and is filled with the default ([Config.fill]).
type Moving struct {
	Mutations func() int
	Bytes     func() int
	Age       func() time.Duration
	TrimEvery func() int
	TrimAfter func() time.Duration
}

// Fixed is a policy that never moves. It fills c once, here.
func Fixed(c Config) Policy {
	c.fill()
	return func() Config { return c }
}

// Live is a policy whose triggers and cadence can change while the node runs
// (the server's dynamic config, through waltz.NewPolicy). static is read once
// and carries the mode, the bounds and the clock. A call costs five source
// reads and a struct copy, so take one snapshot per decision, not per field.
// Each answer is filled after the getters run, since an operator can set a key
// to zero at any moment.
func Live(static Config, m Moving) Policy {
	static.fill()
	return func() Config {
		c := static
		if m.Mutations != nil {
			c.Mutations = m.Mutations()
		}
		if m.Bytes != nil {
			c.Bytes = m.Bytes()
		}
		if m.Age != nil {
			c.Age = m.Age()
		}
		if m.TrimEvery != nil {
			c.TrimEvery = m.TrimEvery()
		}
		if m.TrimAfter != nil {
			c.TrimAfter = m.TrimAfter()
		}
		c.fill()
		return c
	}
}
