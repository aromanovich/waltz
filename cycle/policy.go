package cycle

import "time"

// Policy is where a cycle reads its configuration, called at the decision rather
// than at the acquire, so what a policy that moved changes is the next drain and
// not the next epoch. Do not cache its [Config] on the cycle.
//
// Every call must answer a complete [Config], clock, age and the four bounds
// included, because nothing downstream defaults anything. Filling is unexported,
// so use [Fixed] or [Live]: a source written by hand outside this package hands
// a cycle a nil clock, four zero bounds and a zero age, which is a shard that
// refuses its first write on a goroutine spinning at a whole CPU.
type Policy func() Config

// Moving is the half of the policy a decision re-reads: the drain watermarks and
// the trim cadence, read at [Cycle.add], the age tick and [Cycle.drain].
//
// The rest of [Config] is not here. Sync and DrainOnRead are the mode, and a
// mode that changed mid-flight would change what a caller already inside a write
// was promised. The four bounds are [Config.CheckBudget]'s arithmetic, whose
// purpose is to refuse a node before it boots.
//
// A nil getter means the static value stands, not the zero: a window of no
// mutations drains every write and a trim cadence of zero trims on every drain.
// A getter that *answers* zero is that same reading, except for Age, which has
// none and is filled with the measured default ([Config.fill]).
type Moving struct {
	Mutations func() int
	Bytes     func() int
	Age       func() time.Duration
	TrimEvery func() int
	TrimAfter func() time.Duration
}

// Fixed is the policy that does not move: what a caller holding a [Config] as a
// Go literal has. It fills once, here.
func Fixed(c Config) Policy {
	c.fill()
	return func() Config { return c }
}

// Live is the policy whose watermarks and cadence come from somewhere that can
// change while the node runs — the server's dynamic config, through
// waltz.NewPolicy.
//
// static is read once, here, and carries the mode, the bounds and the clock. A
// call costs five reads of the source and a struct copy, so read sites take one
// snapshot per decision rather than one per field.
//
// The answer is filled and not just the static half: a getter reads a key an
// operator can set to anything at any moment, so a field whose zero has no
// reading has to be completed after the source is read rather than before.
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
