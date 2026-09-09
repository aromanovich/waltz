package waltz

import (
	"fmt"
	"time"

	"go.temporal.io/server/common/dynamicconfig"

	"github.com/aromanovich/waltz/cycle"
)

// measured is [cycle.Defaults] read once, so every setting below states its
// default by reference rather than by copying a number out of it.
var measured = cycle.Defaults()

// The policy's dynamic half as settings of the server's dynamic config, under
// the same `wal.` name the section has. Five are read at the decision that
// consults them; the other four are read once, when the policy is built
// ([atStart]), and their descriptions say so.
//
// Exported so a process configuring the layer from flags or an environment
// variable can build a [dynamicconfig.StaticClient] over these keys rather than
// a second spelling of them.
var (
	WindowMutations = dynamicconfig.NewGlobalIntSetting(
		"wal.windowMutations", measured.Mutations,
		`WindowMutations is the drain trigger in mutations: the window is applied as one
transaction when it holds this many. It sits at the measured collapse knee, so lowering it
gives collapse away and raising it holds more unapplied work per shard.`)

	WindowBytes = dynamicconfig.NewGlobalIntSetting(
		"wal.windowBytes", measured.Bytes,
		`WindowBytes is the same trigger in encoded bytes, whichever trips first.`)

	WindowAge = dynamicconfig.NewGlobalDurationSetting(
		"wal.windowAge", measured.Age,
		`WindowAge drains a tail nothing is pushing on. It is a recovery-budget choice rather
than a measured one: it bounds how long an idle shard's acked-but-unapplied work waits, and
therefore what the next owner would replay. A change takes effect within one window, since
the age timer is re-armed at every tick — which is also why zero or a negative value is read
as the default rather than obeyed: it is the interval that timer is re-armed at.`)

	TrimEvery = dynamicconfig.NewGlobalIntSetting(
		"wal.trimEvery", measured.TrimEvery,
		`TrimEvery is the trim cadence in drains: the log below the applied watermark is
deleted every this many drains. A DeleteRange per drain is a transaction per drain for no
gain; raising it leaves more of the log behind, which is what a post-mortem reads.`)

	TrimAfter = dynamicconfig.NewGlobalDurationSetting(
		"wal.trimAfter", measured.TrimAfter,
		`TrimAfter is the same cadence in time, whichever trips first. Raising TrimEvery alone
does not keep a log: this one fires anyway.`)

	HardMaxEntries = dynamicconfig.NewGlobalIntSetting(
		"wal.hardMaxEntries", measured.HardMaxEntries,
		`HardMaxEntries is invariant I10's bound on one shard's tail in entries: what has been
acked and not yet applied. A shard at the bound refuses its writers with ResourceExhausted
rather than parking them behind the apply. READ AT START-UP: a change needs the history
services restarted. It is read once because it is one half of a bound whose other half is
wal.hardMaxBytes — neither unit works alone, and a node honouring one of the two from a
different edit than the other is a bound nobody wrote.`)

	HardMaxBytes = dynamicconfig.NewGlobalIntSetting(
		"wal.hardMaxBytes", measured.HardMaxBytes,
		`HardMaxBytes is the same bound in encoded bytes — two units because neither works
alone: one workflow near the server's own 8 MB mutable-state limit turns an entries-only bound
into a byte budget with no ceiling. It is arithmetic and not taste: the node's tail budget
divided by the shards it may own. READ AT START-UP: it is a factor of the product
cycle.Config.CheckBudget asserts before the node boots, and a factor that moved afterwards
would be that refusal with nothing behind it.`)

	MaxShards = dynamicconfig.NewGlobalIntSetting(
		"wal.maxShards", measured.MaxShards,
		`MaxShards is what one node may own at once — not the cluster's shard count: the default
256 is 128 in steady state, doubled for a failover. READ AT START-UP.`)

	TailBudgetBytes = dynamicconfig.NewGlobalIntSetting(
		"wal.tailBudgetBytes", measured.TailBudgetBytes,
		`TailBudgetBytes is the RAM one node may hold as unapplied tail. hardMaxBytes × maxShards
must fit in it or the node refuses to start — cycle.Config.CheckBudget is run rather than
written down, because a doc line does not survive a config edit. READ AT START-UP.`)
)

// setting is one row of [settings]: its current and legacy keys, how it binds
// into the policy, and the [cycle.Config] field its
// value lands on. That last one is the guard's — nothing on a running node
// reads it.
type setting struct {
	key dynamicconfig.Key
	// was is the legacy `wal` key refused with migration guidance rather than a
	// generic "invalid keys" error — see [movedKey].
	was string
	// live says whether a running node re-reads this one. Documentation only —
	// what bind writes into is what decides — and the handbook's configuration
	// table is kept in step with it by hand.
	live bool
	// bind installs the setting into the policy being built. A live one puts
	// its getter in the moving half; a start-only one writes its value into the
	// static half, once, here.
	bind  func(*dynamicconfig.Collection, *cycle.Config, *cycle.Moving)
	field func(*cycle.Config) any
}

// moves builds a row a running cycle re-reads: the getter goes into
// [cycle.Moving] and the decision that consults it calls it.
//
// The ends are field selectors so the compiler pairs the types — a duration
// setting cannot be bound to a count — and the guard can recover which field it
// landed on by comparing addresses.
func moves[T any](
	s dynamicconfig.GlobalTypedSetting[T],
	was string,
	into func(*cycle.Moving) *func() T,
	field func(*cycle.Config) *T,
) setting {
	return setting{
		key:  s.Key(),
		was:  was,
		live: true,
		bind: func(dc *dynamicconfig.Collection, _ *cycle.Config, m *cycle.Moving) {
			*into(m) = s.Get(dc)
		},
		field: func(c *cycle.Config) any { return field(c) },
	}
}

// atStart builds a row read once, when the policy is built, because what
// consults it cannot honour a change: three of the four are
// [cycle.Config.CheckBudget]'s factors, asserted before the node boots, and a
// factor that moved afterwards would leave that refusal standing for numbers
// the node no longer runs at.
//
// Such a row must say so in its own description, and the configuration
// reference marks it.
func atStart[T any](
	s dynamicconfig.GlobalTypedSetting[T],
	was string,
	field func(*cycle.Config) *T,
) setting {
	return setting{
		key:  s.Key(),
		was:  was,
		live: false,
		bind: func(dc *dynamicconfig.Collection, c *cycle.Config, _ *cycle.Moving) {
			*field(c) = s.Get(dc)()
		},
		field: func(c *cycle.Config) any { return field(c) },
	}
}

// movedKey refuses a `wal` key that is a dynamic-config setting now, naming the
// setting to write instead; the strict decoder would otherwise report it as
// "invalid keys" and send somebody hunting for a misspelling. A refusal and not
// a silent migration: a node must not run a window the file it was given does
// not describe.
func movedKey(key string) error {
	for _, s := range settings {
		if s.was != key {
			continue
		}
		when := "read once, where the node builds its policy — a change still needs a restart"
		if s.live {
			when = "read at the decision that consults it, so a change needs no restart"
		}
		return fmt.Errorf("waltz: %s.%s is the server's dynamic config now, as %q: it is %s, "+
			"and it is no longer a key of this section", SectionKey, key, s.key, when)
	}
	return nil
}

// settings is the dynamic half of the policy key by key, the counterpart of
// [knobs]: between them every field of [cycle.Config] a deployment can set is
// claimed exactly once (TestEveryPolicyFieldIsConfigurableOnce). A field on
// neither table is one no configuration reaches; a field on both is a number
// two surfaces can disagree about, and there is no precedence rule.
var settings = []setting{
	moves(WindowMutations, "window_mutations",
		func(m *cycle.Moving) *func() int { return &m.Mutations },
		func(c *cycle.Config) *int { return &c.Mutations }),
	moves(WindowBytes, "window_bytes",
		func(m *cycle.Moving) *func() int { return &m.Bytes },
		func(c *cycle.Config) *int { return &c.Bytes }),
	moves(WindowAge, "window_age",
		func(m *cycle.Moving) *func() time.Duration { return &m.Age },
		func(c *cycle.Config) *time.Duration { return &c.Age }),
	moves(TrimEvery, "trim_every",
		func(m *cycle.Moving) *func() int { return &m.TrimEvery },
		func(c *cycle.Config) *int { return &c.TrimEvery }),
	moves(TrimAfter, "trim_after",
		func(m *cycle.Moving) *func() time.Duration { return &m.TrimAfter },
		func(c *cycle.Config) *time.Duration { return &c.TrimAfter }),
	atStart(HardMaxEntries, "hard_max_entries",
		func(c *cycle.Config) *int { return &c.HardMaxEntries }),
	atStart(HardMaxBytes, "hard_max_bytes",
		func(c *cycle.Config) *int { return &c.HardMaxBytes }),
	atStart(MaxShards, "max_shards",
		func(c *cycle.Config) *int { return &c.MaxShards }),
	atStart(TailBudgetBytes, "tail_budget_bytes",
		func(c *cycle.Config) *int { return &c.TailBudgetBytes }),
}

// NewPolicy is the whole policy a node runs: the section's fields and the
// start-only settings read once, the live settings read at every decision that
// consults one.
//
// A nil collection means the settings' own defaults, not a refusal — it is what
// a process with no dynamic config has. A caller that holds a [cycle.Config]
// already passes [cycle.Fixed] straight to [Compose] instead.
func NewPolicy(dc *dynamicconfig.Collection, w WAL) cycle.Policy {
	if dc == nil {
		dc = dynamicconfig.NewNoopCollection()
	}
	static, m := w.StaticConfig(), cycle.Moving{}
	for _, s := range settings {
		s.bind(dc, &static, &m)
	}
	return cycle.Live(static, m)
}
