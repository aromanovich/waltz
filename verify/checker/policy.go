package checker

import (
	"fmt"
	"strconv"
	"time"
)

// The flag names a node is driven by. They are constants here, and the node
// binary registers its flags under them, because [Policy.NodeFlags] renders a
// command line for a process this package does not otherwise know: a name that
// drifted would be a node that fails to parse its own arguments, which is loud,
// and the constants make even that unreachable.
const (
	FlagWindowMutations = "window-mutations"
	FlagWindowBytes     = "window-bytes"
	FlagWindowAge       = "window-age"
	FlagTrimEvery       = "trim-every"
	FlagTrimAfter       = "trim-after"
	FlagCallTimeout     = "call-timeout"
)

// Policy is the run's declared properties **and** the knobs that make them
// true, in one value, because #101's fourth deliverable is exactly that they
// may not be two.
//
// The history is worth carrying: #94 concluded that a chaos run raises
// TrimEvery so that the log is the run's history, and that A6 is gated on the
// result. #95 found the conclusion necessary and not sufficient — the trim
// fires on TrimEvery drains **or** TrimAfter age, whichever trips first, so a
// run merely longer than the 60 s default had its whole history erased and came
// back red against a correct layer. A declaration that sets one trigger is a
// promise nobody keeps.
//
// So: [WholeLog] is a field of the thing that renders the flags, both triggers
// move together, and A10 checks at every sample that they moved.
type Policy struct {
	// WholeLog declares that this run does not trim, so an entry the journal
	// never saw is an entry that never existed. It gates A6. Setting it is what
	// moves both trim triggers out of the way — there is no way to declare it
	// and not mean it, and A10 is what catches meaning it and not getting it.
	WholeLog bool

	// Mutations, Bytes and Age are the cycle's three drain triggers. A harness
	// run is short, so the shipped window (#45's 256/256 KiB knee) would put the
	// whole run in one drain and leave the watermark, the trim and every
	// assertion that stands on them unexercised. They are a property of the run
	// in the same sense WholeLog is, which is why they live here.
	Mutations int
	Bytes     int
	Age       time.Duration

	// CallTimeout bounds one call of the driver. #95 measured it into
	// existence: with no deadline a node on the far side of a partition does
	// not fail, it *hangs* — the record simply stops, which reads exactly like
	// a node that was killed. It is what turns a partition into [Unknown]
	// rather than into silence.
	//
	// [Chaos] sets it generously, because a driver stops at its first non-acked
	// call and a deadline that fires because the sandbox was busy would end a
	// run that had nothing wrong with it. A case that stages a partition wants
	// it *short* instead — #95 measured 15 s into existence for exactly that —
	// and lowering it is the one knob such a case turns.
	CallTimeout time.Duration
}

// The two values a WholeLog run's trim triggers take. They are not "large
// enough for a long run" by measurement — nothing here is a timing — they are
// chosen so that neither trigger can fire within a run this harness could
// plausibly drive, and A10 is what says so for the run that actually happened.
const (
	wholeLogTrimEvery = 1_000_000
	wholeLogTrimAfter = 24 * time.Hour
)

// Chaos is the policy a chaos run is driven under: the whole log kept, a window
// small enough that a short run drains, applies and trims several times over,
// and a deadline on every call.
//
// The window is **32 mutations and not #45's measured knee of 256**, and both
// halves of that have a reason.
//
// 32 was where it could not go before. The research prototype ran at 8 because
// its plugin assembled the drain's statement by concatenation — about a kilobyte
// of query text per mutation, freshly compiled every drain because the text
// differed per batch — and at 32 generated mutations the store answered a
// compilation timeout instead of committing. That arrives as an *ambiguous*
// outcome: the watermark says the batch did not commit and the shard halts by
// design (cycle.resolve) with its tail intact, which is the layer working.
// Making the text a function of which kinds and families a batch carries, and of
// nothing else, removed the ceiling; 32 stays because it is the exact number
// that used to fail.
//
// 256 stays out for a reason of the harness's own, and not a compile one: a
// run here is short, so the shipped window would put it in one drain and leave
// the watermark, the trim and every assertion standing on them unexercised.
func Chaos() Policy {
	return Policy{
		WholeLog:    true,
		Mutations:   32,
		Bytes:       64 << 10,
		Age:         5 * time.Second,
		CallTimeout: 2 * time.Minute,
	}
}

// Validate rejects a policy that cannot mean anything.
func (p Policy) Validate() error {
	if p.Mutations < 1 {
		return fmt.Errorf("checker: the window must hold at least one mutation, got %d", p.Mutations)
	}
	if p.Bytes < 1 {
		return fmt.Errorf("checker: the window's byte bound must be positive, got %d", p.Bytes)
	}
	if p.Age <= 0 {
		return fmt.Errorf("checker: the window's age bound must be positive, got %v", p.Age)
	}
	if p.CallTimeout < 0 {
		return fmt.Errorf("checker: a call deadline cannot be negative, got %v", p.CallTimeout)
	}
	return nil
}

// NodeFlags renders the policy as the arguments a node is started with. It is
// the other half of [Policy]: whatever declares WholeLog is what sets the trim,
// so the two cannot drift.
//
// A run that does not declare WholeLog is deliberately given no trim flags at
// all — it runs at the shipped cadence, which is the only honest meaning of
// "this run does trim", and is the case the trim's own invariant (A5) is worth
// something in.
func (p Policy) NodeFlags() []string {
	flags := []string{
		"-" + FlagWindowMutations, strconv.Itoa(p.Mutations),
		"-" + FlagWindowBytes, strconv.Itoa(p.Bytes),
		"-" + FlagWindowAge, p.Age.String(),
		"-" + FlagCallTimeout, p.CallTimeout.String(),
	}
	if p.WholeLog {
		flags = append(flags,
			"-"+FlagTrimEvery, strconv.Itoa(wholeLogTrimEvery),
			"-"+FlagTrimAfter, wholeLogTrimAfter.String(),
		)
	}
	return flags
}
