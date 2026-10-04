// Package guard holds tests that fail when a design decision is reverted.
//
// Each covers two packages that may not import each other: that cycle.Manager
// still satisfies the wrapper interfaces, and that the backpressure refusal's
// concrete error type survives all the way out. Package dependency rules are
// prose (.claude/rules/no-lint-in-tests.md), not tests here.
//
// The wiring check fails only on a reverted decision. The backpressure cases
// drive a real cycle, so they also fail on a broken layer: treat such a failure
// as a layer bug until the composition is ruled out.
package guard
