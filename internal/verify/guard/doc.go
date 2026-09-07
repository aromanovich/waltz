// Package guard holds the tests whose job is to fail when a decision is
// reverted, rather than when the code is wrong.
//
// Each watches something no other test can see, because it is about two
// packages that may not import each other: the cycle and the wrapper being
// built for one another, and the backpressure refusal's concrete error type
// surviving the whole way out.
//
// What a guard may not be is a lint rule: the dependency rules between packages
// and this tree's own shape are prose (.claude/rules/no-lint-in-tests.md), not
// assertions here.
// Every guard is green on a broken layer and red on a reverted decision, which
// is the opposite of a unit test and the reason they are collected here.
//
// A probe is the easy confusion, and none is shipped here: a guard fails when
// somebody reverts something, a probe answers a number.
package guard
