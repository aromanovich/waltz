#!/usr/bin/env python3
"""Enumerate one mutation class over the tree, run the suite against each, report
the greens.

The other half of [mutation-run.py]. That one applies mutations a session thought
of, one manifest entry at a time; this one *generates* a whole class and leaves
nothing in it out. The difference is the reason the manifest is deliberately not
checked in: a list goes stale and reads as coverage, where a class does neither —
"every adjacent comparison in `fold/`" means the same thing after the code moves,
and a green from it is a line nothing drives rather than a line nobody thought of.

Three classes, and they find different things:

  * `cmp` — move each `<`, `<=`, `>`, `>=` to its adjacent form. Finds the
    off-by-one, which the deletions cannot see: the line is present and wrong by
    one. Most of its greens are equivalences and the triage is the work.
  * `guard` — delete an `if cond { return … }` block, so the condition always
    passes. That is what a failed assertion looks like: a write acked whose
    condition did not hold.
  * `write` — delete a statement that writes: a bare call, or an assignment into a
    field or an element. Finds acked data that reaches no storage. It finds
    nothing at all in a module that only decides, there being no write in it.

**A narrowed judge is a candidate filter and nothing more.** Dropping packages
from the inner loop turns red into green and never the other way, so a green found
with `--judge` is a *candidate* — and a dismissal made on one is unsound, because
the package that was dropped may be the one that catches it. `--confirm` re-runs a
sweep's greens with every package in the judge and reports which of them the whole
suite catches. Shorten the acceptance stream by volume (`WAL_ACCEPTANCE_MUTATIONS`)
rather than by dropping its package.

Three failure modes of the harness itself, each of which quietly inverts the
result — a broken run reads as a tree where nothing is unguarded, which is the one
way a sweep lies:

  * **a full disk reds every mutant.** The run stops rather than reporting that,
    and it drops the build cache on a *cadence* — never "whenever space is short",
    which would make every build a cold one and every cold build of the whole set
    outrun any sane timeout;
  * **a mutant that hangs is not a mutant that passed.** A flipped loop bound or a
    deleted nil check in a page token's encoder turns a pagination into one that
    never advances; the run times it out and counts it red. The budget is **per
    judge** rather than one number, and both directions cost: too short and every
    mutation is a HUNG, which reads as a guarded tree, while too long makes one
    endless loop cost the whole run;
  * **a red with no test named is the harness, not the tree.** `--confirm` counts
    those apart as BROKEN and stops after three in a row, because by then it is
    measuring the machine;
  * **a timeout under a shell kills the shell and not the test.** `go test` is a
    grandchild, so it survives the timeout, holds the output pipe open and goes on
    competing for the machine — one leaked run per hang, and every result after it
    measured on a loaded box. The judge runs in a session of its own and is killed
    as a group.

All four were found by using this script rather than by reasoning about it, and all
four fail in the same direction: a run that judged nothing reads as a tree with
nothing to find.

Every file is restored in a `finally`, so an interrupt leaves the tree as it was —
and that covers an interrupt and not a kill. A `SIGKILL` leaves the file mutated,
which is the worst state to resume from: the next run's *baseline* is the mutation,
so every result is about a tree nobody has. **Before starting a run, check the tree
against the commit it is supposed to be** — `git status` in a checkout, or a
checksum against the original where the sweep runs on a copy — and run the judge
once to see the baseline green. Both take seconds and both were learnt the other
way.

Usage:

    tools/mutation-sweep.py cmp   'fold/*.go,cycle/*.go' out.json
    tools/mutation-sweep.py guard 'cycle/*.go'           out.json --judge './cycle/... ./fold/'
    tools/mutation-sweep.py --confirm out.json confirmed.json

It is not a suite and must not become one, for [mutation-run.py]'s reason: what a
run found belongs in DURABILITY.md, and what it found and dismissed belongs beside
the code.

[mutation-run.py]: mutation-run.py
"""

import json
import os
import re
import signal
import subprocess
import sys
import time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Below this the run stops rather than reporting every remaining mutation as
# caught. mutation-run.py's figure, for its reason.
MIN_FREE_MB = 700
# Above this the cache is dropped before the next mutation, and every 40th
# mutation drops it anyway: a sweep's mutations are all cache misses.
LOW_FREE_MB = 2500
CACHE_EVERY = 40

FULL_JUDGE = "./..."
# Each mutation is a build of the packages under the judge plus a run of them, and
# the first dominates on a cold cache — which is every mutation, a mutation being a
# cache miss by construction. So the budget is per judge and not one number: the
# whole set from cold is minutes, a narrowed one is seconds, and the two failures
# are opposite. **Too short and every mutation is a HUNG**, which the run counts as
# red and a reader counts as a guarded tree. **Too long and one endless loop costs
# the whole budget** — and endless loops are what this class produces: deleting a
# nil check in a page token's encoder turns a pagination into one that never
# advances.
TIMEOUT_NARROW_S = 300
TIMEOUT_FULL_S = 1800


def timeout_for(judge):
    return TIMEOUT_FULL_S if judge == FULL_JUDGE else TIMEOUT_NARROW_S

FLIPS = [("<=", "<"), (">=", ">"), ("<", "<="), (">", ">=")]


def free_mb(path="/"):
    st = os.statvfs(path)
    return st.f_bavail * st.f_frsize / (1 << 20)


def housekeep(n):
    """Drop the cache on a cadence, and stop rather than run out.

    The cadence is deliberately not "whenever space is short": clearing before
    every mutation makes every build a cold one, and a cold build of the whole set
    outruns any sane timeout — so the run would report HUNG for everything, which
    reads as a tree where nothing is unguarded. That failure was found by using
    this script, not by reasoning about it.
    """
    if n and n % CACHE_EVERY == 0:
        subprocess.run("go clean -cache", cwd=REPO, shell=True, capture_output=True)
    if free_mb() < MIN_FREE_MB:
        raise SystemExit(f"stopping: {free_mb():.0f} MB free, and a full disk reds every mutation")


def sources(patterns):
    import pathlib

    out = []
    for pattern in patterns:
        for path in sorted(pathlib.Path(REPO).glob(pattern)):
            name = str(path)
            if name.endswith("_test.go") or ".pb.go" in name:
                continue
            out.append(name)
    return out


def comparisons(path):
    """Each flippable operator, one at a time, as (line index, old line, new line)."""
    out = []
    for i, line in enumerate(open(path).read().split("\n")):
        code = line.split("//")[0]
        if "<-" in code:  # a channel operation is not a comparison
            continue
        for old, new in FLIPS:
            for m in re.finditer(re.escape(old), code):
                j = m.start()
                # Scanning for `<` must not match `<=`, `<<` or `<-`; same for `>`.
                if old in "<>" and (code[j : j + 2] in (old + "=", old + old) or code[j - 1 : j + 1] == old + old):
                    continue
                out.append((i, line, line[:j] + new + line[j + len(old) :], f"{old}->{new}@{j}"))
    return out


def guards(path):
    """`if cond { return … }` blocks, as (first line index, last line index).

    An `if` with an init statement is skipped: deleting it takes the binding with
    it, which is a build error rather than a finding.
    """
    lines = open(path).read().split("\n")
    out = []
    for i, line in enumerate(lines):
        m = re.match(r"^(\t*)if (.+) \{$", line)
        if not m or ":=" in m.group(2):
            continue
        indent = m.group(1)
        close = None
        for j in range(i + 1, min(i + 12, len(lines))):
            if lines[j] == indent + "}":
                close = j
                break
            if lines[j].startswith(indent + "} else"):
                break
        if close is None:
            continue
        body = [x.strip() for x in lines[i + 1 : close] if x.strip() and not x.strip().startswith("//")]
        if body and re.match(r"^(return|panic\(|continue|break)", body[0]):
            out.append((i, close))
    return out


# A statement that is a whole call, and an assignment into a field or an element.
# A plain local assignment is left out: deleting it is a build error.
CALL = re.compile(r"^(\t+)[A-Za-z_][\w.]*\([^;]*\)$")
ASSIGN = re.compile(r"^(\t+)[A-Za-z_][\w.]*(\[[^\]]*\])?(\.[\w.]+)?\s*(=|\+=|-=)\s")


def statements(path):
    out = []
    for i, line in enumerate(open(path).read().split("\n")):
        if "//" in line or line.strip().startswith("return"):
            continue
        if CALL.match(line):
            # `go` and `defer` change a lifetime rather than a write, and a panic
            # is not one either.
            if not re.match(r"^\t+(go |defer |panic\()", line):
                out.append(i)
        elif ASSIGN.match(line) and "." in line.split("=")[0]:
            out.append(i)
    return out


def judged(judge):
    """Run the judge and answer (exit code, tail of its output).

    The process group is its own and is killed as a group on a timeout. A shell
    plus a timeout otherwise kills only the shell: `go test` is a grandchild, so it
    survives, keeps the output pipe open — which can block the harness where it
    means to move on — and goes on competing for the machine every result after it
    is measured on. One leaked run per hang, and this class produces hangs.
    """
    cmd = f"WAL_ACCEPTANCE_MUTATIONS=10000 go test {judge} -count=1"
    p = subprocess.Popen(cmd, cwd=REPO, shell=True, stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT, text=True, start_new_session=True)
    try:
        out, _ = p.communicate(timeout=timeout_for(judge))
        return p.returncode, out[-4000:]
    except subprocess.TimeoutExpired:
        os.killpg(os.getpgid(p.pid), signal.SIGKILL)
        p.communicate()
        # Red, and named: a flipped loop bound that never ends is a finding of its
        # own kind rather than a harness failure.
        return 124, "HUNG"


BUILD_MARKERS = ("build failed", "declared and not used", "missing return", "cannot use", "undefined:", "syntax error")


def verdict(rc, out):
    if rc == 0:
        return "GREEN"
    return "BUILD" if any(s in out for s in BUILD_MARKERS) else "red"


def apply_and_judge(path, mutate, judge):
    """mutate takes the file's lines and returns them changed; the file is always
    restored."""
    orig = open(path).read()
    try:
        open(path, "w").write("\n".join(mutate(orig.split("\n"))))
        return judged(judge)
    finally:
        open(path, "w").write(orig)


def sweep(kind, patterns, outpath, judge):
    items = []
    for path in sources(patterns):
        if kind == "cmp":
            items += [(path, c) for c in comparisons(path)]
        elif kind == "guard":
            items += [(path, g) for g in guards(path)]
        elif kind == "write":
            items += [(path, i) for i in statements(path)]
        else:
            raise SystemExit(f"unknown class {kind}")
    print(f"{len(items)} mutations of class {kind}, judged by {judge}", flush=True)

    results = []
    t0 = time.time()
    for n, (path, item) in enumerate(items):
        housekeep(n)
        rel = os.path.relpath(path, REPO)
        if kind == "cmp":
            i, old, new, label = item

            def mutate(lines, i=i, new=new):
                lines[i] = new
                return lines

            record = dict(file=rel, line=i + 1, label=label, orig=old.strip(), mutated=new.strip())
        elif kind == "guard":
            a, b = item

            def mutate(lines, a=a, b=b):
                lines[a : b + 1] = ["// MUTANT " + x for x in lines[a : b + 1]]
                return lines

            record = dict(file=rel, line=a + 1, label="guard", orig=open(path).read().split("\n")[a].strip())
        else:
            i = item

            def mutate(lines, i=i):
                lines[i] = "// MUTANT " + lines[i]
                return lines

            record = dict(file=rel, line=i + 1, label="write", orig=open(path).read().split("\n")[i].strip())

        rc, out = apply_and_judge(path, mutate, judge)
        record["status"] = verdict(rc, out)
        results.append(record)
        if record["status"] == "GREEN":
            print(f"  GREEN {rel}:{record['line']} {record['label']}  {record['orig'][:110]}", flush=True)
        if n % 25 == 0:
            print(f"  .. {n}/{len(items)} ({time.time() - t0:.0f}s)", flush=True)
        json.dump(results, open(outpath, "w"), indent=1)

    greens = [r for r in results if r["status"] == "GREEN"]
    print(f"done: {len(results)} mutations, {len(greens)} green, {time.time() - t0:.0f}s", flush=True)
    if judge != FULL_JUDGE and greens:
        print(f"the judge was narrowed, so those {len(greens)} are candidates: "
              f"tools/mutation-sweep.py --confirm {outpath} <out.json>", flush=True)


def locate(path, record):
    """The line the record is about, found by content rather than by number: a
    confirm run happens after the sweep, often after the file has moved, and a
    line number is the one part of a record that goes stale. None when the text is
    absent or ambiguous."""
    lines = open(path).read().split("\n")
    i = record["line"] - 1
    if 0 <= i < len(lines) and lines[i].strip() == record["orig"]:
        return i
    hits = [j for j, line in enumerate(lines) if line.strip() == record["orig"]]
    return hits[0] if len(hits) == 1 else None


def confirm(sweeppath, outpath):
    """Re-run a sweep's greens with every package in the judge."""
    greens = [r for r in json.load(open(sweeppath)) if r["status"] == "GREEN"]
    print(f"{len(greens)} candidates, judged by {FULL_JUDGE}", flush=True)

    results = []
    t0 = time.time()
    for n, r in enumerate(greens):
        housekeep(n)
        path = os.path.join(REPO, r["file"])
        i = locate(path, r)
        if i is None:
            results.append(dict(r, verdict="MOVED"))
            print(f"  MOVED       {r['file']}:{r['line']} — the sweep's line is gone or doubled", flush=True)
            continue

        if r["label"] == "guard":
            # The mutation is the whole block, so it is re-derived rather than
            # replayed: a record cannot carry a brace it did not see.
            block = next((g for g in guards(path) if g[0] == i), None)
            if block is None:
                results.append(dict(r, verdict="MOVED"))
                print(f"  MOVED       {r['file']}:{r['line']} — no guard block starts there now", flush=True)
                continue

            def mutate(lines, a=block[0], b=block[1]):
                lines[a : b + 1] = ["// MUTANT " + x for x in lines[a : b + 1]]
                return lines
        elif "mutated" in r:

            def mutate(lines, i=i, r=r):
                lines[i] = lines[i].replace(r["orig"], r["mutated"], 1)
                return lines
        else:

            def mutate(lines, i=i):
                lines[i] = "// MUTANT " + lines[i]
                return lines

        rc, out = apply_and_judge(path, mutate, FULL_JUDGE)
        failing = sorted({m for m in re.findall(r"^--- FAIL: (\w+)", out, re.M)})
        if rc == 0:
            state = "STILL GREEN"
        elif failing:
            state = "CAUGHT"
        else:
            # A red with no named failure is the harness rather than the tree: a
            # build that would not compile, a timeout, a full disk. Calling it
            # CAUGHT is how a broken run comes to read as a guarded tree, so it is
            # named and counted apart — and several in a row stop the run, because
            # by then it is measuring the machine.
            state = "BROKEN"
        results.append(dict(r, verdict=state, failing=failing[:6]))
        print(f"  {state:<11} {r['file']}:{r['line']} {r['label']}"
              + (f"  by {','.join(failing[:3])}" if failing else ""), flush=True)
        json.dump(results, open(outpath, "w"), indent=1)
        if len(results) >= 3 and all(x["verdict"] == "BROKEN" for x in results[-3:]):
            raise SystemExit("stopping: three runs failed with no test named, so this is the harness")

    caught = sum(1 for x in results if x["verdict"] == "CAUGHT")
    broken = sum(1 for x in results if x["verdict"] == "BROKEN")
    print(f"done: {caught} of {len(results)} were caught by the packages the sweep dropped"
          + (f", {broken} could not be judged" if broken else "")
          + f", {time.time() - t0:.0f}s", flush=True)


def main():
    args = sys.argv[1:]
    if args and args[0] == "--confirm":
        if len(args) != 3:
            raise SystemExit(__doc__)
        return confirm(args[1], args[2])
    if len(args) < 3:
        raise SystemExit(__doc__)
    kind, patterns, outpath = args[0], args[1].split(","), args[2]
    judge = FULL_JUDGE
    if "--judge" in args:
        judge = args[args.index("--judge") + 1]
    sweep(kind, patterns, outpath, judge)


if __name__ == "__main__":
    main()
