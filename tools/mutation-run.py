#!/usr/bin/env python3
"""Apply one mutation, run the whole suite, restore, repeat.

This is the mechanical half of the sweep DURABILITY.md's "What this file is
not" describes: comment out the line that writes, move the statement, flip the
comparison, and a green run names an unguarded path.

**It is not a suite and must not become one.** There is no make target, no
`_test.go`, and no checked-in list of mutations — deliberately, and the reason
is in that same section: a mutation run is a thing a session does. A committed
manifest would read as coverage, go stale the moment the code moves, and leave
the next session re-running the last one's list instead of inventing the
mutations it did not think of. What is worth keeping is the runner; what each
run found belongs in DURABILITY.md, and what it found and dismissed belongs
beside the code as a comment, so the next sweep does not re-triage it.

Three properties are the point, and each cost a session to learn:

  * the inner loop is the **whole** `go test ./...`. Narrowing it turns red into
    green and never the other way, so a narrowed loop manufactures findings;
  * every file is restored from a backup after each mutation, including on a
    crash or an interrupt — `finally`, not the happy path;
  * a full disk fails every run, and a failed run reads as "the mutation was
    caught", so every result after that point reports the tree as protected.
    The run stops instead.

Usage:

    tools/mutation-run.py mutations.json [id ...]

The manifest is a list of objects. A single-file mutation is

    {"id": "...", "note": "...", "file": "cycle/cycle.go",
     "find": "<exact text>", "replace": "<exact text>", "count": 1}

and one spanning several edits or files replaces find/replace with

    {"id": "...", "note": "...", "edits": [{"file": ..., "find": ..., "replace": ...}, ...]}

`count` is how many times `find` must occur; a mismatch is a SKIP rather than a
silent partial edit. MUTSWEEP_TARGET narrows the judge on purpose — it is how
"would the differential oracle alone have caught this?" gets measured — and is
never the inner loop:

    MUTSWEEP_TARGET="./internal/verify/acceptance/ -run TestFoldingChanges..." \\
        tools/mutation-run.py mutations.json
"""

import json
import os
import shutil
import subprocess
import sys
import time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Below this the run stops rather than reporting every remaining mutation as
# caught. See the module docstring.
MIN_FREE_MB = 700

# The whole baseline run is seconds and its slowest package is well under this,
# so a mutation that livelocks becomes a quick red instead of waiting out go's
# ten-minute default. A package slower than this under a mutation is a red
# either way.
PACKAGE_TIMEOUT = "45s"


def disk_free_mb():
    # The build cache is what grows across a sweep, and it is often on a
    # different filesystem from the checkout — measuring the checkout's would
    # report room while the one that matters fills.
    where = os.environ.get("GOCACHE") or REPO
    while where != "/" and not os.path.isdir(where):
        where = os.path.dirname(where)
    st = os.statvfs(where)
    return st.f_bavail * st.f_frsize // (1024 * 1024)


def run_suite():
    target = os.environ.get("MUTSWEEP_TARGET", "").split() or ["./..."]
    t0 = time.time()
    p = subprocess.run(
        ["go", "test", *target, "-count=1", "-timeout", PACKAGE_TIMEOUT],
        cwd=REPO, capture_output=True, text=True,
    )
    return p.returncode, p.stdout + p.stderr, time.time() - t0


def failing_tests(out):
    return sorted({ln.split()[2].rstrip(":") for ln in out.splitlines()
                   if ln.strip().startswith("--- FAIL:")})


def apply_edits(m, baks):
    """Back up every file the mutation touches, then edit. Returns a SKIP reason."""
    edits = m.get("edits") or [{"file": m["file"], "find": m["find"],
                                "replace": m["replace"], "count": m.get("count", 1)}]
    for fn in sorted({e.get("file", m.get("file")) for e in edits}):
        path = os.path.join(REPO, fn)
        baks[fn] = path + ".mutbak"
        shutil.copy2(path, baks[fn])
    for e in edits:
        fn = e.get("file", m.get("file"))
        path = os.path.join(REPO, fn)
        src = open(path).read()
        n = src.count(e["find"])
        want = e.get("count", 1)
        if n != want:
            return f"{fn}: pattern occurs {n}x, want {want}"
        open(path, "w").write(src.replace(e["find"], e["replace"]))
    return None


def main(manifest, only):
    muts = json.load(open(manifest))
    if only:
        muts = [m for m in muts if m["id"] in only]
    print(f"disk free before: {disk_free_mb()} MB", flush=True)

    results = []
    for m in muts:
        if disk_free_mb() < MIN_FREE_MB:
            print(f"ABORT: {disk_free_mb()} MB free; a full disk reads as all-caught", flush=True)
            break
        baks = {}
        try:
            skip = apply_edits(m, baks)
            if skip:
                print(f"[{m['id']}] SKIP: {skip}", flush=True)
                results.append({**m, "verdict": "SKIP", "detail": skip})
                continue
            rc, out, secs = run_suite()
            tests = failing_tests(out)
            if rc == 0:
                verdict, detail = "GREEN", ""
            elif not tests and "build failed" in out:
                verdict, detail = "BUILDFAIL", out.strip().splitlines()[0][:200]
            else:
                verdict, detail = "RED", ", ".join(tests[:6]) or out.strip().splitlines()[-1][:200]
            print(f"[{m['id']}] {verdict} ({secs:.0f}s) {m['note']}\n      {detail}", flush=True)
            results.append({**m, "verdict": verdict, "detail": detail, "failing": tests})
        finally:
            for fn, bak in baks.items():
                if os.path.exists(bak):
                    shutil.move(bak, os.path.join(REPO, fn))

    green = [r for r in results if r["verdict"] == "GREEN"]
    print(f"\ndisk free after: {disk_free_mb()} MB")
    print(f"=== {len(green)} GREEN of {len(results)} ===")
    for r in green:
        print(f"  {r['id']}: {r.get('file', '(multi)')} — {r['note']}")
    print("\nA green is a candidate, not a finding: read it before writing a test. "
          "DURABILITY.md's \"Not every green is a hole\" has the three kinds.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    main(sys.argv[1], sys.argv[2:])
