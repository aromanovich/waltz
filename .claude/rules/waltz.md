---
paths:
  - "*.go"
---

# This repo: the root package's composition

The root package (`waltz`) is what a running server composes the layer out of:
the `wal` section of the custom datastore's options, the policy the server's
dynamic config carries, the backends the layer runs over, the registry a tail is
decoded with, and the door out to `temporal.WithCustomDataStoreFactory`. It is
the whole public surface — a caller writes `waltz.` and stops. What to know
before changing any of it:

* **`Compose` is the one composition, and there may not be a second.** Everything
  that runs intercept mode calls it. So do not add a `cycle.NewManager` call
  anywhere else — what a new caller needs is a *parameter*, and the ones that
  exist (backends, policy, categories, logger, handler) are exactly what callers
  vary. **The policy stays a parameter**: a run that varies the window varies it
  and nothing else. It is a `cycle.Policy` — a source read at each decision — so
  a caller holding numbers passes `cycle.Fixed(cfg)` and one reading a dynamic
  config passes `NewPolicy(dc, cfg.WAL)`. `handler` is nil in production — the
  server's arrives later through `wrapper.MetricsSink` — and a noop handed in
  here is worse than nil, because an emitter that already has one ignores the
  handover;
* **the collaborators are a parameter too, and that is what makes the rule above
  keepable.** `Backends` — the log, the writer and the recoverer — is handed to
  `Compose`, which builds none of the three. That is unchanged by there being a
  shipped implementation of each (`wal/memwal`, `cold/memcold`): a composition
  reaching for one itself would be a second configuration of the store, with
  nothing to reconcile it against the one the server was handed, and the caller
  is where both are constructed. While a composition
  built them itself, the rule had no way to be obeyed: `wal.Log` is an interface
  with implementations outside this module (ADR 0002) and `cold.Applier` exists
  so a drain's outcome can be varied without a cluster, and no composition could
  reach either. So a second `cycle.NewManager` grew in the tests, twice.
  `Compose` opens nothing, reaches nothing and takes no context, so a
  composition is two lines and needs no store at all
  (`TestTheCompositionIsReachableWithoutAColdStore`). Whatever the backends hold
  stays the caller's and must outlive the layer, since `Shutdown` drains through
  it;
* **what a composed `Layer` hands back is narrow reads, not the registry.**
  `Totals()` and `ShardStats(id)` are what callers outside this package actually
  want; handing out `cycle.Manager` would also hand out `Manager.Write` and the
  three reads — a second door onto the write path with no wrapper and no epoch
  check in front of it. So resist adding it back for a caller's convenience:
  what that caller needs is a method as narrow as its question. **That rule
  applied to `Shard(id)` itself**: it would hand back the `*cycle.Cycle`, so a
  caller wanting counters would also hold `Retire` and `Close`, where what
  callers ask for is the counters, the epoch and whether the shard is held.
  `ShardStats` answers all three as a value — `cycle.Stats` carries `Epoch` for
  it, a cycle's never moving — and the one caller that kills a cycle says so by
  name, `RetireShard`. What it costs a reader is that a `Stats` is a snapshot
  and not a handle: counters wanted after some event are read again after it.
  `Layer.AbstractFactory` is the method that is not a read: pairing a
  composition with the abstract factory that carries it was written out at every
  call site, and a layer paired with a zero `wrapper.Options` is a node running
  passthrough under a config that says intercept — which no suite can see,
  passthrough behaving identically by construction;
* **the section lives inside the datastore's own `options` map**, and this
  package reads that one key and nothing else
  ([ADR 0006](../../docs/adr/0006-the-wal-configuration-is-a-section-of-the-datastore-options.md)).
  The rest of the map is the cold store plugin's, parsed by the plugin's own
  parser in the binary that builds the base factory — there is deliberately no
  second decoder here to agree with it;
* **the decoder is strict where a plugin's is not** (`ErrorUnused`), and a
  section-shaped key that differs only in case is refused. Both are the same
  judgement: the failure mode of a lenient parse here is a node running the
  other mode with a config file that says otherwise, and nothing anywhere
  reporting it. An **absent** section is still passthrough with no error, which
  is the compatibility claim;
* **the yaml keys are this package's struct, not `cycle.Config` with tags on
  it.** The key names are a promise to whoever wrote the file; the policy's
  fields are the layer's own and get renamed when the layer learns something.
  What keeps the two in step without binding them is
  `TestAnEmptySectionIsTheMeasuredPolicy` — an empty section must equal
  `cycle.Defaults()`, so nobody can leave a second copy of the defaults here;
* **adding a knob is one row in `knobs`**, which is where the two vocabularies
  meet and what `WAL.StaticConfig` is generated from. A knob added to `WAL` and
  left off the table fails `TestEveryKnobReachesADistinctPolicyField` by name, so
  do not hand-write an overlay branch beside it. It is two rows, and the
  zero-means-default rule went with the numbers — an unset *setting* is a key the
  file does not have. **That is a statement about this package and not about the
  tree**: a caller configured from flags has no key to be absent, so an overlay
  onto `WAL.StaticConfig()` reads zero as "the shipped default";
* **there are two tables and the split is a rule, not a preference** (ADR 0006's
  amendment). `knobs` is the section: the two mode flags, no numbers. `settings`
  (`settings.go`) is the whole policy — nine `dynamicconfig` settings, five read
  at the decision (`moves`) and four read once when the policy is built
  (`atStart`). **The line is the cost of a typo, not liveness**: the section's
  decoder is strict, so a misspelt key there is a refusal to start, while a
  misspelt dynamic-config key is `WARN unregistered key` and the default
  standing silently — which is tolerable for a number and not for the mode. A
  new knob is a `settings` row unless a typo in it would change what the node
  *is*. Putting it on both tables is the precedence rule the amendment says this
  design does not have, and `TestEveryPolicyFieldIsConfigurableOnce` fails either
  way round. Do not add a yaml key that shadows a setting — a key that moved is
  refused by name (`movedKey`), and that refusal is the migration;
* **the operator-facing table in the handbook is the operator's copy of those
  two, and it is maintained by hand** ([08-configuration.md](../../docs/handbook/08-configuration.md);
  the test that read the markdown back was deleted for parsing prose to do it).
  It is the deliverable of the consolidation rather than decoration — the
  audience is whoever adds this section to a cluster's config — so **a key added,
  renamed, re-homed or given a different default is edited there in the same
  commit**, and nothing will tell you if it is not. Its "when it is read" column
  is the `live` field of a row, which is why that field exists;
* **the budget refusal happens before anything is opened**, and that ordering is
  the claim: `hard_max_bytes × max_shards ≤ tail_budget_bytes` is
  `cycle.NewManager`'s, reached through `Compose`, which opens nothing and takes
  no context. So an operator whose numbers do not fit is told so by a process
  that never connected, rather than after a connection attempt;
* **`WAL.StaticConfig()` is the section's half and `NewPolicy(dc, w)` is the
  whole.** The first returns `cycle.Defaults()` with the two mode flags overlaid,
  so it is what an assertion about a *section* compares against; every number in
  it is the measured default and is replaced by the second. Reading a configured
  watermark or bound off `WAL.StaticConfig()` reads the default and nothing says
  so;
* **the default is the windowed policy**, so a node killed without a graceful
  stop leaves a tail — which the next owner replays. Do not "fix" the default to
  sync: a default that quietly picked the degenerate window would make "the
  server works" mean less than it says;
* **the registry the layer decodes a tail with comes from here** and is the
  server's own (`TaskCategories`), built by the caller because the layer is
  composed before the fx graph that would otherwise provide it. It is two calls
  into upstream on purpose: a copy of the archival rule would be a second place
  for it to be wrong. `Compose` takes a `waltz.Registry`, which only
  `TaskCategories` and `DefaultTaskCategories` build, so
  `tasks.NewDefaultTaskCategoryRegistry()` no longer reaches a composition — the
  same set today and a different one the day archival is configured, which is
  exactly what this type says. `cycle.Deps` still takes upstream's interface,
  because `mutation.Decode` needs it, and `Registry.Categories` is the unwrap.
  The type does not close the whole of it, so the residue is a rule: **do not
  build the no-archival answer inline**, which `TaskCategories` with a noop
  collection still would;
* **the order is layer-then-server, and close-after-`Start`.** The budget
  assertion has to be a process that does not start; the drain has to run when
  the writers are gone. Moving either is not a refactor;
* **stopping the layer is `Layer.Shutdown(ctx, budget)`, and the detach inside it
  is the point.** A shutdown drain runs where a context has just been
  cancelled — that is what shutdown means — so the budget goes on a context of
  the layer's own (`context.WithoutCancel`). Callers used to write that idiom and
  one of them had it; the rest would have returned at once and left a tail the
  next owner replays, which no log line reports.
  `TestTheShutdownDrainOutlivesTheContextThatAsksForIt` drives it with the
  caller's context already cancelled, and fails if the detach goes. The log is
  closed *after* the drain, since a drain appends.
