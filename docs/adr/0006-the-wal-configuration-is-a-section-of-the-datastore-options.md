# 6. The WAL layer's configuration is a section of the datastore's options

Date: 2026-08-01

## Status

Accepted.

**Amended** — see the last section. What the amendment changes is which knobs the section holds:
it keeps the two mode flags, and every number of the policy is a setting of the server's dynamic
config. Where the layer's configuration lives is unchanged.

## Context

A custom `main` receives the persistence configuration as an opaque `map[string]any` and hands it
to `temporal.WithCustomDataStoreFactory` unparsed. The layer needs to read its own mode out of
that map — whether it is on at all, and how it behaves — while the rest of the map belongs to the
plugin holding the cold store and must reach that plugin untouched.

Three routes, and each has a different cost to a deployment that already exists:

1. **a `wal` section inside the custom datastore's own `options`**;
2. **a new top-level section** in the server's config file, beside `persistence`;
3. **environment variables**, or a second configuration file this binary reads itself.

## Decision

Route 1: a `wal` section inside `persistence.datastores.<default>.customDatastore.options`, read
by `waltz.Parse`, which reads that key and nothing else.

Three properties decide it.

**The options map is the one place the format already allows keys the server does not know.**
`config.CustomDatastoreConfig.Options` is `map[string]any` by design — it is where an
implementation outside the server's tree puts its own settings, and the cold store plugin's own
connection settings are already there.

**A plugin ignores what it has no field for.** The decoders these plugins use drop unknown keys, so
the section is invisible to the one underneath and costs an existing deployment nothing at all — a
file with no `wal` key parses exactly as it did, and comes up in passthrough.
`TestTheSectionParsesBesideTheColdStoresOwnKeys` pins that the two vocabularies coexist; whether a
particular plugin drops the section rather than refusing it is that plugin's property, and a
deployment whose plugin is strict has to be told about the key.

**The mode belongs with the store it is a mode of.** The section says how this datastore's
ExecutionStore and ShardStore behave. A top-level section would say it somewhere the datastore's own
settings are not, and allow the two statements to be about different datastores.

The same placement settles what this package does *not* do: **it neither reads nor validates the
rest of the map.** The plugin's own keys are parsed by the plugin's own parser, in the binary that
builds the base factory — so the endpoint the cold store connects to is by construction the endpoint
the server's own factory will connect to, from the same keys, with the same validation, and there is
no second decoder here to agree with it.

Two further choices follow from where the section sits, and both are refusals:

* **an unknown key inside the section is an error**, though an unknown key outside it stays the
  plugin's business. The section's decoder runs with `ErrorUnused`, because the alternative to
  refusing `snyc: true` is a node running the other mode with a config file that says otherwise
  and nothing anywhere reporting it (`TestAnUnknownKeyIsARefusal`);
* **a section-shaped key that differs only in case is an error.** `WAL:` is dropped by both
  parsers in silence — this one indexes the map, which is case-sensitive, and the plugin has no
  field for it — so it is a node coming up in passthrough under a file that asked for intercept,
  and nothing downstream can see it (`TestAMiscasedSectionKeyIsARefusal`). Case alone and no other
  near miss: an unknown top-level key is the plugin's business and stays allowed, so a refusal
  reaching past capitalisation would be a server refusing to start over a key that is not this
  section's.

## Consequences

**Intercept mode is one line of yaml.** `wal: {}` is the whole minimum; everything in the section
defaults to `cycle.Defaults()`, the measured policy, field for field —
`TestAnEmptySectionIsTheMeasuredPolicy` is what stops a second copy of those numbers growing here,
and so does everything the section no longer holds: the settings take their defaults from
`cycle.Defaults()` by reference.

**The keys are a compatibility surface, and they are this package's struct.** `waltz.WAL` carries
the `mapstructure` tags rather than `cycle.Config` doing so: the key names are a promise to
whoever wrote the file, while the cycle's field names are the layer's own and get renamed when the
layer learns something.

**Configuration errors are refusals to start.** The parse runs before `temporal.NewServer`, so an
unknown key or a tail budget the node cannot hold is a non-zero exit and no listening port, rather
than a log line from a server that is already serving.

**A second custom datastore is out of scope.** The layer reads the *default* store's options,
since the ExecutionStore and ShardStore it decorates are the default store's. A section on a
non-default store would be read by nobody, which is the failure mode to watch for if a deployment
grows one.

## Considered and not taken: a top-level section

A `wal:` block beside `persistence:` reads well and is where a person might look first. Against
it: `config.Config` is a fixed struct that the server's own loader unmarshals, so a key it does
not know is dropped before the binary ever sees the file — reading it would mean loading and
parsing the yaml a second time, in the caller's module, with its own template expansion and its own
environment substitution to keep in step with the server's.

## Considered and not taken: environment variables or a second file

Cheapest to write. But a deployment configures a temporal-server with one file, mounted and
versioned as one thing; a persistence mode that came from somewhere else would be a mode that does
not appear in the artifact people review. Env vars are also the one configuration source that
cannot be validated before the process starts.

## Amendment: the policy is the server's dynamic config; the section is the mode

The section was eleven knobs read exactly once, at start-up. It is **two keys** now — `sync` and
`drain_on_read` — and every *number* of the policy is a setting of the server's own dynamic
config under the same `wal.` name: `wal.windowMutations`, `wal.windowBytes`, `wal.windowAge`,
`wal.trimEvery`, `wal.trimAfter`, `wal.hardMaxEntries`, `wal.hardMaxBytes`, `wal.maxShards`,
`wal.tailBudgetBytes`.

It landed in two steps and the second corrected the first's cut. The first split the surface by
*liveness* — what a node can change while it runs stayed on one side and what it cannot on the
other. That line is wrong for the person this configuration is for. They are enriching a cluster's
config with a section for this persistence layer, and "which file is `hard_max_bytes` in" does not
follow from the name; worse, the server's own tradition already includes dynamic-config settings
that need a restart, so "read once" was never a reason to keep a key out of that file.

**The line is the cost of a typo, and only that.** The section's decoder is strict (`ErrorUnused`),
so a misspelt key there is a refusal to start. A misspelt dynamic-config key is
`WARN unregistered key` and the default standing silently — `file_based_client.go` warns and loads.
So:

* `sync` and `drain_on_read` are on the strict surface. `snyc: true` must not be a node quietly
  running the other mode;
* every number is on the consolidated surface. A mistyped `wal.windowMutatoins` is a node at the
  measured policy — a different order of wrong, and the price of having one place to look.

**Four of the nine are read once, and they say so.** `hard_max_bytes × max_shards ≤
tail_budget_bytes` is asserted when the policy is turned into a composition, so a factor that could
move afterwards would be that refusal with nothing behind it; `hardMaxEntries` is I10's other unit,
read in the same statement as `hardMaxBytes`. Their descriptions carry `READ AT START-UP` and
`TestAStartOnlySettingDoesNotMoveUnderTheNode` holds it. The refusal itself is `cycle.NewManager`'s,
reached through `Compose`, which opens nothing and reaches nothing — so it is still a binary that
does not start rather than a connection attempt followed by a complaint.

**They moved; they never gained a second home.** A key that used to be in the section is refused by
name, and the message says which setting to write instead and whether it needs a restart
(`movedKey`, driven by the same table the settings are declared in;
`TestAKnobThatMovedIsRefusedByName`). A surface *beside* the section would be two vocabularies and
a precedence rule to argue over, with one spelling silently ignored.

**What a setting buys over a key.** `dynamicconfig.NewGlobalIntSetting(name, default, description)`
is one statement carrying the operator-facing name, the default and the description — which for a
section key were a `knobs` row, a `cycle.Defaults()` entry and a doc comment on `waltz.WAL`, in
three places, agreeing by hand. The defaults are `cycle.Defaults()` by reference, not by copy. And
the zero-means-default rule disappeared with the numbers: it existed only because a decoded struct
cannot tell "not written" from "written as zero", while an unset setting is a key the file does not
have.

**The reference is the deliverable.** The handbook's configuration chapter
([08-configuration.md](../handbook/08-configuration.md)) carries one table — key, where it lives,
when it is read, what it defaults to — as the operator's copy of `knobs` and `settings`. It is
maintained by hand: a key added, renamed, re-homed or given a different default is edited there in
the same commit, and nothing reports it if it is not. `TestEveryPolicyFieldIsConfigurableOnce` is
the same claim from the other side: every exported `cycle.Config` field is claimed by exactly one
of the two tables.

**A process with no config file states the same settings.** A caller configuring the layer from
flags builds a `dynamicconfig.StaticClient` over the exported settings and hands the collection to
`NewPolicy`; a caller that already holds numbers passes `cycle.Fixed` to `Compose` instead. Both
are deliberately the production reading path rather than a second one — the failure this design
exists to prevent, one vocabulary spelled twice, is the same failure there.
