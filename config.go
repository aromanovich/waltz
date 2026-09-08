package waltz

import (
	"fmt"
	"strings"

	"github.com/mitchellh/mapstructure"

	"github.com/aromanovich/waltz/cycle"
)

// SectionKey is the key the WAL layer's configuration lives under, inside the
// custom datastore's options. A deployment or a test writing a config writes
// this key.
const SectionKey = "wal"

// WAL is the `wal` section verbatim. The yaml key names are a promise to
// whoever wrote the file, so they are this package's struct and not
// [cycle.Config] with tags on it.
//
// The section is what the layer is; [settings] is how it behaves. A misspelt
// key here is a refusal to start; a misspelt dynamic-config key is a warning
// and the default standing silently.
type WAL struct {
	// Sync drains inside every write and reports the drain's outcome to the
	// caller. Off by default, so the shipped configuration is the windowed one
	// and a hard restart leaves a tail for the next owner to replay.
	Sync bool `mapstructure:"sync"`

	// DrainOnRead answers every read from the cold store, draining the window
	// first — see [cycle.Config.DrainOnRead] for what it costs.
	DrainOnRead bool `mapstructure:"drain_on_read"`
}

// knob is one key of the section: its yaml name, the rule joining the [WAL]
// field to the [cycle.Config] field it sets, and the two ends as pointers so
// the constructors cannot pair a duration with a count.
type knob struct {
	key   string
	apply func(*WAL, *cycle.Config)

	// walField and configField are the guard's, recovering a row's fields by
	// address; nothing on a running node reads them.
	walField    func(*WAL) any
	configField func(*cycle.Config) any
}

// always assigns whatever the key decoded to, a bool having no spare value left
// to mean unset. Sound only because both flags are false in [cycle.Defaults].
func always[T any](key string, from func(*WAL) *T, to func(*cycle.Config) *T) knob {
	return knob{
		key:         key,
		apply:       func(w *WAL, c *cycle.Config) { *to(c) = *from(w) },
		walField:    func(w *WAL) any { return from(w) },
		configField: func(c *cycle.Config) any { return to(c) },
	}
}

// knobs is the section key by key: the one place the yaml's vocabulary and
// [cycle.Config]'s meet, and what the overlay is generated from. A knob added
// to [WAL] and left off here parses, validates and does nothing;
// TestEveryKnobReachesADistinctPolicyField fails it by name.
var knobs = []knob{
	always("sync",
		func(w *WAL) *bool { return &w.Sync },
		func(c *cycle.Config) *bool { return &c.Sync }),
	always("drain_on_read",
		func(w *WAL) *bool { return &w.DrainOnRead },
		func(c *cycle.Config) *bool { return &c.DrainOnRead }),
}

// StaticConfig is [cycle.Defaults] with each [knobs] row the section set
// overlaid; an empty section is exactly [cycle.Defaults].
//
// It is only the static half of what a node runs — [NewPolicy] is the whole —
// so every field [settings] carries is at its default here. Assert a section
// against this; read a live watermark off [NewPolicy].
func (w WAL) StaticConfig() cycle.Config {
	c := cycle.Defaults()
	for _, k := range knobs {
		k.apply(&w, &c)
	}
	return c
}

// Config is one node's WAL configuration as this layer reads it: whether the
// layer is on at all, and the section saying how it behaves. The rest of the
// custom datastore's options belong to the plugin holding the cold store, and
// this package neither reads nor validates them.
//
// Enabled false is passthrough and not an error; the zero Config is that.
type Config struct {
	Enabled bool
	WAL     WAL
}

// Parse reads the `wal` section out of a custom datastore's options. An absent
// section is passthrough with no error, and naming the section is what turns
// intercept mode on.
//
// An unknown key inside the section is an error, because a misspelt `snyc`
// would otherwise be a server silently running the other mode. An unknown key
// outside the section stays the cold store plugin's business, except one spelt
// like the section itself ([checkSectionMiscased]).
func Parse(options map[string]any) (Config, error) {
	raw, ok := options[SectionKey]
	if !ok {
		return Config{}, checkSectionMiscased(options)
	}

	w, err := decodeSection(raw)
	if err != nil {
		return Config{}, err
	}
	return Config{Enabled: true, WAL: w}, nil
}

// checkSectionMiscased refuses an options map whose only section-shaped key
// differs from [SectionKey] in case. Both parsers drop it in silence — this one
// indexes the map, which is case-sensitive, and the cold store plugin's decoder
// has no field for it — so `WAL:` is a node coming up in passthrough under a
// file that asked for intercept, and nothing downstream can see it.
//
// Case alone, and no other near miss: an unknown top-level key is the plugin's
// business and stays allowed (ADR 0006), so a refusal here that reached past
// capitalisation would be a server refusing to start over a key that is not
// this section's.
func checkSectionMiscased(options map[string]any) error {
	for key := range options {
		if key != SectionKey && strings.EqualFold(key, SectionKey) {
			return fmt.Errorf("waltz: the custom datastore options carry %q and this section's key is "+
				"%q: the layer reads it case-sensitively, so a section it cannot see is a node running "+
				"passthrough", key, SectionKey)
		}
	}
	return nil
}

// decodeSection decodes the section's keys and refuses any key that is not one.
// Separate from [Parse] so the guard over [knobs] can drive every key at once
// without also satisfying the node-level checks Parse runs afterwards.
func decodeSection(raw any) (WAL, error) {
	// Before the decoder, so a key that moved is told where it went rather than
	// reported as unrecognised ([movedKey]). It runs off the raw map because
	// moved keys are deliberately absent from [WAL].
	if m, ok := raw.(map[string]any); ok {
		for key := range m {
			if err := movedKey(key); err != nil {
				return WAL{}, err
			}
		}
	}

	var w WAL
	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		Result:      &w,
		ErrorUnused: true,
		// The two the server's own config reading has: yaml numbers and
		// durations written the way every other duration in the file is. No key
		// of the section is a duration today; the hook stays so a duration key
		// added later is not silently read as a string.
		WeaklyTypedInput: true,
		DecodeHook:       mapstructure.StringToTimeDurationHookFunc(),
	})
	if err != nil {
		return WAL{}, fmt.Errorf("waltz: building the %q decoder: %w", SectionKey, err)
	}
	if err := decoder.Decode(raw); err != nil {
		return WAL{}, fmt.Errorf("waltz: the %q section of the custom datastore options: %w", SectionKey, err)
	}
	return w, nil
}
