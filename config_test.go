package waltz

// The `wal` section: what it parses into, and every shape it refuses. No
// cluster and no composition — [Compose] and its graph are waltz_test.go.

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/aromanovich/waltz/cycle"
)

// storeOptions builds a custom datastore's options map — keys belonging to the
// plugin that holds the cold store, which this layer never reads — with the
// `wal` section added when wal is non-nil.
func storeOptions(wal map[string]any) map[string]any {
	options := map[string]any{
		"endpoint": "cold.example.net:2135",
		"database": "/local",
	}
	if wal != nil {
		options[SectionKey] = wal
	}
	return options
}

// Options with no section are neither intercept-with-defaults nor an error.
func TestNoSectionIsPassthrough(t *testing.T) {
	cfg, err := Parse(storeOptions(nil))
	require.NoError(t, err)
	require.Equal(t, Config{}, cfg)
}

// And writing the section is the whole of turning the layer on, so an empty one
// is intercept mode rather than the passthrough above.
func TestAnEmptySectionTurnsTheLayerOn(t *testing.T) {
	cfg, err := Parse(storeOptions(map[string]any{}))
	require.NoError(t, err)
	require.True(t, cfg.Enabled)
}

// A section that says nothing else is cycle.Defaults field for field. Asserted
// as equality rather than a list of numbers, so no second copy of the defaults
// can drift into this package.
func TestAnEmptySectionIsTheMeasuredPolicy(t *testing.T) {
	require.Equal(t, cycle.Defaults(), WAL{}.StaticConfig())
}

// fillWAL gives every exported field of a WAL a value unlike its neighbours'
// and unlike every cycle.Defaults value, so a knob landing on the wrong field
// reads as a wrong number. By reflection, so a field added later is filled too.
func fillWAL(t *testing.T, w *WAL) {
	t.Helper()
	v := reflect.ValueOf(w).Elem()
	n := int64(0)
	next := func() int64 { n++; return 100*n + n }
	for i := range v.NumField() {
		f, name := v.Field(i), v.Type().Field(i).Name
		switch f.Kind() {
		case reflect.Bool:
			// The one value distinguishable from the default, both flags being
			// off in cycle.Defaults.
			f.SetBool(true)
		case reflect.Int, reflect.Int64:
			f.SetInt(next())
		default:
			t.Fatalf("WAL.%s is a %s, which this guard cannot drive: give it a case here, "+
				"because a field it cannot fill is a knob it cannot check", name, f.Type())
		}
	}
}

// fieldName reports, by pointer identity, which exported field of the struct
// behind v the pointer p addresses, or "" for none of them. Unexported fields
// are skipped: in cycle.Config that is the clock, which no key may reach.
func fieldName(v reflect.Value, p any) string {
	for i := range v.NumField() {
		if !v.Type().Field(i).IsExported() {
			continue
		}
		if v.Field(i).Addr().Interface() == p {
			return v.Type().Field(i).Name
		}
	}
	return ""
}

// The knobs table's guard, in three claims: every exported WAL field is on the
// table, every row's yaml name is the mapstructure tag of the field it reads,
// and every row lands its own value on a cycle.Config field of its own.
func TestEveryKnobReachesADistinctPolicyField(t *testing.T) {
	var w WAL
	fillWAL(t, &w)
	wv := reflect.ValueOf(&w).Elem()

	onTable := map[string]string{} // WAL field -> the yaml key that reads it
	for i, k := range knobs {
		name := fieldName(wv, k.walField(&w))
		require.NotEmpty(t, name,
			"knobs[%d] (%q) does not read an exported field of WAL", i, k.key)
		if first, again := onTable[name]; again {
			t.Fatalf("WAL.%s is read by two rows, %q and %q: one of the two keys sets a "+
				"policy field from somebody else's value", name, first, k.key)
		}
		onTable[name] = k.key

		field, _ := wv.Type().FieldByName(name)
		require.Equal(t, field.Tag.Get("mapstructure"), k.key,
			"the %q row reads WAL.%s, whose yaml key is %q: the tag is the promise to whoever "+
				"wrote the file, so the row is the side that is wrong", k.key, name, field.Tag.Get("mapstructure"))
	}
	for i := range wv.NumField() {
		name := wv.Type().Field(i).Name
		if !wv.Type().Field(i).IsExported() {
			continue
		}
		require.Contains(t, onTable, name,
			"WAL.%s is a key of the section that reaches no cycle.Config field: it decodes, it "+
				"passes the strict decoder, and it does nothing. Add it to knobs or say here why it "+
				"is an exception", name)
	}

	c := w.StaticConfig()
	cv := reflect.ValueOf(&c).Elem()
	landed := map[string]string{} // cycle.Config field -> the yaml key that set it
	for _, k := range knobs {
		ptr := k.configField(&c)
		name := fieldName(cv, ptr)
		require.NotEmpty(t, name, "the %q row does not set an exported field of cycle.Config", k.key)
		if first, again := landed[name]; again {
			t.Fatalf("%q and %q both land on cycle.Config.%s: one of the two keys is a key "+
				"a config file can set and the node will ignore", first, k.key, name)
		}
		landed[name] = k.key
		require.Equal(t, reflect.ValueOf(k.walField(&w)).Elem().Interface(),
			reflect.ValueOf(ptr).Elem().Interface(),
			"the section set %q, and cycle.Config.%s did not take its value", k.key, name)
	}

	// The half Policy cannot see: every key decodes into the field its row
	// reads. The whole struct is compared, so a key the decoder dropped shows as
	// a zero where a distinctive value belongs.
	section := map[string]any{}
	for i := range wv.NumField() {
		f := wv.Field(i)
		key := wv.Type().Field(i).Tag.Get("mapstructure")
		if d, ok := f.Interface().(time.Duration); ok {
			// Written as a person writes one, so the decode hook is on the path
			// here too.
			section[key] = d.String()
			continue
		}
		section[key] = f.Interface()
	}
	decoded, err := decodeSection(section)
	require.NoError(t, err)
	require.Equal(t, w, decoded)
}

// The section is read out of a map whose other keys belong to the cold store's
// own plugin, and they do not disturb it.
func TestTheSectionParsesBesideTheColdStoresOwnKeys(t *testing.T) {
	cfg, err := Parse(storeOptions(map[string]any{"sync": true}))
	require.NoError(t, err)
	require.True(t, cfg.Enabled)
	require.True(t, cfg.WAL.StaticConfig().Sync)
}

// A misspelt knob must not silently be that knob at its default, which is a
// node running a mode nobody chose.
func TestAnUnknownKeyIsARefusal(t *testing.T) {
	_, err := Parse(storeOptions(map[string]any{"snyc": true}))
	require.ErrorContains(t, err, "snyc")
}

// The section key is the one key whose misspelling nothing else catches: the
// layer indexes the map for it and the cold store plugin's decoder drops what
// it has no field for, so a miscased section is a node in passthrough under a
// file that asked for intercept.
func TestAMiscasedSectionKeyIsARefusal(t *testing.T) {
	for _, key := range []string{"WAL", "Wal", "wAl"} {
		t.Run(key, func(t *testing.T) {
			options := storeOptions(nil)
			options[key] = map[string]any{"sync": true}

			_, err := Parse(options)
			require.ErrorContains(t, err, key)
			require.ErrorContains(t, err, SectionKey)
		})
	}
}

// And the compatibility claim the refusal above may not cost: an absent section
// is passthrough with no error, so a key this layer does not know is the cold
// store plugin's business rather than a server that will not start.
func TestAnUnknownOptionThatIsNotTheSectionIsStillPassthrough(t *testing.T) {
	options := storeOptions(nil)
	options["some_key_this_layer_has_never_heard_of"] = 42

	cfg, err := Parse(options)
	require.NoError(t, err)
	require.False(t, cfg.Enabled)
}
