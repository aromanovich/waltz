package waltz

import (
	"maps"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"

	"github.com/aromanovich/waltz/cycle"
)

// section is the smallest section there is: present and saying nothing, so a
// policy built from it is [cycle.Defaults] plus whatever the dynamic config
// says.
var section = WAL{}

// filled is [cycle.Defaults] as a node holds it — the same numbers with the
// clock a policy constructor puts there. Bare [cycle.Defaults] has a nil clock,
// which would read as a knob gone missing.
func filled() cycle.Config { return cycle.Fixed(cycle.Defaults())() }

// collection builds a dynamic config holding exactly these keys.
func collection(t *testing.T, values map[dynamicconfig.Key]any) *dynamicconfig.Collection {
	t.Helper()
	client := dynamicconfig.StaticClient{}
	maps.Copy(client, values)
	return dynamicconfig.NewCollection(client, log.NewNoopLogger())
}

// distinct builds a value for the field this row lands on, unlike every
// [cycle.Defaults] value and unlike its neighbours', so a setting that landed on
// the wrong field reads as a wrong number. A duration gets milliseconds so it
// cannot be confused with the count of the same magnitude.
func distinct(t *testing.T, field any, n int) any {
	t.Helper()
	switch p := field.(type) {
	case *int:
		return 100*n + n
	case *time.Duration:
		return time.Duration(100*n+n) * time.Millisecond
	default:
		t.Fatalf("a setting lands on a %T, which this guard cannot drive: give it a case here, "+
			"because a field it cannot fill is a setting it cannot check", p)
		return nil
	}
}

// [settings]' guard: every row lands its own value on a [cycle.Config] field of
// its own. One key is overridden at a time and the whole policy compared, so a
// row bound to its neighbour's field, or one that moves a second field, fails.
func TestEverySettingReachesItsOwnPolicyField(t *testing.T) {
	base := NewPolicy(nil, section)()
	require.Equal(t, filled(), base,
		"a section that names only a folder and a dynamic config that says nothing is the measured policy")

	landed := map[string]dynamicconfig.Key{} // cycle.Config field -> the setting that sets it
	for i, s := range settings {
		want := base
		wv := reflect.ValueOf(&want).Elem()

		name := fieldName(wv, s.field(&want))
		require.NotEmpty(t, name, "settings[%d] (%q) does not set an exported field of cycle.Config", i, s.key)
		if first, again := landed[name]; again {
			t.Fatalf("%q and %q both land on cycle.Config.%s: one of the two is a setting an "+
				"operator can write and the node will ignore", first, s.key, name)
		}
		landed[name] = s.key

		value := distinct(t, s.field(&want), i+1)
		reflect.ValueOf(s.field(&want)).Elem().Set(reflect.ValueOf(value))

		got := NewPolicy(collection(t, map[dynamicconfig.Key]any{s.key: value}), section)()
		require.Equal(t, want, got,
			"the dynamic config set %q, and the policy is not the measured one with cycle.Config.%s "+
				"changed and nothing else", s.key, name)
	}
}

// Between the section and the dynamic config, every field of [cycle.Config] a
// deployment can set is claimed by exactly one of them. A field on neither
// reaches no configuration at all; a field on both is one number with two
// surfaces that can disagree, and there is no precedence rule between them.
func TestEveryPolicyFieldIsConfigurableOnce(t *testing.T) {
	var c cycle.Config
	cv := reflect.ValueOf(&c).Elem()

	claimed := map[string]string{} // cycle.Config field -> what claims it
	claim := func(name, by string) {
		if first, again := claimed[name]; again {
			t.Fatalf("cycle.Config.%s is set by %s and by %s: one number, two configuration "+
				"surfaces, and a precedence rule nobody wrote", name, first, by)
		}
		claimed[name] = by
	}
	for _, k := range knobs {
		claim(fieldName(cv, k.configField(&c)), "the "+SectionKey+" section's "+k.key)
	}
	for _, s := range settings {
		claim(fieldName(cv, s.field(&c)), "the dynamic config's "+s.key.String())
	}

	for i := range cv.NumField() {
		f := cv.Type().Field(i)
		if !f.IsExported() {
			// The clock, which no configuration may reach.
			continue
		}
		require.Contains(t, claimed, f.Name,
			"cycle.Config.%s is a policy field nothing configures: put it on knobs (if a node "+
				"cannot change it while it runs) or on settings (if it can), or say here why it "+
				"is neither", f.Name)
	}
}

// A legacy `wal` key is refused by name and pointed at the replacement setting,
// rather than reported as "invalid keys".
func TestAKnobThatMovedIsRefusedByName(t *testing.T) {
	for _, s := range settings {
		_, err := Parse(storeOptions(map[string]any{s.was: 64}))
		require.ErrorContains(t, err, s.was, "the refusal names the key the file actually has")
		require.ErrorContains(t, err, s.key.String(), "and the setting to write instead")
	}
}

// A live setting through a real dynamic config client: the policy a composed
// node holds answers with the new number at the next call, no re-composition
// and no re-acquire. The rest of the policy must stand unchanged.
func TestAChangeTakesEffectWithoutRebuildingThePolicy(t *testing.T) {
	client := dynamicconfig.NewMemoryClient()
	policy := NewPolicy(dynamicconfig.NewCollection(client, log.NewNoopLogger()), section)

	require.Equal(t, filled(), policy(), "nothing overridden is the measured policy")

	cleanup := client.OverrideValue(WindowMutations.Key(), 512)
	want := filled()
	want.Mutations = 512
	require.Equal(t, want, policy(),
		"the same policy value answers with the new window: no re-composition, no re-acquire")

	cleanup()
	require.Equal(t, filled(), policy(), "and an override withdrawn is the default again")
}

// The start-only settings are read when the policy is built and a change needs
// a restart, as their descriptions say. A bound that quietly started moving
// would leave [cycle.Config.CheckBudget], asserted before the node booted,
// standing over numbers the node no longer runs at.
func TestAStartOnlySettingDoesNotMoveUnderTheNode(t *testing.T) {
	client := dynamicconfig.NewMemoryClient()
	policy := NewPolicy(dynamicconfig.NewCollection(client, log.NewNoopLogger()), section)

	for _, s := range settings {
		if s.live {
			continue
		}
		value := distinct(t, s.field(&cycle.Config{}), 7)
		cleanup := client.OverrideValue(s.key, value)

		got := policy()
		require.Equal(t, filled(), got,
			"%q changed under a policy that was already built, and it is a start-only setting", s.key)

		// A policy built afterwards does read it, so the assertion above is
		// about when it is read and not whether.
		fresh := NewPolicy(dynamicconfig.NewCollection(client, log.NewNoopLogger()), section)()
		require.Equal(t, value, reflect.ValueOf(s.field(&fresh)).Elem().Interface(),
			"%q was not read at all, which is not what start-only means", s.key)
		cleanup()
	}
}
