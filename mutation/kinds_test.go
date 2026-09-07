package mutation

// The kind guard holds the table in kinds.go and the code to each other: a
// field of [Mutation] with no row, or a row whose accessors read the
// neighbouring field, fails here rather than in a drain months later. It says
// nothing about behaviour, which fold, check and apply guard themselves.

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

// Four claims: every row is filled in, the rows and the exported fields of
// [Mutation] are one to one, each row's accessors read the field its slot
// points at, and the fan-outs [Mutation.Kind] and [Mutation.ShardID] agree with
// the row.
func TestEveryKindIsDeclaredOnceAndReachesItsField(t *testing.T) {
	mutationType := reflect.TypeFor[Mutation]()

	var probe Mutation
	byName := map[string]Kind{}
	byField := map[string]Kind{}
	for k := KindInvalid + 1; int(k) < KindCount; k++ {
		row := kinds[k]

		require.NotEmpty(t, row.name, "kind %d has no name: it would print as the empty string "+
			"in every error and every metric tag", int(k))
		require.NotEqual(t, "invalid", row.name, "kind %d calls itself %q, which is what a "+
			"mutation holding no request reports", int(k), row.name)
		require.NotNil(t, row.slot, "kind %s has no slot: the row points at no field of Mutation, "+
			"so nothing can build the mutation that carries it", row.name)
		require.NotNil(t, row.present, "kind %s has no present accessor", row.name)
		require.NotNil(t, row.shard, "kind %s has no shard accessor", row.name)
		require.NotNil(t, row.rangeID, "kind %s says nothing about the epoch it is fenced with: "+
			"noRangeID is the answer for a request that carries none", row.name)
		require.NotNil(t, row.events, "kind %s says nothing about the new events its request "+
			"carries: noEvents is the answer for a request that carries none, and a nil row is a "+
			"mutation acked over history nodes nobody wrote", row.name)

		if first, again := byName[row.name]; again {
			t.Fatalf("kinds %d and %d are both called %q: two kinds that read the same in a "+
				"log line are two kinds nobody can tell apart there", int(first), int(k), row.name)
		}
		byName[row.name] = k

		field := fieldName(&probe, row.slot)
		require.NotEmpty(t, field, "kind %s's slot is not the address of an exported field of "+
			"Mutation: it has to be one, because that field is what every caller sets and what "+
			"Kind() reads", row.name)
		if first, again := byField[field]; again {
			t.Fatalf("kinds %s and %s both travel in Mutation.%s: one of the two is a kind no "+
				"mutation can ever hold", first, k, field)
		}
		byField[field] = k
	}

	// The other direction, which the selectors cannot state: a field of
	// Mutation with no row.
	for f := range mutationType.Fields() {
		if !f.IsExported() {
			continue
		}
		require.Contains(t, byField, f.Name,
			"Mutation.%s is a request no kind declares: it compiles, it can be set, and "+
				"Kind() answers KindInvalid for it — so every caller refuses the mutation "+
				"rather than carrying it. Give it a constant and a row in kinds.go", f.Name)
	}

	// Each row against a mutation holding its field and nothing else. Shard ids
	// differ per kind, so a row reading its neighbour's field is a wrong number
	// rather than a zero two rows agree on.
	for k := KindInvalid + 1; int(k) < KindCount; k++ {
		row := kinds[k]
		t.Run(row.name, func(t *testing.T) {
			shardID := int32(100 + int(k))
			field := fieldName(&probe, row.slot)
			m := onlyField(t, row.slot, shardID)

			require.True(t, row.present(m), "the %s row's present accessor does not see "+
				"Mutation.%s set", row.name, field)
			for other := KindInvalid + 1; int(other) < KindCount; other++ {
				if other == k {
					continue
				}
				require.False(t, kinds[other].present(m), "the %s row's present accessor "+
					"answers yes to a mutation holding only Mutation.%s", kinds[other].name, field)
			}

			require.Equal(t, k, m.Kind(), "Mutation.Kind does not report %s for a mutation "+
				"holding only Mutation.%s", row.name, field)
			require.Equal(t, shardID, row.shard(m), "the %s row's shard accessor does not read "+
				"Mutation.%s's shard id", row.name, field)
			require.Equal(t, row.shard(m), m.ShardID(), "Mutation.ShardID and the %s row "+
				"disagree: one shard is one log and one apply transaction, so this is the "+
				"routing key for everything above", row.name)
			require.Equal(t, row.name, k.String())

			// Only that the enumerations answer for every kind; what they hold
			// is taskslots_test.go's, eventslots_test.go's and
			// rangeid_test.go's. A row reading its neighbour's field panics
			// here, the mutation holding one request and no other.
			require.NotPanics(t, func() { m.TaskSlots() },
				"Mutation.TaskSlots panics on a %s", row.name)
			require.NotPanics(t, func() { m.EventSlots() },
				"Mutation.EventSlots panics on a %s", row.name)
			require.NotPanics(t, func() { m.RangeID() },
				"Mutation.RangeID panics on a %s", row.name)
		})
	}
}

// [Kind.String] indexes the table, so an out-of-range Kind must not panic.
func TestOnlyTheEnumerationHasAName(t *testing.T) {
	require.Equal(t, "invalid", KindInvalid.String())
	require.Equal(t, "invalid", Kind(-1).String())
	require.Equal(t, "invalid", Kind(KindCount).String())
	require.Equal(t, "invalid", Kind(KindCount+99).String())
	require.Equal(t, KindInvalid, Mutation{}.Kind(),
		"a mutation holding no request is KindInvalid")
}

// fieldName reports which exported field of [Mutation] a row's slot is the
// address of, by pointer identity, and the empty string for anything else.
func fieldName(m *Mutation, slot func(*Mutation) any) string {
	p := slot(m)
	v := reflect.ValueOf(m).Elem()
	for i := range v.NumField() {
		f := v.Type().Field(i)
		if f.IsExported() && v.Field(i).Addr().Interface() == p {
			return f.Name
		}
	}
	return ""
}

// onlyField builds a mutation holding only the field a row's slot points at,
// carrying a request that is zero apart from the shard id. It fills by
// reflection so that it names no request type, which would be one more place a
// new kind could be forgotten.
func onlyField(t *testing.T, slot func(*Mutation) any, shardID int32) Mutation {
	t.Helper()

	var m Mutation
	f := reflect.ValueOf(slot(&m)).Elem()
	require.Equal(t, reflect.Pointer, f.Kind(),
		"the slot does not point at a pointer field: Mutation is a struct of pointers because "+
			"exactly one of them is set, which is what Kind() reads")

	req := reflect.New(f.Type().Elem())
	shard := req.Elem().FieldByName("ShardID")
	require.True(t, shard.IsValid() && shard.Type() == reflect.TypeFor[int32](),
		"%s has no ShardID int32: a request that does not name a shard cannot be routed to "+
			"a log or a drain, and Mutation.ShardID has nothing to return for it", f.Type())
	shard.SetInt(int64(shardID))
	f.Set(req)

	return m
}
