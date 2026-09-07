package fold

// The two consumers of a delta are one list spelled twice: mergeMutation folds
// it onto another delta and applyMutationToSnapshot folds it onto whole state,
// and every collection the request carries has to reach both. Neither spelling
// names the other, so a pair added to one alone compiles and no unit test that
// names its fields exists to fail — the differential oracle is the first thing
// to notice, and it needs a cluster. Both tests here drive the enumeration off
// the request type's own fields instead, so the pair nobody folded fails by
// name in this package.

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
)

// Each Upsert/Delete pair of a delta, against both folds: the upsert arrives
// and the delete takes its key away, in the pair's own map for one and in the
// snapshot's home for the same collection for the other.
func TestEveryCollectionReachesBothFolds(t *testing.T) {
	mutationType := reflect.TypeFor[p.InternalWorkflowMutation]()
	snapshotType := reflect.TypeFor[p.InternalWorkflowSnapshot]()

	upserts, deletes := 0, 0
	for f := range mutationType.Fields() {
		if strings.HasPrefix(f.Name, "Delete") {
			deletes++
		}
		name, isUpsert := strings.CutPrefix(f.Name, "Upsert")
		if !isUpsert {
			continue
		}
		upserts++

		t.Run(name, func(t *testing.T) {
			del, ok := mutationType.FieldByName("Delete" + name)
			require.True(t, ok, "Upsert%s has no Delete%s beside it: the folds resolve a "+
				"collection as a pair, and an upsert set with no delete set has no arm here", name, name)
			state, ok := snapshotType.FieldByName(name)
			require.True(t, ok, "a delta upserts %s and a snapshot has no such field: "+
				"applyMutationToSnapshot has nowhere to put it, so the collection would fold "+
				"into an accumulated delta and vanish from accumulated whole state", name)
			require.Equal(t, f.Type, state.Type, "a delta's Upsert%s is %s and a snapshot's "+
				"%s is %s: one of the two folds is putting the collection somewhere it does not fit",
				name, f.Type, name, state.Type)

			kept, gone := mapKey(t, f.Type.Key(), 1), mapKey(t, f.Type.Key(), 2)
			value := mapValue(f.Type.Elem())

			// The delta under test: one key upserted, another deleted.
			src := &p.InternalWorkflowMutation{}
			ups := reflect.MakeMap(f.Type)
			ups.SetMapIndex(kept, value)
			fieldOf(src, f.Index).Set(ups)
			dels := reflect.MakeMap(del.Type)
			dels.SetMapIndex(gone, reflect.ValueOf(struct{}{}))
			fieldOf(src, del.Index).Set(dels)

			t.Run("onto another delta", func(t *testing.T) {
				dst := &p.InternalWorkflowMutation{}
				prior := reflect.MakeMap(f.Type)
				prior.SetMapIndex(gone, value)
				fieldOf(dst, f.Index).Set(prior)

				mergeMutation(dst, src)

				merged, removed := fieldOf(dst, f.Index), fieldOf(dst, del.Index)
				require.True(t, merged.MapIndex(kept).IsValid(), "mergeMutation does not fold "+
					"Upsert%s: the collection is missing from its list, so a window's later "+
					"writes to it are dropped on the drain", name)
				require.Equal(t, value.Interface(), merged.MapIndex(kept).Interface(),
					"mergeMutation folds Upsert%s under the right key and the wrong value", name)
				require.False(t, merged.MapIndex(gone).IsValid(), "mergeMutation leaves a key "+
					"in Upsert%s that a later delta deleted: emitted unresolved, the store orders "+
					"every delete before every upsert and the key silently survives", name)
				require.True(t, removed.MapIndex(gone).IsValid(), "mergeMutation does not fold "+
					"Delete%s: the deletion never reaches the store", name)
			})

			t.Run("onto whole state", func(t *testing.T) {
				snap := &p.InternalWorkflowSnapshot{}
				held := reflect.MakeMap(state.Type)
				held.SetMapIndex(gone, value)
				fieldOf(snap, state.Index).Set(held)

				applyMutationToSnapshot(snap, src)

				applied := fieldOf(snap, state.Index)
				require.True(t, applied.MapIndex(kept).IsValid(), "applyMutationToSnapshot does "+
					"not fold Upsert%s onto %s: a delta following a snapshot in the window loses "+
					"the collection, and the run is written back short of it", name, name)
				require.Equal(t, value.Interface(), applied.MapIndex(kept).Interface(),
					"applyMutationToSnapshot folds Upsert%s under the right key and the wrong value", name)
				require.False(t, applied.MapIndex(gone).IsValid(), "applyMutationToSnapshot does "+
					"not fold Delete%s onto %s: the row is written back holding state the delta "+
					"removed", name, name)
			})
		})
	}

	require.NotZero(t, upserts, "no field of a delta is named Upsert*: this test enumerates the "+
		"collections by that prefix, so it has just judged nothing")
	require.Equal(t, upserts, deletes, "a delta has %d Upsert fields and %d Delete fields: the "+
		"folds resolve collections as pairs, and an odd one out is a set no arm reads", upserts, deletes)
}

// notFolded names the fields both folds deliberately leave to dst. They say
// which workflow the accumulator is for, so a delta cannot disagree with the
// record it folds into and copying them would say nothing.
var notFolded = map[string]bool{"NamespaceID": true, "WorkflowID": true, "RunID": true}

// The tail the two lists share by name: whatever a snapshot and a delta both
// have, both folds must take from src, or neither must. The comparison needs no
// second list of the fields — the two types' own overlap is the list — and it
// holds the exceptions to being exceptions in the same direction.
func TestBothFoldsTakeTheSameFieldsFromTheDelta(t *testing.T) {
	mutationType := reflect.TypeFor[p.InternalWorkflowMutation]()
	snapshotType := reflect.TypeFor[p.InternalWorkflowSnapshot]()

	src := &p.InternalWorkflowMutation{}
	fillScalars(t, reflect.ValueOf(src).Elem())
	src.Tasks = map[tasks.Category][]p.InternalHistoryTask{
		tasks.CategoryTransfer: {{Key: tasks.NewImmediateKey(1)}},
	}

	dstMutation := &p.InternalWorkflowMutation{}
	mergeMutation(dstMutation, src)
	dstSnapshot := &p.InternalWorkflowSnapshot{}
	applyMutationToSnapshot(dstSnapshot, src)

	seen := map[string]bool{}
	for f := range mutationType.Fields() {
		sf, ok := snapshotType.FieldByName(f.Name)
		if !ok {
			continue
		}
		require.Equal(t, f.Type, sf.Type, "a delta's %s is %s and a snapshot's is %s: the two "+
			"folds cannot be carrying the same fact into both", f.Name, f.Type, sf.Type)
		seen[f.Name] = true

		want := reflect.ValueOf(src).Elem().FieldByIndex(f.Index)
		merged := reflect.ValueOf(dstMutation).Elem().FieldByIndex(f.Index)
		applied := reflect.ValueOf(dstSnapshot).Elem().FieldByIndex(sf.Index)

		if notFolded[f.Name] {
			require.True(t, merged.IsZero(), "mergeMutation now takes %s from the delta, and "+
				"notFolded still calls it a field the folds leave alone: decide which, and if "+
				"the fold is right applyMutationToSnapshot owes the same line", f.Name)
			require.True(t, applied.IsZero(), "applyMutationToSnapshot now takes %s from the "+
				"delta, and notFolded still calls it a field the folds leave alone: decide "+
				"which, and if the fold is right mergeMutation owes the same line", f.Name)
			continue
		}
		require.Equal(t, want.Interface(), merged.Interface(), "mergeMutation does not take %s "+
			"from the delta, and applyMutationToSnapshot does: the two folds disagree about what "+
			"a delta says, so a window drains differently for the sake of whether a snapshot "+
			"preceded it", f.Name)
		require.Equal(t, want.Interface(), applied.Interface(), "applyMutationToSnapshot does "+
			"not take %s from the delta, and mergeMutation does: the two folds disagree about "+
			"what a delta says, so a window drains differently for the sake of whether a "+
			"snapshot preceded it", f.Name)
	}

	require.NotZero(t, len(seen), "a delta and a snapshot share no field name: this test "+
		"compares the two folds over that overlap, so it has just judged nothing")
	for name := range notFolded {
		require.True(t, seen[name], "notFolded excuses %s, which is no longer a field both a "+
			"delta and a snapshot have: an exception nothing reaches excuses nothing", name)
	}
}

// fieldOf addresses one field of a struct behind a pointer.
func fieldOf(v any, index []int) reflect.Value {
	return reflect.ValueOf(v).Elem().FieldByIndex(index)
}

// mapKey builds the nth distinct key of a collection's key type.
func mapKey(t *testing.T, kt reflect.Type, n int) reflect.Value {
	t.Helper()

	k := reflect.New(kt).Elem()
	switch kt.Kind() {
	case reflect.String:
		k.SetString(fmt.Sprintf("key-%d", n))
	case reflect.Int64:
		k.SetInt(int64(n))
	default:
		t.Fatalf("a collection keyed by %s: this test builds string and int64 keys, and a new "+
			"key type needs an arm here before either fold can be said to carry the collection", kt)
	}
	return k
}

// mapValue builds a value a fold can be seen to have moved: a fresh pointer
// where the collection holds one, so the assertion is identity rather than two
// zero values agreeing.
func mapValue(vt reflect.Type) reflect.Value {
	if vt.Kind() == reflect.Pointer {
		return reflect.New(vt.Elem())
	}
	return reflect.New(vt).Elem()
}

// fillScalars sets every non-collection field of a delta to a distinguishable
// value, so that a field the comparison reads is never zero on both sides for
// want of having been set.
func fillScalars(t *testing.T, v reflect.Value) {
	t.Helper()

	for i := range v.NumField() {
		f := v.Field(i)
		switch f.Kind() {
		case reflect.Map:
			// The collections are TestEveryCollectionReachesBothFolds's.
		case reflect.Pointer:
			f.Set(reflect.New(f.Type().Elem()))
		case reflect.String:
			f.SetString(v.Type().Field(i).Name)
		case reflect.Int64:
			f.SetInt(int64(i) + 1)
		case reflect.Bool:
			f.SetBool(true)
		default:
			t.Fatalf("%s is a %s, which this test cannot build a value of: a field left zero "+
				"here is one the comparison below cannot tell a fold from a miss on",
				v.Type().Field(i).Name, f.Type())
		}
	}
}
