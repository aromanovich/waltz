package mutation

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

// [Mutation.RangeID] is the epoch the write is fenced with (I11), so a row
// answering zero for a request that names one fences nothing, and a row
// answering a number for a request that names none fences against a shard
// nobody holds. Which requests have the field is Temporal's decision, so it is
// read rather than listed here.
func TestTheRangeIDIsTheRequestsOwnOrZero(t *testing.T) {
	const rangeID = 41

	for k := KindInvalid + 1; int(k) < KindCount; k++ {
		t.Run(k.String(), func(t *testing.T) {
			var m Mutation
			slot := reflect.ValueOf(kinds[k].slot(&m)).Elem()
			request := reflect.New(slot.Type().Elem())
			slot.Set(request)

			field := request.Elem().FieldByName("RangeID")
			if !field.IsValid() {
				require.Zero(t, m.RangeID(), "kind %s's request carries no rangeID, so it names "+
					"no epoch and the drain's own CAS is what fences it", k)
				return
			}

			require.Equal(t, reflect.TypeFor[int64](), field.Type())
			field.SetInt(rangeID)
			require.Equal(t, int64(rangeID), m.RangeID(),
				"kind %s carries a rangeID and must be fenced with it", k)
		})
	}

	require.Zero(t, Mutation{}.RangeID(), "a mutation holding no request names no epoch")
}
