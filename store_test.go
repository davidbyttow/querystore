package querystore

import (
	"os"
	"strconv"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore(t *testing.T) {
	dir := lo.Must(os.MkdirTemp(os.TempDir(), "store*"))
	// defer os.RemoveAll(dir)

	t.Logf("temp dir: %s", dir)

	fs, err := OpenColumnFS(dir)
	require.NoError(t, err)
	defer fs.Close()

	cs := NewColumnarStore(fs)

	for i := range 100 {
		rec := map[string]any{
			"main":       true,
			"val":        i,
			"val_string": strconv.Itoa(i),
		}
		assert.NoError(t, cs.Append(rec))
	}

	q := &Query{
		Aggregator: AggregatorCount,
		Filters: []Filter{
			{
				Attribute: "val",
				Condition: ConditionEquals,
				Value:     49,
			},
		},
	}
	rows, err := cs.Query(q)
	require.NoError(t, err)
	spew.Dump(rows)
}
