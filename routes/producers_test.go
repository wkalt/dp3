package routes_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/mcap"
	"github.com/wkalt/dp3/routes"
	"github.com/wkalt/dp3/treemgr"
)

func TestProducersHandler(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		assertion string
		producers []string
	}{
		{
			"no producers",
			[]string{},
		},
		{
			"one producer",
			[]string{"db"},
		},
		{
			"two producers",
			[]string{"producer1", "producer2"},
		},
	}

	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			tmgr, done := treemgr.TestTreeManager(ctx, t)
			defer done()
			buf := &bytes.Buffer{}
			mcap.WriteFile(t, buf, []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
			for _, producer := range c.producers {
				require.NoError(t, tmgr.Receive(ctx, "db", producer, bytes.NewReader(buf.Bytes())))
				require.NoError(t, tmgr.ForceFlush(ctx))
			}
			url, finish := routes.MakeTestRoutes(ctx, t, tmgr)
			defer finish()
			request, err := http.NewRequestWithContext(
				ctx,
				http.MethodGet,
				url+"/databases/db/producers",
				nil,
			)
			require.NoError(t, err)
			resp, err := http.DefaultClient.Do(request)
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)
			var producers []string
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&producers))
			require.ElementsMatch(t, c.producers, producers)
		})
	}
}
