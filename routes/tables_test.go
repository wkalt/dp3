package routes_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/routes"
	"github.com/wkalt/dp3/treemgr"
)

func TestTablesHandler(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		assertion               string
		database                string
		producer                string
		topic                   string
		historical              bool
		expectedResponseCode    int
		expectedResponseMessage string
	}{
		{
			"valid request",
			"db",
			"producer",
			"topic",
			true,
			http.StatusOK,
			"",
		},
	}

	tmgr, done := treemgr.TestTreeManager(ctx, t)
	defer done()

	f, err := os.Open("../example-data/fix.mcap")
	require.NoError(t, err)

	require.NoError(t, tmgr.Receive(ctx, "db", "device", f))
	require.NoError(t, tmgr.ForceFlush(ctx))

	url, finish := routes.MakeTestRoutes(ctx, t, tmgr)
	defer finish()

	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			req := routes.TablesRequest{
				Database:   c.database,
				Producer:   c.producer,
				Topic:      c.topic,
				Historical: c.historical,
			}
			body, err := json.Marshal(req)
			require.NoError(t, err)

			request, err := http.NewRequestWithContext(
				ctx,
				http.MethodGet,
				url+"/databases/"+c.database+"/tables", bytes.NewReader(body))
			require.NoError(t, err)

			resp, err := http.DefaultClient.Do(request)
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, c.expectedResponseCode, resp.StatusCode)

			if c.expectedResponseMessage != "" {
				body, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				require.Contains(t, string(body), c.expectedResponseMessage)
			}
		})
	}
}
