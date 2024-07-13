package routes_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
	"testing"

	"github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/server/mcap"
	"github.com/wkalt/dp3/server/routes"
	"github.com/wkalt/dp3/server/treemgr"
)

func TestQueryHandler(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		assertion               string
		query                   string
		expectedResponseCode    int
		expectedResponseMessage string
		expectedMessageCount    int
	}{
		{
			"full scan",
			"from device /fix;",
			http.StatusOK,
			"",
			126238,
		},
		{
			"query missing semicolon",
			"from device /fix",
			http.StatusBadRequest,
			"queries must be terminated with a semicolon",
			0,
		},
		{
			"invalid field",
			"from device /fix where /fix.foo = 10;",
			http.StatusBadRequest,
			"field /fix.foo not found",
			0,
		},
		{
			"query with invalid aliases",
			"from device /fix as foo where bar.foo = 10;",
			http.StatusBadRequest,
			"unresolved table alias: bar",
			0,
		},
		{
			"invalid table",
			"from /foo /fix;",
			http.StatusBadRequest,
			"table db:/foo:/fix not found",
			0,
		},
	}

	tmgr, done := treemgr.TestTreeManager(ctx, t)
	defer done()

	f, err := os.Open("../../example-data/fix.mcap")
	require.NoError(t, err)

	require.NoError(t, tmgr.Receive(ctx, "db", "device", f))
	require.NoError(t, tmgr.ForceFlush(ctx))

	url, finish := routes.MakeTestRoutes(t, tmgr)
	defer finish()

	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			request := routes.QueryRequest{
				Query: c.query,
			}
			body, err := json.Marshal(request)
			require.NoError(t, err)

			url := url + "/databases/db/query"

			req, err := http.NewRequestWithContext(
				ctx, http.MethodPost, url, bytes.NewBuffer(body),
			)
			require.NoError(t, err)

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				body, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				require.Contains(t, string(body), c.expectedResponseMessage)
				return
			}

			data, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			reader, err := mcap.NewReader(bytes.NewReader(data))
			require.NoError(t, err)

			it, err := reader.Messages()
			require.NoError(t, err)

			messages := 0
			for {
				_, _, _, err := it.NextInto(nil)
				if err != nil {
					break
				}
				messages++
			}
			require.Equal(t, c.expectedMessageCount, messages)
		})
	}
}
