package routes_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/routes"
	"github.com/wkalt/dp3/treemgr"
)

func TestImportHandler(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		assertion               string
		database                string
		producer                string
		filepath                string
		expectedResponseCode    int
		expectedResponseMessage string
	}{
		{
			"valid request",
			"db",
			"producer",
			"../example-data/fix.mcap",
			http.StatusOK,
			"",
		},
		{
			"missing producer",
			"db",
			"",
			"../example-data/fix.mcap",
			http.StatusBadRequest,
			"missing producerId",
		},
		{
			"missing path",
			"db",
			"producer",
			"",
			http.StatusBadRequest,
			"missing path",
		},
		{
			"invalid path",
			"db",
			"producer",
			"../example-data/does-not-exist.mcap",
			http.StatusBadRequest,
			"error opening file",
		},
	}

	tmgr, done := treemgr.TestTreeManager(ctx, t)
	defer done()

	url, finish := routes.MakeTestRoutes(ctx, t, tmgr)
	defer finish()

	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			req := routes.ImportRequest{
				ProducerID: c.producer,
				Path:       c.filepath,
			}
			body, err := json.Marshal(req)
			require.NoError(t, err)

			request, err := http.NewRequestWithContext(
				ctx,
				http.MethodPost,
				url+fmt.Sprintf("/databases/%s/import", c.database),
				bytes.NewReader(body),
			)
			require.NoError(t, err)

			resp, err := http.DefaultClient.Do(request)
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, c.expectedResponseCode, resp.StatusCode)
			if c.expectedResponseCode != http.StatusOK {
				body, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				require.Contains(t, string(body), c.expectedResponseMessage)
				return
			}
		})
	}
}
