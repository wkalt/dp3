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

func TestDeleteHandler(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		assertion string
		db        string
		producer  string
		topic     string
		start     uint64
		end       uint64

		expectedResponseCode    int
		expectedResponseMessage string
	}{
		{
			"valid request",
			"db",
			"producer",
			"/fix",
			0,
			100,
			http.StatusOK,
			"",
		},
		{
			"missing producer",
			"db",
			"",
			"topic",
			0,
			100,
			http.StatusBadRequest,
			"missing producerId",
		},
		{
			"missing topic",
			"db",
			"producer",
			"",
			0,
			100,
			http.StatusBadRequest,
			"missing topic",
		},
		{
			"missing database",
			"",
			"producer",
			"topic",
			0,
			100,
			http.StatusBadRequest,
			"missing database",
		},
		{
			"end before start",
			"db",
			"producer",
			"topic",
			100,
			0,
			http.StatusBadRequest,
			"end must be greater than start",
		},
		{
			"valid request for topic that does not exist",
			"db",
			"producer",
			"topic-does-not-exist",
			0,
			100,
			http.StatusNotFound,
			"topic topic-does-not-exist not found",
		},
	}

	tmgr, done := treemgr.TestTreeManager(ctx, t)
	defer done()

	f, err := os.Open("../example-data/fix.mcap")
	require.NoError(t, err)

	require.NoError(t, tmgr.Receive(ctx, "db", "producer", f))
	require.NoError(t, tmgr.ForceFlush(ctx))

	url, finish := routes.MakeTestRoutes(ctx, t, tmgr)
	defer finish()

	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			req := routes.DeleteRequest{
				Database:   c.db,
				ProducerID: c.producer,
				Topic:      c.topic,
				Start:      c.start,
				End:        c.end,
			}
			body, err := json.Marshal(req)
			require.NoError(t, err)

			request, err := http.NewRequestWithContext(ctx, http.MethodPost, url+"/delete", bytes.NewReader(body))
			require.NoError(t, err)

			resp, err := http.DefaultClient.Do(request)
			require.NoError(t, err)
			defer resp.Body.Close()
			if c.expectedResponseCode != http.StatusOK || resp.StatusCode != c.expectedResponseCode {
				body, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				require.Contains(t, string(body), c.expectedResponseMessage)
			}
			require.Equal(t, c.expectedResponseCode, resp.StatusCode)
		})
	}
}
