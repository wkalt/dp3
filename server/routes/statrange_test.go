package routes_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/server/routes"
	"github.com/wkalt/dp3/server/treemgr"
)

func TestStatRangeHandler(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		assertion               string
		database                string
		producer                string
		start                   uint64
		end                     uint64
		topic                   string
		granularity             uint64
		expectedResponseCode    int
		expectedResponseMessage string
	}{
		{
			"valid request",
			"db",
			"producer",
			0,
			100,
			"topic",
			1,
			http.StatusOK,
			"",
		},
		{
			"missing database",
			"",
			"producer",
			0,
			100,
			"topic",
			1,
			http.StatusBadRequest,
			"missing database",
		},
		{
			"missing producer",
			"db",
			"",
			0,
			100,
			"topic",
			1,
			http.StatusBadRequest,
			"missing producer",
		},
		{
			"missing topic",
			"db",
			"producer",
			0,
			100,
			"",
			1,
			http.StatusBadRequest,
			"missing topic",
		},
	}

	tmgr, done := treemgr.TestTreeManager(ctx, t)
	defer done()

	url, finish := routes.MakeTestRoutes(t, tmgr)
	defer finish()

	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			req := routes.StatRangeRequest{
				Database:    c.database,
				Producer:    c.producer,
				Start:       c.start,
				End:         c.end,
				Topic:       c.topic,
				Granularity: c.granularity,
			}
			body, err := json.Marshal(req)
			require.NoError(t, err)

			request, err := http.NewRequestWithContext(ctx, http.MethodPost, url+"/statrange", bytes.NewReader(body))
			require.NoError(t, err)

			resp, err := http.DefaultClient.Do(request)
			require.NoError(t, err)
			defer resp.Body.Close()
		})
	}
}
