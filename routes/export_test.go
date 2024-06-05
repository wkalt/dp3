package routes

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/treemgr"
)

func TestExportHandler(t *testing.T) {
	ctx := context.Background()
	tmgr, done := treemgr.TestTreeManager(ctx, t)
	defer done()
	requestBody := ExportRequest{
		Database: "db",
		Producer: "sampleProducer",
		Topics:   map[string]uint64{"topic1": 0, "topic2": 0},
		Start:    123,
		End:      456,
	}
	jsonBody, err := json.Marshal(requestBody)
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "/export", bytes.NewBuffer(jsonBody))
	if err != nil {
		t.Error(err)
	}
	rr := httptest.NewRecorder()
	handler := newExportHandler(tmgr)
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusAccepted {
		t.Errorf("Expected status code %d, but got %d", http.StatusOK, rr.Code)
	}
}
