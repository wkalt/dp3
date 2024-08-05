package routes

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/wkalt/dp3/server/treemgr"
	"github.com/wkalt/dp3/server/util/httputil"
)

type AvailableStatisticsRequest struct {
	Topic       string  `json:"topic"`
	Producer    *string `json:"producer"`
	StartSecs   int     `json:"startSecs"`
	EndSecs     int     `json:"endSecs"`
	Granularity int     `json:"granularity"`
}

type AvailableStatistic struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type AvailableStatisticsResponse struct {
	Field      string
	FieldType  string
	Statistics []AvailableStatistic
}

func parseAvailableStatsRequest(req *http.Request) (AvailableStatisticsRequest, error) {
	query := req.URL.Query()
	topic, err := url.QueryUnescape(query.Get("topic"))
	if err != nil {
		return AvailableStatisticsRequest{}, fmt.Errorf("failed to parse topic: %w", err)
	}
	if topic == "" {
		return AvailableStatisticsRequest{}, errors.New("missing topic")
	}
	var producer *string
	producerStr, err := url.QueryUnescape(query.Get("producer"))
	if err != nil {
		return AvailableStatisticsRequest{}, fmt.Errorf("failed to parse producer: %w", err)
	}
	if producerStr != "" {
		producer = &producerStr
	}

	start := 0
	end := ^0
	if startStr := query.Get("start"); startStr != "" {
		s, err := strconv.Atoi(startStr)
		if err != nil {
			return AvailableStatisticsRequest{}, fmt.Errorf("failed to parse start: %w", err)
		}
		start = s
	}
	if endStr := query.Get("end"); endStr != "" {
		e, err := strconv.Atoi(endStr)
		if err != nil {
			return AvailableStatisticsRequest{}, fmt.Errorf("failed to parse end: %w", err)
		}
		end = e
	}

	granularity := 60
	if granularityStr := query.Get("granularity"); granularityStr != "" {
		granularity, err = strconv.Atoi(granularityStr)
		if err != nil {
			return AvailableStatisticsRequest{}, fmt.Errorf("failed to parse granularity: %w", err)
		}
	}
	return AvailableStatisticsRequest{
		Topic:       topic,
		Producer:    producer,
		StartSecs:   start,
		EndSecs:     end,
		Granularity: granularity,
	}, nil
}

func newAvailableStatisticsHandler(tmgr *treemgr.TreeManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		database := mux.Vars(r)["database"]
		req, err := parseAvailableStatsRequest(r)
		if err != nil {
			httputil.BadRequest(ctx, w, "error decoding request: %s", err)
			return
		}
		stats, err := tmgr.ListStatistics(ctx, database, req.Topic, req.Producer, req.StartSecs, req.EndSecs, req.Granularity)
		if err != nil {
			httputil.InternalServerError(ctx, w, "error listing statistics: %s", err)
			return
		}
		if err := json.NewEncoder(w).Encode(stats); err != nil {
			httputil.InternalServerError(ctx, w, "failed to encode response: %s", err)
			return
		}
	}
}
