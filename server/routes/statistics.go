package routes

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/wkalt/dp3/server/treemgr"
	"github.com/wkalt/dp3/server/util/httputil"
)

type StatisticsRequest struct {
	Topic           string   `json:"topic"`
	Producer        *string  `json:"producer"`
	Field           string   `json:"field"`
	Statistics      []string `json:"statistics"`
	StartSecs       int      `json:"startSecs"`
	EndSecs         int      `json:"endSecs"`
	Granularity     int      `json:"granularity"`
	GroupByProducer bool     `json:"groupByProducer"`
}

type StatisticsResponse struct {
	Producers    []string `json:"producers"`
	SchemaHashes []string `json:"schemaHashes"`
	Breaks       []int    `json:"breaks"` // seconds
	// statistic (min, max, etc) to list of values. Values and breaks have same count.
	Statistics map[string][]any `json:"statistics"`
}

func parseStatisticsRequest(req *http.Request) (StatisticsRequest, error) {
	query := req.URL.Query()
	topic := query.Get("topic")
	if topic == "" {
		return StatisticsRequest{}, errors.New("missing topic")
	}
	var producer *string
	producerStr := query.Get("producer")
	if producerStr != "" {
		producer = &producerStr
	}

	field := query.Get("field")

	statistics := query["statistics"]
	if len(statistics) == 0 {
		return StatisticsRequest{}, errors.New("missing statistics")
	}

	start := 0
	end := ^0
	if startStr := query.Get("start"); startStr != "" {
		s, err := strconv.Atoi(startStr)
		if err != nil {
			return StatisticsRequest{}, fmt.Errorf("failed to parse start: %w", err)
		}
		start = s
	}
	if endStr := query.Get("end"); endStr != "" {
		e, err := strconv.Atoi(endStr)
		if err != nil {
			return StatisticsRequest{}, fmt.Errorf("failed to parse end: %w", err)
		}
		end = e
	}
	var err error
	granularity := 60
	granularityStr := query.Get("granularity")
	if granularityStr != "" {
		granularity, err = strconv.Atoi(granularityStr)
		if err != nil {
			return StatisticsRequest{}, fmt.Errorf("failed to parse granularity: %w", err)
		}
	}

	groupByProducer := query.Get("groupByProducer") == "true"
	return StatisticsRequest{
		Topic:           topic,
		Producer:        producer,
		Field:           field,
		Statistics:      statistics,
		StartSecs:       start,
		EndSecs:         end,
		Granularity:     granularity,
		GroupByProducer: groupByProducer,
	}, nil
}

func newStatisticsHandler(tmgr *treemgr.TreeManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		database := mux.Vars(r)["database"]
		req, err := parseStatisticsRequest(r)
		if err != nil {
			httputil.BadRequest(ctx, w, "failed to parse statistics request: %w", err)
			return
		}
		resp, err := tmgr.Statistics(
			ctx,
			database,
			req.Topic,
			req.Producer,
			req.Field,
			req.Statistics,
			req.StartSecs,
			req.EndSecs,
			req.Granularity,
			req.GroupByProducer,
		)
		if err != nil {
			httputil.InternalServerError(ctx, w, "failed to get statistics: %w", err)
			return
		}
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			httputil.InternalServerError(ctx, w, "failed to encode response: %w", err)
			return
		}
	}
}
