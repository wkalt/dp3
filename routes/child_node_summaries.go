package routes

import (
	"encoding/json"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/wkalt/dp3/treemgr"
	"github.com/wkalt/dp3/util/httputil"
)

type SummarizeChildrenRequest struct {
	Producer        string `json:"producer"`
	BucketWidthSecs int    `json:"bucketWidthSecs"`
	StartSecs       int64  `json:"startSecs"`
	EndSecs         int64  `json:"endSecs"`
	Topic           string `json:"topic"`
}

func (scr *SummarizeChildrenRequest) parse(req *http.Request) {
	var err error
	scr.Producer = req.URL.Query().Get("producer")
	scr.BucketWidthSecs, err = strconv.Atoi(req.URL.Query().Get("bucketWidthSecs"))
	if err != nil {
		scr.BucketWidthSecs = math.MaxInt
	}
	scr.Topic = req.URL.Query().Get("topic")
	scr.StartSecs, err = strconv.ParseInt(req.URL.Query().Get("start"), 10, 64)
	if err != nil {
		scr.StartSecs = 0
	}
	scr.EndSecs, err = strconv.ParseInt(req.URL.Query().Get("end"), 10, 64)
	if err != nil {
		scr.EndSecs = math.MaxInt64
	}
}

func summarizeChildrenHandler(tmgr *treemgr.TreeManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		req := &SummarizeChildrenRequest{}
		req.parse(r)
		var topics []string
		if req.Topic != "" {
			topics = []string{req.Topic}
		}

		database := mux.Vars(r)["database"]
		summary, err := tmgr.SummarizeChildren(
			ctx,
			database,
			req.Producer,
			topics,
			time.Unix(req.StartSecs, 0),
			time.Unix(req.EndSecs, 0),
			req.BucketWidthSecs,
		)
		if err != nil {
			httputil.InternalServerError(ctx, w, "failed to summarize children: %s", err)
			return
		}
		if err := json.NewEncoder(w).Encode(summary); err != nil {
			httputil.InternalServerError(ctx, w, "failed to encode response: %s", err)
			return
		}
	}
}
