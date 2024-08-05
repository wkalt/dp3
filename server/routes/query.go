package routes

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/wkalt/dp3/server/executor"
	"github.com/wkalt/dp3/server/mcap"
	"github.com/wkalt/dp3/server/plan"
	"github.com/wkalt/dp3/server/ql"
	"github.com/wkalt/dp3/server/rootmap"
	"github.com/wkalt/dp3/server/treemgr"
	"github.com/wkalt/dp3/server/util"
	"github.com/wkalt/dp3/server/util/httputil"
	"github.com/wkalt/dp3/server/util/log"
)

/*
The query route receives query strings in the dp3 query language, compiles them
into an execution tree, and executes the query.
*/

////////////////////////////////////////////////////////////////////////////////

// QueryRequest represents a query request.
type QueryRequest struct {
	Query string `json:"query"`

	// These are all possible to express in the query language directly, but
	// tooling may wish to use these params to modify the user's query for
	// presentation.
	Explain    bool `json:"explain"`
	Limit      int  `json:"limit"`
	Offset     int  `json:"offset"`
	StampsOnly bool `json:"skeleton"`
}

func (req QueryRequest) validate() error {
	if req.Query == "" {
		return errors.New("missing query")
	}
	return nil
}

func writeJSONExplain(w io.Writer, r io.Reader) error {
	buf := &bytes.Buffer{}
	if _, err := io.Copy(buf, r); err != nil {
		return fmt.Errorf("error copying explain output: %w", err)
	}
	reader, err := mcap.NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		return fmt.Errorf("error creating reader: %w", err)
	}
	info, err := reader.Info()
	if err != nil {
		return fmt.Errorf("error reading info: %w", err)
	}
	for _, idx := range info.MetadataIndexes {
		if idx.Name == "query" {
			metadata, err := reader.GetMetadata(idx.Offset)
			if err != nil {
				return fmt.Errorf("error reading metadata: %w", err)
			}
			context := &util.Context{}
			if err := json.Unmarshal([]byte(metadata.Metadata["context"]), context); err != nil {
				return fmt.Errorf("error unmarshalling context: %w", err)
			}
			explain := context.Print()
			output, err := json.Marshal(map[string]interface{}{
				"explain": explain,
			})
			if err != nil {
				return fmt.Errorf("error marshalling explain: %w", err)
			}
			if _, err := w.Write(output); err != nil {
				return fmt.Errorf("error writing explain: %w", err)
			}
			return nil
		}
	}
	return errors.New("query metadata not found")
}

func streamQueryResults(
	ctx context.Context,
	w http.ResponseWriter,
	qp *plan.Node,
	sf executor.ScanFactory,
	explain bool,
	limit int,
	offset int,
	skeleton bool,
	json bool,
) (err error) {
	var output io.Writer = w
	done := make(chan error, 1)
	var shutdown = func() error {
		return nil
	}
	if json {
		piperead, pipewrite := io.Pipe()
		go func() {
			if explain {
				if err := writeJSONExplain(w, piperead); err != nil {
					done <- err
					return
				}
				done <- nil
			}

			if err := mcap.ToJSON(w, piperead); err != nil {
				done <- err
				return
			}
			done <- nil
		}()
		output = pipewrite
		shutdown = func() error {
			pipewrite.Close()
			return <-done
		}
	}
	if err := executor.Run(ctx, output, qp, sf, explain, limit, offset, skeleton); err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}
	return shutdown()
}

// newQueryHandler creates a new query handler.
func newQueryHandler(tmgr *treemgr.TreeManager) http.HandlerFunc { //nolint:funlen
	parser := ql.NewParser()
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		req := QueryRequest{}
		database := mux.Vars(r)["database"]
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			httputil.BadRequest(ctx, w, "error decoding request: %s", err)
			return
		}
		log.Infow(ctx, "query request", "database", database, "query", req.Query)
		if err := req.validate(); err != nil {
			httputil.BadRequest(ctx, w, "invalid request: %s", err)
			return
		}
		if !strings.HasSuffix(req.Query, ";") {
			httputil.BadRequest(ctx, w, "queries must be terminated with a semicolon")
			return
		}
		ast, err := parser.ParseString("", req.Query)
		if err != nil {
			httputil.BadRequest(ctx, w, "error parsing query: %s", err)
			return
		}

		if ast.Truncate != nil { //nolint: nestif
			producer := ast.Truncate.Producer
			topic := ast.Truncate.Topic
			timestamp := time.Now().UnixNano()
			if !ast.Truncate.Now {
				if timestamp, err = ast.Truncate.Time.Nanos(); err != nil {
					httputil.BadRequest(ctx, w, "error parsing timestamp: %s", err)
					return
				}
			}
			if err := tmgr.Truncate(ctx, database, producer, topic, timestamp); err != nil {
				httputil.InternalServerError(ctx, w, "error truncating: %s", err)
				return
			}
			if err := mcap.WriteEmptyFile(w); err != nil {
				httputil.InternalServerError(ctx, w, "error writing empty response: %s", err)
			}
			return
		}

		getProducers := func(topics []string) ([]string, error) {
			return tmgr.Producers(ctx, database, topics)
		}

		qp, err := plan.CompileQuery(database, *ast.Query, getProducers)
		if err != nil {
			if errors.Is(err, plan.BadPlanError{}) {
				httputil.BadRequest(ctx, w, "%w", err)
				return
			}
			httputil.InternalServerError(ctx, w, "error compiling query: %s", err)
			return
		}
		ctx = util.WithContext(ctx, "query")
		json := r.Header.Get("Accept") == "application/json"
		if err := streamQueryResults(ctx, w, qp, tmgr.NewTreeIterator,
			ast.Query.Explain || req.Explain,
			req.Limit,
			req.Offset,
			req.StampsOnly,
			json,
		); err != nil {
			fieldNotFound := executor.FieldNotFoundError{}
			if errors.As(err, &fieldNotFound) {
				httputil.BadRequest(ctx, w, "%w", fieldNotFound)
				return
			}
			tableNotFound := rootmap.TableNotFoundError{}
			if errors.As(err, &tableNotFound) {
				httputil.BadRequest(ctx, w, "%w", tableNotFound)
				return
			}
			if err := clientError(err); err != nil {
				log.Infof(ctx, "Client closed connection: %s", err)
				return
			}
			httputil.InternalServerError(ctx, w, "error executing query: %s", err)
			return
		}
	}
}
