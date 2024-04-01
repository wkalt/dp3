package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/relvacode/iso8601"
	"github.com/spf13/cobra"
	cutil "github.com/wkalt/dp3/client/dp3/util"
	"github.com/wkalt/dp3/nodestore"
	"github.com/wkalt/dp3/routes"
)

var (
	statrangeProducerID  string
	statrangeTopic       string
	statrangeStart       string
	statrangeEnd         string
	statrangeGranularity uint64
)

// statrangeCmd represents the statrange command
var statrangeCmd = &cobra.Command{
	Use:   "statrange",
	Short: "Retrieve statistics for a topic during a range of time",
	Run: func(cmd *cobra.Command, args []string) {
		start, err := iso8601.Parse([]byte(statrangeStart))
		if err != nil {
			bailf("error parsing start date: %s", err)
		}
		end, err := iso8601.Parse([]byte(statrangeEnd))
		if err != nil {
			bailf("error parsing end date: %s", err)
		}
		req := &routes.StatRangeRequest{
			ProducerID:  statrangeProducerID,
			Start:       uint64(start.UnixNano()),
			End:         uint64(end.UnixNano()),
			Topic:       statrangeTopic,
			Granularity: statrangeGranularity * 1e9,
		}
		buf := &bytes.Buffer{}
		if err = json.NewEncoder(buf).Encode(req); err != nil {
			bailf("error encoding request: %s", err)
		}
		resp, err := http.Post("http://localhost:8089/statrange", "application/json", buf)
		if err != nil {
			bailf("error calling statrange: %s", err)
		}
		defer resp.Body.Close()

		cutil.MustOK(resp)

		response := []nodestore.StatRange{}
		if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
			bailf("error decoding response: %s", err)
		}

		headers := []string{"Start", "End", "Schema", "Type", "Field", "Name", "Value"}
		data := [][]string{}
		for _, record := range response {
			start := time.Unix(0, int64(record.Start)).Format(time.RFC3339)
			end := time.Unix(0, int64(record.End)).Format(time.RFC3339)
			schema := record.SchemaHash[:7]
			data = append(data, []string{
				start, end, schema, string(record.Type), record.Field, record.Name, fmt.Sprintf("%v", record.Value)},
			)
		}

		cutil.PrintTable(os.Stdout, headers, data)
	},
}

func init() {
	rootCmd.AddCommand(statrangeCmd)

	statrangeCmd.PersistentFlags().StringVarP(&statrangeProducerID, "producer", "p", "", "Producer ID")
	statrangeCmd.MarkPersistentFlagRequired("producer")
	statrangeCmd.PersistentFlags().StringVarP(&statrangeTopic, "topic", "t", "", "Topic")
	statrangeCmd.MarkPersistentFlagRequired("topic")
	statrangeCmd.PersistentFlags().StringVarP(&statrangeStart, "start", "s", "1970-01-01T00:00:00Z", "Start time")
	statrangeCmd.PersistentFlags().StringVarP(&statrangeEnd, "end", "e", "2050-01-01T00:00:00Z", "End time")

	statrangeCmd.PersistentFlags().Uint64VarP(&statrangeGranularity, "granularity", "g", 60*60, "Granularity in seconds")
}
