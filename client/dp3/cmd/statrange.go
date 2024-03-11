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
	"github.com/wkalt/dp3/client/dp3/util"
	"github.com/wkalt/dp3/routes"
	"github.com/wkalt/dp3/tree"
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
			panic(err)
		}
		end, err := iso8601.Parse([]byte(statrangeEnd))
		if err != nil {
			panic(err)
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
			panic(err)
		}
		resp, err := http.Post("http://localhost:8089/statrange", "application/json", buf)
		if err != nil {
			panic(err)
		}
		defer resp.Body.Close()

		util.MustOK(resp)

		response := []tree.StatRange{}
		if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
			panic(err)
		}
		headers := []string{"Start", "End", "MessageCount"}
		data := [][]string{}
		for _, record := range response {
			start := time.Unix(0, int64(record.Start)).Format(time.RFC3339)
			end := time.Unix(0, int64(record.End)).Format(time.RFC3339)
			data = append(data, []string{start, end, fmt.Sprintf("%d", record.Statistics.MessageCount)})
		}
		util.PrintTable(os.Stdout, headers, data)
	},
}

func init() {
	rootCmd.AddCommand(statrangeCmd)

	statrangeCmd.PersistentFlags().StringVarP(&statrangeProducerID, "producer", "p", "", "Producer ID")
	statrangeCmd.MarkPersistentFlagRequired("producer")
	statrangeCmd.PersistentFlags().StringVarP(&statrangeTopic, "topic", "t", "", "Topic")
	statrangeCmd.MarkPersistentFlagRequired("topic")
	statrangeCmd.PersistentFlags().StringVarP(&statrangeStart, "start", "s", "", "Start time")
	statrangeCmd.MarkPersistentFlagRequired("start")
	statrangeCmd.PersistentFlags().StringVarP(&statrangeEnd, "end", "e", "", "End time")
	statrangeCmd.MarkPersistentFlagRequired("end")

	statrangeCmd.PersistentFlags().Uint64VarP(&statrangeGranularity, "granularity", "g", 60*60, "Granularity in seconds")
}
