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
	"github.com/wkalt/dp3/routes"
	"github.com/wkalt/dp3/tree"
	"github.com/wkalt/dp3/util"
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

		response := []tree.StatRange{}
		if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
			bailf("error decoding response: %s", err)
		}

		headers := []string{"Start", "End", "Volume", "MessageCount", "Frequency"}
		data := [][]string{}
		for _, record := range response {
			start := time.Unix(0, int64(record.Start)).Format(time.RFC3339)
			end := time.Unix(0, int64(record.End)).Format(time.RFC3339)
			volume := util.HumanBytes(record.Statistics.ByteCount)
			count := fmt.Sprintf("%d", record.Statistics.MessageCount)
			var frequency string
			if record.Statistics.MessageCount > 0 {
				start := record.Statistics.MinObservedTime
				end := record.Statistics.MaxObservedTime
				frequency = util.HumanFrequency(float64(record.Statistics.MessageCount) /
					(float64(end-start) / 1e9))
			} else {
				frequency = "N/A"
			}
			data = append(data, []string{start, end, volume, count, frequency})
		}

		fmt.Println()
		fmt.Println("Message statistics")
		cutil.PrintTable(os.Stdout, headers, data)

		headers = []string{"Start", "End", "Field", "Type", "Min", "Max", "Mean", "Sum"}
		data = [][]string{}
		for _, record := range response {
			start := time.Unix(0, int64(record.Start)).Format(time.RFC3339)
			end := time.Unix(0, int64(record.End)).Format(time.RFC3339)
			for _, k := range util.Okeys(record.Statistics.NumStats) {
				stats, ok := record.Statistics.NumStats[k]
				if !ok {
					continue
				}
				fieldName := record.Statistics.Fields[k].Name
				fieldType := record.Statistics.Fields[k].Value.String()
				min := fmt.Sprintf("%f", stats.Min)
				max := fmt.Sprintf("%f", stats.Max)
				mean := fmt.Sprintf("%f", stats.Mean)
				sum := fmt.Sprintf("%f", stats.Sum)
				data = append(data, []string{start, end, fieldName, fieldType, min, max, mean, sum})
			}
		}

		fmt.Println()
		fmt.Println("Numeric field statistics")
		cutil.PrintTable(os.Stdout, headers, data)

		headers = []string{"Start", "End", "Field", "Type", "Min", "Max"}
		data = [][]string{}
		for _, record := range response {
			start := time.Unix(0, int64(record.Start)).Format(time.RFC3339)
			end := time.Unix(0, int64(record.End)).Format(time.RFC3339)
			for _, k := range util.Okeys(record.Statistics.TextStats) {
				stats, ok := record.Statistics.TextStats[k]
				if !ok {
					continue
				}
				fieldName := record.Statistics.Fields[k].Name
				fieldType := record.Statistics.Fields[k].Value.String()
				data = append(data, []string{start, end, fieldName, fieldType, stats.Min, stats.Max})
			}
		}

		fmt.Println()
		fmt.Println("Text field statistics")
		cutil.PrintTable(os.Stdout, headers, data)
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
