package cmd

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"

	"github.com/relvacode/iso8601"
	"github.com/spf13/cobra"
	"github.com/wkalt/dp3/client/dp3/util"
	"github.com/wkalt/dp3/routes"
)

var (
	exportProducerID string
	exportEndDate    string
	exportStartDate  string
	exportTopics     []string
	exportJSON       bool
)

// exportCmd represents the export command
var exportCmd = &cobra.Command{
	Use:   "export",
	Short: "Query dp3 for export",
	Run: func(cmd *cobra.Command, args []string) {
		start, err := iso8601.Parse([]byte(exportStartDate))
		if err != nil {
			bailf("error parsing start date: %s", err)
		}
		end, err := iso8601.Parse([]byte(exportEndDate))
		if err != nil {
			bailf("error parsing end date: %s", err)
		}
		messageRequest := &routes.ExportRequest{
			Start:      uint64(start.UnixNano()),
			End:        uint64(end.UnixNano()),
			ProducerID: exportProducerID,
			Topics:     exportTopics,
		}
		buf := &bytes.Buffer{}
		if err := json.NewEncoder(buf).Encode(messageRequest); err != nil {
			bailf("error encoding request: %s", err)
		}
		resp, err := http.Post("http://localhost:8089/export", "application/json", buf)
		if err != nil {
			bailf("error calling export: %s", err)
		}
		util.MustOK(resp)

		if exportJSON {
			if err := util.MCAPToJSON(os.Stdout, resp.Body); err != nil {
				bailf("error converting to JSON: %s", err)
			}
			return
		}

		if _, err = os.Stdout.ReadFrom(resp.Body); err != nil {
			bailf("error reading response: %s", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(exportCmd)

	exportCmd.PersistentFlags().StringVarP(&exportProducerID, "producer", "p", "", "Producer ID")
	exportCmd.PersistentFlags().StringVarP(&exportStartDate, "start", "s", "", "Start date")
	exportCmd.PersistentFlags().StringVarP(&exportEndDate, "end", "e", "", "End date")
	exportCmd.PersistentFlags().StringArrayVarP(&exportTopics, "topics", "t", []string{}, "Topics to query")
	exportCmd.PersistentFlags().BoolVarP(&exportJSON, "json", "", false, "Output in JSON format")

	exportCmd.MarkFlagRequired("producer")
	exportCmd.MarkFlagRequired("start")
	exportCmd.MarkFlagRequired("end")
}
