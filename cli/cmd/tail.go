/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"time"

	"github.com/relvacode/iso8601"
	"github.com/spf13/cobra"
	"github.com/wkalt/dp3/cli/util"
	"github.com/wkalt/dp3/server/mcap"
	"github.com/wkalt/dp3/server/routes"
)

var (
	tailProducer  string
	tailEndDate   string
	tailStartDate string
	tailDatabase  string
	tailTopics    []string
)

// tailCmd represents the tail command
var tailCmd = &cobra.Command{
	Use:   "tail",
	Short: "Tail logs for a selection of topics",
	Run: func(cmd *cobra.Command, args []string) {
		if exportStartDate == "" {
			exportStartDate = "1970-01-01"
		}
		if exportEndDate == "" {
			exportEndDate = "2050-01-01"
		}
		start, err := iso8601.Parse([]byte(exportStartDate))
		if err != nil {
			bailf("error parsing start date: %s", err)
		}
		end, err := iso8601.Parse([]byte(exportEndDate))
		if err != nil {
			bailf("error parsing end date: %s", err)
		}
		topics := make(map[string]uint64)
		for _, topic := range tailTopics {
			topics[topic] = 0
		}

		for {
			messageRequest := &routes.ExportRequest{
				Database: tailDatabase,
				Start:    uint64(start.UnixNano()),
				End:      uint64(end.UnixNano()),
				Producer: tailProducer,
				Topics:   topics,
			}
			buf := &bytes.Buffer{}
			if err := json.NewEncoder(buf).Encode(messageRequest); err != nil {
				bailf("error encoding request: %s", err)
			}
			resp, err := httpc.Post(serverURL+"/export", "application/json", buf)
			if err != nil {
				bailf("error calling export: %s", err)
			}
			util.MustOK(resp)

			if resp.StatusCode == http.StatusAccepted {
				time.Sleep(500 * time.Millisecond)
			}

			topicsHeader := resp.Header.Get("X-Topics")
			if err := json.Unmarshal([]byte(topicsHeader), &topics); err != nil {
				bailf("error decoding topics header: %s", err)
			}
			if err := mcap.MCAPToJSON(os.Stdout, resp.Body); err != nil {
				bailf("error converting to JSON: %s", err)
			}
		}

	},
}

func init() {
	rootCmd.AddCommand(tailCmd)

	tailCmd.PersistentFlags().StringVarP(&tailDatabase, "database", "d", "", "Database name")
	tailCmd.PersistentFlags().StringVarP(&tailProducer, "producer", "p", "", "Producer ID")
	tailCmd.PersistentFlags().StringVarP(&tailStartDate, "start", "s", "", "Start date")
	tailCmd.PersistentFlags().StringVarP(&tailEndDate, "end", "e", "", "End date")
	tailCmd.PersistentFlags().StringArrayVarP(&tailTopics, "topics", "t", []string{}, "Topics to query")

	tailCmd.MarkFlagRequired("producer")
	tailCmd.MarkFlagRequired("database")
}
