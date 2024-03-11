/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/wkalt/dp3/client/dp3/util"
	"github.com/wkalt/dp3/routes"
)

var (
	importProducerID string
)

// importCmd represents the import command
var importCmd = &cobra.Command{
	Use:   "import --producer my-robot [file]",
	Short: "Import an MCAP file into dp3",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			cmd.Usage()
			return
		}
		producerID := importProducerID
		filename := args[0]
		abs, err := filepath.Abs(filename)
		if err != nil {
			bailf("error getting absolute path: %s", err)
		}
		req := &routes.ImportRequest{
			ProducerID: producerID,
			Path:       abs,
		}
		buf := &bytes.Buffer{}
		if err = json.NewEncoder(buf).Encode(req); err != nil {
			bailf("error encoding request: %s", err)
		}
		resp, err := http.Post("http://localhost:8089/import", "application/json", buf)
		if err != nil {
			bailf("error calling import: %s", err)
		}
		defer resp.Body.Close()
		util.MustOK(resp)
		_, err = os.Stdout.ReadFrom(resp.Body)
		if err != nil {
			bailf("error reading response: %s", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(importCmd)
	importCmd.PersistentFlags().StringVarP(&importProducerID, "producer", "p", "", "Producer ID")
	importCmd.MarkFlagRequired("producer")
}
