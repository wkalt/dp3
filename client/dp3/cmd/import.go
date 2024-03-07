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
	"github.com/wkalt/dp3/routes"
)

var (
	importFilename   string
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
			panic(err)
		}
		req := &routes.ImportRequest{
			ProducerID: producerID,
			Path:       abs,
		}
		buf := &bytes.Buffer{}
		if err = json.NewEncoder(buf).Encode(req); err != nil {
			panic(err)
		}
		resp, err := http.Post("http://localhost:8089/import", "application/json", buf)
		if err != nil {
			panic(err)
		}
		_, err = os.Stdout.ReadFrom(resp.Body)
		if err != nil {
			panic(err)
		}
	},
}

func init() {
	rootCmd.AddCommand(importCmd)
	importCmd.PersistentFlags().StringVarP(&importProducerID, "producer", "p", "", "Producer ID")
	importCmd.MarkFlagRequired("producer")

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// importCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// importCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
