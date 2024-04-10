/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/wkalt/dp3/routes"
	"golang.org/x/sync/errgroup"
)

var (
	importProducerID  string
	importWorkerCount int
)

func doImport(producer string, paths []string, workers int) error {
	g := &errgroup.Group{}
	g.SetLimit(workers)
	for _, path := range paths {
		g.Go(func() error {
			abs, err := filepath.Abs(path)
			if err != nil {
				return fmt.Errorf("error getting absolute path: %w", err)
			}
			req := &routes.ImportRequest{
				ProducerID: producer,
				Path:       abs,
			}
			buf := &bytes.Buffer{}
			if err = json.NewEncoder(buf).Encode(req); err != nil {
				return fmt.Errorf("error encoding request: %s", err)
			}

			resp, err := http.Post("http://localhost:8089/import", "application/json", buf)
			if err != nil {
				return fmt.Errorf("error calling import: %s", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				body, err := io.ReadAll(resp.Body)
				if err != nil {
					return fmt.Errorf("unexpected status code: %s", resp.Status)
				}
				return fmt.Errorf("failed to import %s: %s", path, body)
			}
			return nil
		})
	}
	return g.Wait()
}

// importCmd represents the import command
var importCmd = &cobra.Command{
	Use:   "import --producer my-robot [file]",
	Short: "Import an MCAP file into dp3",
	Run: func(cmd *cobra.Command, paths []string) {
		if len(paths) < 1 {
			cmd.Usage()
			return
		}
		if err := doImport(importProducerID, paths, importWorkerCount); err != nil {
			bailf("Import error: %s", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(importCmd)
	importCmd.PersistentFlags().StringVarP(&importProducerID, "producer", "p", "", "Producer ID")
	importCmd.PersistentFlags().IntVarP(&importWorkerCount, "workers", "w", 1, "Worker count")
	importCmd.MarkFlagRequired("producer")
}
