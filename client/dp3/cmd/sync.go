package cmd

import (
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
)

var syncCmd = &cobra.Command{
	Use:   "sync",
	Short: "Sync current WAL",
	Run: func(cmd *cobra.Command, args []string) {
		_, err := http.Post("http://localhost:8089/sync", "application/json", nil)
		if err != nil {
			fmt.Println("error calling sync", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(syncCmd)
}
