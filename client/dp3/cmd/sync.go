package cmd

import (
	"net/http"

	"github.com/spf13/cobra"
	"github.com/wkalt/dp3/client/dp3/util"
)

var syncCmd = &cobra.Command{
	Use:   "sync",
	Short: "Sync current WAL",
	Run: func(cmd *cobra.Command, args []string) {
		resp, err := http.Post("http://localhost:8089/sync", "application/json", nil)
		if err != nil {
			bailf("error calling sync: %s", err)
		}
		util.MustOK(resp)
	},
}

func init() {
	rootCmd.AddCommand(syncCmd)
}
