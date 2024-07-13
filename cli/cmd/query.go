package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/wkalt/dp3/cli/util"
	"github.com/wkalt/dp3/server/mcap"
	"github.com/wkalt/dp3/server/routes"
)

var (
	queryJSON bool
)

// queryCmd represents the query command
var queryCmd = &cobra.Command{
	Use: "query [single-quoted string]",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			fmt.Println("query requires exactly one single-quoted query string")
			return
		}
		messageRequest := &routes.QueryRequest{
			Query: args[0],
		}
		buf := &bytes.Buffer{}
		if err := json.NewEncoder(buf).Encode(messageRequest); err != nil {
			bailf("error encoding request: %s", err)
		}
		resp, err := httpc.Post(serverURL+"/query", "application/json", buf)
		if err != nil {
			bailf("error calling export: %s", err)
		}
		util.MustOK(resp)
		if exportJSON {
			if err := mcap.ToJSON(os.Stdout, resp.Body); err != nil {
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
	rootCmd.AddCommand(queryCmd)
	queryCmd.PersistentFlags().BoolVarP(&exportJSON, "json", "", false, "Output in JSON format")
}
