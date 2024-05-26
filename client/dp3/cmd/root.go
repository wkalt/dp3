package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var serverURL string

var rootCmd = &cobra.Command{
	Use:   "dp3",
	Short: "dp3 client and server",
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func bailf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&serverURL, "server-url", "", "http://localhost:8089", "server-url")
}
