package cmd

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/wkalt/dp3/service"
)

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Start the dp3 server",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		svc := service.NewDP3Service()
		svc.Start(ctx)
	},
}

func init() {
	rootCmd.AddCommand(serverCmd)
}
