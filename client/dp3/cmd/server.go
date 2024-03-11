package cmd

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/wkalt/dp3/service"
)

var (
	serverPort               int
	serverCacheSizeMegabytes int
	serverDataDir            string
)

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Start the dp3 server",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		svc := service.NewDP3Service()
		if err := svc.Start(ctx,
			service.WithPort(serverPort),
			service.WithCacheSizeMegabytes(uint64(serverCacheSizeMegabytes)),
			service.WithDataDir(serverDataDir),
		); err != nil {
			bailf("error starting server: %s", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(serverCmd)

	serverCmd.PersistentFlags().IntVarP(&serverPort, "port", "p", 8089, "Port to listen on")
	serverCmd.PersistentFlags().IntVarP(&serverCacheSizeMegabytes, "cache-size", "c", 1024, "Cache size in megabytes")
	serverCmd.PersistentFlags().StringVarP(&serverDataDir, "data-dir", "d", "data", "Data directory")
}
