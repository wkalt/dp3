package cmd

import (
	"context"
	"log/slog"

	"github.com/spf13/cobra"
	"github.com/wkalt/dp3/service"
)

var (
	serverPort               int
	serverCacheSizeMegabytes int
	serverDataDir            string
	serverLogLevel           string
)

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Start the dp3 server",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		svc := service.NewDP3Service()
		logLevel := slog.LevelInfo
		if serverLogLevel != "" {
			switch serverLogLevel {
			case "debug":
				logLevel = slog.LevelDebug
			case "info":
				logLevel = slog.LevelInfo
			case "warn":
				logLevel = slog.LevelWarn
			case "error":
				logLevel = slog.LevelError
			default:
				bailf("invalid log level: %s", serverLogLevel)
			}
		}
		if err := svc.Start(ctx,
			service.WithPort(serverPort),
			service.WithCacheSizeMegabytes(uint64(serverCacheSizeMegabytes)),
			service.WithDataDir(serverDataDir),
			service.WithLogLevel(logLevel),
		); err != nil {
			bailf("error starting server: %s", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(serverCmd)

	serverCmd.PersistentFlags().IntVarP(&serverPort, "port", "p", 8089, "Port to listen on")
	serverCmd.PersistentFlags().IntVarP(&serverCacheSizeMegabytes, "cache-size", "c", 1024, "Cache size in megabytes")
	serverCmd.PersistentFlags().StringVarP(&serverDataDir, "data-dir", "d", "", "Data directory")
	serverCmd.PersistentFlags().StringVarP(&serverLogLevel, "log-level", "l", "info", "Log level")

	serverCmd.MarkPersistentFlagRequired("data-dir")
}
