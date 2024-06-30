package cmd

import (
	"context"
	"log/slog"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/spf13/cobra"
	"github.com/wkalt/dp3/server/service"
	"github.com/wkalt/dp3/server/storage"
)

var (
	serverPort               int
	serverCacheSizeMegabytes int
	serverLogLevel           string
	serverSyncWorkers        int
	serverDBPath             string
	serverWALDir             string
	allowedOrigins           []string
	serverSharedKey          string

	// Directory storage provider options
	serverDataDir string

	// S3 storage provider options
	serverS3Endpoint  string
	serverS3AccessKey string
	serverS3SecretKey string
	serverS3Bucket    string
	serverS3UseTLS    bool
	serverS3Region    string
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
		s3requested := serverS3Endpoint != "" ||
			serverS3AccessKey != "" ||
			serverS3SecretKey != "" ||
			serverS3Bucket != ""
		if serverDataDir != "" && s3requested {
			bailf("cannot specify both --data-dir and S3 options")
		}
		if serverDataDir == "" && !s3requested {
			bailf("must specify either --data-dir or S3 options")
		}

		var store storage.Provider
		if serverDataDir == "" {
			mc, err := minio.New(serverS3Endpoint, &minio.Options{
				Creds:  credentials.NewStaticV4(serverS3AccessKey, serverS3SecretKey, ""),
				Secure: serverS3UseTLS,
				Region: serverS3Region,
			})
			if err != nil {
				bailf("error creating S3 client: %s", err)
			}
			store = storage.NewS3Store(mc, serverS3Bucket)
		} else {
			var err error
			store, err = storage.NewDirectoryStore(serverDataDir)
			if err != nil {
				bailf("error creating directory store: %s", err)
			}
		}
		opts := []service.DP3Option{
			service.WithPort(serverPort),
			service.WithCacheSizeMegabytes(uint64(serverCacheSizeMegabytes)),
			service.WithLogLevel(logLevel),
			service.WithStorageProvider(store),
			service.WithWALDir(serverWALDir),
			service.WithDatabasePath(serverDBPath),
			service.WithSyncWorkers(serverSyncWorkers),
			service.WithSharedKey(serverSharedKey),
		}
		if len(allowedOrigins) > 0 {
			opts = append(opts, service.WithAllowedOrigins(allowedOrigins))
		}
		if err := svc.Start(ctx, opts...); err != nil {
			bailf("Shutdown error: %s", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(serverCmd)

	serverCmd.PersistentFlags().IntVarP(&serverPort, "port", "p", 8089, "Port to listen on")
	serverCmd.PersistentFlags().IntVarP(&serverCacheSizeMegabytes, "cache-size", "c", 1024, "Cache size in megabytes")
	serverCmd.PersistentFlags().StringVarP(&serverDataDir, "data-dir", "d", "", "Data directory (for directory storage)")
	serverCmd.PersistentFlags().StringVarP(&serverWALDir, "wal-dir", "", "waldir", "WAL directory")
	serverCmd.PersistentFlags().StringVarP(&serverDBPath, "db-path", "", "dp3.db", "rootmap database location")
	serverCmd.PersistentFlags().StringVarP(&serverLogLevel, "log-level", "l", "info", "Log level")
	serverCmd.PersistentFlags().IntVarP(&serverSyncWorkers, "sync-workers", "", 2, "Sync workers")
	serverCmd.PersistentFlags().StringVarP(&serverSharedKey, "shared-key", "", "", "shared authentication key")

	serverCmd.PersistentFlags().StringSliceVarP(&allowedOrigins, "allowed-origins", "o", []string{}, "Allowed origins")

	serverCmd.PersistentFlags().StringVar(&serverS3Endpoint, "s3-endpoint", "", "S3 endpoint (for S3 storage)")
	serverCmd.PersistentFlags().StringVar(&serverS3AccessKey, "s3-access-key-id", "", "S3 access key ID (for S3 storage)")
	serverCmd.PersistentFlags().StringVar(&serverS3SecretKey, "s3-secret-key", "", "S3 secret key (for S3 storage)")
	serverCmd.PersistentFlags().StringVar(&serverS3Bucket, "s3-bucket", "", "S3 bucket (for S3 storage)")
	serverCmd.PersistentFlags().BoolVarP(&serverS3UseTLS, "s3-tls", "t", false, "Use TLS (for S3 storage)")
	serverCmd.PersistentFlags().StringVar(&serverS3Region, "s3-region", "", "S3 region")
}
