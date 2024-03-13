package service

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/wkalt/dp3/nodestore"
	"github.com/wkalt/dp3/rootmap"
	"github.com/wkalt/dp3/routes"
	"github.com/wkalt/dp3/treemgr"
	"github.com/wkalt/dp3/util"
	"github.com/wkalt/dp3/util/log"
	"github.com/wkalt/dp3/versionstore"
)

/*
This file is the main entrypoint for DP3 server startup.
*/

////////////////////////////////////////////////////////////////////////////////

const (
	gigabyte = 1024 * 1024 * 1024
)

type DP3 struct {
}

// NewDP3Service creates a new DP3 service.
func NewDP3Service() *DP3 {
	return &DP3{}
}

// Start starts the DP3 service.
func (dp3 *DP3) Start(ctx context.Context, options ...DP3Option) error { //nolint:funlen
	opts, err := readOpts(options...)
	if err != nil {
		return fmt.Errorf("failed to read options: %w", err)
	}
	slog.SetLogLoggerLevel(opts.LogLevel)
	log.Debugf(ctx, "Debug logging enabled")
	store := opts.StorageProvider
	cache := util.NewLRU[nodestore.NodeID, nodestore.Node](opts.CacheSizeBytes)
	walpath := "wal.db"
	db, err := sql.Open("sqlite3", walpath)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	if err = db.Ping(); err != nil {
		return fmt.Errorf("failed to ping database at %s: %w", walpath, err)
	}
	wal, err := nodestore.NewSQLWAL(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to open wal at %s: %w", walpath, err)
	}
	ns := nodestore.NewNodestore(store, cache, wal)
	rm, err := rootmap.NewSQLRootmap(db)
	if err != nil {
		return fmt.Errorf("failed to open rootmap: %w", err)
	}
	vs := versionstore.NewSQLVersionstore(db, 1e9)
	tmgr := treemgr.NewTreeManager(ns, vs, rm, 2, opts.SyncWorkers)
	// go tmgr.StartWALSyncLoop(ctx)
	r := routes.MakeRoutes(tmgr)
	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", opts.Port),
		Handler: r,
	}
	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Errorf(ctx, "failed to start server: %s", err)
		}
	}()
	log.Infow(ctx, "Started server",
		"port", opts.Port,
		"cache", util.HumanBytes(opts.CacheSizeBytes),
		"storage", store,
	)
	<-done
	log.Infof(ctx, "Allowing 10 seconds for existing connections to close")
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer func() {
		cancel()
	}()
	if err := srv.Shutdown(ctx); err != nil {
		return fmt.Errorf("failed to shut down server: %w", err)
	}
	log.Infof(ctx, "Server stopped")
	return nil
}

func readOpts(opts ...DP3Option) (*DP3Options, error) {
	options := DP3Options{
		CacheSizeBytes: 1 * gigabyte,
		Port:           8089,
		LogLevel:       slog.LevelInfo,
		SyncWorkers:    10,
	}
	for _, opt := range opts {
		opt(&options)
	}
	if options.StorageProvider == nil {
		return nil, errors.New("storage provider is required")
	}

	return &options, nil
}
