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

	"net/http/pprof"

	"github.com/gorilla/mux"
	"github.com/wkalt/dp3/server/nodestore"
	"github.com/wkalt/dp3/server/rootmap"
	"github.com/wkalt/dp3/server/routes"
	"github.com/wkalt/dp3/server/schemastore"
	"github.com/wkalt/dp3/server/treemgr"
	"github.com/wkalt/dp3/server/util"
	"github.com/wkalt/dp3/server/util/log"
	"github.com/wkalt/dp3/server/versionstore"
	"github.com/wkalt/dp3/server/wal"
)

/*
This file is the main entrypoint for DP3 server startup.
*/

////////////////////////////////////////////////////////////////////////////////

const (
	gigabyte = 1024 * 1024 * 1024
)

type DP3 struct{}

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
	dbpath := opts.DatabasePath + "?_journal=WAL&mode=rwc"
	log.Infof(ctx, "Opening database at %s", dbpath)
	db, err := sql.Open("sqlite3", dbpath)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()
	if err = db.Ping(); err != nil {
		return fmt.Errorf("failed to ping database at %s: %w", dbpath, err)
	}

	ns := nodestore.NewNodestore(store, cache)
	rm, err := rootmap.NewSQLRootmap(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to open rootmap: %w", err)
	}

	versiondb, err := sql.Open("sqlite3", "versions.db")
	if err != nil {
		return fmt.Errorf("failed to open version database: %w", err)
	}
	defer versiondb.Close()
	if err = versiondb.Ping(); err != nil {
		return fmt.Errorf("failed to ping database at %s: %w", dbpath, err)
	}
	vs := versionstore.NewVersionStore(versiondb, 1e9)

	walopts := []wal.Option{
		wal.WithInactiveBatchMergeInterval(2 * time.Second),
		wal.WithGCInterval(10 * time.Second),
	}
	if err := util.EnsureDirectoryExists(opts.WALDir); err != nil {
		return fmt.Errorf("failed to ensure WAL directory exists: %w", err)
	}

	ss := schemastore.NewSchemaStore(store, "schemas", 1000)
	tmgr, err := treemgr.NewTreeManager(
		ctx,
		ns,
		ss,
		vs,
		rm,
		treemgr.WithSyncWorkers(opts.SyncWorkers),
		treemgr.WithWALOpts(walopts...),
		treemgr.WithWALDir(opts.WALDir),
		treemgr.WithWALBufferSize(10000),
	)
	if err != nil {
		return fmt.Errorf("failed to create tree manager: %w", err)
	}
	log.Infof(ctx, "Building routes with allowed origins %+v", opts.AllowedOrigins)
	r := routes.MakeRoutes(tmgr, opts.AllowedOrigins, opts.SharedKey)
	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", opts.Port),
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
	}

	sigint := make(chan os.Signal, 1)
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigint, syscall.SIGINT)
	signal.Notify(sigterm, syscall.SIGTERM)

	startErr := make(chan error)
	go func() {
		log.Infow(ctx, "Starting server",
			"port", opts.Port, "cache", util.HumanBytes(opts.CacheSizeBytes), "storage", store)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			startErr <- err
		}
	}()

	go func() {
		r := mux.NewRouter()
		r.PathPrefix("/debug/pprof/").HandlerFunc(pprof.Index)
		log.Infof(ctx, "Starting pprof server on :6060")
		srv := &http.Server{
			Addr:              "localhost:6060",
			Handler:           r,
			ReadHeaderTimeout: 5 * time.Second,
		}
		if err := srv.ListenAndServe(); err != nil {
			log.Errorf(ctx, "failed to start pprof server: %s", err)
		}
	}()

	select {
	case <-sigint:
		log.Infof(ctx, "Received SIGINT")
	case <-sigterm:
		log.Infof(ctx, "Received SIGTERM")
	case err := <-startErr:
		return fmt.Errorf("failed to start server: %w", err)
	}

	log.Infof(ctx, "Allowing 10 seconds for existing connections to close")
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	errs := make(chan error)
	success := make(chan bool)

	go func() {
		if err := srv.Shutdown(ctx); err != nil {
			errs <- err
		} else {
			log.Infof(ctx, "Server stopped")
			success <- true
		}
	}()

	select {
	case <-sigint:
		return errors.New("forceful shutdown on second interrupt")
	case err := <-errs:
		return fmt.Errorf("server shutdown failed: %w", err)
	case <-success:
		return nil
	}
}

func readOpts(opts ...DP3Option) (*DP3Options, error) {
	options := DP3Options{
		CacheSizeBytes: 1 * gigabyte,
		Port:           8089,
		LogLevel:       slog.LevelInfo,
		SyncWorkers:    4,
		DatabasePath:   "dp3.db",
		WALDir:         "waldir",
		AllowedOrigins: []string{
			"http://localhost:5174",
			"http://localhost:5173",
			"http://localhost:8080",
		},
		SharedKey: "",
	}
	for _, opt := range opts {
		opt(&options)
	}
	if options.StorageProvider == nil {
		return nil, errors.New("storage provider is required")
	}

	return &options, nil
}
