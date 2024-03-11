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
	"path"
	"path/filepath"
	"syscall"
	"time"

	"github.com/wkalt/dp3/nodestore"
	"github.com/wkalt/dp3/rootmap"
	"github.com/wkalt/dp3/routes"
	"github.com/wkalt/dp3/storage"
	"github.com/wkalt/dp3/treemgr"
	"github.com/wkalt/dp3/util"
	"github.com/wkalt/dp3/util/log"
	"github.com/wkalt/dp3/versionstore"
)

const (
	gigabyte = 1024 * 1024 * 1024
)

type DP3 struct {
}

func readOpts(opts ...DP3Option) DP3Options {
	options := DP3Options{
		CacheSizeBytes: 1 * gigabyte,
		Port:           8089,
		DataDir:        "data",
		LogLevel:       slog.LevelInfo,
	}
	for _, opt := range opts {
		opt(&options)
	}
	return options
}

func (dp3 *DP3) Start(ctx context.Context, options ...DP3Option) error {
	opts := readOpts(options...)
	slog.SetLogLoggerLevel(opts.LogLevel)
	store := storage.NewDirectoryStore(opts.DataDir)
	cache := util.NewLRU[nodestore.NodeID, nodestore.Node](opts.CacheSizeBytes)
	walpath := path.Join(opts.DataDir, "wal.db")
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
	tmgr := treemgr.NewTreeManager(ns, vs, rm, 2)
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
	datadir, err := filepath.Abs(opts.DataDir)
	if err != nil {
		return fmt.Errorf("failed to get absolute path of data dir: %w", err)
	}
	log.Infow(ctx, "Started server",
		"port", opts.Port,
		"cache", util.HumanBytes(opts.CacheSizeBytes),
		"datadir", datadir,
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

func NewDP3Service() *DP3 {
	return &DP3{}
}
