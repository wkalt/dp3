package service

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
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

type DP3 struct {
}

func (dp3 *DP3) Start(ctx context.Context) error {
	store := storage.NewDirectoryStore("data")
	cache := util.NewLRU[nodestore.NodeID, nodestore.Node](1000)
	db, err := sql.Open("sqlite3", "wal.db")
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	wal, err := nodestore.NewSQLWAL(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to open wal: %w", err)
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
		Addr:    fmt.Sprintf(":%d", 8089),
		Handler: r,
	}

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Errorf(ctx, "failed to start server: %s", err)
		}
	}()

	log.Infow(ctx, "Started server", "port", 8089)
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
