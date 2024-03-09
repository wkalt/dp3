package service

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"

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
	log.Infow(ctx, "Starting server", "port", 8089)
	if err := srv.ListenAndServe(); err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}
	return nil
}

func NewDP3Service() *DP3 {
	return &DP3{}
}
