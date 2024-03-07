package versionstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	_ "github.com/mattn/go-sqlite3"
)

type sqlversionstore struct {
	db          *sql.DB
	cachesize   int
	initialized bool
	max         uint64

	mtx sync.Mutex
	c   uint64
}

func NewSQLVersionstore(db *sql.DB, cachesize int) Versionstore {
	return &sqlversionstore{
		db:        db,
		cachesize: cachesize,
	}
}

func (vs *sqlversionstore) initialize(ctx context.Context) error {
	_, err := vs.db.ExecContext(ctx, "create table if not exists versions(counter bigint not null)")
	if err != nil {
		return fmt.Errorf("failed to create versions table: %w", err)
	}
	if err = vs.reserve(ctx, vs.cachesize); err != nil {
		return fmt.Errorf("failed to reserve versions: %w", err)
	}
	vs.initialized = true
	return nil
}

func (vs *sqlversionstore) reserve(ctx context.Context, n int) error {
	var newMax uint64
	err := vs.db.QueryRowContext(ctx, "update versions set counter = counter + $1 returning counter", n).Scan(&newMax)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("failed to update version counter: %w", err)
		}
		err = vs.db.QueryRowContext(ctx, "insert into versions (counter) values ($1) returning counter", n).Scan(&newMax)
		if err != nil {
			return fmt.Errorf("failed to initialize versions table: %w", err)
		}
	}
	vs.max = newMax
	vs.c = newMax - uint64(n)
	return nil
}

func (vs *sqlversionstore) Next(ctx context.Context) (uint64, error) {
	if !vs.initialized {
		if err := vs.initialize(ctx); err != nil {
			return 0, fmt.Errorf("failed to initialize version store: %w", err)
		}
	}
	vs.mtx.Lock()
	vs.c++
	vs.mtx.Unlock()
	if vs.c > vs.max {
		if err := vs.reserve(ctx, vs.cachesize); err != nil {
			return 0, err
		}
		return vs.Next(ctx)
	}
	return vs.c, nil
}
