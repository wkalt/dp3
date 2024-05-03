package rootmap

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
)

/*
SQLVersionStore is a version store backed by a SQL database. It is used to
generate version numbers for rootmap entries. The version store is initialized
with a reservation of version numbers to reduce the number of database writes
required to generate a new version number. The reservation size is the number of
version numbers to reserve in each batch. The version store is thread-safe. The
only database that has been used or tested is SQLite.
*/

////////////////////////////////////////////////////////////////////////////////

type sqlVersionStore struct {
	reservationSize int
	max             uint64
	initialized     bool
	mtx             sync.Mutex
	c               uint64
}

func (vs *sqlVersionStore) initializeVersionStore(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, "create table if not exists versions(counter bigint not null)")
	if err != nil {
		return fmt.Errorf("failed to create versions table: %w", err)
	}
	if err = vs.reserveVersionStore(ctx, db, vs.reservationSize); err != nil {
		return fmt.Errorf("failed to reserve versions: %w", err)
	}
	vs.initialized = true
	return nil
}

func (vs *sqlVersionStore) reserveVersionStore(ctx context.Context, db *sql.DB, n int) error {
	var newMax uint64
	err := db.QueryRowContext(ctx, "update versions set counter = counter + $1 returning counter", n).Scan(&newMax)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("failed to update version counter: %w", err)
		}
		err = db.QueryRowContext(ctx, "insert into versions (counter) values ($1) returning counter", n).Scan(&newMax)
		if err != nil {
			return fmt.Errorf("failed to initialize versions table: %w", err)
		}
	}
	vs.max = newMax
	vs.c = newMax - uint64(n) + 1
	return nil
}

func (vs *sqlVersionStore) NextVersion(ctx context.Context, db *sql.DB) (uint64, error) {
	if !vs.initialized {
		if err := vs.initializeVersionStore(ctx, db); err != nil {
			return 0, fmt.Errorf("failed to initialize version store: %w", err)
		}
	}
	vs.mtx.Lock()
	vs.c++
	vs.mtx.Unlock()
	if vs.c > vs.max {
		if err := vs.reserveVersionStore(ctx, db, vs.reservationSize); err != nil {
			return 0, err
		}
		return vs.NextVersion(ctx, db)
	}
	return vs.c, nil
}

func NewSQLVersionStore(ctx context.Context, reservationSize int) *sqlVersionStore {
	return &sqlVersionStore{
		reservationSize: reservationSize,
		mtx:             sync.Mutex{},
	}
}
