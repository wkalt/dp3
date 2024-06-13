package rootmap

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/mattn/go-sqlite3"
	"github.com/wkalt/dp3/nodestore"
	"golang.org/x/exp/maps"
)

/*
SQLRootmap is a rootmap implementation backed by a sql.DB. The only database
that has been used or tested is SQLite.
*/

////////////////////////////////////////////////////////////////////////////////

type sqlRootmap struct {
	db  *sql.DB
	mtx *sync.Mutex

	tables    map[table]int64
	tablesMtx *sync.RWMutex
}

func (rm *sqlRootmap) initialize(ctx context.Context) error {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()
	if err := Migrate(rm.db); err != nil {
		return fmt.Errorf("failed to migrate database: %w", err)
	}
	if _, err := rm.db.ExecContext(ctx, `
	PRAGMA foreign_keys = ON;
	`); err != nil {
		return fmt.Errorf("failed to execute pragmas: %w", err)
	}
	if err := rm.initializeTableCache(); err != nil {
		return fmt.Errorf("failed to initialize table cache: %w", err)
	}
	return nil
}

func (rm *sqlRootmap) getTableID(database, producer, topic string) int64 {
	rm.tablesMtx.RLock()
	defer rm.tablesMtx.RUnlock()
	if tableID, ok := rm.tables[table{database, producer, topic}]; ok {
		return tableID
	}
	return -1
}

func (rm *sqlRootmap) putTableID(database, producer, topic string, tableID int64) {
	rm.tablesMtx.Lock()
	defer rm.tablesMtx.Unlock()
	rm.tables[table{database, producer, topic}] = tableID
}

func (rm *sqlRootmap) initializeTableCache() error {
	tempTables := make(map[table]int64)
	tx, err := rm.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	rows, err := tx.Query("SELECT database, producer, topic, id FROM tables")
	if err != nil {
		if err := tx.Rollback(); err != nil {
			return fmt.Errorf("failed to rollback transaction: %w", err)
		}
		return fmt.Errorf("failed to read from tables: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var database, producer, topic string
		var tableID int64
		if err := rows.Scan(&database, &producer, &topic, &tableID); err != nil {
			if err := tx.Rollback(); err != nil {
				return fmt.Errorf("failed to rollback transaction: %w", err)
			}
			return fmt.Errorf("failed to scan table data: %w", err)
		}
		tempTables[table{database, producer, topic}] = tableID
	}
	if err := rows.Err(); err != nil {
		if err := tx.Rollback(); err != nil {
			return fmt.Errorf("failed to rollback transaction: %w", err)
		}
		return fmt.Errorf("final error in rows handling: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("transaction commit failed: %w", err)
	}

	rm.tables = tempTables
	return nil
}

func (rm *sqlRootmap) withTransaction(
	f func(tx *sql.Tx) error,
) error {
	tx, err := rm.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	if err := f(tx); err != nil {
		if err := tx.Rollback(); err != nil {
			return fmt.Errorf("failed to rollback transaction: %w", err)
		}
		return fmt.Errorf("failed to execute transaction: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

func (rm *sqlRootmap) getLatestByTopicQuery(
	database string,
	producer string,
	topics map[string]uint64,
) (query string, params []interface{}) {
	sb := strings.Builder{}
	sb.WriteString(`
	select tables.topic,
	tables.producer,
	r1.storage_prefix,
	r1.node_id,
	r1.version,
	r1.timestamp,
	latest_truncations.version as truncation_version
	from tables inner join rootmap r1 
	on tables.id = r1.table_id
	left join rootmap r2
	on (r1.table_id = r2.table_id and r1.version < r2.version)
	left join latest_truncations
	on latest_truncations.table_id = tables.id
	where r2.rowid is null
	and tables.database = ?
	`)
	params = []any{database}
	if producer != "" {
		sb.WriteString(" and tables.producer = ? ")
		params = append(params, producer)
	}
	if len(topics) > 0 {
		sb.WriteString(" and tables.topic in (")
		for i, topic := range maps.Keys(topics) {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString("?")
			params = append(params, topic)
		}
		sb.WriteString(")")
	}
	return sb.String(), params
}

func (rm *sqlRootmap) Put(
	ctx context.Context,
	database string,
	producer string,
	topic string,
	version uint64,
	prefix string,
	nodeID nodestore.NodeID,
) error {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()
	err := rm.withTransaction(func(tx *sql.Tx) error {
		var tableID int64
		err := tx.QueryRowContext(ctx, `
		select id from tables where producer = ? and topic = ? and database = ? 
		`, producer, topic, database).Scan(&tableID)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("failed to read from tables: %w", err)
		}
		if errors.Is(err, sql.ErrNoRows) {
			err := tx.QueryRowContext(ctx, `
			insert into tables (database, producer, topic) values (?, ?, ?)
			returning id
			`, database, producer, topic).Scan(&tableID)
			if err != nil {
				return fmt.Errorf("failed to insert into tables: %w", err)
			}
			rm.putTableID(database, producer, topic, tableID)
		}

		timestamp := time.Now().UTC().Format(time.RFC3339Nano)

		_, err = tx.ExecContext(ctx, `
		insert into rootmap (
			table_id,
			version,
			storage_prefix,
			timestamp,
			node_id
		) values (?, ?, ?, ?, ?)`,
			tableID, version, prefix, timestamp, hex.EncodeToString(nodeID[:]),
		)
		if err != nil {
			var err sqlite3.Error
			if errors.As(err, &sqlite3.Error{Code: sqlite3.ErrConstraint}) {
				return ErrRootAlreadyExists
			}
			return fmt.Errorf("failed to store to rootmap: %w", err)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (rm *sqlRootmap) GetHistorical(
	ctx context.Context,
	database string,
	producer string,
	topic string,
) (listings []RootListing, err error) {
	rows, err := rm.db.QueryContext(ctx, `
	select storage_prefix,
	node_id,
	rootmap.version,
	rootmap.timestamp,
	latest_truncations.version as truncation_version
	from rootmap inner join tables on tables.id = rootmap.table_id
	left join latest_truncations
	on latest_truncations.table_id = tables.id
	where tables.database = ? 
	and tables.producer = ?
	and tables.topic = ?
	order by rootmap.version desc
	`, database, producer, topic)
	if err != nil {
		return listings, fmt.Errorf("failed to read from rootmap: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var prefix, timestamp, nodeID string
		var truncationVersion *uint64
		var version uint64
		if err := rows.Scan(&prefix, &nodeID, &version, &timestamp, &truncationVersion); err != nil {
			return listings, fmt.Errorf("failed to read from rootmap: %w", err)
		}
		minVersion := uint64(0)
		if truncationVersion != nil {
			if version <= *truncationVersion {
				continue
			}
			minVersion = *truncationVersion + 1
		}
		decoded, err := hex.DecodeString(nodeID)
		if err != nil {
			return listings, fmt.Errorf("failed to decode node ID: %w", err)
		}
		listings = append(listings, RootListing{
			prefix, producer, topic, nodestore.NodeID(decoded), version, timestamp, minVersion,
		})
	}
	if err := rows.Err(); err != nil {
		return listings, fmt.Errorf("failed to read from rootmap: %w", err)
	}
	return listings, nil
}

func (rm *sqlRootmap) GetLatestByTopic(
	ctx context.Context,
	database string,
	producer string,
	topics map[string]uint64,
) ([]RootListing, error) {
	query, params := rm.getLatestByTopicQuery(database, producer, topics)
	rows, err := rm.db.QueryContext(ctx, query, params...)
	if err != nil {
		return nil, fmt.Errorf("failed to read from rootmap: %w", err)
	}
	defer rows.Close()

	listings := make([]RootListing, 0, len(topics))
	for rows.Next() {
		var version uint64
		var truncationVersion *uint64
		var nodeID, topic, prefix, producer, timestamp string
		if err := rows.Scan(&topic, &producer, &prefix, &nodeID,
			&version, &timestamp, &truncationVersion); err != nil {
			return nil, fmt.Errorf("failed to read from rootmap: %w", err)
		}
		decoded, err := hex.DecodeString(nodeID)
		if err != nil {
			return nil, fmt.Errorf("failed to decode node ID: %w", err)
		}
		minVersion := topics[topic]
		if truncationVersion != nil {
			if minVersion > 0 && *truncationVersion >= minVersion {
				continue
			}
			minVersion = max(minVersion, *truncationVersion+1)
		}
		if version > topics[topic] {
			listings = append(listings, RootListing{
				prefix, producer, topic, nodestore.NodeID(decoded), version, timestamp, minVersion,
			})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read from rootmap: %w", err)
	}
	return listings, nil
}

func (rm *sqlRootmap) GetLatest(
	ctx context.Context,
	database string,
	producer string,
	topic string,
) (prefix string, nodeID nodestore.NodeID, version uint64, truncationVersion uint64, err error) {
	var truncationVersionPtr *int64
	var nodeIDStr string

	err = rm.db.QueryRowContext(ctx, `
	select storage_prefix, node_id, rootmap.version, latest_truncations.version
	from tables inner join rootmap on tables.id = rootmap.table_id
	left join latest_truncations on latest_truncations.table_id = tables.id
	where tables.database = ?
	and tables.producer = ?
	and tables.topic = ?
	order by rootmap.version desc limit 1
	`, database, producer, topic,
	).Scan(&prefix, &nodeIDStr, &version, &truncationVersionPtr)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return prefix, nodeID, version, truncationVersion, NewTableNotFoundError(database, producer, topic)
		}
		return prefix, nodeID, version, truncationVersion, fmt.Errorf("failed to read from rootmap: %w", err)
	}
	decoded, err := hex.DecodeString(nodeIDStr)
	if err != nil {
		return prefix, nodeID, version, truncationVersion, fmt.Errorf("failed to decode node ID: %w", err)
	}
	if truncationVersionPtr != nil {
		truncationVersion = uint64(*truncationVersionPtr)
	}
	return prefix, nodestore.NodeID(decoded), version, truncationVersion, nil
}

func (rm *sqlRootmap) Truncate(
	ctx context.Context,
	database string,
	producer string,
	topic string,
	timestamp int64,
) error {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()
	tableID := rm.getTableID(database, producer, topic)
	if tableID == -1 {
		return NewTableNotFoundError(database, producer, topic)
	}
	truncationTime := time.Unix(0, timestamp).UTC()
	truncateTimeStr := truncationTime.Format(time.RFC3339Nano)

	_, err := rm.db.ExecContext(ctx, `
	insert into truncations (table_id, version)
	select tables.id, rootmap.version
	from tables inner join rootmap
	on tables.id = rootmap.table_id
	where rootmap.timestamp <= ?
	and tables.database = ?
	and tables.id = ?
	order by rootmap.timestamp desc
	limit 1
	`, truncateTimeStr, database, tableID)
	if err != nil {
		return fmt.Errorf("failed to insert into truncations: %w", err)
	}
	return nil
}

func (rm *sqlRootmap) Databases(ctx context.Context) ([]string, error) {
	rows, err := rm.db.QueryContext(ctx, `
	select distinct database from tables
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to read from tables: %w", err)
	}
	defer rows.Close()
	databases := []string{}
	for rows.Next() {
		var database string
		if err := rows.Scan(&database); err != nil {
			return nil, fmt.Errorf("failed to read from tables: %w", err)
		}
		databases = append(databases, database)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read from tables: %w", err)
	}
	return databases, nil
}

func (rm *sqlRootmap) Producers(
	ctx context.Context, database string,
) ([]string, error) {
	rows, err := rm.db.QueryContext(
		ctx, `select distinct producer from tables
		where database = ?`, database)
	if err != nil {
		return nil, fmt.Errorf("failed to read from tables: %w", err)
	}
	defer rows.Close()
	producers := []string{}
	for rows.Next() {
		var producer string
		if err := rows.Scan(&producer); err != nil {
			return nil, fmt.Errorf("failed to read from tables: %w", err)
		}
		producers = append(producers, producer)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read from tables: %w", err)
	}
	return producers, nil
}

func (rm *sqlRootmap) Topics(ctx context.Context, database string) ([]string, error) {
	rows, err := rm.db.QueryContext(ctx,
		`select distinct topic from tables
		where database = ?
		`, database)
	if err != nil {
		return nil, fmt.Errorf("failed to read from tables: %w", err)
	}
	defer rows.Close()
	topics := []string{}
	for rows.Next() {
		var topic string
		if err := rows.Scan(&topic); err != nil {
			return nil, fmt.Errorf("failed to read from tables: %w", err)
		}
		topics = append(topics, topic)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read from tables: %w", err)
	}
	return topics, nil
}

func (rm *sqlRootmap) Get(
	ctx context.Context,
	database string,
	producer string,
	topic string,
	version uint64,
) (prefix string, nodeID nodestore.NodeID, err error) {
	var nodeIDStr string
	var truncationVersion *int64
	err = rm.db.QueryRowContext(ctx, `
	select storage_prefix, node_id, latest_truncations.version
	from tables inner join rootmap
	on tables.id = rootmap.table_id
	left join latest_truncations
	on latest_truncations.table_id = tables.id
	where tables.database = ?
	and tables.producer = ?
	and tables.topic = ?
	and rootmap.version = ?
	`, database, producer, topic, version,
	).Scan(&prefix, &nodeIDStr, &truncationVersion)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return prefix, nodeID, NewTableNotFoundError(database, producer, topic)
		}
		return prefix, nodeID, fmt.Errorf("failed to read from rootmap: %w", err)
	}
	decoded, err := hex.DecodeString(nodeIDStr)
	if err != nil {
		return prefix, nodeID, fmt.Errorf("failed to decode node ID: %w", err)
	}
	return prefix, nodestore.NodeID(decoded), nil
}

func NewSQLRootmap(ctx context.Context, db *sql.DB, opts ...Option) (Rootmap, error) {
	rm := &sqlRootmap{
		db:        db,
		mtx:       &sync.Mutex{},
		tables:    make(map[table]int64),
		tablesMtx: &sync.RWMutex{},
	}
	err := rm.initialize(ctx)
	if err != nil {
		return nil, err
	}
	return rm, nil
}
