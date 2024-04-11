package rootmap

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/mattn/go-sqlite3"
	"github.com/wkalt/dp3/nodestore"
	"golang.org/x/exp/maps"
)

/*
SQLRootmap is a rootmap implementation backed by a sql.DB. The only database
that has been used or tested is sqlite.
*/

type sqlRootmap struct {
	db  *sql.DB
	mtx *sync.Mutex
}

func (rm *sqlRootmap) initialize() error {
	var maxApplied int64
	err := rm.db.QueryRow("select max(version) from schema_migrations").Scan(&maxApplied)
	if err == nil && maxApplied == 1 {
		return nil
	}
	if _, err := rm.db.Exec(`
	create table if not exists rootmap (
		producer_id text not null,
		topic text not null,
		version bigint not null,
		storage_prefix text not null,
		node_id text not null,
		timestamp text not null default current_timestamp,
		primary key (producer_id, topic, version)
	);

	create table schema_migrations(
		version bigint not null,
		timestamp text not null default current_timestamp
	);

	insert into schema_migrations(version) values (1);
	`); err != nil {
		return fmt.Errorf("failed to migrate database: %w", err)
	}
	return nil
}

func (rm *sqlRootmap) Put(
	ctx context.Context,
	producerID string,
	topic string,
	version uint64,
	prefix string,
	nodeID nodestore.NodeID,
) error {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()
	_, err := rm.db.ExecContext(ctx, `
	insert into rootmap (producer_id, topic, version, storage_prefix, node_id) values ($1, $2, $3, $4, $5)`,
		producerID, topic, version, prefix, hex.EncodeToString(nodeID[:]),
	)
	if err != nil {
		var err sqlite3.Error
		if errors.As(err, &sqlite3.Error{Code: sqlite3.ErrConstraint}) {
			return ErrRootAlreadyExists
		}
		return fmt.Errorf("failed to store to rootmap: %w", err)
	}
	return nil
}

func (rm *sqlRootmap) GetLatestByTopic(
	ctx context.Context,
	producerID string,
	topics map[string]uint64,
) ([]RootListing, error) {
	sb := strings.Builder{}
	params := []any{producerID}
	sb.WriteString(`
	select r1.topic, r1.storage_prefix, r1.node_id, r1.version
	from rootmap r1 left join rootmap r2
	on (r1.topic = r2.topic and r1.producer_id = r2.producer_id and r1.version < r2.version)
	where r2.rowid is null
	and r1.producer_id = $1
	`)
	if len(topics) > 0 {
		sb.WriteString(" and r1.topic in (")
		for i, topic := range maps.Keys(topics) {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(fmt.Sprintf("$%d", i+2))
			params = append(params, topic)
		}
		sb.WriteString(")")
	}
	var maxVersion uint64
	rows, err := rm.db.QueryContext(ctx, sb.String(), params...)
	if err != nil {
		return nil, fmt.Errorf("failed to read from rootmap: %w", err)
	}
	defer rows.Close()

	listings := make([]RootListing, 0, len(topics))
	for rows.Next() {
		var nodeID string
		var version uint64
		var topic string
		var prefix string
		if err := rows.Scan(&topic, &prefix, &nodeID, &version); err != nil {
			return nil, fmt.Errorf("failed to read from rootmap: %w", err)
		}
		if version > maxVersion {
			maxVersion = version
		}
		decoded, err := hex.DecodeString(nodeID)
		if err != nil {
			return nil, fmt.Errorf("failed to decode node ID: %w", err)
		}
		if version > topics[topic] {
			listings = append(listings, RootListing{
				prefix, topic, nodestore.NodeID(decoded), version, topics[topic],
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
	producerID string,
	topic string,
) (string, nodestore.NodeID, uint64, error) {
	var prefix string
	var nodeID string
	var version uint64
	err := rm.db.QueryRowContext(ctx, `
	select storage_prefix, node_id, version
	from rootmap
	where producer_id = $1 and topic = $2
	order by version desc limit 1`,
		producerID, topic,
	).Scan(&prefix, &nodeID, &version)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return prefix, nodestore.NodeID{}, 0, StreamNotFoundError{producerID, topic}
		}
		return prefix, nodestore.NodeID{}, 0, fmt.Errorf("failed to read from rootmap: %w", err)
	}
	decoded, err := hex.DecodeString(nodeID)
	if err != nil {
		return prefix, nodestore.NodeID{}, 0, fmt.Errorf("failed to decode node ID: %w", err)
	}
	return prefix, nodestore.NodeID(decoded), version, nil
}

func (rm *sqlRootmap) Get(
	ctx context.Context, producerID string, topic string, version uint64) (string, nodestore.NodeID, error) {
	var nodeID string
	var prefix string
	err := rm.db.QueryRowContext(ctx, `
	select storage_prefix, node_id from rootmap where producer_id = $1 and topic = $2 and version = $3`,
		producerID, topic, version,
	).Scan(&prefix, &nodeID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return prefix, nodestore.NodeID{}, StreamNotFoundError{producerID, topic}
		}
		return prefix, nodestore.NodeID{}, fmt.Errorf("failed to read from rootmap: %w", err)
	}
	decoded, err := hex.DecodeString(nodeID)
	if err != nil {
		return prefix, nodestore.NodeID{}, fmt.Errorf("failed to decode node ID: %w", err)
	}
	return prefix, nodestore.NodeID(decoded), nil
}

func NewSQLRootmap(db *sql.DB) (Rootmap, error) {
	rm := &sqlRootmap{
		db:  db,
		mtx: &sync.Mutex{},
	}
	err := rm.initialize()
	if err != nil {
		return nil, err
	}
	return rm, nil
}
