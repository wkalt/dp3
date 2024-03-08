package rootmap

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/wkalt/dp3/nodestore"
)

type sqlRootmap struct {
	db *sql.DB
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
		stream_id uuid not null,
		version bigint not null,
		node_id text not null,
		timestamp text not null default current_timestamp,
		primary key (stream_id, version)
	);

	create index rootmap_stream_id_timestamp_idx on rootmap (stream_id, timestamp);

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
	streamID string,
	version uint64,
	nodeID nodestore.NodeID,
) error {
	_, err := rm.db.ExecContext(ctx, `
	insert into rootmap (producer_id, topic, stream_id, version, node_id) values ($1, $2, $3, $4, $5)`,
		producerID, topic, streamID, version, hex.EncodeToString(nodeID[:]),
	)
	if err != nil {
		return fmt.Errorf("failed to store to rootmap: %w", err)
	}
	return nil
}

func (rm *sqlRootmap) GetLatest(ctx context.Context, streamID string) (nodestore.NodeID, uint64, error) {
	var nodeID string
	var version uint64
	err := rm.db.QueryRowContext(ctx, `
	select node_id, version from rootmap where stream_id = $1 order by version desc limit 1`,
		streamID,
	).Scan(&nodeID, &version)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nodestore.NodeID{}, 0, StreamNotFoundError{streamID}
		}
		return nodestore.NodeID{}, 0, fmt.Errorf("failed to read from rootmap: %w", err)
	}
	decoded, err := hex.DecodeString(nodeID)
	if err != nil {
		return nodestore.NodeID{}, 0, fmt.Errorf("failed to decode node ID: %w", err)
	}
	return nodestore.NodeID(decoded), version, nil
}

func (rm *sqlRootmap) Get(ctx context.Context, streamID string, version uint64) (nodestore.NodeID, error) {
	var nodeID string
	err := rm.db.QueryRowContext(ctx, `
	select node_id from rootmap where stream_id = $1 and version = $2`,
		streamID, version,
	).Scan(&nodeID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nodestore.NodeID{}, StreamNotFoundError{streamID}
		}
		return nodestore.NodeID{}, fmt.Errorf("failed to read from rootmap: %w", err)
	}
	decoded, err := hex.DecodeString(nodeID)
	if err != nil {
		return nodestore.NodeID{}, fmt.Errorf("failed to decode node ID: %w", err)
	}
	return nodestore.NodeID(decoded), nil
}

func NewSQLRootmap(db *sql.DB) (Rootmap, error) {
	rm := &sqlRootmap{
		db: db,
	}
	err := rm.initialize()
	if err != nil {
		return nil, err
	}
	return rm, nil
}
