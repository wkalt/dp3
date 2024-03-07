package nodestore

import (
	"context"
	"database/sql"
	"fmt"

	"golang.org/x/exp/maps"
)

type sqlWAL struct {
	db *sql.DB
}

func NewSQLWAL(ctx context.Context, db *sql.DB) (WAL, error) {
	wal := &sqlWAL{db: db}
	if err := wal.initialize(ctx); err != nil {
		return nil, err
	}
	return wal, nil
}

func (w *sqlWAL) initialize(ctx context.Context) error {
	_, err := w.db.ExecContext(ctx, `
	create table if not exists wal (
		id serial primary key,
		stream_id text,
		node_id text,
		version bigint,
		deleted text,
		data blob
	);
	create index if not exists wal_stream_id_idx on wal(stream_id);
	create index if not exists wal_node_id_idx on wal(node_id);
	create unique index if not exists wal_stream_node_uniq_idx on wal(stream_id, node_id);
	`)
	if err != nil {
		return fmt.Errorf("failed to create wal: %w", err)
	}
	return nil
}

func (w *sqlWAL) Put(ctx context.Context, entry WALEntry) error {
	stmt := `insert into wal (stream_id, node_id, version, data)
	values
	($1, $2, $3, $4)`
	params := []interface{}{entry.StreamID, entry.NodeID, entry.Version, entry.Data}
	_, err := w.db.ExecContext(ctx, stmt, params...)
	if err != nil {
		return fmt.Errorf("failed to insert wal: %w", err)
	}
	return nil
}

func (w *sqlWAL) GetStream(ctx context.Context, streamID string) ([][]NodeID, error) {
	stmt := `select node_id, version from wal where stream_id = $1 and deleted is null order by id`
	rows, err := w.db.QueryContext(ctx, stmt, streamID)
	if err != nil {
		return nil, fmt.Errorf("failed to get stream %s from wal: %w", streamID, err)
	}
	defer rows.Close()
	var result [][]NodeID
	var group []NodeID
	var last uint64 = 0
	for rows.Next() {
		var version uint64
		var nodeID NodeID
		if err := rows.Scan(&nodeID, &version); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		if version != last {
			if len(group) > 0 {
				result = append(result, group)
			}
			group = []NodeID{nodeID}
			last = version
		} else {
			group = append(group, nodeID)
		}
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("failed to get stream %s from wal: %w", streamID, rows.Err())
	}
	if len(group) > 0 {
		result = append(result, group)
	}
	return result, nil
}

func (w *sqlWAL) Get(ctx context.Context, nodeID NodeID) (data []byte, err error) {
	stmt := `select data from wal where node_id = $1 and deleted is null`
	err = w.db.QueryRowContext(ctx, stmt, nodeID).Scan(&data)
	if err != nil {
		return nil, fmt.Errorf("failed to get node %s from wal: %w", nodeID, err)
	}
	return data, nil
}

func (w *sqlWAL) Delete(ctx context.Context, nodeID NodeID) error {
	stmt := `update wal set deleted = current_timestamp where node_id = $1`
	_, err := w.db.ExecContext(ctx, stmt, nodeID)
	if err != nil {
		return fmt.Errorf("failed to delete node %s from wal: %w", nodeID, err)
	}
	return nil
}

func (w *sqlWAL) List(ctx context.Context) (paths []WALListing, err error) {
	stmt := `select stream_id, version, node_id from wal where deleted is null order by id`
	rows, err := w.db.QueryContext(ctx, stmt)
	if err != nil {
		return nil, fmt.Errorf("failed to list wal: %w", err)
	}
	streams := make(map[string]WALListing)
	defer rows.Close()
	for rows.Next() {
		var entry WALEntry
		if err := rows.Scan(&entry.StreamID, &entry.Version, &entry.NodeID); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		if _, ok := streams[entry.StreamID]; !ok {
			streams[entry.StreamID] = WALListing{StreamID: entry.StreamID, Versions: make(map[uint64][]NodeID)}
		}
		if _, ok := streams[entry.StreamID].Versions[entry.Version]; !ok {
			streams[entry.StreamID].Versions[entry.Version] = []NodeID{entry.NodeID}
		}
		streams[entry.StreamID].Versions[entry.Version] = append(
			streams[entry.StreamID].Versions[entry.Version], entry.NodeID)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("failed to list wal: %w", rows.Err())
	}
	return maps.Values(streams), nil
}
