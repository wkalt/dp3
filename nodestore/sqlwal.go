package nodestore

import (
	"database/sql"
	"fmt"
)

type sqlWAL struct {
	db *sql.DB
}

func NewSQLWAL(db *sql.DB) (WAL, error) {
	wal := &sqlWAL{db: db}
	if err := wal.initialize(); err != nil {
		return nil, err
	}
	return wal, nil
}

func (w *sqlWAL) initialize() error {
	_, err := w.db.Exec(`
	create table if not exists wal (
		id serial primary key,
		stream_id text,
		node_id text,
		version bigint,
		data blob
	);
	create index if not exists wal_stream_id_idx on wal(stream_id);
	create index if not exists wal_node_id_idx on wal(node_id);
	create unique index if not exists wal_stream_node_uniq_idx on wal(stream_id, node_id);
	`)
	if err != nil {
		return err
	}
	return nil
}

func (w *sqlWAL) Put(entry WALEntry) error {
	stmt := `insert into wal (stream_id, node_id, version, data)
	values
	($1, $2, $3, $4)`
	params := []interface{}{entry.StreamID, entry.NodeID, entry.Version, entry.Data}
	_, err := w.db.Exec(stmt, params...)
	if err != nil {
		return err
	}
	return nil
}

func (w *sqlWAL) GetStream(streamID string) ([][]NodeID, error) {
	stmt := `select node_id, version from wal where stream_id = $1 order by id`
	rows, err := w.db.Query(stmt, streamID)
	if err != nil {
		return nil, err
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
	if len(group) > 0 {
		result = append(result, group)
	}
	return result, nil
}

func (w *sqlWAL) Get(nodeID NodeID) (data []byte, err error) {
	stmt := `select data from wal where node_id = $1`
	err = w.db.QueryRow(stmt, nodeID).Scan(&data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (w *sqlWAL) List() (entries []WALEntry, err error) {
	stmt := `select stream_id, node_id, version, data from wal order by id`
	rows, err := w.db.Query(stmt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var entry WALEntry
		if err := rows.Scan(&entry.StreamID, &entry.NodeID, &entry.Version, &entry.Data); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		entries = append(entries, entry)
	}
	return entries, nil
}
