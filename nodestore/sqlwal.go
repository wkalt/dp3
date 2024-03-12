package nodestore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/wkalt/dp3/util"
)

/*
SQLWAL is a write-ahead log for the nodestore backed by an SQL database. The
only database that has been used or tested is sqlite.
*/

////////////////////////////////////////////////////////////////////////////////

type sqlWAL struct {
	db  *sql.DB
	mtx *sync.Mutex
}

func NewSQLWAL(ctx context.Context, db *sql.DB) (WAL, error) {
	wal := &sqlWAL{db: db, mtx: &sync.Mutex{}}
	if err := wal.initialize(ctx); err != nil {
		return nil, err
	}
	return wal, nil
}

func (w *sqlWAL) initialize(ctx context.Context) error {
	_, err := w.db.ExecContext(ctx, `
	create table if not exists wal (
		id serial primary key,
		producer_id text,
		topic text,
		node_id text,
		version bigint,
		deleted text,
		data blob
	);
	create unique index if not exists producer_id_topic_node_id_uniq_idx on wal(producer_id, topic, node_id);
	create index if not exists wal_node_id_idx on wal(node_id);
	`)
	if err != nil {
		return fmt.Errorf("failed to create wal: %w", err)
	}
	return nil
}

func (w *sqlWAL) Put(ctx context.Context, entry WALEntry) error {
	w.mtx.Lock()
	defer w.mtx.Unlock()
	stmt := `insert into wal (producer_id, topic, node_id, version, data)
	values
	($1, $2, $3, $4, $5)`
	params := []interface{}{entry.ProducerID, entry.Topic, entry.NodeID, entry.Version, entry.Data}
	_, err := w.db.ExecContext(ctx, stmt, params...)
	if err != nil {
		return fmt.Errorf("failed to insert wal: %w", err)
	}
	return nil
}

func (w *sqlWAL) Get(ctx context.Context, nodeID NodeID) (data []byte, err error) {
	w.mtx.Lock()
	defer w.mtx.Unlock()
	stmt := `select data from wal where node_id = $1 and deleted is null`
	err = w.db.QueryRowContext(ctx, stmt, nodeID).Scan(&data)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, NodeNotFoundError{nodeID}
		}
		return nil, fmt.Errorf("failed to get node %s from wal: %w", nodeID, err)
	}
	return data, nil
}

func (w *sqlWAL) Delete(ctx context.Context, nodeID NodeID) error {
	w.mtx.Lock()
	defer w.mtx.Unlock()
	stmt := `update wal set deleted = current_timestamp where node_id = $1`
	_, err := w.db.ExecContext(ctx, stmt, nodeID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return NodeNotFoundError{nodeID}
		}
		return fmt.Errorf("failed to delete node %s from wal: %w", nodeID, err)
	}
	return nil
}

func (w *sqlWAL) List(ctx context.Context) (paths []WALListing, err error) {
	w.mtx.Lock()
	defer w.mtx.Unlock()
	stmt := `select producer_id, topic, version, node_id from wal where deleted is null order by id`
	rows, err := w.db.QueryContext(ctx, stmt)
	if err != nil {
		return nil, fmt.Errorf("failed to list wal: %w", err)
	}
	streams := make(map[string]WALListing)
	defer rows.Close()
	for rows.Next() {
		var entry WALEntry
		if err := rows.Scan(&entry.ProducerID, &entry.Topic, &entry.Version, &entry.NodeID); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		key := entry.ProducerID + entry.Topic
		if _, ok := streams[key]; !ok {
			streams[key] = WALListing{
				ProducerID: entry.ProducerID,
				Topic:      entry.Topic,
				Versions:   make(map[uint64][]NodeID),
			}
		}
		if _, ok := streams[key].Versions[entry.Version]; !ok {
			streams[key].Versions[entry.Version] = []NodeID{entry.NodeID}
		}
		streams[key].Versions[entry.Version] = append(
			streams[key].Versions[entry.Version], entry.NodeID)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("failed to list wal: %w", rows.Err())
	}
	result := make([]WALListing, 0, len(streams))
	for _, k := range util.Okeys(streams) {
		result = append(result, streams[k])
	}
	return result, nil
}
