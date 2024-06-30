package wal

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/google/uuid"
	"github.com/wkalt/dp3/server/tree"
	"github.com/wkalt/dp3/server/util"
	"github.com/wkalt/dp3/server/util/log"
	"golang.org/x/exp/maps"
)

/*
The WAL manager is responsible for managing the write-ahead log, including
read/write interfaces, log rotation, recovery, and scheduling of WAL data for
merge into the tree.

The WAL manager is instantiated and controlled by the tree manager. On startup,
it does a linear scan of all data existing in the WAL, playing records through
the WAL state machine.

There are two kinds of state the walmgr stores in memory: "pending inserts" and
"pending merges". The pending inserts map is an association between "tree IDs"
(composed of producer + topic) to "batches". A batch is a collection of inserts
to a single tree ID. A single call to "receive" may produce multiple batches, or
a batch may span multiple calls to receive.

After either the size of a batch exceeds a configured threshold, or a configured
amount of time has passed since the last update to a batch, a "merge request"
covering the addresses of the batch is recorded to the WAL, and the batch is
moved from "pending inserts" to "pending merges" in the walmgr's state.

The actual merge of a batch into the tree is handled by the treemgr. At the
conclusion of the merge, the treemgr calls a callback on the batch that causes
the walmgr to record a "merge complete" record.

After scanning the WAL (recovery), the state in the WAL manager is up-to-date,
reflecting any inserts that have been stored but not yet queued for merge, as
well as any batches requested for merge that have not yet completed.

It notifies the treemgr of pending merges over a shared channel, and is then
ready to take new requests.

There are three kinds of records stored in the WAL:
* inserts: contain the data to be stored in the tree. The data of an insert
  record is a "partial tree" of new nodes, in leaf-to-root order. It is the
  byte serialization of a "memtree".
* merge requests: contain the addresses of a batch of inserts that are ready to
  be merged, as well as a UUID-valued identifier.
* merge complete: contain the UUID of a batch that has been successfully merged.

Addresses in the WAL are 24 bytes long, composed of a file ID, an offset, and a
length, each 8 bytes. WAL files are named according to the decimal
representation of the file ID.
*/

////////////////////////////////////////////////////////////////////////////////

// WALManager is the write-ahead log manager.
type WALManager struct {
	f              io.WriteCloser
	writer         *Writer
	counter        uint64
	pendingInserts map[TreeID]*Batch
	pendingMerges  map[string]*Batch
	mtx            *sync.Mutex
	config         *config
	merges         chan<- *Batch
	waldir         string
}

// NewWALManager constructs a new WAL manager.
func NewWALManager(
	ctx context.Context,
	waldir string,
	merges chan<- *Batch,
	opts ...Option,
) (*WALManager, error) {
	conf := &config{
		mergeSizeThreshold:         2 * gigabyte,
		staleBatchThreshold:        5 * time.Second,
		targetFileSize:             2 * gigabyte,
		inactiveBatchMergeInterval: 0,
		gcInterval:                 0,
	}
	for _, opt := range opts {
		opt(conf)
	}
	wmgr := &WALManager{
		merges:         merges,
		mtx:            &sync.Mutex{},
		config:         conf,
		pendingInserts: map[TreeID]*Batch{},
		pendingMerges:  map[string]*Batch{},
		waldir:         waldir,
	}
	if conf.inactiveBatchMergeInterval > 0 {
		go func() {
			for range time.NewTicker(conf.inactiveBatchMergeInterval).C {
				if err := wmgr.mergeInactiveBatches(ctx); err != nil {
					log.Errorf(ctx, "Failed to merge inactive batches: %s", err)
				}
			}
		}()
	}
	if conf.gcInterval > 0 {
		go func() {
			for range time.NewTicker(conf.gcInterval).C {
				n, err := wmgr.RemoveStaleFiles(ctx)
				if err != nil {
					log.Errorf(ctx, "Failed to remove stale files: %s", err)
					continue
				}
				if n > 0 {
					log.Infof(ctx, "Removed %d stale WAL files", n)
				}
			}
		}()
	}
	return wmgr, nil
}

// Insert data into the WAL for a producer and topic. The data should be the
// serialized representation of a memtree, i.e a partial tree.
func (w *WALManager) Insert(
	ctx context.Context,
	database string,
	producer string,
	topic string,
	schemas []*mcap.Schema,
	data []byte,
) (Address, error) {
	w.mtx.Lock()
	defer w.mtx.Unlock()
	tid := TreeID{database, producer, topic}
	bat, ok := w.pendingInserts[tid]
	if !ok {
		id := uuid.New().String()
		bat = w.newBatch(id, database, producer, topic)
		w.pendingInserts[tid] = bat
	}
	addr, err := w.insert(tid, bat.ID, schemas, data)
	if err != nil {
		return Address{}, err
	}
	bat.Size += len(data)
	bat.LastUpdate = time.Now()
	bat.Addrs = append(bat.Addrs, addr)

	// if the batch is over a certain size now, merge it.
	if bat.Size > w.config.mergeSizeThreshold {
		if err := w.mergeBatch(bat); err != nil {
			return addr, err
		}
	}
	if w.writer.size() > int64(w.config.targetFileSize) {
		if err := w.Rotate(); err != nil {
			return addr, err
		}
	}
	return addr, nil
}

// GetReader gets a tree.TreeReader from a WAL address, presumed to be a leaf
// address. The tree reader is backed by a limited reader over the data portion
// of the insert record.
func (w *WALManager) GetReader(addr Address) (*InsertRecord, tree.TreeReader, error) {
	f, err := w.openr(addr.object())
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open wal file: %w", err)
	}
	defer f.Close()

	_, err = f.Seek(addr.offset(), io.SeekStart)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to seek to record offset: %w", err)
	}

	recordHeader, headerlen, err := ParseInsertRecordHeader(f)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse insert record header: %w", err)
	}

	path := w.waldir + "/" + addr.object()

	// account for the header of the insert record, and the trailing crc32
	dataOffset := int(addr.offset() + int64(headerlen))
	dataLength := addr.length() - headerlen - 4
	factory := func() (io.ReadSeekCloser, error) {
		f, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("failed to open file: %w", err)
		}
		return f, nil
	}
	reader, err := tree.NewFileTree(factory, dataOffset, dataLength)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create tree reader: %w", err)
	}
	return recordHeader, reader, nil
}

// Get data from the WAL at a given address.
func (w *WALManager) Get(addr Address) ([]byte, error) {
	f, err := w.openr(addr.object())
	if err != nil {
		return nil, fmt.Errorf("failed to open wal file: %w", err)
	}
	defer f.Close()
	_, err = f.Seek(addr.offset(), io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to record offset: %w", err)
	}
	header := make([]byte, 1+8)
	_, err = io.ReadFull(f, header)
	if err != nil {
		return nil, fmt.Errorf("failed to read record header: %w", err)
	}
	var offset int
	var rectype uint8
	var length uint64
	offset += util.ReadU8(header[offset:], &rectype)
	if rectype != uint8(WALInsert) {
		return nil, fmt.Errorf("expected insert record at %s, got %s", addr, RecordType(rectype))
	}
	util.ReadU64(header[offset:], &length)
	body := make([]byte, length+4)
	_, err = io.ReadFull(f, body)
	if err != nil {
		return nil, fmt.Errorf("failed to read record body: %w", err)
	}
	dataEnd := len(body) - 4
	computed := crc32.ChecksumIEEE(header)
	computed = crc32.Update(computed, crc32.IEEETable, body[:dataEnd])
	crc := binary.LittleEndian.Uint32(body[dataEnd:])
	if crc != computed {
		return nil, CRCMismatchError{crc, computed}
	}
	insert := ParseInsertRecord(body[:dataEnd])
	return insert.Data, nil
}

// ForceMerge forces the merge of all pending inserts in the WAL. This is used
// in tests only - in operation merges are async.
func (w *WALManager) ForceMerge(ctx context.Context) (int, error) {
	batches := []*Batch{}
	w.mtx.Lock()
	defer w.mtx.Unlock()

	keys := maps.Keys(w.pendingInserts)
	sort.Slice(keys, func(i, j int) bool {
		return w.pendingInserts[keys[i]].Topic > w.pendingInserts[keys[j]].Topic
	})

	for _, k := range keys {
		batch := w.pendingInserts[k]
		delete(w.pendingInserts, k)
		batches = append(batches, batch)
	}
	c := 0
	for _, batch := range batches {
		if err := w.mergeBatch(batch); err != nil {
			return c, fmt.Errorf("failed to merge wal batch: %w", err)
		}
		c++
	}
	return c, nil
}

// WALStats contains statistics about the WAL manager.
type WALStats struct {
	PendingInserts map[TreeID][]Address
	PendingMerges  map[string]*Batch
}

// Stats returns statistics about the WAL manager.
func (w *WALManager) Stats() WALStats {
	w.mtx.Lock()
	defer w.mtx.Unlock()
	inserts := map[TreeID][]Address{}
	for k, v := range w.pendingInserts {
		inserts[k] = v.Addrs
	}
	merges := map[string]*Batch{}
	for k, v := range w.pendingMerges {
		merges[k] = v
	}

	return WALStats{
		PendingInserts: inserts,
		PendingMerges:  merges,
	}
}

// Recover scans the files in the waldir and replays them through the walmgr's
// internal state. Recover must be called before the WAL manager is ready to
// use.
func (w *WALManager) Recover(ctx context.Context) error {
	w.mtx.Lock()
	defer w.mtx.Unlock()
	paths, err := listDir(w.waldir)
	if err != nil {
		return fmt.Errorf("failed to list wal directory: %w", err)
	}
	ids := make([]uint64, 0, len(paths))
	log.Infof(ctx, "Replaying %d log files", len(paths))
	for _, path := range paths {
		id, err := strconv.ParseUint(path, 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse wal path: %w", err)
		}
		ids = append(ids, id)
	}
	if len(paths) == 0 {
		return w.Rotate()
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})
	for _, id := range ids {
		if err := w.scanfile(ctx, id); err != nil {
			return err
		}
	}

	// todo this can block if there is a lot of pending merges. Ensure the
	// channel consumers are started up first.
	keys := maps.Keys(w.pendingMerges)

	// todo why do we need to sort these?
	sort.Slice(keys, func(i, j int) bool {
		return w.pendingMerges[keys[i]].Topic > w.pendingMerges[keys[j]].Topic
	})
	for _, k := range keys {
		w.merges <- w.pendingMerges[k]
		log.Infof(ctx, "Enqueued pending merge %s", k)
	}
	err = w.setfile(ids[len(ids)-1])
	if err != nil {
		return err
	}
	log.Infow(ctx, "Recovery complete",
		"pending inserts", len(w.pendingInserts),
		"pending merges", len(w.pendingMerges),
	)
	return nil
}

func listDir(dir string) ([]string, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to list directory: %w", err)
	}
	result := make([]string, 0, len(files))
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		result = append(result, f.Name())
	}
	return result, nil
}

func (w *WALManager) mergeBatch(batch *Batch) error {
	if _, _, err := w.writer.WriteMergeRequest(MergeRequestRecord{
		Database: batch.Database,
		Producer: batch.Producer,
		Topic:    batch.Topic,
		BatchID:  batch.ID,
		Addrs:    batch.Addrs,
	}); err != nil {
		return err
	}
	delete(w.pendingInserts, TreeID{
		Database: batch.Database, Producer: batch.Producer, Topic: batch.Topic,
	})
	w.pendingMerges[batch.ID] = batch
	w.merges <- batch
	return nil
}

// merges bounded length queue; bound = 1 in tests?

func (w *WALManager) mergeInactiveBatches(ctx context.Context) error {
	w.mtx.Lock()
	defer w.mtx.Unlock()
	batches := []*Batch{}
	keys := maps.Keys(w.pendingInserts)
	sort.Slice(keys, func(i, j int) bool {
		return w.pendingInserts[keys[i]].Topic > w.pendingInserts[keys[j]].Topic
	})
	for _, k := range keys {
		batch := w.pendingInserts[k]
		if time.Since(batch.LastUpdate) > w.config.staleBatchThreshold {
			delete(w.pendingInserts, k)
			batches = append(batches, batch)
		}
	}
	if len(batches) == 0 {
		return nil
	}
	log.Infof(ctx, "Merging %d inactive batches", len(batches))
	for _, batch := range batches {
		if err := w.mergeBatch(batch); err != nil {
			return err
		}
	}
	return nil
}

func (w *WALManager) insert(tid TreeID, batchID string, schemas []*mcap.Schema, data []byte) (Address, error) {
	addr, _, err := w.writer.WriteInsert(InsertRecord{
		Database: tid.Database,
		Producer: tid.Producer,
		Topic:    tid.Topic,
		BatchID:  batchID,
		Schemas:  schemas,
		Data:     data,
	})
	if err != nil {
		return Address{}, err
	}
	return addr, nil
}

func (w *WALManager) openw(path string) (*os.File, error) {
	f, err := os.OpenFile(w.waldir+"/"+path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	return f, nil
}

func (w *WALManager) openr(path string) (*os.File, error) {
	f, err := os.OpenFile(w.waldir+"/"+path, os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	return f, nil
}

func (w *WALManager) completeMerge(id string) error {
	w.mtx.Lock()
	defer w.mtx.Unlock()
	if _, _, err := w.writer.WriteMergeComplete(MergeCompleteRecord{
		BatchID: id,
	}); err != nil {
		return err
	}
	delete(w.pendingMerges, id)
	return nil
}

func (w *WALManager) RemoveStaleFiles(ctx context.Context) (removed int, err error) {
	w.mtx.Lock()
	defer w.mtx.Unlock()
	files, err := listDir(w.waldir)
	if err != nil {
		return removed, fmt.Errorf("failed to list directory: %w", err)
	}
	// get all objects represented in the pending state
	objects := map[string]struct{}{}
	for _, batch := range w.pendingInserts {
		for _, addr := range batch.Addrs {
			objects[addr.object()] = struct{}{}
		}
	}
	for _, batch := range w.pendingMerges {
		for _, addr := range batch.Addrs {
			objects[addr.object()] = struct{}{}
		}
	}
	remove := []string{}
	currentFile := strconv.FormatUint(w.counter, 10)
	for _, file := range files {
		if file == currentFile {
			continue
		}
		if _, ok := objects[file]; !ok {
			remove = append(remove, file)
		}
	}
	if len(remove) > 0 {
		log.Infof(ctx, "Removing %d stale WAL files: %v", len(remove), remove)
		for _, file := range remove {
			if err := os.Remove(path.Join(w.waldir, file)); err != nil {
				return removed, fmt.Errorf("failed to remove file: %w", err)
			}
			removed++
		}
	}
	return removed, nil
}

func (w *WALManager) Rotate() error {
	if w.f != nil {
		if err := w.f.Close(); err != nil {
			return fmt.Errorf("failed to close log file: %w", err)
		}
	}
	w.counter++
	f, err := w.openw(strconv.FormatUint(w.counter, 10))
	if err != nil {
		return fmt.Errorf("failed to create new log file: %w", err)
	}
	w.f = f
	w.writer, err = NewWriter(f, w.counter, 0)
	if err != nil {
		return fmt.Errorf("failed to create new log writer: %w", err)
	}

	return nil
}

func (w *WALManager) scanfile(ctx context.Context, id uint64) error {
	f, err := w.openr(strconv.FormatUint(id, 10))
	if err != nil {
		return fmt.Errorf("failed to open wal file: %w", err)
	}
	defer f.Close()
	r, err := NewReader(f)
	if err != nil {
		return fmt.Errorf("failed to construct wal reader: %w", err)
	}
	for {
		rectype, data, err := r.Next()
		if err != nil {
			switch {
			case errors.Is(err, io.ErrUnexpectedEOF):
				n, err := f.Seek(r.offset, io.SeekStart)
				if err != nil {
					return fmt.Errorf("failed to seek to last good record end: %w", err)
				}
				if err := f.Truncate(n); err != nil {
					return fmt.Errorf("failed to truncate file: %w", err)
				}
				log.Infof(ctx, "Truncated log file at %d", n)
				return nil
			case errors.Is(err, io.EOF):
				return nil
			}
			return fmt.Errorf("failed to read next wal record: %w", err)
		}
		switch rectype {
		case WALInsert:
			rec := ParseInsertRecord(data)
			bat, ok := w.pendingInserts[NewTreeID(rec.Database, rec.Producer, rec.Topic)]
			if !ok {
				bat = &Batch{
					ID:       rec.BatchID,
					Database: rec.Database,
					Producer: rec.Producer,
					Topic:    rec.Topic,
					Addrs:    []Address{},
					OnDone: func() error {
						return w.completeMerge(rec.BatchID)
					},
				}
				w.pendingInserts[NewTreeID(rec.Database, rec.Producer, rec.Topic)] = bat
			}
			bat.Size += len(rec.Data)
			bat.Addrs = append(bat.Addrs, rec.Addr)
		case WALMergeRequest:
			rec := ParseMergeRequestRecord(data)
			key := NewTreeID(rec.Database, rec.Producer, rec.Topic)
			batch := w.pendingInserts[key]
			delete(w.pendingInserts, key)
			w.pendingMerges[rec.BatchID] = batch
		case WALMergeComplete:
			rec := ParseMergeCompleteRecord(data)
			delete(w.pendingMerges, rec.BatchID)
		}
	}
}

func (w *WALManager) setfile(id uint64) error {
	f, err := w.openw(strconv.FormatUint(id, 10))
	if err != nil {
		return fmt.Errorf("failed to open wal file: %w", err)
	}
	n, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("failed to seek to file end: %w", err)
	}
	w.f = f
	w.counter = id
	w.writer, err = NewWriter(f, id, n)
	if err != nil {
		return fmt.Errorf("failed to construct wal writer: %w", err)
	}
	return nil
}

func (w *WALManager) newBatch(
	id string,
	database string,
	producer string,
	topic string,
) *Batch {
	return &Batch{
		ID:       id,
		Database: database,
		Producer: producer,
		Topic:    topic,
		Size:     0,
		Addrs:    []Address{},
		OnDone: func() error {
			return w.completeMerge(id)
		},
	}
}
