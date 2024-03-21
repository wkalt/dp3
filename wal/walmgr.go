package wal

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wkalt/dp3/util"
	"github.com/wkalt/dp3/util/log"
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
					log.Errorf(ctx, "failed to merge inactive batches: %s", err)
				}
			}
		}()
	}
	return wmgr, nil
}

// Insert data into the WAL for a producer and topic. The data should be the
// serialized representation of a memtree, i.e a partial tree.
func (w *WALManager) Insert(producer string, topic string, data []byte) (Address, error) {
	tid := TreeID{producer, topic}
	bat, ok := w.pendingInserts[tid]
	if !ok {
		id := uuid.New().String()
		bat = w.newBatch(id, producer, topic)
		w.pendingInserts[tid] = bat
	}
	addr, err := w.insert(tid, bat.ID, data)
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
		if err := w.rotate(); err != nil {
			return addr, err
		}
	}
	return addr, nil
}

// Get data from the WAL at a given address.
func (w *WALManager) Get(addr Address) ([]byte, error) {
	w.mtx.Lock()
	defer w.mtx.Unlock()
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

	keys := maps.Keys(w.pendingInserts)
	sort.Slice(keys, func(i, j int) bool {
		return w.pendingInserts[keys[i]].Topic > w.pendingInserts[keys[j]].Topic
	})

	for _, k := range keys {
		batch := w.pendingInserts[k]
		delete(w.pendingInserts, k)
		batches = append(batches, batch)
	}
	w.mtx.Unlock()
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
		return w.rotate()
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
		Producer: batch.ProducerID,
		Topic:    batch.Topic,
		BatchID:  batch.ID,
		Addrs:    batch.Addrs,
	}); err != nil {
		return err
	}
	w.mtx.Lock()
	delete(w.pendingInserts, TreeID{Producer: batch.ProducerID, Topic: batch.Topic})
	w.pendingMerges[batch.ID] = batch
	w.mtx.Unlock()
	w.merges <- batch
	return nil
}

// merges bounded length queue; bound = 1 in tests?

func (w *WALManager) mergeInactiveBatches(ctx context.Context) error {
	batches := []*Batch{}
	w.mtx.Lock()
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
	w.mtx.Unlock()
	if len(batches) == 0 {
		return nil
	}
	t0 := time.Now()
	log.Infof(ctx, "Merging %d inactive batches", len(batches))
	for _, batch := range batches {
		if err := w.mergeBatch(batch); err != nil {
			return err
		}
	}
	log.Infof(ctx, "Merged %d inactive batches in %s", len(batches), time.Since(t0))
	return nil
}

func (w *WALManager) insert(tid TreeID, batchID string, data []byte) (Address, error) {
	addr, _, err := w.writer.WriteInsert(InsertRecord{
		Producer: tid.Producer,
		Topic:    tid.Topic,
		BatchID:  batchID,
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
	if _, _, err := w.writer.WriteMergeComplete(MergeCompleteRecord{
		BatchID: id,
	}); err != nil {
		return err
	}
	w.mtx.Lock()
	delete(w.pendingMerges, id)
	w.mtx.Unlock()
	return nil
}

func (w *WALManager) rotate() error {
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
			bat, ok := w.pendingInserts[TreeID{Producer: rec.Producer, Topic: rec.Topic}]
			if !ok {
				bat = &Batch{
					ID:         rec.BatchID,
					ProducerID: rec.Producer,
					Topic:      rec.Topic,
					Addrs:      []Address{},
					OnDone: func() error {
						return w.completeMerge(rec.BatchID)
					},
				}
				w.pendingInserts[TreeID{Producer: rec.Producer, Topic: rec.Topic}] = bat
			}
			bat.Size += len(rec.Data)
			bat.Addrs = append(bat.Addrs, rec.Addr)
		case WALMergeRequest:
			rec := ParseMergeRequestRecord(data)
			key := TreeID{Producer: rec.Producer, Topic: rec.Topic}
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
	producerID string,
	topic string,
) *Batch {
	return &Batch{
		ID:         id,
		ProducerID: producerID,
		Topic:      topic,
		Size:       0,
		Addrs:      []Address{},
		OnDone: func() error {
			return w.completeMerge(id)
		},
	}
}