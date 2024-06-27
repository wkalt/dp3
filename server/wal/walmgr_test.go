package wal_test

import (
	"context"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/server/wal"
)

func TestWALRotation(t *testing.T) {
	ctx := context.Background()
	tmpdir, err := os.MkdirTemp("", "wal_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpdir)
	merges := make(chan *wal.Batch)

	wm, err := wal.NewWALManager(
		ctx, tmpdir, merges,
		wal.WithTargetFileSize(1024),
	)
	require.NoError(t, err)
	require.NoError(t, wm.Recover(ctx))
	for i := 0; i < 100; i++ {
		_, err := wm.Insert(ctx, "db", "producer", "topic", []byte{0x01, 0x02})
		require.NoError(t, err)
	}

	paths, err := os.ReadDir(tmpdir)
	require.NoError(t, err)

	require.Len(t, paths, 11)
}

func TestWALLookups(t *testing.T) {
	cases := []struct {
		assertion string
		records   []wal.RecordType
	}{
		{
			"insert",
			[]wal.RecordType{wal.WALInsert},
		},
		{
			"2x insert",
			[]wal.RecordType{wal.WALInsert, wal.WALInsert},
		},
		{
			"2x insert + merge request",
			[]wal.RecordType{wal.WALInsert, wal.WALInsert, wal.WALMergeRequest},
		},
		{
			"2x insert + merge request + merge complete",
			[]wal.RecordType{wal.WALInsert, wal.WALInsert, wal.WALMergeRequest, wal.WALMergeComplete},
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			ctx := context.Background()
			tmpdir, err := os.MkdirTemp("", "wal_test")
			require.NoError(t, err)
			defer os.RemoveAll(tmpdir)
			wm, _, teardown := testWALManager(ctx, t, tmpdir, 10000, func(batch *wal.Batch) {
				require.NoError(t, batch.Finish())
			})
			require.NoError(t, wm.Recover(ctx))
			addrs := []wal.Address{}
			data := make([]byte, 100)
			for _, record := range c.records {
				if record == wal.WALInsert {
					addr, err := wm.Insert(ctx, "db", "producer", "topic", data)
					require.NoError(t, err)
					addrs = append(addrs, addr)
				}
				// todo don't think this is testing anything
			}
			for _, addr := range addrs {
				value, err := wm.Get(addr)
				require.NoError(t, err)
				require.Equal(t, data, value)
			}
			teardown()
		})
	}
}

func TestWALRecovery(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		assertion             string
		processBatches        bool
		mergeSizeThreshold    int
		inserts               []string // producers
		expectedInsertsCounts map[string]int
		expectedMerges        map[string]int
		expectedBatchCount    int
	}{
		{
			"empty",
			true,
			100,
			[]string{},
			map[string]int{},
			map[string]int{},
			0,
		},
		{
			"one insert unprocessed",
			true,
			1000,
			[]string{
				"producer",
			},
			map[string]int{
				"producer": 1,
			},
			map[string]int{},
			0,
		},
		{
			"two inserts unprocessed",
			true,
			1000,
			[]string{
				"producer",
				"producer",
			},
			map[string]int{
				"producer": 2,
			},
			map[string]int{},
			0,
		},
		{
			"two inserts unprocessed, two producers",
			true,
			1000,
			[]string{
				"producer",
				"producer2",
			},
			map[string]int{
				"producer":  1,
				"producer2": 1,
			},
			map[string]int{},
			0,
		},
		{
			"one insert processed",
			true,
			50,
			[]string{
				"producer",
			},
			map[string]int{},
			map[string]int{},
			1,
		},
		{
			"two processed one unprocessed",
			true,
			150,
			[]string{"producer", "producer", "producer"},
			map[string]int{"producer": 1},
			map[string]int{},
			1,
		},
		{
			"requested for processing but not processed",
			false,
			150,
			[]string{"producer", "producer", "producer"},
			map[string]int{"producer": 1},
			map[string]int{
				"producer": 2,
			},
			1,
		},
	}

	for _, c := range cases {
		for i := 0; i < 1; i++ {
			t.Run(c.assertion, func(t *testing.T) {
				tmpdir, err := os.MkdirTemp("", "wal_test")
				require.NoError(t, err)
				defer os.RemoveAll(tmpdir)

				wm, await, teardown := testWALManager(ctx, t, tmpdir, c.mergeSizeThreshold, func(batch *wal.Batch) {
					if c.processBatches {
						require.NoError(t, batch.Finish())
					}
				})

				require.NoError(t, wm.Recover(ctx))
				topic := "topic"
				for _, producer := range c.inserts {
					data := make([]byte, 100)
					_, err := wm.Insert(ctx, "db", producer, topic, data)
					require.NoError(t, err)
				}
				await(c.expectedBatchCount) // wait for any batches to process prior to shutdown
				teardown()

				wm2, _, teardown := testWALManager(ctx, t, tmpdir, c.mergeSizeThreshold, func(batch *wal.Batch) {
					if c.processBatches {
						require.NoError(t, batch.Finish())
					}
				})
				require.NoError(t, wm2.Recover(ctx))
				teardown()

				for treeID, inserts := range wm2.Stats().PendingInserts {
					require.Equal(t, len(inserts), c.expectedInsertsCounts[treeID.Producer])
				}

				foundMerges := map[string]int{}
				for _, batch := range wm2.Stats().PendingMerges {
					foundMerges[batch.Producer] = len(batch.Addrs)
				}
				require.Equal(t, c.expectedMerges, foundMerges)
			})
		}
	}
}

func TestScenarios(t *testing.T) {
	t.Run("truncated final write on shutdown is truncated", func(t *testing.T) {
		ctx := context.Background()
		tmpdir, err := os.MkdirTemp("", "wal_test")
		require.NoError(t, err)
		defer os.RemoveAll(tmpdir)

		wm, _, teardown := testWALManager(ctx, t, tmpdir, 1000, func(batch *wal.Batch) {
			require.NoError(t, batch.Finish())
		})
		require.NoError(t, wm.Recover(ctx))
		_, err = wm.Insert(ctx, "db", "producer", "topic", []byte{0x01, 0x02})
		require.NoError(t, err)
		teardown()

		// truncate the last record to simulate unclean shutdown
		f, err := os.OpenFile(tmpdir+"/1", os.O_RDWR, 0644)
		require.NoError(t, err)
		n, err := f.Seek(-2, io.SeekEnd)
		require.NoError(t, err)
		require.NoError(t, f.Truncate(n))
		require.NoError(t, f.Close())

		// New wmgr should see a clean recovery. The truncated record should be
		// gone. Inserts should be received as normal.
		wm2, _, teardown := testWALManager(ctx, t, tmpdir, 1000, func(batch *wal.Batch) {
			require.NoError(t, batch.Finish())
		})
		require.NoError(t, wm2.Recover(ctx))
		require.Empty(t, wm2.Stats().PendingInserts)
		_, err = wm2.Insert(ctx, "db", "producer", "topic", []byte{0x01, 0x02})
		require.NoError(t, err)
		require.Len(t, wm2.Stats().PendingInserts, 1)
		teardown()

		// New instance successfully picks up inserts over truncated WAL.
		wm3, _, teardown := testWALManager(ctx, t, tmpdir, 1000, func(batch *wal.Batch) {
			require.NoError(t, batch.Finish())
		})
		require.NoError(t, wm3.Recover(ctx))
		require.Len(t, wm3.Stats().PendingInserts, 1)
		teardown()
	})

	t.Run("flushes split across a single insert", func(t *testing.T) {

	})

	t.Run("multiple topics interleaved", func(t *testing.T) {

	})

	t.Run("log pruning does not delete data we need", func(t *testing.T) {
		ctx := context.Background()
		tmpdir, err := os.MkdirTemp("", "wal_test")
		require.NoError(t, err)
		defer os.RemoveAll(tmpdir)

		wm, await, teardown := testWALManager(ctx, t, tmpdir, 1000, func(batch *wal.Batch) {
			require.NoError(t, batch.Finish())
		})
		require.NoError(t, wm.Recover(ctx))
		_, err = wm.Insert(ctx, "db", "producer", "topic", []byte{0x01, 0x02})
		require.NoError(t, err)

		removed, err := wm.RemoveStaleFiles(ctx)
		require.NoError(t, err)
		require.Zero(t, removed)

		n, err := wm.ForceMerge(ctx)
		require.NoError(t, err)
		await(n)

		// still zero - it's the active wal file
		removed, err = wm.RemoveStaleFiles(ctx)
		require.NoError(t, err)
		require.Zero(t, removed)

		require.NoError(t, wm.Rotate())

		// File gets removed this time, since it's not active and merges have
		// been consumed.
		removed, err = wm.RemoveStaleFiles(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, removed)
		teardown()
	})

	t.Run("logs rotate based on size", func(t *testing.T) {

	})
}

func testWALManager(
	ctx context.Context,
	t *testing.T,
	tmpdir string,
	threshold int,
	batchfn func(*wal.Batch),
) (*wal.WALManager, func(n int), func()) {
	t.Helper()
	merges := make(chan *wal.Batch)
	wm, err := wal.NewWALManager(
		ctx,
		tmpdir,
		merges,
		wal.WithMergeSizeThreshold(threshold),
	)
	require.NoError(t, err)
	done := make(chan bool)
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case batch := <-merges:
				batchfn(batch)
				done <- true
			}
		}
	}()
	await := func(n int) {
		for i := 0; i < n; i++ {
			<-done
		}
	}
	teardown := func() {
		cancel()
	}
	return wm, await, teardown
}
