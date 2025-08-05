package badger

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v3"
	pbstore "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/v1"
	"github.com/streamingfast/substreams-foundational-store/sink"
)

// Set stores a single entry in Badger
func (s *Store) Set(entry *pbstore.Entry, blockNumber uint64) error {
	start := time.Now()
	defer func() {
		sink.StoreSetAllDuration.ObserveDuration(time.Since(start))
		sink.DatabaseSetOperations.Inc()
		sink.DatabaseKeysProcessed.Inc()
	}()

	// Prepend block_number as bytes to the value
	blockNumBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(blockNumBytes, blockNumber)

	// Combine block number bytes with the value
	valueWithBlockNum := append(blockNumBytes, entry.Value.Value...)

	err := s.db.Update(func(txn *badger.Txn) error {
		// Use the entry.Key value with the combined value
		err := txn.Set(entry.Key, valueWithBlockNum)
		if err != nil {
			return fmt.Errorf("failed to set value in Badger: %w", err)
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to update Badger: %w", err)
	}

	return nil
}

// SetAll stores multiple entries in Badger
func (s *Store) SetAll(entries []*pbstore.Entry, blockNumber uint64) error {
	start := time.Now()
	defer func() {
		sink.StoreSetAllDuration.ObserveDuration(time.Since(start))
		sink.EntriesProcessed.AddInt(len(entries))
		// Update Badger size metrics periodically
		lsm, vlog := s.db.Size()
		totalSize := uint64(lsm + vlog)
		if totalSize > 0 {
			sink.BadgerStoreSize.SetUint64(totalSize)
		}
	}()

	if len(entries) == 0 {
		return nil
	}

	// Use a batch writer for better performance with multiple entries
	wb := s.db.NewWriteBatch()
	defer wb.Cancel()

	for _, entry := range entries {
		// Prepend block_number as bytes to the value
		blockNumBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(blockNumBytes, blockNumber)

		// Combine block number bytes with the value
		valueWithBlockNum := append(blockNumBytes, entry.Value.Value...)

		err := wb.Set(entry.Key, valueWithBlockNum)
		if err != nil {
			return fmt.Errorf("failed to add entry to batch: %w", err)
		}
	}

	flushStart := time.Now()
	err := wb.Flush()
	if err != nil {
		sink.BadgerFlushErrors.Inc()
		return fmt.Errorf("failed to flush batch to Badger: %w", err)
	}
	sink.BadgerFlushDuration.ObserveDuration(time.Since(flushStart))
	sink.BadgerFlushCount.Inc()

	return nil
}
