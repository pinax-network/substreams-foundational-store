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
	executionStart := time.Now()
	defer func() {
		sink.DatabaseExecutionDuration.ObserveDuration(time.Since(executionStart))
		sink.DatabaseSetOperations.Inc()
		sink.DatabaseKeysProcessed.Inc()
	}()

	// Prepend block_number and block_hash as bytes to the value
	blockNumBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(blockNumBytes, blockNumber)

	// Combine block number, block hash, and value
	valueWithBlockInfo := blockNumBytes
	valueWithBlockInfo = append(valueWithBlockInfo, entry.Value.Value...)

	badgerStart := time.Now()
	err := s.db.Update(func(txn *badger.Txn) error {
		// Use the entry.Key value with the combined value
		setStart := time.Now()
		err := txn.Set(entry.Key, valueWithBlockInfo)
		sink.BadgerSetOperationDuration.ObserveDuration(time.Since(setStart))
		sink.BadgerSetOperationCount.Inc()

		if err != nil {
			return fmt.Errorf("failed to set value in Badger: %w", err)
		}
		return nil
	})
	sink.BadgerTransactionDuration.ObserveDuration(time.Since(badgerStart))
	sink.BadgerTransactionCount.Inc()

	if err != nil {
		sink.DatabaseSetErrors.Inc()
		return fmt.Errorf("failed to update Badger: %w", err)
	}

	return nil
}

// SetAll stores multiple entries in Badger
func (s *Store) SetAll(entries []*pbstore.Entry, blockNumber uint64) error {
	executionStart := time.Now()
	defer func() {
		sink.DatabaseExecutionDuration.ObserveDuration(time.Since(executionStart))
		sink.StoreSetAllDuration.ObserveDuration(time.Since(executionStart))
		sink.EntriesProcessed.AddInt(len(entries))
		sink.DatabaseSetOperations.AddInt(len(entries))
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
	badgerStart := time.Now()
	wb := s.db.NewWriteBatch()
	defer wb.Cancel()

	for _, entry := range entries {
		// Prepend block_number and block_hash as bytes to the value
		blockNumBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(blockNumBytes, blockNumber)

		// Combine block number, block hash, and value
		valueWithBlockInfo := blockNumBytes
		valueWithBlockInfo = append(valueWithBlockInfo, entry.Value.Value...)

		setStart := time.Now()
		err := wb.Set(entry.Key, valueWithBlockInfo)
		sink.BadgerSetOperationDuration.ObserveDuration(time.Since(setStart))
		sink.BadgerSetOperationCount.Inc()

		if err != nil {
			return fmt.Errorf("failed to add entry to batch: %w", err)
		}
	}

	flushStart := time.Now()
	err := wb.Flush()
	batchWriteDuration := time.Since(flushStart)
	sink.BadgerBatchWriteDuration.ObserveDuration(batchWriteDuration)
	sink.BadgerBatchWriteCount.Inc()

	if err != nil {
		sink.BadgerFlushErrors.Inc()
		sink.DatabaseSetErrors.AddInt(len(entries))
		return fmt.Errorf("failed to flush batch to Badger: %w", err)
	}
	sink.BadgerFlushDuration.ObserveDuration(batchWriteDuration)
	sink.BadgerFlushCount.Inc()
	sink.BadgerTransactionDuration.ObserveDuration(time.Since(badgerStart))
	sink.BadgerTransactionCount.Inc()

	return nil
}
