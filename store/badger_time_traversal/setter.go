package badger_time_traversal

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v3"
	pbstore "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/v1"
	"github.com/streamingfast/substreams-foundational-store/sink"
)

// makeTimeTraversalKey creates a composite key by appending the block number to the original key
// Format: original_key + block_number (8 bytes, big-endian)
func makeTimeTraversalKey(originalKey []byte, blockNumber uint64) []byte {
	// Create a new key with original key + 8 bytes for block number
	compositeKey := make([]byte, len(originalKey)+8)

	// Copy original key
	copy(compositeKey, originalKey)

	// Append block number as 8 bytes (big-endian)
	binary.BigEndian.PutUint64(compositeKey[len(originalKey):], blockNumber)

	return compositeKey
}

// Set stores a single entry in Badger with time traversal support
func (s *Store) Set(entry *pbstore.Entry, blockNumber uint64) error {
	executionStart := time.Now()
	defer func() {
		sink.DatabaseExecutionDuration.ObserveDuration(time.Since(executionStart))
		sink.DatabaseSetOperations.Inc()
		sink.DatabaseKeysProcessed.Inc()
	}()

	// Create composite key with block number
	compositeKey := makeTimeTraversalKey(entry.Key, blockNumber)

	// Store the actual value (without prepending block info like original implementation)
	// The block info is now encoded in the key itself
	value := entry.Value.Value

	badgerStart := time.Now()
	err := s.db.Update(func(txn *badger.Txn) error {
		setStart := time.Now()
		err := txn.Set(compositeKey, value)
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

// SetAll stores multiple entries in Badger with time traversal support
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
		// Create composite key with block number
		compositeKey := makeTimeTraversalKey(entry.Key, blockNumber)

		// Store the actual value (without prepending block info)
		value := entry.Value.Value

		setStart := time.Now()
		err := wb.Set(compositeKey, value)
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
