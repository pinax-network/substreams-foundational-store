package badger

import (
	"encoding/binary"
	"fmt"

	"github.com/dgraph-io/badger/v3"
	pbstore "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/v1"
)

// Set stores a single entry in Badger
func (s *Store) Set(entry *pbstore.Entry, blockNumber uint64) error {
	// Prepend block_number and block_hash as bytes to the value
	blockNumBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(blockNumBytes, blockNumber)

	// Combine block number, block hash, and value
	valueWithBlockInfo := blockNumBytes
	valueWithBlockInfo = append(valueWithBlockInfo, entry.Value.Value...)

	err := s.db.Update(func(txn *badger.Txn) error {
		// Use the entry.Key value with the combined value
		err := txn.Set(entry.Key, valueWithBlockInfo)
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
	if len(entries) == 0 {
		return nil
	}

	// Use a batch writer for better performance with multiple entries
	wb := s.db.NewWriteBatch()
	defer wb.Cancel()

	for _, entry := range entries {
		// Prepend block_number and block_hash as bytes to the value
		blockNumBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(blockNumBytes, blockNumber)

		// Combine block number, block hash, and value
		valueWithBlockInfo := blockNumBytes
		valueWithBlockInfo = append(valueWithBlockInfo, entry.Value.Value...)

		err := wb.Set(entry.Key, valueWithBlockInfo)
		if err != nil {
			return fmt.Errorf("failed to add entry to batch: %w", err)
		}
	}

	err := wb.Flush()
	if err != nil {
		return fmt.Errorf("failed to flush batch to Badger: %w", err)
	}

	return nil
}
