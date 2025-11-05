package badger_time_traversal

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/dgraph-io/badger/v3"
	pbmodel "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/model/v2"
)

// makeTimeTraversalKey creates a composite key by appending the reversed block number to the original key
// Format: original_key + (math.MaxUint64 - block_number) (8 bytes, big-endian)
// This reverses the ordering so newer blocks come first in lexicographic order
func makeTimeTraversalKey(originalKey []byte, blockNumber uint64) []byte {
	// Create a new key with original key + 8 bytes for block number
	compositeKey := make([]byte, len(originalKey)+8)

	// Copy original key
	copy(compositeKey, originalKey)

	// Append reversed block number as 8 bytes (big-endian)
	// This makes newer blocks (higher block numbers) sort first
	reversedBlockNumber := math.MaxUint64 - blockNumber
	binary.BigEndian.PutUint64(compositeKey[len(originalKey):], reversedBlockNumber)

	return compositeKey
}

// Set stores a single entry in Badger with time traversal support
func (s *Store) Set(entry *pbmodel.Entry, IfNotExist bool, blockNumber uint64) error {
	if IfNotExist {
		// For time traversal, check if any version of this key exists
		err := s.db.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchSize = 10
			it := txn.NewIterator(opts)
			defer it.Close()

			prefix := entry.Key.Bytes
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				// If we find any key with this prefix, it means the key exists
				return nil
			}
			return badger.ErrKeyNotFound
		})
		if err == nil {
			// Key exists, skip this entry
			return nil
		}
		if err != badger.ErrKeyNotFound {
			return fmt.Errorf("failed to check existence of key: %w", err)
		}
		// Key doesn't exist, proceed with insertion
	}

	// Create composite key with block number
	compositeKey := makeTimeTraversalKey(entry.Key.Bytes, blockNumber)

	// Store the actual value (without prepending block info like original implementation)
	// The block info is now encoded in the key itself
	value := entry.Value.Value

	err := s.db.Update(func(txn *badger.Txn) error {
		err := txn.Set(compositeKey, value)
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

// SetAll stores multiple entries in Badger with time traversal support
func (s *Store) SetAll(entries []*pbmodel.Entry, IfNotExist bool, blockNumber uint64) error {
	if len(entries) == 0 {
		return nil
	}

	// Use a batch writer for better performance with multiple entries
	wb := s.db.NewWriteBatch()
	defer wb.Cancel()

	for _, entry := range entries {
		if IfNotExist {
			// For time traversal, check if any version of this key exists
			err := s.db.View(func(txn *badger.Txn) error {
				opts := badger.DefaultIteratorOptions
				opts.PrefetchSize = 10
				it := txn.NewIterator(opts)
				defer it.Close()

				prefix := entry.Key.Bytes
				for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
					// If we find any key with this prefix, it means the key exists
					return nil
				}
				return badger.ErrKeyNotFound
			})
			if err == nil {
				// Key exists, skip this entry
				continue
			}
			if err != badger.ErrKeyNotFound {
				return fmt.Errorf("failed to check existence of key: %w", err)
			}
			// Key doesn't exist, proceed with insertion
		}

		// Create composite key with block number
		compositeKey := makeTimeTraversalKey(entry.Key.Bytes, blockNumber)

		// Store the actual value (without prepending block info)
		value := entry.Value.Value

		err := wb.Set(compositeKey, value)
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
