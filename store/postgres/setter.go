package postgres

import (
	"fmt"
	"time"

	pbstore "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/v1"
)

func (s *Store) Set(entry *pbstore.Entry, blockNumber uint64, blockHash []byte) error {
	if entry == nil {
		return fmt.Errorf("entry cannot be nil")
	}

	// Use the prepared insert statement to insert the entry
	// The statement expects: block_number, block_hash, key, value, create_time
	_, err := s.insertStatement.Exec(blockNumber, blockHash, entry.Key, entry.Value.Value, time.Now())
	if err != nil {
		return fmt.Errorf("failed to insert entry: %w", err)
	}

	return nil
}

func (s *Store) SetAll(entries []*pbstore.Entry, blockNumber uint64, blockHash []byte) error {
	if len(entries) == 0 {
		return nil
	}

	// Begin a transaction
	tx, err := s.db.Beginx()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Use the existing prepared statement from the foundational-store
	insertStmt := s.insertStatement
	if insertStmt == nil {
		_ = tx.Rollback()
		return fmt.Errorf("insert statement is nil")
	}

	// Insert each entry
	for _, entry := range entries {
		if entry == nil {
			_ = tx.Rollback()
			return fmt.Errorf("entry cannot be nil")
		}

		// Use the block_number and block_hash from the parameters
		_, err := insertStmt.Exec(blockNumber, blockHash, entry.Key, entry.Value.Value, time.Now())
		if err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("failed to insert entry: %w", err)
		}
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}
