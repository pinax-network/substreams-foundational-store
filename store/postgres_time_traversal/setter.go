package postgres_time_traversal

import (
	"fmt"
	"time"

	pbstore "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/v1"
	"github.com/streamingfast/substreams-foundational-store/sink"
)

func (s *Store) Set(entry *pbstore.Entry, blockNumber uint64) error {
	// Track total execution time
	executionStart := time.Now()
	defer func() {
		sink.DatabaseExecutionDuration.ObserveDuration(time.Since(executionStart))
		sink.DatabaseSetOperations.Inc()
		sink.DatabaseKeysProcessed.Inc()
	}()

	if entry == nil {
		return fmt.Errorf("entry cannot be nil")
	}

	// Use the prepared insert statement to insert the entry
	// The statement expects: block_number, key, value, create_time
	_, err := s.insertStatement.Exec(blockNumber, entry.Key, entry.Value.Value, time.Now())
	if err != nil {
		sink.DatabaseSetErrors.Inc()
		return fmt.Errorf("failed to insert entry: %w", err)
	}

	return nil
}

func (s *Store) SetAll(entries []*pbstore.Entry, blockNumber uint64) error {
	// Track total execution time
	executionStart := time.Now()
	defer func() {
		sink.DatabaseExecutionDuration.ObserveDuration(time.Since(executionStart))
		sink.DatabaseSetOperations.AddInt(len(entries))
		sink.DatabaseKeysProcessed.AddInt(len(entries))
	}()

	if len(entries) == 0 {
		return nil
	}

	// Begin a transaction for better performance and consistency
	tx, err := s.db.Beginx()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Use the existing prepared statement from the store
	insertStmt := s.insertStatement
	if insertStmt == nil {
		_ = tx.Rollback()
		return fmt.Errorf("insert statement is nil")
	}

	// Insert each entry with the same block number
	for _, entry := range entries {
		if entry == nil {
			_ = tx.Rollback()
			return fmt.Errorf("entry cannot be nil")
		}

		// Use the block_number from the parameter
		_, err := insertStmt.Exec(blockNumber, entry.Key, entry.Value.Value, time.Now())
		if err != nil {
			_ = tx.Rollback()
			sink.DatabaseSetErrors.AddInt(len(entries))
			return fmt.Errorf("failed to insert entry: %w", err)
		}
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		sink.DatabaseSetErrors.AddInt(len(entries))
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}
