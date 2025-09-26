package badger_time_traversal

import (
	"fmt"
	"os"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/options"
	"github.com/streamingfast/substreams-foundational-store/store"
	"go.uber.org/zap"
)

// Store implements the foundational-store.Store interface for Badger DB with time traversal
// Time traversal is achieved by appending block numbers to keys: original_key + block_number
// Keys with highest block numbers are scanned first through reverse iteration
type Store struct {
	db         *badger.DB
	typeUrl    string
	numWorkers int
	logger     *zap.Logger
}

// NewStore creates a new Badger foundational-store with time traversal support
func NewStore(dsn *store.DSN, typeUrl string, numWorkers int, logger *zap.Logger) (*Store, error) {
	// Provide default logger if nil
	if logger == nil {
		logger = zap.NewNop()
	}
	// Extract Badger-specific parameters
	// For Badger, we'll use the Database field to hold the path to the Badger DB directory
	dbPath := dsn.Database

	// Ensure the directory exists
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory for Badger DB: %w", err)
	}

	opts := badger.DefaultOptions(dbPath).
		WithBlockCacheSize(512 << 20). // 512MB
		WithIndexCacheSize(0).
		WithBloomFalsePositive(0.001).
		WithValueThreshold(128 << 10). // 128KB
		WithCompression(options.None).
		WithNumMemtables(5).
		WithValueLogFileSize(256 << 20).
		WithMemTableSize(512 << 20).
		WithNumGoroutines(32)

	// Open the Badger database
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open Badger DB: %w", err)
	}

	// Create foundational-store with provided values
	store := &Store{
		db:         db,
		typeUrl:    typeUrl,
		numWorkers: numWorkers,
		logger:     logger,
	}

	store.logger.Info("badger time traversal foundational-store initialized",
		zap.String("path", dbPath),
		zap.Int("workers", store.numWorkers))

	return store, nil
}

// Close closes the Badger database
func (s *Store) Close() error {
	return s.db.Close()
}

// GetDB returns the underlying Badger database
func (s *Store) GetDB() *badger.DB {
	return s.db
}

// GetTypeURL returns the type URL for the stored values
func (s *Store) GetTypeURL() string {
	return s.typeUrl
}
