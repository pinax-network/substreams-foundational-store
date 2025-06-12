package badger

import (
	"fmt"
	"os"

	"github.com/dgraph-io/badger/v3"
	"github.com/streamingfast/substreams-foundationnal-store/store"
)

// Store implements the store.Store interface for Badger DB
type Store struct {
	db         *badger.DB
	typeUrl    string
	numWorkers int
}

// StoreOption is a function that configures a Store
type StoreOption func(*Store)

// WithNumWorkers sets the number of workers for parallel operations
func WithNumWorkers(numWorkers int) StoreOption {
	return func(s *Store) {
		s.numWorkers = numWorkers
	}
}

// NewStore creates a new Badger store
func NewStore(dsn *store.DSN, typeUrl string, opts ...StoreOption) (*Store, error) {
	// Extract Badger-specific parameters
	// For Badger, we'll use the Database field to hold the path to the Badger DB directory
	dbPath := dsn.Database

	// Ensure the directory exists
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory for Badger DB: %w", err)
	}

	// Open the Badger database
	badgerOpts := badger.DefaultOptions(dbPath)
	db, err := badger.Open(badgerOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open Badger DB: %w", err)
	}

	// Create store with default values
	store := &Store{
		db:         db,
		typeUrl:    typeUrl,
		numWorkers: 10, // Default to 10 workers
	}

	// Apply options
	for _, opt := range opts {
		opt(store)
	}

	fmt.Printf("Badger store initialized at %s with %d workers\n", dbPath, store.numWorkers)
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
