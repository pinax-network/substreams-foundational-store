package cache

import (
	"fmt"
	"sync"

	pbstore "github.com/streamingfast/substreams-foundationnal-store/pb/store"
	"github.com/streamingfast/substreams-foundationnal-store/store"
)

// Store implements the store.Store interface by wrapping another store
// and caching entries in memory until flushUpToBlock is changed.
type Store struct {
	wrapped        store.Store
	cache          map[string]*pbstore.Entry // key -> Entry
	flushUpToBlock uint64
	mu             sync.RWMutex
}

// NewStore creates a new cache store that wraps the provided store.
func NewStore(wrapped store.Store) *Store {
	return &Store{
		wrapped:        wrapped,
		cache:          make(map[string]*pbstore.Entry),
		flushUpToBlock: 0,
	}
}

// Set stores a single entry in the cache.
// If the entry's block number is <= flushUpToBlock, it's also stored in the wrapped store.
func (s *Store) Set(entry *pbstore.Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Store in cache
	s.cache[string(entry.Key)] = entry

	// If the entry's block number is <= flushUpToBlock, also store it in the wrapped store
	if entry.BlockNumber <= s.flushUpToBlock {
		if err := s.wrapped.Set(entry); err != nil {
			return fmt.Errorf("failed to set entry in wrapped store: %w", err)
		}
	}

	return nil
}

// SetAll stores multiple entries in the cache.
// Entries with block numbers <= flushUpToBlock are also stored in the wrapped store.
func (s *Store) SetAll(entries []*pbstore.Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Store all entries in cache
	for _, entry := range entries {
		s.cache[string(entry.Key)] = entry
	}

	// Collect entries that need to be flushed to the wrapped store
	var toFlush []*pbstore.Entry
	for _, entry := range entries {
		if entry.BlockNumber <= s.flushUpToBlock {
			toFlush = append(toFlush, entry)
		}
	}

	// Flush collected entries to the wrapped store
	if len(toFlush) > 0 {
		if err := s.wrapped.SetAll(toFlush); err != nil {
			return fmt.Errorf("failed to set entries in wrapped store: %w", err)
		}
	}

	return nil
}

// Get retrieves a single entry.
// First checks the cache, then falls back to the wrapped store if not found.
func (s *Store) Get(request *pbstore.GetRequest) (*pbstore.GetResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check cache first
	if entry, ok := s.cache[string(request.Key)]; ok {
		// If the entry's block number is <= the requested block number, return it
		if entry.BlockNumber <= request.BlockNumber {
			return &pbstore.GetResponse{
				Response: pbstore.ResponseCode_FOUND,
				Value:    entry.Value,
			}, nil
		}
	}

	// If not found in cache or block number is too high, check the wrapped store
	return s.wrapped.Get(request)
}

// GetAll retrieves multiple entries.
// Checks the cache first for each key, then falls back to the wrapped store for keys not found in cache.
func (s *Store) GetAll(request *pbstore.GetAllRequest) (*pbstore.GetAllResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Prepare response
	response := &pbstore.GetAllResponse{
		Entries: make([]*pbstore.ResponseEntry, 0, len(request.Keys)),
	}

	// Keys that need to be fetched from the wrapped store
	var keysToFetch [][]byte

	// Check cache first for each key
	for _, key := range request.Keys {
		keyStr := string(key)
		if entry, ok := s.cache[keyStr]; ok {
			// If the entry's block number is <= the requested block number, use it
			if entry.BlockNumber <= request.BlockNumber {
				response.Entries = append(response.Entries, &pbstore.ResponseEntry{
					Key: key,
					Response: &pbstore.GetResponse{
						Response: pbstore.ResponseCode_FOUND,
						Value:    entry.Value,
					},
				})
				continue
			}
		}

		// If not found in cache or block number is too high, add to keys to fetch
		keysToFetch = append(keysToFetch, key)
	}

	// If there are keys to fetch from the wrapped store
	if len(keysToFetch) > 0 {
		wrappedRequest := &pbstore.GetAllRequest{
			BlockNumber: request.BlockNumber,
			OmitDeleted: request.OmitDeleted,
			Keys:        keysToFetch,
		}

		wrappedResponse, err := s.wrapped.GetAll(wrappedRequest)
		if err != nil {
			return nil, fmt.Errorf("failed to get entries from wrapped store: %w", err)
		}

		// Add entries from wrapped store to response
		response.Entries = append(response.Entries, wrappedResponse.Entries...)
	}

	return response, nil
}

// FlushUpToBlock flushes all entries with block numbers <= blockNum to the wrapped store.
func (s *Store) FlushUpToBlock(blockNum uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Update flushUpToBlock
	s.flushUpToBlock = blockNum

	// Collect entries to flush
	var toFlush []*pbstore.Entry
	for _, entry := range s.cache {
		if entry.BlockNumber <= blockNum {
			toFlush = append(toFlush, entry)
		}
	}

	// Flush collected entries to the wrapped store
	if len(toFlush) > 0 {
		if err := s.wrapped.SetAll(toFlush); err != nil {
			return fmt.Errorf("failed to flush entries to wrapped store: %w", err)
		}

		// Remove flushed entries from cache
		for _, entry := range toFlush {
			delete(s.cache, string(entry.Key))
		}
	}

	return nil
}

// EvictUpToBlock removes all keys from the cache where the block number is >= to upToBlockNumber.
func (s *Store) EvictUpToBlock(upToBlockNumber uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Collect keys to evict
	var keysToEvict []string
	for key, entry := range s.cache {
		if entry.BlockNumber >= upToBlockNumber {
			keysToEvict = append(keysToEvict, key)
		}
	}

	// Remove evicted entries from cache
	for _, key := range keysToEvict {
		delete(s.cache, key)
	}

	return nil
}
