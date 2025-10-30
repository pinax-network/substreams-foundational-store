package ForkAware

import (
	"fmt"
	"sync"

	pbmodel "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/model/v1"
	pbservice "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/service/v1"
	"github.com/streamingfast/substreams-foundational-store/store"
)

type cachedEntry struct {
	entry       *pbmodel.Entry
	blockNumber uint64
}

// Store implements the foundational-store.Store interface by wrapping another foundational-store
// and caching entries in memory until flushUpToBlock is changed.
type Store struct {
	wrapped        store.Store
	cache          map[string]cachedEntry // key -> Entry
	flushUpToBlock uint64
	mu             sync.RWMutex
}

// NewStore creates a new ForkAware foundational-store that wraps the provided foundational-store.
func NewStore(wrapped store.Store) *Store {
	return &Store{
		wrapped:        wrapped,
		cache:          make(map[string]cachedEntry),
		flushUpToBlock: 0,
	}
}

// Set stores a single entry in the ForkAware.
// If the entry's block number is <= flushUpToBlock, it's also stored in the wrapped foundational-store.
func (s *Store) Set(entry *pbmodel.Entry, blockNumber uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Store in ForkAware
	s.cache[string(entry.Key.Bytes)] = cachedEntry{
		entry:       entry,
		blockNumber: blockNumber,
	}

	// If the entry's block number is <= flushUpToBlock, also foundational-store it in the wrapped foundational-store
	if blockNumber <= s.flushUpToBlock {
		if err := s.wrapped.Set(entry, blockNumber); err != nil {
			return fmt.Errorf("failed to set entry in wrapped foundational-store: %w", err)
		}
	}

	return nil
}

// SetAll stores multiple entries in the ForkAware.
// Entries with block numbers <= flushUpToBlock are also stored in the wrapped foundational-store.
func (s *Store) SetAll(entries []*pbmodel.Entry, blockNumber uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Store all entries in ForkAware
	for _, entry := range entries {
		s.cache[string(entry.Key.Bytes)] = cachedEntry{
			entry:       entry,
			blockNumber: blockNumber,
		}
	}

	// Collect entries that need to be flushed to the wrapped foundational-store
	var toFlush []*pbmodel.Entry
	for _, entry := range entries {
		if blockNumber <= s.flushUpToBlock {
			toFlush = append(toFlush, entry)
		}
	}

	// Flush collected entries to the wrapped foundational-store
	if len(toFlush) > 0 {
		if err := s.wrapped.SetAll(toFlush, blockNumber); err != nil {
			return fmt.Errorf("failed to set entries in wrapped foundational-store: %w", err)
		}
	}

	return nil
}

// Get retrieves a single entry by delegating to the wrapped store and overlaying cache when possible.
func (s *Store) Get(request *pbservice.GetRequest) (*pbservice.GetResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check cache first
	if cached, ok := s.cache[string(request.Key.Bytes)]; ok {
		if cached.blockNumber <= request.BlockNumber {
			return &pbservice.GetResponse{Entry: &pbmodel.QueriedEntry{Code: pbmodel.ResponseCode_RESPONSE_CODE_FOUND, Entry: cached.entry}}, nil
		}
	}
	return s.wrapped.Get(request)
}

// GetAll delegates to the wrapped store for simplicity.
func (s *Store) GetAll(request *pbservice.GetAllRequest) (*pbservice.GetAllResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.wrapped.GetAll(request)
}

// GetFirst delegates to the wrapped store for simplicity.
func (s *Store) GetFirst(request *pbservice.GetFirstRequest) (*pbservice.GetResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.wrapped.GetFirst(request)
}

// GetAllFirst delegates to the wrapped store for simplicity.
func (s *Store) GetAllFirst(request *pbservice.GetAllRequest) (*pbservice.GetAllResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.wrapped.GetAllFirst(request)
}

// FlushUpToBlock flushes all entries with block numbers <= blockNum to the wrapped foundational-store.
func (s *Store) FlushUpToBlock(blockNum uint64) error {

	s.mu.Lock()
	defer s.mu.Unlock()

	// Update flushUpToBlock
	s.flushUpToBlock = blockNum

	// Collect entries to flush
	var toFlush []*pbmodel.Entry
	for _, cached := range s.cache {
		if cached.blockNumber <= blockNum {
			toFlush = append(toFlush, cached.entry)
		}
	}

	// Flush collected entries to the wrapped foundational-store
	if len(toFlush) > 0 {
		if err := s.wrapped.SetAll(toFlush, blockNum); err != nil {
			return fmt.Errorf("failed to flush entries to wrapped foundational-store: %w", err)
		}

		// Remove flushed entries from ForkAware
		for _, entry := range toFlush {
			delete(s.cache, string(entry.Key.Bytes))
		}
	}

	return nil
}

// EvictUpToBlock removes all keys from the ForkAware where the block number is >= to upToBlockNumber.
func (s *Store) EvictUpToBlock(upToBlockNumber uint64) error {

	s.mu.Lock()
	defer s.mu.Unlock()

	// Collect keys to evict
	var keysToEvict []string
	for key, cached := range s.cache {
		if cached.blockNumber >= upToBlockNumber {
			keysToEvict = append(keysToEvict, key)
		}
	}

	// Remove evicted entries from ForkAware
	for _, key := range keysToEvict {
		delete(s.cache, key)
	}

	return nil
}
