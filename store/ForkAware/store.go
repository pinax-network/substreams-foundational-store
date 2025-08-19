package ForkAware

import (
	"fmt"
	"sync"
	"time"

	pbstore "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/v1"
	"github.com/streamingfast/substreams-foundational-store/sink"
	"github.com/streamingfast/substreams-foundational-store/store"
)

type cachedEntry struct {
	entry       *pbstore.Entry
	blockNumber uint64
	blockHash   []byte
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
func (s *Store) Set(entry *pbstore.Entry, blockNumber uint64, blockHash []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Store in ForkAware
	s.cache[string(entry.Key)] = cachedEntry{
		entry:       entry,
		blockNumber: blockNumber,
		blockHash:   blockHash,
	}

	// If the entry's block number is <= flushUpToBlock, also foundational-store it in the wrapped foundational-store
	if blockNumber <= s.flushUpToBlock {
		if err := s.wrapped.Set(entry, blockNumber, blockHash); err != nil {
			return fmt.Errorf("failed to set entry in wrapped foundational-store: %w", err)
		}
	}

	return nil
}

// SetAll stores multiple entries in the ForkAware.
// Entries with block numbers <= flushUpToBlock are also stored in the wrapped foundational-store.
func (s *Store) SetAll(entries []*pbstore.Entry, blockNumber uint64, blockHash []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Store all entries in ForkAware
	for _, entry := range entries {
		s.cache[string(entry.Key)] = cachedEntry{
			entry:       entry,
			blockNumber: blockNumber,
			blockHash:   blockHash,
		}
	}

	// Collect entries that need to be flushed to the wrapped foundational-store
	var toFlush []*pbstore.Entry
	for _, entry := range entries {
		if blockNumber <= s.flushUpToBlock {
			toFlush = append(toFlush, entry)
		}
	}

	// Flush collected entries to the wrapped foundational-store
	if len(toFlush) > 0 {
		if err := s.wrapped.SetAll(toFlush, blockNumber, blockHash); err != nil {
			return fmt.Errorf("failed to set entries in wrapped foundational-store: %w", err)
		}
	}

	return nil
}

// Get retrieves a single entry.
// First checks the ForkAware, then falls back to the wrapped foundational-store if not found.
func (s *Store) Get(request *pbstore.GetRequest) (*pbstore.GetResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check ForkAware first
	if cached, ok := s.cache[string(request.Key)]; ok {
		// If the block number is <= the requested block number and block hash matches, return it
		if cached.blockNumber <= request.BlockNumber {
			// Validate block hash matches if provided
			if len(request.BlockHash) > 0 && len(cached.blockHash) > 0 {
				if string(request.BlockHash) == string(cached.blockHash) {
					return &pbstore.GetResponse{
						Response: pbstore.ResponseCode_FOUND,
						Value:    cached.entry.Value,
					}, nil
				}
			} else if len(request.BlockHash) == 0 || len(cached.blockHash) == 0 {
				// If either hash is empty, just check block number for backward compatibility
				return &pbstore.GetResponse{
					Response: pbstore.ResponseCode_FOUND,
					Value:    cached.entry.Value,
				}, nil
			}
		}
	}

	// If not found in ForkAware or block number is too high, check the wrapped foundational-store
	return s.wrapped.Get(request)
}

// GetAll retrieves multiple entries.
// Checks the ForkAware first for each key, then falls back to the wrapped foundational-store for keys not found in ForkAware.
func (s *Store) GetAll(request *pbstore.GetAllRequest) (*pbstore.GetAllResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Prepare response
	response := &pbstore.GetAllResponse{
		Entries: make([]*pbstore.ResponseEntry, 0, len(request.Keys)),
	}

	// Keys that need to be fetched from the wrapped foundational-store
	var keysToFetch [][]byte

	// Check ForkAware first for each key
	for _, key := range request.Keys {
		keyStr := string(key)
		found := false
		if cached, ok := s.cache[keyStr]; ok {
			// If the block number is <= the requested block number and block hash matches, use it
			if cached.blockNumber <= request.BlockNumber {
				// Validate block hash matches if provided
				if len(request.BlockHash) > 0 && len(cached.blockHash) > 0 {
					if string(request.BlockHash) == string(cached.blockHash) {
						response.Entries = append(response.Entries, &pbstore.ResponseEntry{
							Key: key,
							Response: &pbstore.GetResponse{
								Response: pbstore.ResponseCode_FOUND,
								Value:    cached.entry.Value,
							},
						})
						found = true
					}
				} else if len(request.BlockHash) == 0 || len(cached.blockHash) == 0 {
					// If either hash is empty, just check block number for backward compatibility
					response.Entries = append(response.Entries, &pbstore.ResponseEntry{
						Key: key,
						Response: &pbstore.GetResponse{
							Response: pbstore.ResponseCode_FOUND,
							Value:    cached.entry.Value,
						},
					})
					found = true
				}
			}
		}

		// If not found in ForkAware or block number is too high, add to keys to fetch
		if !found {
			keysToFetch = append(keysToFetch, key)
		}
	}

	// If there are keys to fetch from the wrapped foundational-store
	if len(keysToFetch) > 0 {
		wrappedRequest := &pbstore.GetAllRequest{
			BlockNumber: request.BlockNumber,
			BlockHash:   request.BlockHash,
			OmitDeleted: request.OmitDeleted,
			Keys:        keysToFetch,
		}

		wrappedResponse, err := s.wrapped.GetAll(wrappedRequest)
		if err != nil {
			return nil, fmt.Errorf("failed to get entries from wrapped foundational-store: %w", err)
		}

		// Add entries from wrapped foundational-store to response
		response.Entries = append(response.Entries, wrappedResponse.Entries...)
	}

	return response, nil
}

// FlushUpToBlock flushes all entries with block numbers <= blockNum to the wrapped foundational-store.
func (s *Store) FlushUpToBlock(blockNum uint64) error {
	start := time.Now()
	defer func() {
		sink.StoreFlushDuration.ObserveDuration(time.Since(start))
	}()

	s.mu.Lock()
	defer s.mu.Unlock()

	// Update flushUpToBlock
	s.flushUpToBlock = blockNum

	// Collect entries to flush grouped by blockNumber and blockHash
	type flushGroup struct {
		entries     []*pbstore.Entry
		blockNumber uint64
		blockHash   []byte
	}
	var flushGroups []flushGroup
	groupMap := make(map[string]*flushGroup)

	for _, cached := range s.cache {
		if cached.blockNumber <= blockNum {
			// Create a key from blockNumber and blockHash
			groupKey := fmt.Sprintf("%d-%x", cached.blockNumber, cached.blockHash)
			
			if group, exists := groupMap[groupKey]; exists {
				group.entries = append(group.entries, cached.entry)
			} else {
				newGroup := &flushGroup{
					entries:     []*pbstore.Entry{cached.entry},
					blockNumber: cached.blockNumber,
					blockHash:   cached.blockHash,
				}
				groupMap[groupKey] = newGroup
				flushGroups = append(flushGroups, *newGroup)
			}
		}
	}

	// Flush each group separately
	for _, group := range flushGroups {
		if err := s.wrapped.SetAll(group.entries, group.blockNumber, group.blockHash); err != nil {
			return fmt.Errorf("failed to flush entries to wrapped foundational-store: %w", err)
		}
	}

	// Remove flushed entries from ForkAware
	for _, cached := range s.cache {
		if cached.blockNumber <= blockNum {
			delete(s.cache, string(cached.entry.Key))
		}
	}

	return nil
}

// EvictUpToBlock removes all keys from the ForkAware where the block number is >= to upToBlockNumber.
func (s *Store) EvictUpToBlock(upToBlockNumber uint64) error {
	start := time.Now()
	defer func() {
		sink.StoreEvictDuration.ObserveDuration(time.Since(start))
	}()

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
