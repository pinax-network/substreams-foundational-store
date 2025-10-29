package badger_time_traversal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sync"

	"github.com/dgraph-io/badger/v3"
	pbmodel "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/model/v1"
	pbservice "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/service/v1"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/anypb"
)

// workItem represents a work item for parallel processing
type workItem struct {
	index int
	key   *pbmodel.Key
}

// extractBlockNumberFromKey extracts the block number from a composite key
// Returns the original key and block number (reversed from the stored value)
func extractBlockNumberFromKey(compositeKey []byte) ([]byte, uint64) {
	if len(compositeKey) < 8 {
		return compositeKey, 0
	}

	originalKeyLen := len(compositeKey) - 8
	originalKey := compositeKey[:originalKeyLen]
	reversedBlockNumber := binary.BigEndian.Uint64(compositeKey[originalKeyLen:])

	// Reverse the block number back to original value
	blockNumber := math.MaxUint64 - reversedBlockNumber

	return originalKey, blockNumber
}

// Get retrieves a single entry from Badger with time traversal support
// It finds the entry with the highest block number that is <= the requested block number
func (s *Store) Get(request *pbservice.GetRequest) (*pbservice.GetResponse, error) {
	var foundValue []byte
	var found bool

	err := s.db.View(func(txn *badger.Txn) error {
		badgerOptions := badger.DefaultIteratorOptions
		badgerOptions.PrefetchValues = false
		badgerOptions.PrefetchSize = 100

		start := makeTimeTraversalKey(request.Key.Bytes, request.BlockNumber)
		exclusiveEnd := append(makeTimeTraversalKey(request.Key.Bytes, 0), 0)

		it := txn.NewIterator(badgerOptions)
		defer it.Close()

		for it.Seek(start); it.Valid() && bytes.Compare(it.Item().Key(), exclusiveEnd) == -1; it.Next() {
			item := it.Item()
			val, err := item.ValueCopy(nil)
			if err != nil {
				return fmt.Errorf("copying value: %w", err)
			}
			foundValue = val
			found = true
			break
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("getting value from Badger: %w", err)
	}

	resp := &pbservice.GetResponse{BlockReached: true}
	if !found {
		resp.Entry = &pbmodel.QueriedEntry{Code: pbmodel.ResponseCode_RESPONSE_CODE_NOT_FOUND, Entry: &pbmodel.Entry{Key: request.Key}}
		return resp, nil
	}

	resp.Entry = &pbmodel.QueriedEntry{
		Code:  pbmodel.ResponseCode_RESPONSE_CODE_FOUND,
		Entry: &pbmodel.Entry{Key: request.Key, Value: &anypb.Any{TypeUrl: s.typeUrl, Value: foundValue}},
	}
	return resp, nil
}

// GetAll retrieves multiple entries from Badger using goroutines for parallelism with time traversal support
func (s *Store) GetAll(request *pbservice.GetAllRequest) (*pbservice.GetAllResponse, error) {
	if len(request.Keys) == 0 {
		return &pbservice.GetAllResponse{
			BlockReached: true,
			Entries:      &pbmodel.QueriedEntries{},
		}, nil
	}

	// Create entries slice with same length as keys to preserve order
	entries := make([]*pbmodel.QueriedEntry, len(request.Keys))

	// Use worker pool for parallel processing
	numWorkers := s.numWorkers
	if numWorkers > len(request.Keys) {
		numWorkers = len(request.Keys)
	}

	// Channel for work distribution with index to preserve order
	workChan := make(chan workItem, len(request.Keys))

	// Send work items to channel
	for i, key := range request.Keys {
		workChan <- workItem{index: i, key: key}
	}
	close(workChan)

	// Use mutex to protect entries slice
	var mutex sync.Mutex
	var keysFoundCount int

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.getAllWorker(workChan, entries, &mutex, &keysFoundCount, request.BlockNumber)
		}()
	}

	// Wait for all workers to complete
	wg.Wait()

	return &pbservice.GetAllResponse{
		BlockReached: true,
		Entries:      &pbmodel.QueriedEntries{Entries: entries},
	}, nil
}

// getAllWorker processes work items and stores results in the entries slice at correct index
func (s *Store) getAllWorker(workChan <-chan workItem, entries []*pbmodel.QueriedEntry, mutex *sync.Mutex, keysFoundCount *int, blockNumber uint64) {
	for work := range workChan {
		// Use the Get method for each key
		resp, err := s.Get(&pbservice.GetRequest{
			Key:         work.key,
			BlockNumber: blockNumber,
		})

		// Create queried entry
		var queriedEntry *pbmodel.QueriedEntry
		if err != nil {
			// Log error and create NOT_FOUND response
			s.logger.Error("failed to get key", zap.String("key", string(work.key.Bytes)), zap.Error(err))
			queriedEntry = &pbmodel.QueriedEntry{
				Code:  pbmodel.ResponseCode_RESPONSE_CODE_NOT_FOUND,
				Entry: &pbmodel.Entry{Key: work.key},
			}
		} else {
			// Use the entry from Get method
			queriedEntry = resp.Entry

			// Count found keys
			if resp.Entry.Code == pbmodel.ResponseCode_RESPONSE_CODE_FOUND {
				mutex.Lock()
				*keysFoundCount++
				mutex.Unlock()
			}
		}

		// Store result at correct index to preserve order
		mutex.Lock()
		entries[work.index] = queriedEntry
		mutex.Unlock()
	}
}

// GetFirst returns the value for the first key >= the requested key in lexicographic order.
// For time-traversal storage, this returns the oldest value for that key (lowest block number).
func (s *Store) GetFirst(request *pbservice.GetFirstRequest) (*pbservice.GetResponse, error) {
	var foundValue []byte
	var found bool

	err := s.db.View(func(txn *badger.Txn) error {
		base := request.Key.Bytes

		// Now fetch the oldest version for that base key by reverse-iterating the base range
		begin := make([]byte, len(base)+8)
		copy(begin, base) // base + 0x00..00
		end := make([]byte, len(base)+8+1)
		copy(end, base)
		for i := len(base); i < len(base)+8; i++ {
			end[i] = 0xFF
		}
		end[len(base)+8] = 0x00 // exclusive upper bound

		opts := badger.DefaultIteratorOptions
		opts.Reverse = true
		opts.PrefetchValues = false
		rev := txn.NewIterator(opts)
		defer rev.Close()
		rev.Seek(end)
		if rev.Valid() {
			it2 := rev.Item()
			k2 := it2.Key()
			if bytes.Compare(k2, begin) >= 0 && bytes.Compare(k2, end) == -1 {
				v, e := it2.ValueCopy(nil)
				if e != nil {
					return e
				}
				foundValue = v
				found = true
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("badger_time_traversal GetFirst: %w", err)
	}
	if !found {
		return &pbservice.GetResponse{
			BlockReached: true,
			Entry:        &pbmodel.QueriedEntry{Code: pbmodel.ResponseCode_RESPONSE_CODE_NOT_FOUND, Entry: &pbmodel.Entry{Key: request.Key}},
		}, nil
	}
	return &pbservice.GetResponse{
		BlockReached: true,
		Entry: &pbmodel.QueriedEntry{
			Code:  pbmodel.ResponseCode_RESPONSE_CODE_FOUND,
			Entry: &pbmodel.Entry{Key: request.Key, Value: &anypb.Any{TypeUrl: s.typeUrl, Value: foundValue}},
		},
	}, nil
}
