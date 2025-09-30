package badger_time_traversal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sync"

	"github.com/dgraph-io/badger/v3"
	pbstore "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/v1"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/anypb"
)

// workItem represents a work item for parallel processing
type workItem struct {
	index int
	key   []byte
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
func (s *Store) Get(request *pbstore.GetRequest) (*pbstore.GetResponse, error) {
	var foundValue []byte
	var found bool

	err := s.db.View(func(txn *badger.Txn) error {
		// Create iterator options for efficient forward scanning
		badgerOptions := badger.DefaultIteratorOptions
		badgerOptions.PrefetchValues = false
		badgerOptions.PrefetchSize = 100

		start := makeTimeTraversalKey(request.Key, request.BlockNumber)
		exclusiveEnd := append(makeTimeTraversalKey(request.Key, 0), 0)

		bit := txn.NewIterator(badgerOptions)
		defer bit.Close()

		var err error

		// Since block numbers are reversed, we scan forward and the first valid entry
		// will be the one with the highest block number <= requested block number
		for bit.Seek(start); bit.Valid() && bytes.Compare(bit.Item().Key(), exclusiveEnd) == -1; bit.Next() {
			item := bit.Item()

			found = true
			foundValue, err = item.ValueCopy(nil)

			if err != nil {
				return fmt.Errorf("copying value: %w", err)
			}
			break
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("getting value from Badger: %w", err)
	}

	if !found {
		return &pbstore.GetResponse{
			Code: pbstore.ResponseCode_RESPONSE_CODE_NOT_FOUND,
		}, nil
	}

	return &pbstore.GetResponse{
		Code: pbstore.ResponseCode_RESPONSE_CODE_FOUND,
		Value: &anypb.Any{
			TypeUrl: s.typeUrl,
			Value:   foundValue,
		},
	}, nil
}

// GetAll retrieves multiple entries from Badger using goroutines for parallelism with time traversal support
func (s *Store) GetAll(request *pbstore.GetAllRequest) (*pbstore.GetAllResponse, error) {
	if len(request.Keys) == 0 {
		return &pbstore.GetAllResponse{
			Entries: []*pbstore.ResponseEntry{},
		}, nil
	}

	// Create entries slice with same length as keys to preserve order
	entries := make([]*pbstore.ResponseEntry, len(request.Keys))

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

	return &pbstore.GetAllResponse{
		Entries: entries,
	}, nil
}

// getAllWorker processes work items and stores results in the entries slice at correct index
func (s *Store) getAllWorker(workChan <-chan workItem, entries []*pbstore.ResponseEntry, mutex *sync.Mutex, keysFoundCount *int, blockNumber uint64) {
	for work := range workChan {
		// Use the Get method for each key
		resp, err := s.Get(&pbstore.GetRequest{
			Key:         work.key,
			BlockNumber: blockNumber,
		})

		// Create response entry
		var responseEntry *pbstore.ResponseEntry
		if err != nil {
			// Log error and create NOT_FOUND response
			s.logger.Error("failed to get key", zap.String("key", string(work.key)), zap.Error(err))
			responseEntry = &pbstore.ResponseEntry{
				Key: work.key,
				Response: &pbstore.GetResponse{
					Code: pbstore.ResponseCode_RESPONSE_CODE_NOT_FOUND,
				},
			}
		} else {
			// Use the response from Get method
			responseEntry = &pbstore.ResponseEntry{
				Key:      work.key,
				Response: resp,
			}

			// Count found keys
			if resp.Code == pbstore.ResponseCode_RESPONSE_CODE_FOUND {
				mutex.Lock()
				*keysFoundCount++
				mutex.Unlock()
			}
		}

		// Store result at correct index to preserve order
		mutex.Lock()
		entries[work.index] = responseEntry
		mutex.Unlock()
	}
}
