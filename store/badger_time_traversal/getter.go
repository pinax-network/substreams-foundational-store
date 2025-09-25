package badger_time_traversal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
	pbstore "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/v1"
	"github.com/streamingfast/substreams-foundational-store/sink"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/anypb"
)

// workItem represents a work item for parallel processing
type workItem struct {
	index int
	key   []byte
}

// extractBlockNumberFromKey extracts the block number from a composite key
// Returns the original key and block number
func extractBlockNumberFromKey(compositeKey []byte) ([]byte, uint64) {
	if len(compositeKey) < 8 {
		return compositeKey, 0
	}

	originalKeyLen := len(compositeKey) - 8
	originalKey := compositeKey[:originalKeyLen]
	blockNumber := binary.BigEndian.Uint64(compositeKey[originalKeyLen:])

	return originalKey, blockNumber
}

// Get retrieves a single entry from Badger with time traversal support
// It finds the entry with the highest block number that is <= the requested block number
func (s *Store) Get(request *pbstore.GetRequest) (*pbstore.GetResponse, error) {
	// Track total execution time
	executionStart := time.Now()
	defer sink.DatabaseExecutionDuration.ObserveDuration(time.Since(executionStart))

	// Track key requests - single key per Get call
	sink.DatabaseKeysRequestedTotal.Inc()
	sink.DatabaseCallCount.Inc()

	defer func() {
		lsm, vlog := s.db.Size()
		totalSize := uint64(lsm + vlog)
		if totalSize > 0 {
			sink.BadgerStoreSize.SetUint64(totalSize)
			sink.BadgerLSMSize.SetUint64(uint64(lsm))
			sink.BadgerVLogSize.SetUint64(uint64(vlog))
		}
		sink.DatabaseKeysProcessed.Inc()
	}()

	var foundValue []byte
	var found bool

	// Track Badger-specific operation time
	badgerStart := time.Now()
	err := s.db.View(func(txn *badger.Txn) error {
		// Create iterator options for efficient forward scanning
		badgerOptions := badger.DefaultIteratorOptions
		badgerOptions.PrefetchValues = true
		badgerOptions.PrefetchSize = 100

		// Define iteration bounds for efficient scanning
		// Start from the beginning of this key's versions (block 0)
		start := makeTimeTraversalKey(request.Key, 0)
		// End just after the requested key's last possible version (requested block + 1)
		exclusiveEnd := makeTimeTraversalKey(request.Key, request.BlockNumber+1)

		bit := txn.NewIterator(badgerOptions)
		defer bit.Close()

		var err error
		var bestBlockNumber uint64 = 0
		var bestFound bool

		// Scan forward through all versions of this key up to the requested block
		for bit.Seek(start); bit.Valid() && bytes.Compare(bit.Item().Key(), exclusiveEnd) == -1; bit.Next() {
			item := bit.Item()
			key := item.Key()

			// Extract original key and block number from composite key
			originalKey, blockNumber := extractBlockNumberFromKey(key)

			// Verify this is the correct key (should always be true given our bounds)
			if !bytes.Equal(originalKey, request.Key) {
				continue
			}

			// Check if this block number is valid for our request
			if blockNumber <= request.BlockNumber {
				// Keep track of the highest valid block number found
				if !bestFound || blockNumber > bestBlockNumber {
					bestBlockNumber = blockNumber
					bestFound = true

					getStart := time.Now()
					err = item.Value(func(val []byte) error {
						// Make a copy of the value as it's only valid within this transaction
						foundValue = append([]byte{}, val...)
						return nil
					})
					sink.BadgerGetOperationDuration.ObserveDuration(time.Since(getStart))
					sink.BadgerGetOperationCount.Inc()

					if err != nil {
						return err
					}
				}
			}
		}

		if bestFound {
			found = true
		}

		return nil
	})
	sink.BadgerTransactionDuration.ObserveDuration(time.Since(badgerStart))
	sink.BadgerTransactionCount.Inc()

	if err != nil {
		sink.DatabaseGetErrors.Inc()
		return nil, fmt.Errorf("failed to get value from Badger: %w", err)
	}

	if !found {
		// Track no keys found for this call
		sink.DatabaseGetMisses.Inc()
		return &pbstore.GetResponse{
			Response: pbstore.ResponseCode_RESPONSE_CODE_NOT_FOUND,
		}, nil
	}

	// Track found keys - single key found
	sink.DatabaseKeysFoundTotal.Inc()
	sink.DatabaseGetHits.Inc()

	return &pbstore.GetResponse{
		Response: pbstore.ResponseCode_RESPONSE_CODE_FOUND,
		Value: &anypb.Any{
			TypeUrl: s.typeUrl,
			Value:   foundValue,
		},
	}, nil
}

// GetAll retrieves multiple entries from Badger using goroutines for parallelism with time traversal support
func (s *Store) GetAll(request *pbstore.GetAllRequest) (*pbstore.GetAllResponse, error) {
	// Track total execution time
	executionStart := time.Now()
	defer sink.DatabaseExecutionDuration.ObserveDuration(time.Since(executionStart))

	// Track key requests
	sink.DatabaseKeysRequestedTotal.AddInt(len(request.Keys))
	sink.DatabaseCallCount.Inc()

	defer func() {
		lsm, vlog := s.db.Size()
		totalSize := uint64(lsm + vlog)
		if totalSize > 0 {
			sink.BadgerStoreSize.SetUint64(totalSize)
			sink.BadgerLSMSize.SetUint64(uint64(lsm))
			sink.BadgerVLogSize.SetUint64(uint64(vlog))
		}
		sink.DatabaseKeysProcessed.AddInt(len(request.Keys))
	}()

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

	// Track found keys
	sink.DatabaseKeysFoundTotal.AddInt(keysFoundCount)
	if keysFoundCount > 0 {
		sink.DatabaseGetHits.Inc()
	} else {
		sink.DatabaseGetMisses.Inc()
	}

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
					Response: pbstore.ResponseCode_RESPONSE_CODE_NOT_FOUND,
				},
			}
		} else {
			// Use the response from Get method
			responseEntry = &pbstore.ResponseEntry{
				Key:      work.key,
				Response: resp,
			}

			// Count found keys
			if resp.Response == pbstore.ResponseCode_RESPONSE_CODE_FOUND {
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
