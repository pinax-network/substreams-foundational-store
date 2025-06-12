package badger

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/dgraph-io/badger/v3"
	"github.com/streamingfast/substreams-foundationnal-store/pb/store"
	"google.golang.org/protobuf/types/known/anypb"
)

// Get retrieves a single entry from Badger
func (s *Store) Get(request *store.GetRequest) (*store.GetResponse, error) {
	var storedValue []byte
	var found bool

	err := s.db.View(func(txn *badger.Txn) error {
		// Directly look up the key
		item, err := txn.Get(request.Key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				// Key not found, return nil error to indicate not found
				return nil
			}
			return err
		}

		// Key found
		found = true

		// Get the value
		err = item.Value(func(val []byte) error {
			// Make a copy of the value as it's only valid within this transaction
			storedValue = append([]byte{}, val...)
			return nil
		})
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to get value from Badger: %w", err)
	}

	if !found {
		return &store.GetResponse{
			Response: store.ResponseCode_NOT_FOUND,
		}, nil
	}

	// Extract the block number and the actual value
	// Ensure we have at least 8 bytes for the block number
	if len(storedValue) < 8 {
		return nil, fmt.Errorf("invalid stored value: expected at least 8 bytes for block number")
	}

	// Extract the block number from the first 8 bytes
	// This is extracted for potential logging or debugging purposes
	// but not included in the response as the GetResponse struct doesn't have a block_number field
	blockNumber := binary.BigEndian.Uint64(storedValue[:8])

	// The actual value is everything after the first 8 bytes
	actualValue := storedValue[8:]

	if request.BlockNumber < blockNumber {
		return &store.GetResponse{
			Response: store.ResponseCode_NOT_FOUND,
		}, nil
	}

	return &store.GetResponse{
		Response: store.ResponseCode_FOUND,
		Value: &anypb.Any{
			TypeUrl: s.typeUrl,
			Value:   actualValue,
		},
	}, nil
}

// GetAll retrieves multiple entries from Badger using goroutines for parallelism
func (s *Store) GetAll(request *store.GetAllRequest) (*store.GetAllResponse, error) {
	// Create a slice to store the entries
	var entries []*store.ResponseEntry

	// Create a transaction
	err := s.db.View(func(txn *badger.Txn) error {
		// Use a channel to distribute keys to workers
		keyChan := make(chan []byte, len(request.Keys))
		for _, key := range request.Keys {
			keyChan <- key
		}
		close(keyChan)

		// Use a channel to collect errors
		errChan := make(chan error, len(request.Keys))

		// Use a mutex to protect the entries slice
		var mutex sync.Mutex

		// Use the configured number of workers
		numWorkers := s.numWorkers
		if len(request.Keys) < numWorkers {
			numWorkers = len(request.Keys)
		}

		// Use a wait group to wait for all workers to finish
		var wg sync.WaitGroup
		wg.Add(numWorkers)

		// Start workers
		for i := 0; i < numWorkers; i++ {
			go func() {
				defer wg.Done()

				for key := range keyChan {
					var value []byte

					// Directly look up the key
					item, err := txn.Get(key)
					if err != nil {
						if err == badger.ErrKeyNotFound {
							// Key not found, add a NOT_FOUND response
							mutex.Lock()
							entries = append(entries,
								&store.ResponseEntry{
									Key: key,
									Response: &store.GetResponse{
										Response: store.ResponseCode_NOT_FOUND,
									},
								})
							mutex.Unlock()
							continue
						}
						errChan <- err
						return
					}

					// Get the value
					err = item.Value(func(val []byte) error {
						// Make a copy of the value as it's only valid within this transaction
						value = append([]byte{}, val...)
						return nil
					})
					if err != nil {
						errChan <- err
						return
					}

					// Extract the block number and the actual value
					// Ensure we have at least 8 bytes for the block number
					if len(value) < 8 {
						errChan <- fmt.Errorf("invalid stored value: expected at least 8 bytes for block number")
						return
					}

					// Extract the block number from the first 8 bytes
					// This is extracted for potential logging or debugging purposes
					// but not included in the response as the GetResponse struct doesn't have a block_number field
					blockNumber := binary.BigEndian.Uint64(value[:8])
					if request.BlockNumber < blockNumber {
						mutex.Lock()
						entries = append(entries, &store.ResponseEntry{
							Key: key,
							Response: &store.GetResponse{
								Response: store.ResponseCode_NOT_FOUND,
							},
						})
						mutex.Unlock()
						continue
					}

					// The actual value is everything after the first 8 bytes
					actualValue := value[8:]

					mutex.Lock()
					entries = append(entries,
						&store.ResponseEntry{
							Key: key,
							Response: &store.GetResponse{
								Response: store.ResponseCode_FOUND,
								Value: &anypb.Any{
									TypeUrl: s.typeUrl,
									Value:   actualValue,
								},
							},
						})
					mutex.Unlock()
				}
			}()
		}

		// Wait for all workers to finish
		wg.Wait()

		// Check for errors
		select {
		case err := <-errChan:
			return err
		default:
			return nil
		}
	})

	if err != nil {
		return nil, fmt.Errorf("failed to get values from Badger: %w", err)
	}

	// Ensure we only have one entry per key
	uniqueEntries := make(map[string]*store.ResponseEntry)
	for _, entry := range entries {
		key := string(entry.Key)
		uniqueEntries[key] = entry
	}

	// Convert the map back to a slice
	finalEntries := make([]*store.ResponseEntry, 0, len(uniqueEntries))
	for _, entry := range uniqueEntries {
		finalEntries = append(finalEntries, entry)
	}

	return &store.GetAllResponse{
		Entries: finalEntries,
	}, nil
}
