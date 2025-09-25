package badger

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/dgraph-io/badger/v3"
	pbstore "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/v1"
	"google.golang.org/protobuf/types/known/anypb"
)

// Get retrieves a single entry from Badger
func (s *Store) Get(request *pbstore.GetRequest) (*pbstore.GetResponse, error) {
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
		return &pbstore.GetResponse{
			Response: pbstore.ResponseCode_RESPONSE_CODE_NOT_FOUND,
		}, nil
	}

	// Extract the block number and the actual value
	// Ensure we have at least 8 bytes for the block number
	if len(storedValue) < 8 {
		return nil, fmt.Errorf("invalid stored value: expected at least 8 bytes for block number")
	}

	// Extract the block number from the first 8 bytes
	blockNumber := binary.BigEndian.Uint64(storedValue[:8])

	// The actual value is everything after the block number
	actualValue := storedValue[8:]

	if request.BlockNumber < blockNumber {
		return &pbstore.GetResponse{
			Response: pbstore.ResponseCode_RESPONSE_CODE_NOT_FOUND,
		}, nil
	}

	// Note: Block hash validation is not supported in this implementation
	// as the setter does not store block hash information

	return &pbstore.GetResponse{
		Response: pbstore.ResponseCode_RESPONSE_CODE_FOUND,
		Value: &anypb.Any{
			TypeUrl: s.typeUrl,
			Value:   actualValue,
		},
	}, nil
}

// GetAll retrieves multiple entries from Badger using goroutines for parallelism
func (s *Store) GetAll(request *pbstore.GetAllRequest) (*pbstore.GetAllResponse, error) {
	// Create a slice to store the entries
	var entries []*pbstore.ResponseEntry
	var keysFoundCount int

	// Create a transaction
	err := s.db.View(func(txn *badger.Txn) error {
		// Use a channel to distribute keys to workers
		keyChan := make(chan []byte, len(request.Keys))
		seenKeys := make(map[string]bool)
		for _, key := range request.Keys {
			if seenKeys[string(key)] {
				continue
			}
			sKey := hex.EncodeToString(key)
			keyChan <- key
			seenKeys[sKey] = true
		}
		close(keyChan)

		// Use a channel to collect errors
		errChan := make(chan error, len(request.Keys))

		// Use a mutex to protect the entries slice and key count
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
								&pbstore.ResponseEntry{
									Key: key,
									Response: &pbstore.GetResponse{
										Response: pbstore.ResponseCode_RESPONSE_CODE_NOT_FOUND,
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
					blockNumber := binary.BigEndian.Uint64(value[:8])

					// The actual value is everything after the block number
					actualValue := value[8:]

					if request.BlockNumber < blockNumber {
						mutex.Lock()
						entries = append(entries, &pbstore.ResponseEntry{
							Key: key,
							Response: &pbstore.GetResponse{
								Response: pbstore.ResponseCode_RESPONSE_CODE_NOT_FOUND,
							},
						})
						mutex.Unlock()
						continue
					}

					// Note: Block hash validation is not supported in this implementation
					// as the setter does not store block hash information

					mutex.Lock()
					keysFoundCount++
					entries = append(entries,
						&pbstore.ResponseEntry{
							Key: key,
							Response: &pbstore.GetResponse{
								Response: pbstore.ResponseCode_RESPONSE_CODE_FOUND,
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

	return &pbstore.GetAllResponse{
		Entries: entries,
	}, nil
}
