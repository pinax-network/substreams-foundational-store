package badger

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/dgraph-io/badger/v3"
	pbmodel "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/model/v2"
	pbservice "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/service/v2"
	"google.golang.org/protobuf/types/known/anypb"
)

// Get retrieves a single entry from Badger
func (s *Store) Get(request *pbservice.GetRequest) (*pbservice.GetResponse, error) {
	var storedValue []byte
	var found bool

	err := s.db.View(func(txn *badger.Txn) error {
		// Directly look up the key
		item, err := txn.Get(request.Key.Bytes)
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

	resp := &pbservice.GetResponse{BlockReached: true}
	if !found {
		resp.Entry = &pbmodel.QueriedEntry{Code: pbmodel.ResponseCode_RESPONSE_CODE_NOT_FOUND, Entry: &pbmodel.Entry{Key: request.Key}}
		return resp, nil
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
		resp.Entry = &pbmodel.QueriedEntry{Code: pbmodel.ResponseCode_RESPONSE_CODE_NOT_FOUND, Entry: &pbmodel.Entry{Key: request.Key}}
		return resp, nil
	}

	// Note: Block hash validation is not supported in this implementation
	// as the setter does not store block hash information

	resp.Entry = &pbmodel.QueriedEntry{
		Code: pbmodel.ResponseCode_RESPONSE_CODE_FOUND,
		Entry: &pbmodel.Entry{
			Key:   request.Key,
			Value: &anypb.Any{TypeUrl: s.typeUrl, Value: actualValue},
		},
	}
	return resp, nil
}

// GetAll retrieves multiple entries from Badger using goroutines for parallelism
func (s *Store) GetAll(request *pbservice.GetAllRequest) (*pbservice.GetAllResponse, error) {
	// Create a slice to store the entries in order
	entries := make([]*pbmodel.QueriedEntry, 0, len(request.Keys))

	// Create a transaction
	err := s.db.View(func(txn *badger.Txn) error {
		// Use a channel to distribute keys to workers
		keyChan := make(chan *pbmodel.Key, len(request.Keys))
		seenKeys := make(map[string]bool)
		for _, key := range request.Keys {
			if seenKeys[string(key.Bytes)] {
				continue
			}
			sKey := hex.EncodeToString(key.Bytes)
			keyChan <- key
			seenKeys[sKey] = true
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
					item, err := txn.Get(key.Bytes)
					if err != nil {
						if err == badger.ErrKeyNotFound {
							// Key not found, add a NOT_FOUND response
							mutex.Lock()
							entries = append(entries,
								&pbmodel.QueriedEntry{Code: pbmodel.ResponseCode_RESPONSE_CODE_NOT_FOUND, Entry: &pbmodel.Entry{Key: key}})
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
						entries = append(entries, &pbmodel.QueriedEntry{Code: pbmodel.ResponseCode_RESPONSE_CODE_NOT_FOUND, Entry: &pbmodel.Entry{Key: key}})
						mutex.Unlock()
						continue
					}

					// Note: Block hash validation is not supported in this implementation
					// as the setter does not store block hash information

					mutex.Lock()
					entries = append(entries,
						&pbmodel.QueriedEntry{Code: pbmodel.ResponseCode_RESPONSE_CODE_FOUND, Entry: &pbmodel.Entry{Key: key, Value: &anypb.Any{TypeUrl: s.typeUrl, Value: actualValue}}},
					)
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

	return &pbservice.GetAllResponse{
		BlockReached: true,
		Entries:      &pbmodel.QueriedEntries{Entries: entries},
	}, nil
}

// GetFirst returns the first entry with key >= requested key (lexicographic order)
func (s *Store) GetFirst(request *pbservice.GetFirstRequest) (*pbservice.GetResponse, error) {
	return s.Get(&pbservice.GetRequest{
		Key:         request.Key,
		BlockNumber: request.BlockNumber,
	})
}

// GetAllFirst returns, for each requested key, the first entry with key >= that key
func (s *Store) GetAllFirst(request *pbservice.GetAllRequest) (*pbservice.GetAllResponse, error) {
	entries := make([]*pbmodel.QueriedEntry, 0, len(request.Keys))
	for _, key := range request.Keys {
		resp, err := s.GetFirst(&pbservice.GetFirstRequest{Key: key, BlockNumber: request.BlockNumber, BlockHash: request.BlockHash})
		if err != nil {
			return nil, err
		}
		entries = append(entries, resp.Entry)
	}
	return &pbservice.GetAllResponse{BlockReached: true, Entries: &pbmodel.QueriedEntries{Entries: entries}}, nil
}
