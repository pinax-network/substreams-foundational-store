package badger

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/dgraph-io/badger/v3"
	pbstore "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/v1"
	"github.com/streamingfast/substreams-foundational-store/sink"
	"google.golang.org/protobuf/types/known/anypb"
)

// Get retrieves a single entry from Badger
func (s *Store) Get(request *pbstore.GetRequest) (*pbstore.GetResponse, error) {
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
		sink.DatabaseGetErrors.Inc()
		return nil, fmt.Errorf("failed to get value from Badger: %w", err)
	}

	if !found {
		sink.DatabaseGetMisses.Inc()
		return &pbstore.GetResponse{
			Response: pbstore.ResponseCode_NOT_FOUND,
		}, nil
	}

	sink.DatabaseGetHits.Inc()

	// Extract the block number, block hash, and the actual value
	// Ensure we have at least 8 bytes for the block number + block hash length
	requestBlockHashLen := len(request.BlockHash)
	minLength := 8 + requestBlockHashLen
	if len(storedValue) < minLength {
		return nil, fmt.Errorf("invalid stored value: expected at least %d bytes for block number and hash", minLength)
	}

	// Extract the block number from the first 8 bytes
	blockNumber := binary.BigEndian.Uint64(storedValue[:8])
	
	// Extract the stored block hash
	storedBlockHash := storedValue[8 : 8+requestBlockHashLen]
	
	// The actual value is everything after the block number and hash
	actualValue := storedValue[8+requestBlockHashLen:]

	if request.BlockNumber < blockNumber {
		return &pbstore.GetResponse{
			Response: pbstore.ResponseCode_NOT_FOUND_BLOCK_NOT_REACHED,
		}, nil
	}

	// Validate block hash matches
	if len(request.BlockHash) > 0 && len(storedBlockHash) > 0 {
		if string(request.BlockHash) != string(storedBlockHash) {
			return &pbstore.GetResponse{
				Response: pbstore.ResponseCode_NOT_FOUND,
			}, nil
		}
	}

	return &pbstore.GetResponse{
		Response: pbstore.ResponseCode_FOUND,
		Value: &anypb.Any{
			TypeUrl: s.typeUrl,
			Value:   actualValue,
		},
	}, nil
}

// GetAll retrieves multiple entries from Badger using goroutines for parallelism
func (s *Store) GetAll(request *pbstore.GetAllRequest) (*pbstore.GetAllResponse, error) {
	defer func() {
		sink.DatabaseKeysProcessed.AddInt(len(request.Keys))
	}()

	// Create a slice to foundational-store the entries
	var entries []*pbstore.ResponseEntry

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
								&pbstore.ResponseEntry{
									Key: key,
									Response: &pbstore.GetResponse{
										Response: pbstore.ResponseCode_NOT_FOUND,
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

					// Extract the block number, block hash, and the actual value
					// Ensure we have at least 8 bytes for the block number + block hash length
					requestBlockHashLen := len(request.BlockHash)
					minLength := 8 + requestBlockHashLen
					if len(value) < minLength {
						errChan <- fmt.Errorf("invalid stored value: expected at least %d bytes for block number and hash", minLength)
						return
					}

					// Extract the block number from the first 8 bytes
					blockNumber := binary.BigEndian.Uint64(value[:8])
					
					// Extract the stored block hash
					storedBlockHash := value[8 : 8+requestBlockHashLen]
					
					// The actual value is everything after the block number and hash
					actualValue := value[8+requestBlockHashLen:]

					if request.BlockNumber < blockNumber {
						mutex.Lock()
						entries = append(entries, &pbstore.ResponseEntry{
							Key: key,
							Response: &pbstore.GetResponse{
								Response: pbstore.ResponseCode_NOT_FOUND_BLOCK_NOT_REACHED,
							},
						})
						mutex.Unlock()
						continue
					}

					// Validate block hash matches
					if len(request.BlockHash) > 0 && len(storedBlockHash) > 0 {
						if string(request.BlockHash) != string(storedBlockHash) {
							mutex.Lock()
							entries = append(entries, &pbstore.ResponseEntry{
								Key: key,
								Response: &pbstore.GetResponse{
									Response: pbstore.ResponseCode_NOT_FOUND,
								},
							})
							mutex.Unlock()
							continue
						}
					}

					mutex.Lock()
					entries = append(entries,
						&pbstore.ResponseEntry{
							Key: key,
							Response: &pbstore.GetResponse{
								Response: pbstore.ResponseCode_FOUND,
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
	uniqueEntries := make(map[string]*pbstore.ResponseEntry)
	for _, entry := range entries {
		key := string(entry.Key)
		uniqueEntries[key] = entry
	}

	// Convert the map back to a slice
	finalEntries := make([]*pbstore.ResponseEntry, 0, len(uniqueEntries))
	for _, entry := range uniqueEntries {
		finalEntries = append(finalEntries, entry)
	}

	return &pbstore.GetAllResponse{
		Entries: finalEntries,
	}, nil
}
