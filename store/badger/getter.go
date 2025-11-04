package badger

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/dgraph-io/badger/v3"
	pbmodel "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/model/v2"
	pbservice "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/service/v2"
	"google.golang.org/protobuf/types/known/anypb"
)

// Get retrieves entries for the provided keys from Badger
func (s *Store) Get(request *pbservice.GetRequest) (*pbservice.GetResponse, error) {
	entries := make([]*pbmodel.QueriedEntry, len(request.Keys))

	// Use a semaphore to limit concurrent goroutines to numWorkers
	sem := make(chan struct{}, s.numWorkers)
	var wg sync.WaitGroup
	errChan := make(chan error, 1) // Buffered to avoid blocking

	for i, key := range request.Keys {
		wg.Add(1)
		go func(index int, k *pbmodel.Key) {
			defer wg.Done()

			// Acquire semaphore
			sem <- struct{}{}
			defer func() { <-sem }()

			err := s.db.View(func(txn *badger.Txn) error {
				item, err := txn.Get(k.Bytes)
				if err != nil {
					if err == badger.ErrKeyNotFound {
						entries[index] = &pbmodel.QueriedEntry{Code: pbmodel.ResponseCode_RESPONSE_CODE_NOT_FOUND, Entry: &pbmodel.Entry{Key: k}}
						return nil
					}
					return err
				}

				var storedValue []byte
				if err := item.Value(func(val []byte) error {
					storedValue = append([]byte{}, val...)
					return nil
				}); err != nil {
					return err
				}

				if len(storedValue) < 8 {
					return fmt.Errorf("invalid stored value: expected at least 8 bytes for block number")
				}
				blockNumber := binary.BigEndian.Uint64(storedValue[:8])
				actualValue := storedValue[8:]
				if request.BlockNumber < blockNumber {
					entries[index] = &pbmodel.QueriedEntry{Code: pbmodel.ResponseCode_RESPONSE_CODE_NOT_FOUND, Entry: &pbmodel.Entry{Key: k}}
					return nil
				}
				entries[index] = &pbmodel.QueriedEntry{
					Code:  pbmodel.ResponseCode_RESPONSE_CODE_FOUND,
					Entry: &pbmodel.Entry{Key: k, Value: &anypb.Any{TypeUrl: s.typeUrl, Value: actualValue}},
				}
				return nil
			})
			if err != nil {
				select {
				case errChan <- err:
				default:
				}
			}
		}(i, key)
	}

	wg.Wait()

	select {
	case err := <-errChan:
		return nil, fmt.Errorf("failed to get values from Badger: %w", err)
	default:
	}

	return &pbservice.GetResponse{BlockReached: true, Entries: &pbmodel.QueriedEntries{Entries: entries}}, nil
}

// GetFirst for badger (non time-traversal) behaves like Get on exact keys
func (s *Store) GetFirst(request *pbservice.GetRequest) (*pbservice.GetResponse, error) {
	// For non time-traversal store, 'first' is simply the exact key lookup
	return s.Get(request)
}
