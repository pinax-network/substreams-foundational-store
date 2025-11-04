package badger_time_traversal

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/dgraph-io/badger/v3"
	pbmodel "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/model/v2"
	pbservice "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/service/v2"
	"google.golang.org/protobuf/types/known/anypb"
)

// Get retrieves entries from Badger with time traversal support
// For each key, it finds the entry with the highest block number that is <= the requested block number
func (s *Store) Get(request *pbservice.GetRequest) (*pbservice.GetResponse, error) {
	entries := make([]*pbmodel.QueriedEntry, len(request.Keys))

	// Use a semaphore to limit concurrent goroutines to numWorkers
	sem := make(chan struct{}, s.numWorkers)
	var wg sync.WaitGroup
	errChan := make(chan error, 1) // Buffered to avoid blocking

	for i, k := range request.Keys {
		wg.Add(1)
		go func(index int, key *pbmodel.Key) {
			defer wg.Done()

			// Acquire semaphore
			sem <- struct{}{}
			defer func() { <-sem }()

			err := s.db.View(func(txn *badger.Txn) error {
				badgerOptions := badger.DefaultIteratorOptions
				badgerOptions.PrefetchValues = false
				badgerOptions.PrefetchSize = 100
				it := txn.NewIterator(badgerOptions)
				defer it.Close()

				start := makeTimeTraversalKey(key.Bytes, request.BlockNumber)
				exclusiveEnd := append(makeTimeTraversalKey(key.Bytes, 0), 0)

				found := false
				var foundValue []byte
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

				if !found {
					entries[index] = &pbmodel.QueriedEntry{Code: pbmodel.ResponseCode_RESPONSE_CODE_NOT_FOUND, Entry: &pbmodel.Entry{Key: key}}
					return nil
				}
				entries[index] = &pbmodel.QueriedEntry{
					Code:  pbmodel.ResponseCode_RESPONSE_CODE_FOUND,
					Entry: &pbmodel.Entry{Key: key, Value: &anypb.Any{TypeUrl: s.typeUrl, Value: foundValue}},
				}
				return nil
			})
			if err != nil {
				select {
				case errChan <- err:
				default:
				}
			}
		}(i, k)
	}

	wg.Wait()

	select {
	case err := <-errChan:
		return nil, fmt.Errorf("getting values from Badger: %w", err)
	default:
	}

	return &pbservice.GetResponse{BlockReached: true, Entries: &pbmodel.QueriedEntries{Entries: entries}}, nil
}

// GetFirst returns, for each requested key, the oldest value for that key (lowest block number)
func (s *Store) GetFirst(request *pbservice.GetRequest) (*pbservice.GetResponse, error) {
	entries := make([]*pbmodel.QueriedEntry, len(request.Keys))

	// Use a semaphore to limit concurrent goroutines to numWorkers
	sem := make(chan struct{}, s.numWorkers)
	var wg sync.WaitGroup
	errChan := make(chan error, 1) // Buffered to avoid blocking

	for i, k := range request.Keys {
		wg.Add(1)
		go func(index int, key *pbmodel.Key) {
			defer wg.Done()

			// Acquire semaphore
			sem <- struct{}{}
			defer func() { <-sem }()

			err := s.db.View(func(txn *badger.Txn) error {
				opts := badger.DefaultIteratorOptions
				opts.Reverse = true
				opts.PrefetchValues = false
				rev := txn.NewIterator(opts)
				defer rev.Close()

				base := key.Bytes
				begin := make([]byte, len(base)+8)
				copy(begin, base)
				end := make([]byte, len(base)+8+1)
				copy(end, base)
				for i := len(base); i < len(base)+8; i++ {
					end[i] = 0xFF
				}
				end[len(base)+8] = 0x00 // exclusive upper bound

				rev.Seek(end)
				if rev.Valid() {
					it2 := rev.Item()
					k2 := it2.Key()
					if bytes.Compare(k2, begin) >= 0 && bytes.Compare(k2, end) == -1 {
						v, e := it2.ValueCopy(nil)
						if e != nil {
							return e
						}
						entries[index] = &pbmodel.QueriedEntry{Code: pbmodel.ResponseCode_RESPONSE_CODE_FOUND, Entry: &pbmodel.Entry{Key: key, Value: &anypb.Any{TypeUrl: s.typeUrl, Value: v}}}
						return nil
					}
				}
				entries[index] = &pbmodel.QueriedEntry{Code: pbmodel.ResponseCode_RESPONSE_CODE_NOT_FOUND, Entry: &pbmodel.Entry{Key: key}}
				return nil
			})
			if err != nil {
				select {
				case errChan <- err:
				default:
				}
			}
		}(i, k)
	}

	wg.Wait()

	select {
	case err := <-errChan:
		return nil, fmt.Errorf("badger_time_traversal GetFirst: %w", err)
	default:
	}

	return &pbservice.GetResponse{BlockReached: true, Entries: &pbmodel.QueriedEntries{Entries: entries}}, nil
}
