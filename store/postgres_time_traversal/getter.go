package postgres_time_traversal

import (
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"
	pbstore "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/v1"
	"github.com/streamingfast/substreams-foundational-store/sink"
	"google.golang.org/protobuf/types/known/anypb"
)

type Entry struct {
	BlockNumber uint64    `db:"block_number"`
	Key         []byte    `db:"key"`
	Value       []byte    `db:"value"`
	CreateTime  time.Time `db:"create_time"`
}

type KeyOnlyEntry struct {
	BlockNumber uint64 `db:"block_number"`
	Key         []byte `db:"key"`
}

func (s *Store) Get(request *pbstore.GetRequest) (*pbstore.GetResponse, error) {
	// Track total execution time
	executionStart := time.Now()
	defer func() {
		sink.DatabaseExecutionDuration.ObserveDuration(time.Since(executionStart))
	}()

	// Track key requests - single key per Get call
	sink.DatabaseKeysRequestedTotal.Inc()
	sink.DatabaseCallCount.Inc()
	defer func() {
		sink.DatabaseKeysProcessed.Inc()
	}()

	entry := &Entry{}
	// Use time traversal query: find highest block <= requested block
	err := s.selectStatement.Get(entry, request.Key, request.BlockNumber)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// Track no keys found for this call
			sink.DatabaseGetMisses.Inc()
			return &pbstore.GetResponse{
				Response: pbstore.ResponseCode_RESPONSE_CODE_NOT_FOUND,
			}, nil
		}
		sink.DatabaseGetErrors.Inc()
		return nil, err
	}

	// Track found keys - single key found
	sink.DatabaseKeysFoundTotal.Inc()
	sink.DatabaseGetHits.Inc()

	return &pbstore.GetResponse{
		Response: pbstore.ResponseCode_RESPONSE_CODE_FOUND,
		Value: &anypb.Any{
			TypeUrl: s.typeUrl,
			Value:   entry.Value,
		},
	}, nil
}

func (s *Store) GetAll(request *pbstore.GetAllRequest) (*pbstore.GetAllResponse, error) {
	// Track total execution time
	executionStart := time.Now()
	defer func() {
		sink.DatabaseExecutionDuration.ObserveDuration(time.Since(executionStart))
	}()

	// Track key requests - number of keys in this GetAll call
	sink.DatabaseKeysRequestedTotal.AddInt(len(request.Keys))
	sink.DatabaseCallCount.Inc()
	defer func() {
		sink.DatabaseKeysProcessed.AddInt(len(request.Keys))
	}()

	// Use time traversal query for multiple keys
	rows, err := s.selectAnyStatement.Queryx(pq.Array(request.Keys), request.BlockNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to select entries: %w", err)
	}

	var entriesMap = make(map[string]*Entry)
	for rows.Next() {
		entry := &Entry{}
		err := rows.StructScan(entry)
		if err != nil {
			return nil, fmt.Errorf("failed to scan entry: %w", err)
		}
		entriesMap[hex.EncodeToString(entry.Key)] = entry
	}

	var keysFoundCount int
	out := []*pbstore.ResponseEntry{}
	for _, key := range request.Keys {
		entry, found := entriesMap[hex.EncodeToString(key)]
		if !found {
			out = append(out, &pbstore.ResponseEntry{
				Key: key,
				Response: &pbstore.GetResponse{
					Response: pbstore.ResponseCode_RESPONSE_CODE_NOT_FOUND,
					Value: &anypb.Any{
						TypeUrl: s.typeUrl,
						Value:   nil,
					},
				},
			})
			continue
		}
		keysFoundCount++
		out = append(out, &pbstore.ResponseEntry{
			Key: key,
			Response: &pbstore.GetResponse{
				Response: pbstore.ResponseCode_RESPONSE_CODE_FOUND,
				Value: &anypb.Any{
					TypeUrl: s.typeUrl,
					Value:   entry.Value,
				},
			},
		})
	}

	// Track found keys - number of keys found in this GetAll call
	sink.DatabaseKeysFoundTotal.AddInt(keysFoundCount)

	return &pbstore.GetAllResponse{
		Entries: out,
	}, nil
}

// GetKeyOnly retrieves only the key (no value) for time traversal
// This is useful when you only need to check if a key exists at a given block
func (s *Store) GetKeyOnly(key []byte, blockNumber uint64) (*KeyOnlyEntry, error) {
	// Track total execution time
	executionStart := time.Now()
	defer func() {
		sink.DatabaseExecutionDuration.ObserveDuration(time.Since(executionStart))
	}()

	// Track key requests
	sink.DatabaseKeysRequestedTotal.Inc()
	sink.DatabaseCallCount.Inc()
	defer func() {
		sink.DatabaseKeysProcessed.Inc()
	}()

	entry := &KeyOnlyEntry{}
	err := s.selectKeyOnlyStatement.Get(entry, key, blockNumber)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			sink.DatabaseGetMisses.Inc()
			return nil, nil // Key not found
		}
		sink.DatabaseGetErrors.Inc()
		return nil, err
	}

	sink.DatabaseKeysFoundTotal.Inc()
	sink.DatabaseGetHits.Inc()

	return entry, nil
}

// GetAllKeysOnly retrieves only keys (no values) for multiple keys with time traversal
func (s *Store) GetAllKeysOnly(keys [][]byte, blockNumber uint64) ([]*KeyOnlyEntry, error) {
	// Track total execution time
	executionStart := time.Now()
	defer func() {
		sink.DatabaseExecutionDuration.ObserveDuration(time.Since(executionStart))
	}()

	// Track key requests
	sink.DatabaseKeysRequestedTotal.AddInt(len(keys))
	sink.DatabaseCallCount.Inc()
	defer func() {
		sink.DatabaseKeysProcessed.AddInt(len(keys))
	}()

	rows, err := s.selectAllKeyOnlyStatement.Queryx(pq.Array(keys), blockNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to select key-only entries: %w", err)
	}

	var entries []*KeyOnlyEntry
	var keysFoundCount int
	for rows.Next() {
		entry := &KeyOnlyEntry{}
		err := rows.StructScan(entry)
		if err != nil {
			return nil, fmt.Errorf("failed to scan key-only entry: %w", err)
		}
		entries = append(entries, entry)
		keysFoundCount++
	}

	// Track found keys
	sink.DatabaseKeysFoundTotal.AddInt(keysFoundCount)

	return entries, nil
}
