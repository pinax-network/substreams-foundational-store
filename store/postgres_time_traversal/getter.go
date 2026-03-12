package postgres_time_traversal

import (
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"
	pbmodel "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/model/v2"
	pbservice "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/service/v2"
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

func (s *Store) Get(request *pbservice.GetRequest) (*pbservice.GetResponse, error) {
	// Use time traversal query for multiple keys
	keys := make([][]byte, len(request.Keys))
	for i, k := range request.Keys {
		keys[i] = k.Bytes
	}
	rows, err := s.selectAnyStatement.Queryx(pq.Array(keys), request.BlockNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to select entries: %w", err)
	}

	var entriesMap = make(map[string]*Entry)
	for rows.Next() {
		entry := &Entry{}
		if err := rows.StructScan(entry); err != nil {
			return nil, fmt.Errorf("failed to scan entry: %w", err)
		}
		entriesMap[hex.EncodeToString(entry.Key)] = entry
	}

	out := make([]*pbmodel.QueriedEntry, len(request.Keys))
	for i, key := range request.Keys {
		entry, found := entriesMap[hex.EncodeToString(key.Bytes)]
		if !found {
			out[i] = &pbmodel.QueriedEntry{Code: pbmodel.ResponseCode_RESPONSE_CODE_NOT_FOUND, Entry: &pbmodel.Entry{Key: key}}
			continue
		}
		out[i] = &pbmodel.QueriedEntry{
			Code: pbmodel.ResponseCode_RESPONSE_CODE_FOUND,
			Entry: &pbmodel.Entry{
				Key:   key,
				Value: &anypb.Any{TypeUrl: s.typeUrl, Value: entry.Value},
			},
		}
	}

	return &pbservice.GetResponse{BlockReached: true, Entries: &pbmodel.QueriedEntries{Entries: out}}, nil
}

// GetFirst retrieves, for each requested key, the first entry (by key >= requested) with its latest value
func (s *Store) GetFirst(request *pbservice.GetRequest) (*pbservice.GetResponse, error) {
	out := make([]*pbmodel.QueriedEntry, len(request.Keys))
	for i, key := range request.Keys {
		entry := &Entry{}
		err := s.selectFirstStmt.Get(entry, key.Bytes)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				out[i] = &pbmodel.QueriedEntry{Code: pbmodel.ResponseCode_RESPONSE_CODE_NOT_FOUND, Entry: &pbmodel.Entry{Key: key}}
				continue
			}
			return nil, err
		}
		out[i] = &pbmodel.QueriedEntry{
			Code:  pbmodel.ResponseCode_RESPONSE_CODE_FOUND,
			Entry: &pbmodel.Entry{Key: key, Value: &anypb.Any{TypeUrl: s.typeUrl, Value: entry.Value}},
		}
	}
	return &pbservice.GetResponse{BlockReached: true, Entries: &pbmodel.QueriedEntries{Entries: out}}, nil
}

// GetKeyOnly retrieves only the key (no value) for time traversal
// This is useful when you only need to check if a key exists at a given block
func (s *Store) GetKeyOnly(key []byte, blockNumber uint64) (*KeyOnlyEntry, error) {
	entry := &KeyOnlyEntry{}
	err := s.selectKeyOnlyStatement.Get(entry, key, blockNumber)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil // Key not found
		}
		return nil, err
	}

	return entry, nil
}

// GetAllKeysOnly retrieves only keys (no values) for multiple keys with time traversal
func (s *Store) GetAllKeysOnly(keys [][]byte, blockNumber uint64) ([]*KeyOnlyEntry, error) {
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

	return entries, nil
}
