package postgres

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
	BlockHash   []byte    `db:"block_hash"`
	Key         []byte    `db:"key"`
	Value       []byte    `db:"value"`
	CreateTime  time.Time `db:"create_time"`
}

func (s *Store) Get(request *pbservice.GetRequest) (*pbservice.GetResponse, error) {
	rows, err := s.selectAnyStatement.Queryx(pq.Array(request.Keys))
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

	entries := make([]*pbmodel.QueriedEntry, len(request.Keys))
	for i, key := range request.Keys {
		entry, found := entriesMap[hex.EncodeToString(key.Bytes)]
		if !found {
			entries[i] = &pbmodel.QueriedEntry{Code: pbmodel.ResponseCode_RESPONSE_CODE_NOT_FOUND, Entry: &pbmodel.Entry{Key: key}}
			continue
		}
		entries[i] = &pbmodel.QueriedEntry{
			Code:  pbmodel.ResponseCode_RESPONSE_CODE_FOUND,
			Entry: &pbmodel.Entry{Key: key, Value: &anypb.Any{TypeUrl: s.typeUrl, Value: entry.Value}},
		}
	}

	return &pbservice.GetResponse{BlockReached: true, Entries: &pbmodel.QueriedEntries{Entries: entries}}, nil
}

// GetFirst retrieves, for each requested key, the first entry with key >= requested key in lexicographic order
func (s *Store) GetFirst(request *pbservice.GetRequest) (*pbservice.GetResponse, error) {
	entries := make([]*pbmodel.QueriedEntry, len(request.Keys))
	for i, key := range request.Keys {
		entry := &Entry{}
		err := s.selectFirstStmt.Get(entry, key)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				entries[i] = &pbmodel.QueriedEntry{Code: pbmodel.ResponseCode_RESPONSE_CODE_NOT_FOUND, Entry: &pbmodel.Entry{Key: key}}
				continue
			}
			return nil, err
		}
		entries[i] = &pbmodel.QueriedEntry{Code: pbmodel.ResponseCode_RESPONSE_CODE_FOUND, Entry: &pbmodel.Entry{Key: key, Value: &anypb.Any{TypeUrl: s.typeUrl, Value: entry.Value}}}
	}
	return &pbservice.GetResponse{BlockReached: true, Entries: &pbmodel.QueriedEntries{Entries: entries}}, nil
}
