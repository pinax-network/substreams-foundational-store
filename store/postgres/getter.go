package postgres

import (
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"
	"github.com/streamingfast/substreams-foundationnal-store/pb/store"
	"google.golang.org/protobuf/types/known/anypb"
)

type Entry struct {
	BlockNumber uint64    `db:"block_number"`
	BlockHash   string    `db:"block_hash"`
	Key         []byte    `db:"key"`
	Value       []byte    `db:"value"`
	CreateTime  time.Time `db:"create_time"`
}

func (s *Store) Get(request *store.GetRequest) (*store.GetResponse, error) {
	entry := &Entry{}
	err := s.selectStatement.Get(entry, request.Key)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return &store.GetResponse{
				Response: store.ResponseCode_NOT_FOUND,
			}, nil
		}
		return nil, err
	}

	return &store.GetResponse{
		Response: store.ResponseCode_FOUND,
		Value: &anypb.Any{
			TypeUrl: s.typeUrl,
			Value:   entry.Value,
		},
	}, nil
}

func (s *Store) GetAll(request *store.GetAllRequest) (*store.GetAllResponse, error) {
	rows, err := s.selectAnyStatement.Queryx(pq.Array(request.Keys))
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

	out := []*store.ResponseEntry{}
	for _, key := range request.Keys {
		entry, found := entriesMap[hex.EncodeToString(key)]
		if !found {
			out =
				append(out, &store.ResponseEntry{
					Key: key,
					Response: &store.GetResponse{
						Response: store.ResponseCode_NOT_FOUND,
						Value: &anypb.Any{
							TypeUrl: s.typeUrl,
							Value:   nil,
						},
					},
				})
			continue
		}
		out = append(out, &store.ResponseEntry{
			Key: key,
			Response: &store.GetResponse{
				Response: store.ResponseCode_FOUND,
				Value: &anypb.Any{
					TypeUrl: s.typeUrl,
					Value:   entry.Value,
				},
			},
		})
	}
	return &store.GetAllResponse{
		Entries: out,
	}, nil
}
