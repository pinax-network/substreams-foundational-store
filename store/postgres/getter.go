package postgres

import (
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"
	pbstore "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/v1"
	"google.golang.org/protobuf/types/known/anypb"
)

type Entry struct {
	BlockNumber uint64    `db:"block_number"`
	BlockHash   string    `db:"block_hash"`
	Key         []byte    `db:"key"`
	Value       []byte    `db:"value"`
	CreateTime  time.Time `db:"create_time"`
}

func (s *Store) Get(request *pbstore.GetRequest) (*pbstore.GetResponse, error) {
	entry := &Entry{}
	err := s.selectStatement.Get(entry, request.Key)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return &pbstore.GetResponse{
				Response: pbstore.ResponseCode_NOT_FOUND,
			}, nil
		}
		return nil, err
	}

	return &pbstore.GetResponse{
		Response: pbstore.ResponseCode_FOUND,
		Value: &anypb.Any{
			TypeUrl: s.typeUrl,
			Value:   entry.Value,
		},
	}, nil
}

func (s *Store) GetAll(request *pbstore.GetAllRequest) (*pbstore.GetAllResponse, error) {
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

	out := []*pbstore.ResponseEntry{}
	for _, key := range request.Keys {
		entry, found := entriesMap[hex.EncodeToString(key)]
		if !found {
			out =
				append(out, &pbstore.ResponseEntry{
					Key: key,
					Response: &pbstore.GetResponse{
						Response: pbstore.ResponseCode_NOT_FOUND,
						Value: &anypb.Any{
							TypeUrl: s.typeUrl,
							Value:   nil,
						},
					},
				})
			continue
		}
		out = append(out, &pbstore.ResponseEntry{
			Key: key,
			Response: &pbstore.GetResponse{
				Response: pbstore.ResponseCode_FOUND,
				Value: &anypb.Any{
					TypeUrl: s.typeUrl,
					Value:   entry.Value,
				},
			},
		})
	}
	return &pbstore.GetAllResponse{
		Entries: out,
	}, nil
}
