package store

import (
	pbmodel "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/model/v2"
	pbservice "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/service/v2"
)

type Store interface {
	Set(entry *pbmodel.Entry, blockNumber uint64) error
	SetAll(entries []*pbmodel.Entry, blockNumber uint64) error
	Get(request *pbservice.GetRequest) (*pbservice.GetResponse, error)
	GetAll(request *pbservice.GetAllRequest) (*pbservice.GetAllResponse, error)
	GetFirst(request *pbservice.GetFirstRequest) (*pbservice.GetResponse, error)
	GetAllFirst(request *pbservice.GetAllRequest) (*pbservice.GetAllResponse, error)
}

type ForkawareStore interface {
	Store
	FlushUpToBlock(blockNum uint64) error
	EvictUpToBlock(upToBlockNumber uint64) error
}
