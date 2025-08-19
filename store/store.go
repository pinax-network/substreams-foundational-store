package store

import (
	pbstore "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/v1"
)

type Store interface {
	Set(entry *pbstore.Entry, blockNumber uint64, blockHash []byte) error
	SetAll(entries []*pbstore.Entry, blockNumber uint64, blockHash []byte) error
	Get(request *pbstore.GetRequest) (*pbstore.GetResponse, error)
	GetAll(request *pbstore.GetAllRequest) (*pbstore.GetAllResponse, error)
}

type ForkawareStore interface {
	Store
	FlushUpToBlock(blockNum uint64) error
	EvictUpToBlock(upToBlockNumber uint64) error
}
