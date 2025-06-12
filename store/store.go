package store

import (
	"github.com/streamingfast/substreams-foundationnal-store/pb/store"
)

type Store interface {
	Set(entry *store.Entry) error
	SetAll(entries []*store.Entry) error
	Get(request *store.GetRequest) (*store.GetResponse, error)
	GetAll(request *store.GetAllRequest) (*store.GetAllResponse, error)
}

type ForkawareStore interface {
	Store
	FlushUpToBlock(blockNum uint64) error
	EvictUpToBlock(upToBlockNumber uint64) error
}
