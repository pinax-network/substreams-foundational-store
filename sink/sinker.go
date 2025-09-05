package sink

import (
	"context"
	"fmt"
	"time"

	pbstore "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/v1"
	"github.com/streamingfast/substreams-foundational-store/store"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	sink "github.com/streamingfast/substreams/sink"
	"go.uber.org/zap"
)

const (
	DefaultFlushQueueSize = 3
)

type Sinker struct {
	store          store.ForkawareStore
	logger         *zap.Logger
	cursorFilePath string

	// Batching fields
	batchBuffer    []*pbstore.Entry
	batchSize      int
	batchSizeBytes int
	maxBatchTime   time.Duration
	maxBatchBytes  int
	batchStartTime time.Time
	cursorHistory  map[string]*sink.Cursor
}

func NewSinker(store store.ForkawareStore, logger *zap.Logger, cursorFilePath string, batchSize int, maxBatchTime time.Duration, flushQueueSize int) *Sinker {
	logger = logger.Named("foundational-store-sinker")

	if batchSize <= 0 {
		batchSize = 1000
	}
	if maxBatchTime <= 0 {
		maxBatchTime = 30 * time.Second
	}
	if flushQueueSize <= 0 {
		flushQueueSize = DefaultFlushQueueSize
	}

	sinker := &Sinker{
		store:          store,
		logger:         logger,
		cursorFilePath: cursorFilePath,
		batchBuffer:    make([]*pbstore.Entry, 0, batchSize),
		batchSize:      batchSize,
		maxBatchTime:   maxBatchTime,

		// this represents ~80% badger size
		maxBatchBytes:  8 * 1024 * 1024,
		batchSizeBytes: 0,
	}
	return sinker
}

func (s *Sinker) HandleBlockScopedData(ctx context.Context, data *pbsubstreamsrpc.BlockScopedData, isLive *bool, cursor *sink.Cursor) error {

	var entriesCount int

	s.cursorHistory[data.Clock.Id] = cursor

	// Process data if present
	if data.Output != nil && data.Output.MapOutput != nil && data.Output.MapOutput.Value != nil {
		entries := &pbstore.Entries{}
		if err := data.Output.MapOutput.UnmarshalTo(entries); err != nil {
			return fmt.Errorf("unmarshaling map output to Entry: %w", err)
		}

		entriesCount = len(entries.Entries)

		if err := s.store.SetAll(entries.Entries, data.GetClock().Number); err != nil {
			return fmt.Errorf("setting foundational-store entry: %w", err)
		}
	}

	lib := cursor.LIB.Num()
	err := s.store.FlushUpToBlock(lib)
	if err != nil {
		return fmt.Errorf("flushing up to block up to lib %d: %w", lib, err)
	}

	c := s.cursorHistory[cursor.LIB.ID()]

	// Always save the cursor to a file, regardless of whether there was output data
	if err := SaveCursorToFile(c, s.cursorFilePath, s.logger); err != nil {
		CursorSaveErrors.Inc()
		return fmt.Errorf("saving cursor to file %w", err)
	}

	for _, c := range s.cursorHistory {
		if c.Block().Num() <= lib {
			delete(s.cursorHistory, c.Block().ID())
		}
	}

	blockNum := data.GetClock().Number
	RecordBlockProcessing(entriesCount, blockNum)

	return nil
}

func (s *Sinker) HandleBlockUndoSignal(ctx context.Context, undoSignal *pbsubstreamsrpc.BlockUndoSignal, cursor *sink.Cursor) error {
	blockNum := undoSignal.LastValidBlock.Number

	evictStart := time.Now()
	if err := s.store.EvictUpToBlock(blockNum); err != nil {
		return fmt.Errorf("failed to evict data up to block %d: %w", blockNum, err)
	}
	StoreEvictDuration.ObserveDuration(time.Since(evictStart))

	// Save the cursor to a file after handling the undo signal
	if err := SaveCursorToFile(cursor, s.cursorFilePath, s.logger); err != nil {
		CursorSaveErrors.Inc()
		s.logger.Warn("failed to save cursor to file after undo signal", zap.Error(err))
		// Don't return an error here, as we don't want to fail the processing
	}

	s.logger.Debug("evicted data due to undo signal",
		zap.Uint64("block_number", blockNum))

	return nil
}
