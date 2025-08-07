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
	DefaultFlushQueueSize = 100
)

type Handler struct {
	store          store.ForkawareStore
	typeUrl        string
	logger         *zap.Logger
	cursorFilePath string

	// Batching fields (protected by batchMutex)
	batchBuffer    []*pbstore.Entry
	batchSize      int
	batchSizeBytes int
	maxBatchTime   time.Duration
	maxBatchBytes  int
	batchStartTime time.Time

	// Async flush fields
	flushQueue      chan *flushRequest
	flushWorkerDone chan struct{}
	flushError      chan error // for errors coming from the database, when something gets through, the world stops
	shutdown        chan struct{}
}

func NewSinker(typeUrl string, store store.ForkawareStore, logger *zap.Logger, cursorFilePath string, batchSize int, maxBatchTime time.Duration, flushQueueSize int) *Handler {
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

	handler := &Handler{
		store:          store,
		typeUrl:        typeUrl,
		logger:         logger,
		cursorFilePath: cursorFilePath,
		batchBuffer:    make([]*pbstore.Entry, 0, batchSize),
		batchSize:      batchSize,
		maxBatchTime:   maxBatchTime,
		// this represents ~80% badger size
		maxBatchBytes:   8 * 1024 * 1024,
		batchSizeBytes:  0,
		flushQueue:      make(chan *flushRequest, flushQueueSize),
		flushWorkerDone: make(chan struct{}),
		flushError:      make(chan error, 1),
		shutdown:        make(chan struct{}),
	}

	// Start the flush worker
	go handler.flushWorker()

	return handler
}

func (h *Handler) HandleBlockScopedData(ctx context.Context, data *pbsubstreamsrpc.BlockScopedData, isLive *bool, cursor *sink.Cursor) error {
	// Check for async flush errors first - if flush failed, stop processing
	select {
	case err := <-h.flushError:
		return fmt.Errorf("error during last flush: %w", err)
	default:
	}

	var entriesCount int

	// Process data if present
	if data.Output != nil && data.Output.MapOutput != nil && data.Output.MapOutput.Value != nil {

		entries := &pbstore.Entries{}
		if err := data.Output.MapOutput.UnmarshalTo(entries); err != nil {
			return fmt.Errorf("unmarshalling map output to Entry: %w", err)
		}

		entriesCount = len(entries.Entries)

		// Add entries to batch buffer instead of immediate insert
		if err := h.addToBatch(entries.Entries); err != nil {
			return fmt.Errorf("adding entries to batch: %w", err)
		}
	}

	lib := cursor.LIB.Num()

	if !h.shouldFlush() {
		return nil
	}

	// Get the batch data to flush
	batch, batchBytes := h.GetPendingBatchAndReset(data.Clock.Number)

	if len(batch) == 0 {
		return nil // Nothing to flush
	}

	// Submit flush request to background worker with batch data
	req := &flushRequest{
		blockNumber: lib,
		cursor:      cursor,
		batch:       batch,
		batchBytes:  batchBytes,
	}

	select {
	case h.flushQueue <- req:
		FlushQueueDepth.SetUint64(uint64(len(h.flushQueue)))
	case err := <-h.flushError:
		return fmt.Errorf("error during last flush: %w", err)
	case <-ctx.Done():
		return ctx.Err()
	}

	blockNum := data.GetClock().Number
	RecordBlockProcessing(entriesCount, blockNum)

	return nil
}

func (h *Handler) Close() error {

	// Get any remaining batch entries and send to flush worker
	// FIXME ulysse
	//	batch, batchBytes := h.GetPendingBatchAndReset(0)
	//	if len(batch) > 0 {
	//		req := &flushRequest{
	//			blockNumber: 0,
	//			cursor:      nil,
	//			batch:       batch,
	//			batchBytes:  batchBytes,
	//		}
	//
	//		// Send final batch (blocking to ensure it gets queued)
	//		h.flushQueue <- req
	//		FlushQueueDepth.SetUint64(uint64(len(h.flushQueue)))
	//	}
	//
	// Signal shutdown to flush worker and wait for it to finish
	close(h.shutdown)
	<-h.flushWorkerDone

	return nil
}

func (h *Handler) HandleBlockUndoSignal(ctx context.Context, undoSignal *pbsubstreamsrpc.BlockUndoSignal, cursor *sink.Cursor) error {

	blockNum := undoSignal.LastValidBlock.Number

	if err := h.FlushPendingBatch(blockNum); err != nil {
		h.logger.Warn("Failed to flush pending batch before undo", zap.Error(err))
	}

	evictStart := time.Now()
	if err := h.store.EvictUpToBlock(blockNum); err != nil {
		return fmt.Errorf("failed to evict data up to block %d: %w", blockNum, err)
	}
	StoreEvictDuration.ObserveDuration(time.Since(evictStart))

	// Save the cursor to a file after handling the undo signal
	if err := h.saveCursorToFile(cursor); err != nil {
		CursorSaveErrors.Inc()
		h.logger.Warn("Failed to save cursor to file after undo signal", zap.Error(err))
		// Don't return an error here, as we don't want to fail the processing
	}

	h.logger.Debug("Evicted data due to undo signal",
		zap.Uint64("block_number", blockNum))

	return nil
}
