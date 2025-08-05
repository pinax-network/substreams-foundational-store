package sink

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	pbstore "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/v1"
	"github.com/streamingfast/substreams-foundational-store/store"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	sink "github.com/streamingfast/substreams/sink"
	"go.uber.org/zap"
)

// LoadCursorFromFile attempts to load a cursor from the specified file.
// Returns nil if the file doesn't exist or if there's an error reading it.
func LoadCursorFromFile(logger *zap.Logger, cursorFilePath string) *sink.Cursor {
	// Use the provided cursor file path or default to "state.cursor" in the current directory
	var cursorPath string
	if cursorFilePath != "" {
		cursorPath = cursorFilePath
	} else {
		// Use the current working directory for simplicity
		dir, err := os.Getwd()
		if err != nil {
			dir = "."
		}
		cursorPath = filepath.Join(dir, "state.cursor")
	}

	data, err := os.ReadFile(cursorPath)
	if err != nil {
		if !os.IsNotExist(err) {
			logger.Warn("Failed to read cursor file", zap.Error(err))
		}
		return nil
	}

	cursorStr := string(data)
	cursor, err := sink.NewCursor(cursorStr)
	if err != nil {
		logger.Warn("Failed to create cursor from string", zap.Error(err))
		return nil
	}

	logger.Info("Loaded cursor from file", zap.String("path", cursorPath))
	return cursor
}

type Handler struct {
	store          store.ForkawareStore
	typeUrl        string
	logger         *zap.Logger
	cursorFilePath string

	// Batching fields
	batchBuffer    []*pbstore.Entry
	batchSize      int
	batchSizeBytes int
	maxBatchTime   time.Duration
	maxBatchBytes  int
	batchStartTime time.Time
	batchTimer     *time.Timer
	mu             sync.Mutex
}

func NewSinker(typeUrl string, store store.ForkawareStore, logger *zap.Logger, cursorFilePath string, batchSize int, maxBatchTime time.Duration) *Handler {
	logger = logger.Named("foundational-store-sinker")

	if batchSize <= 0 {
		batchSize = 1000
	}
	if maxBatchTime <= 0 {
		maxBatchTime = 30 * time.Second
	}

	return &Handler{
		store:          store,
		typeUrl:        typeUrl,
		logger:         logger,
		cursorFilePath: cursorFilePath,
		batchBuffer:    make([]*pbstore.Entry, 0, batchSize),
		batchSize:      batchSize,
		maxBatchTime:   maxBatchTime,
		maxBatchBytes:  8 * 1024 * 1024,
		batchSizeBytes: 0,
	}
}

func (h *Handler) HandleBlockScopedData(ctx context.Context, data *pbsubstreamsrpc.BlockScopedData, isLive *bool, cursor *sink.Cursor) error {
	var entriesCount int

	// Process data if present
	if data.Output != nil && data.Output.MapOutput != nil && data.Output.MapOutput.Value != nil {

		entries := &pbstore.Entries{}
		if err := data.Output.MapOutput.UnmarshalTo(entries); err != nil {
			return fmt.Errorf("unmarshalling map output to Entry: %w", err)
		}

		entriesCount = len(entries.Entries)

		// Add entries to batch buffer instead of immediate insert
		if err := h.addToBatch(entries.Entries, data.GetClock().Number); err != nil {
			return fmt.Errorf("adding entries to batch: %w", err)
		}
	}

	lib := cursor.LIB.Num()

	flushStart := time.Now()
	if err := h.store.FlushUpToBlock(lib); err != nil {
		return fmt.Errorf("flushing data up to block %d: %w", lib, err)
	}
	StoreFlushDuration.ObserveDuration(time.Since(flushStart))

	// Always save the cursor to a file, regardless of whether there was output data
	if err := h.saveCursorToFile(cursor); err != nil {
		CursorSaveErrors.Inc()
		return fmt.Errorf("saving cursor to file %w", err)
	}

	blockNum := data.GetClock().Number
	RecordBlockProcessing(entriesCount, blockNum)

	return nil
}

// saveCursorToFile saves the cursor to a file
func (h *Handler) saveCursorToFile(cursor *sink.Cursor) error {
	if err := sink.WriteCursor(h.cursorFilePath, cursor); err != nil {
		return fmt.Errorf("writing cursor to file: %w", err)
	}
	return nil
}

func (h *Handler) addToBatch(entries []*pbstore.Entry, blockNumber uint64) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	var newBytes int
	for _, entry := range entries {
		newBytes += len(entry.Key) + len(entry.Value.Value)
	}

	// Add entries to batch buffer
	h.batchBuffer = append(h.batchBuffer, entries...)
	h.batchSizeBytes += newBytes

	// Start timer on first entry if not already started
	if len(h.batchBuffer) == len(entries) {
		h.batchStartTime = time.Now()
		if h.batchTimer != nil {
			h.batchTimer.Stop()
		}
		h.batchTimer = time.AfterFunc(h.maxBatchTime, func() {
			h.mu.Lock()
			defer h.mu.Unlock()
			if len(h.batchBuffer) > 0 {
				h.logger.Debug("Flushing batch due to timeout",
					zap.Duration("elapsed", time.Since(h.batchStartTime)),
					zap.Int("entries", len(h.batchBuffer)))
				_ = h.flushBatchLocked(blockNumber)
			}
		})
	}

	if len(h.batchBuffer) >= h.batchSize ||
		h.batchSizeBytes >= h.maxBatchBytes {
		return h.flushBatchLocked(blockNumber)
	}

	return nil
}

func (h *Handler) flushBatchLocked(blockNumber uint64) error {
	if len(h.batchBuffer) == 0 {
		return nil
	}

	setAllStart := time.Now()

	// Create a copy of the buffer to send to store
	batchToFlush := make([]*pbstore.Entry, len(h.batchBuffer))
	copy(batchToFlush, h.batchBuffer)
	flushedBytes := h.batchSizeBytes

	// Reset batch state
	h.batchBuffer = h.batchBuffer[:0]
	h.batchSizeBytes = 0
	if h.batchTimer != nil {
		h.batchTimer.Stop()
		h.batchTimer = nil
	}

	h.mu.Unlock()
	defer h.mu.Lock()

	// Perform the actual store operation
	if err := h.store.SetAll(batchToFlush, blockNumber); err != nil {
		return fmt.Errorf("setting foundational-store batch: %w", err)
	}

	StoreSetAllDuration.ObserveDuration(time.Since(setAllStart))
	h.logger.Debug("Flushed batch to store",
		zap.Int("batch_size", len(batchToFlush)),
		zap.Int("batch_bytes", flushedBytes),
		zap.Uint64("block_number", blockNumber))

	return nil
}

func (h *Handler) FlushPendingBatch(blockNumber uint64) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.flushBatchLocked(blockNumber)
}

func (h *Handler) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.batchTimer != nil {
		h.batchTimer.Stop()
		h.batchTimer = nil
	}

	// Flush any pending entries
	return h.flushBatchLocked(0)
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
