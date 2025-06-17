package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	pbstore "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/v1"
	"github.com/streamingfast/substreams-foundational-store/store"
	sink "github.com/streamingfast/substreams-sink"
	"github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	"go.uber.org/zap"
)

// LoadCursorFromFile attempts to load a cursor from the specified file.
// Returns nil if the file doesn't exist or if there's an error reading it.
func LoadCursorFromFile(logger *zap.Logger, cursorFilePath string) *sink.Cursor {
	// Use the provided cursor file path or default to "cursor.json" in the current directory
	var cursorPath string
	if cursorFilePath != "" {
		cursorPath = cursorFilePath
	} else {
		// Use the current working directory for simplicity
		dir, err := os.Getwd()
		if err != nil {
			dir = "."
		}
		cursorPath = filepath.Join(dir, "cursor.json")
	}

	data, err := os.ReadFile(cursorPath)
	if err != nil {
		if !os.IsNotExist(err) {
			logger.Warn("Failed to read cursor file", zap.Error(err))
		}
		return nil
	}

	var cursor sink.Cursor
	if err := json.Unmarshal(data, &cursor); err != nil {
		logger.Warn("Failed to unmarshal cursor", zap.Error(err))
		return nil
	}

	logger.Info("Loaded cursor from file", zap.String("path", cursorPath))
	return &cursor
}

type Handler struct {
	store          store.ForkawareStore
	typeUrl        string
	logger         *zap.Logger
	cursorFilePath string
}

func NewSinker(typeUrl string, store store.ForkawareStore, logger *zap.Logger, cursorFilePath string) *Handler {
	logger = logger.Named("foundational-store-sinker")

	return &Handler{
		store:          store,
		typeUrl:        typeUrl,
		logger:         logger,
		cursorFilePath: cursorFilePath,
	}
}

func (h *Handler) HandleBlockScopedData(ctx context.Context, data *pbsubstreamsrpc.BlockScopedData, isLive *bool, cursor *sink.Cursor) error {
	if data.Output == nil || data.Output.MapOutput == nil {
		return nil
	}

	// According to the issue description, data.Output.MapOutput is a pb.foundational-store.Entry
	entry := &pbstore.Entry{}
	if err := data.Output.MapOutput.UnmarshalTo(entry); err != nil {
		return fmt.Errorf("failed to unmarshal map output to Entry: %w", err)
	}

	// Store the entry using the provided foundational-store
	if err := h.store.Set(entry); err != nil {
		return fmt.Errorf("failed to foundational-store entry: %w", err)
	}

	lib := cursor.LIB.Num()

	if err := h.store.FlushUpToBlock(lib); err != nil {
		return fmt.Errorf("failed to flush data up to block %d: %w", lib, err)
	}

	// Save the cursor to a file
	if err := h.saveCursorToFile(cursor); err != nil {
		h.logger.Warn("Failed to save cursor to file", zap.Error(err))
		// Don't return an error here, as we don't want to fail the processing
	}

	h.logger.Debug("Stored and flushed entry",
		zap.Uint64("block_number", entry.BlockNumber),
		zap.String("key", fmt.Sprintf("%x", entry.Key)),
		zap.String("type_url", h.typeUrl))

	return nil
}

// saveCursorToFile saves the cursor to a file
func (h *Handler) saveCursorToFile(cursor *sink.Cursor) error {

	// Marshal the cursor to JSON
	data, err := json.Marshal(cursor)
	if err != nil {
		return fmt.Errorf("failed to marshal cursor: %w", err)
	}

	// Write the cursor to the file
	if err := os.WriteFile(h.cursorFilePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write cursor to file: %w", err)
	}

	h.logger.Debug("Saved cursor to file", zap.String("path", h.cursorFilePath))
	return nil
}

func (h *Handler) HandleBlockUndoSignal(ctx context.Context, undoSignal *pbsubstreamsrpc.BlockUndoSignal, cursor *sink.Cursor) error {

	blockNum := undoSignal.LastValidBlock.Number
	if err := h.store.EvictUpToBlock(blockNum); err != nil {
		return fmt.Errorf("failed to evict data up to block %d: %w", blockNum, err)
	}

	// Save the cursor to a file after handling the undo signal
	if err := h.saveCursorToFile(cursor); err != nil {
		h.logger.Warn("Failed to save cursor to file after undo signal", zap.Error(err))
		// Don't return an error here, as we don't want to fail the processing
	}

	h.logger.Debug("Evicted data due to undo signal",
		zap.Uint64("block_number", blockNum))

	return nil
}
