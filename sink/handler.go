package sink

import (
	"context"
	"fmt"

	pbstore "github.com/streamingfast/substreams-foundationnal-store/pb/store"
	"github.com/streamingfast/substreams-foundationnal-store/store"
	sink "github.com/streamingfast/substreams-sink"
	"github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	"go.uber.org/zap"
)

type Handler struct {
	store   store.ForkawareStore
	typeUrl string
	logger  *zap.Logger
}

func NewSinker(typeUrl string, store store.ForkawareStore, logger *zap.Logger) *Handler {
	logger = logger.Named("store-sinker")

	return &Handler{
		store:   store,
		typeUrl: typeUrl,
		logger:  logger,
	}
}

func (h *Handler) HandleBlockScopedData(ctx context.Context, data *pbsubstreamsrpc.BlockScopedData, isLive *bool, cursor *sink.Cursor) error {
	if data.Output == nil || data.Output.MapOutput == nil {
		return nil
	}

	// According to the issue description, data.Output.MapOutput is a pb.store.Entry
	entry := &pbstore.Entry{}
	if err := data.Output.MapOutput.UnmarshalTo(entry); err != nil {
		return fmt.Errorf("failed to unmarshal map output to Entry: %w", err)
	}

	// Store the entry using the provided store
	if err := h.store.Set(entry); err != nil {
		return fmt.Errorf("failed to store entry: %w", err)
	}

	blockNum := cursor.Block().Num()

	if err := h.store.FlushUpToBlock(blockNum); err != nil {
		return fmt.Errorf("failed to flush data up to block %d: %w", blockNum, err)
	}

	h.logger.Debug("Stored and flushed entry",
		zap.Uint64("block_number", entry.BlockNumber),
		zap.String("key", fmt.Sprintf("%x", entry.Key)),
		zap.String("type_url", h.typeUrl))

	return nil
}

func (h *Handler) HandleBlockUndoSignal(ctx context.Context, undoSignal *pbsubstreamsrpc.BlockUndoSignal, cursor *sink.Cursor) error {

	blockNum := undoSignal.LastValidBlock.Number
	if err := h.store.EvictUpToBlock(blockNum); err != nil {
		return fmt.Errorf("failed to evict data up to block %d: %w", blockNum, err)
	}

	h.logger.Debug("Evicted data due to undo signal",
		zap.Uint64("block_number", blockNum))

	return nil
}
