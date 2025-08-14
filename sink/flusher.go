package sink

import (
	"time"

	"github.com/streamingfast/shutter"
	pbstore "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/v1"
	"github.com/streamingfast/substreams-foundational-store/store"
	sink "github.com/streamingfast/substreams/sink"
	"go.uber.org/zap"
)

// BatchRequest represents a batch to be flushed
type BatchRequest struct {
	blockNumber    uint64
	cursor         *sink.Cursor
	cursorFilePath string
	batch          []*pbstore.Entry
	batchBytes     int
}

// Flusher handles the actual flushing logic independently from Handler
type Flusher struct {
	store   store.ForkawareStore
	logger  *zap.Logger
	shutter *shutter.Shutter

	// Configuration
	batchSize     int
	maxBatchBytes int
	maxBatchTime  time.Duration

	// Communication channels
	batchQueue chan *BatchRequest
	errorChan  chan error
	done       chan struct{}
}

// NewFlusher creates a new Flusher instance
func NewFlusher(store store.ForkawareStore, logger *zap.Logger, shutter *shutter.Shutter, batchSize int, maxBatchBytes int, maxBatchTime time.Duration, queueSize int) *Flusher {
	if queueSize <= 0 {
		queueSize = DefaultFlushQueueSize
	}

	f := &Flusher{
		store:         store,
		logger:        logger.Named("flusher"),
		shutter:       shutter,
		batchSize:     batchSize,
		maxBatchBytes: maxBatchBytes,
		maxBatchTime:  maxBatchTime,
		batchQueue:    make(chan *BatchRequest, queueSize),
		errorChan:     make(chan error, DefaultFlushQueueSize+1),
		done:          make(chan struct{}),
	}

	// Flusher waits for done when terminating
	shutter.OnTerminating(func(err error) {
		<-f.done
		f.logger.Info("Flusher worker completed")
	})

	// Start the flush worker
	go f.startWorker()

	return f
}

// SubmitBatch sends a batch to be flushed
func (f *Flusher) SubmitBatch(req *BatchRequest) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		f.batchQueue <- req
		FlushQueueDepth.SetUint64(uint64(len(f.batchQueue)))
		close(done)
	}()
	return done
}

// ErrorChan returns the channel for receiving flush errors
func (f *Flusher) ErrorChan() <-chan error {
	return f.errorChan
}

func (f *Flusher) sendError(err error) {
	select {
	case f.errorChan <- err:
	default:
		f.logger.Error("Error channel full, dropping error", zap.Error(err))
	}
}

// startWorker runs the flush worker loop
func (f *Flusher) startWorker() {
	defer close(f.done)

	for {
		select {
		case req := <-f.batchQueue:
			FlushQueueDepth.SetUint64(uint64(len(f.batchQueue)))

			f.processBatch(req)

		case <-f.shutter.Terminating():
			f.logger.Info("Flusher shutting down")
			return
		}
	}
}

// processBatch handles the actual database ops
func (f *Flusher) processBatch(req *BatchRequest) {
	asyncFlushStart := time.Now()

	// Store batch data if we have any
	if len(req.batch) > 0 {
		setAllStart := time.Now()
		err := f.store.SetAll(req.batch, req.blockNumber)
		setAllDuration := time.Since(setAllStart)
		StoreSetAllDuration.ObserveDuration(setAllDuration)

		if err != nil {
			AsyncFlushDuration.ObserveDuration(time.Since(asyncFlushStart))
			AsyncFlushErrors.Inc()
			f.logger.Error("Failed to store batch to database",
				zap.Uint64("block", req.blockNumber),
				zap.Int("batch_size", len(req.batch)),
				zap.Int("batch_bytes", req.batchBytes),
				zap.Error(err))
			f.sendError(err)
			return
		}

		f.logger.Debug("Successfully stored batch",
			zap.Uint64("block", req.blockNumber),
			zap.Int("batch_size", len(req.batch)),
			zap.Int("batch_bytes", req.batchBytes))
	}

	// Flush to disk
	flushStart := time.Now()
	err := f.store.FlushUpToBlock(req.blockNumber)
	flushDuration := time.Since(flushStart)
	StoreFlushDuration.ObserveDuration(flushDuration)

	if err != nil {
		AsyncFlushDuration.ObserveDuration(time.Since(asyncFlushStart))
		AsyncFlushErrors.Inc()
		f.logger.Error("Failed to flush to database",
			zap.Uint64("block", req.blockNumber),
			zap.Error(err))
		f.sendError(err)
		return
	}

	// Save cursor if provided
	if req.cursor != nil {
		if err := SaveCursorToFile(req.cursor, req.cursorFilePath, f.logger); err != nil {
			CursorSaveErrors.Inc()
			AsyncFlushDuration.ObserveDuration(time.Since(asyncFlushStart))
			AsyncFlushErrors.Inc()
			f.logger.Error("Failed to save cursor after flush",
				zap.Uint64("block", req.blockNumber),
				zap.Error(err))
			f.sendError(err)
			return
		}
	}

	totalDuration := time.Since(asyncFlushStart)
	AsyncFlushDuration.ObserveDuration(totalDuration)
	AsyncFlushOperations.Inc()
}
