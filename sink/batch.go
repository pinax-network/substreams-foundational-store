package sink

import (
	"time"

	pbstore "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/v1"
)

// addToBatch adds entries to the batch buffer with proper synchronization
func (h *Handler) addToBatch(entries []*pbstore.Entry) error {
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
	}

	return nil
}

// GetPendingBatchAndReset returns the pending batch and its size in bytes, then resets the batch state
func (h *Handler) GetPendingBatchAndReset(blockNumber uint64) ([]*pbstore.Entry, int) {

	if len(h.batchBuffer) == 0 {
		return nil, 0
	}

	// take ownership of current batch buffer
	batchBuffer := h.batchBuffer
	batchBytes := h.batchSizeBytes

	// Reset batch state with fresh slice (pre-allocate capacity)
	h.batchBuffer = make([]*pbstore.Entry, 0, h.batchSize)
	h.batchSizeBytes = 0
	h.batchStartTime = time.Time{}

	return batchBuffer, batchBytes
}
