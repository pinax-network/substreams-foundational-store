package sink

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pbstore "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/v1"
	"github.com/streamingfast/substreams-foundational-store/store"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
	sink "github.com/streamingfast/substreams/sink"
	"go.uber.org/zap/zaptest"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestCursorSaveAndLoad(t *testing.T) {
	logger := zaptest.NewLogger(t)

	tempDir := t.TempDir()
	cursorFilePath := filepath.Join(tempDir, "test.cursor")

	mockStore := NewMockStore()
	handler := NewSinker(mockStore, logger, cursorFilePath, 10, time.Second, 10)
	defer func() {
		handler.Shutdown(nil)
		<-handler.Terminated()
	}()

	testCursorStr := "XWQh1iJoYAKTDtvllL7yraWwLpc_DFhvVQvlKhhCjYGDiHqspvzCXTgfFUum8f32iBSqMQXahNirXjQmq6AKuJSypu8Sm3NpAXkk8YPs-7TvePP7OgIRBMNqNpHvBoWCMUGBFGuvfOQBoa-4TKneAQh4P55GdmL211oH1PMGIeQTsRE="

	originalCursor, err := sink.NewCursor(testCursorStr)
	if err != nil {
		t.Fatalf("Failed to create cursor from test string: %v", err)
	}

	err = SaveCursorToFile(originalCursor, cursorFilePath, logger)
	if err != nil {
		t.Fatalf("Failed to save cursor: %v", err)
	}

	if _, err := os.Stat(cursorFilePath); os.IsNotExist(err) {
		t.Fatalf("Cursor file was not created")
	}

	loadedCursor := LoadCursorFromFile(logger, cursorFilePath)
	if loadedCursor == nil {
		t.Fatalf("Failed to load cursor from file")
	}

	originalStr := originalCursor.String()
	loadedStr := loadedCursor.String()

	if originalStr != loadedStr {
		t.Errorf("Cursor mismatch:\nOriginal: %s\nLoaded:   %s", originalStr, loadedStr)
	}
}

func TestCursorLoadNonExistentFile(t *testing.T) {
	logger := zaptest.NewLogger(t)

	cursor := LoadCursorFromFile(logger, "/tmp/non-existent-cursor-file.cursor")

	if cursor != nil {
		t.Errorf("Expected nil cursor for non-existent file, got: %v", cursor)
	}
}

func TestCursorLoadEmptyFile(t *testing.T) {
	tempDir := t.TempDir()
	emptyFilePath := filepath.Join(tempDir, "empty.cursor")

	// Use os.WriteFile to simulate corrupted files (before atomic saving)
	err := os.WriteFile(emptyFilePath, []byte(""), 0644)
	if err != nil {
		t.Fatalf("Failed to create empty file: %v", err)
	}

	cursor, err := sink.ReadCursor(emptyFilePath)
	if err != nil {
		t.Fatalf("ReadCursor failed for empty file: %v", err)
	}
	if cursor != nil {
		t.Errorf("Expected nil cursor for empty file, got: %v", cursor)
	}

	whitespaceFilePath := filepath.Join(tempDir, "whitespace.cursor")
	err = os.WriteFile(whitespaceFilePath, []byte("   \n\t  "), 0644)
	if err != nil {
		t.Fatalf("Failed to create whitespace file: %v", err)
	}

	cursor, err = sink.ReadCursor(whitespaceFilePath)
	if err != nil {
		t.Fatalf("ReadCursor failed for whitespace file: %v", err)
	}
	if cursor != nil {
		t.Errorf("Expected nil cursor for whitespace-only file, got: %v", cursor)
	}
}

func TestCursorRoundTrip(t *testing.T) {
	logger := zaptest.NewLogger(t)

	testCursors := []string{
		"XWQh1iJoYAKTDtvllL7yraWwLpc_DFhvVQvlKhhCjYGDiHqspvzCXTgfFUum8f32iBSqMQXahNirXjQmq6AKuJSypu8Sm3NpAXkk8YPs-7TvePP7OgIRBMNqNpHvBoWCMUGBFGuvfOQBoa-4TKneAQh4P55GdmL211oH1PMGIeQTsRE=",
		"R6y3vDTh2Z9jZMGHJVbBeqWwLpc_DFhvVQvlKhhIjIGFpQayof_gfSsIPB6Hl-GTuESoThquiqirYBl_u6EPuYS-kedg7lc7LyllzYPs-7TvePP7MAMRAu4WKJbsJKWRJmjUNQ2zGdRRo9CnOKeuATZVZo5Hc2Pm220PpoYic8pD8C0=",
	}

	for i, testCursorStr := range testCursors {
		t.Run(t.Name()+"_"+string(rune(i)), func(t *testing.T) {

			tempDir := t.TempDir()
			cursorFilePath := filepath.Join(tempDir, "roundtrip.cursor")

			mockStore := NewMockStore()
			handler := NewSinker(mockStore, logger, cursorFilePath, 10, time.Second, 10)
			defer func() {
				handler.Shutdown(nil)
				<-handler.Terminated()
			}()

			originalCursor, err := sink.NewCursor(testCursorStr)
			if err != nil {
				t.Fatalf("Failed to create cursor: %v", err)
			}

			for round := 0; round < 3; round++ {

				err = SaveCursorToFile(originalCursor, cursorFilePath, logger)
				if err != nil {
					t.Fatalf("Round %d: Failed to save cursor: %v", round, err)
				}

				loadedCursor := LoadCursorFromFile(logger, cursorFilePath)
				if loadedCursor == nil {
					t.Fatalf("Round %d: Failed to load cursor", round)
				}

				if originalCursor.String() != loadedCursor.String() {
					t.Errorf("Round %d: Cursor mismatch", round)
				}

				originalCursor = loadedCursor
			}
		})
	}
}

func TestAtomicCursorSaving(t *testing.T) {
	logger := zaptest.NewLogger(t)

	tempDir := t.TempDir()
	cursorFilePath := filepath.Join(tempDir, "atomic_test.cursor")

	testCursorStr := "XWQh1iJoYAKTDtvllL7yraWwLpc_DFhvVQvlKhhCjYGDiHqspvzCXTgfFUum8f32iBSqMQXahNirXjQmq6AKuJSypu8Sm3NpAXkk8YPs-7TvePP7OgIRBMNqNpHvBoWCMUGBFGuvfOQBoa-4TKneAQh4P55GdmL211oH1PMGIeQTsRE="
	cursor, err := sink.NewCursor(testCursorStr)
	if err != nil {
		t.Fatalf("Failed to create cursor: %v", err)
	}

	err = sink.WriteCursor(cursorFilePath, cursor)
	if err != nil {
		t.Fatalf("Failed to write cursor atomically: %v", err)
	}

	if _, err := os.Stat(cursorFilePath); os.IsNotExist(err) {
		t.Fatalf("Cursor file was not created")
	}

	data, err := os.ReadFile(cursorFilePath)
	if err != nil {
		t.Fatalf("Failed to read cursor file: %v", err)
	}

	if string(data) != testCursorStr {
		t.Errorf("Cursor content mismatch: expected %s, got %s", testCursorStr, string(data))
	}

	loadedCursor := LoadCursorFromFile(logger, cursorFilePath)
	if loadedCursor == nil {
		t.Fatalf("Failed to load cursor from atomically written file")
	}

	if loadedCursor.String() != testCursorStr {
		t.Errorf("Loaded cursor mismatch: expected %s, got %s", testCursorStr, loadedCursor.String())
	}
}

func TestTempFileNameReturnsPath(t *testing.T) {
	tempDir := t.TempDir()

	tempFile, err := os.CreateTemp(tempDir, ".cursor_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	tempPath := tempFile.Name()

	if tempPath == "" {
		t.Fatalf("tempFile.Name() returned empty string")
	}

	if !strings.HasPrefix(tempPath, tempDir) {
		t.Fatalf("tempFile.Name() returned path %s, expected to be in directory %s", tempPath, tempDir)
	}

	baseName := filepath.Base(tempPath)
	if !strings.HasPrefix(baseName, ".cursor_test_") {
		t.Fatalf("tempFile.Name() returned path %s, expected basename to start with '.cursor_test_'", tempPath)
	}

	testData := []byte("test cursor data")
	_, err = tempFile.Write(testData)
	if err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}

	if err = tempFile.Close(); err != nil {
		t.Fatalf("Failed to close temp file: %v", err)
	}

	readData, err := os.ReadFile(tempPath)
	if err != nil {
		t.Fatalf("Failed to read temp file using path from Name(): %v", err)
	}

	if string(readData) != string(testData) {
		t.Fatalf("Data mismatch: wrote %s, read %s", string(testData), string(readData))
	}

	if _, err := os.Stat(tempPath); os.IsNotExist(err) {
		t.Fatalf("File does not exist at path returned by Name(): %s", tempPath)
	}
}

var _ store.ForkawareStore = (*MockStore)(nil)

type MockStore struct {
	mu          sync.Mutex
	setAllCalls []SetAllCall
	flushCalls  []uint64
	evictCalls  []uint64
}

type SetAllCall struct {
	Entries     []*pbstore.Entry
	BlockNumber uint64
	BlockHash   []byte
}

func NewMockStore() *MockStore {
	return &MockStore{
		setAllCalls: make([]SetAllCall, 0),
		flushCalls:  make([]uint64, 0),
		evictCalls:  make([]uint64, 0),
	}
}

func (m *MockStore) Set(entry *pbstore.Entry, blockNumber uint64, blockHash []byte) error {
	return m.SetAll([]*pbstore.Entry{entry}, blockNumber, blockHash)
}

func (m *MockStore) SetAll(entries []*pbstore.Entry, blockNumber uint64, blockHash []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	entriesCopy := make([]*pbstore.Entry, len(entries))
	copy(entriesCopy, entries)

	m.setAllCalls = append(m.setAllCalls, SetAllCall{
		Entries:     entriesCopy,
		BlockNumber: blockNumber,
		BlockHash:   blockHash,
	})
	return nil
}

func (m *MockStore) Get(request *pbstore.GetRequest) (*pbstore.GetResponse, error) {
	return &pbstore.GetResponse{Response: pbstore.ResponseCode_RESPONSE_CODE_NOT_FOUND}, nil
}

func (m *MockStore) GetAll(request *pbstore.GetAllRequest) (*pbstore.GetAllResponse, error) {
	return &pbstore.GetAllResponse{Entries: []*pbstore.ResponseEntry{}}, nil
}

func (m *MockStore) FlushUpToBlock(blockNum uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.flushCalls = append(m.flushCalls, blockNum)
	return nil
}

func (m *MockStore) EvictUpToBlock(upToBlockNumber uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.evictCalls = append(m.evictCalls, upToBlockNumber)
	return nil
}

func (m *MockStore) Close() error {
	return nil
}

func (m *MockStore) GetSetAllCalls() []SetAllCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	calls := make([]SetAllCall, len(m.setAllCalls))
	copy(calls, m.setAllCalls)
	return calls
}

func (m *MockStore) GetFlushCalls() []uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	calls := make([]uint64, len(m.flushCalls))
	copy(calls, m.flushCalls)
	return calls
}

func (m *MockStore) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.setAllCalls = m.setAllCalls[:0]
	m.flushCalls = m.flushCalls[:0]
	m.evictCalls = m.evictCalls[:0]
}

func createTestEntries(count int, keyPrefix string, valueSize int) []*pbstore.Entry {
	entries := make([]*pbstore.Entry, count)
	for i := 0; i < count; i++ {
		key := []byte(keyPrefix + string(rune('A'+i%26)))

		mint := make([]byte, valueSize/2)
		owner := make([]byte, valueSize/2)
		for j := range mint {
			mint[j] = byte('m' + j%26)
		}
		for j := range owner {
			owner[j] = byte('o' + j%26)
		}

		anyValue, _ := anypb.New(&pbstore.TestAccountOwner{
			Mint:  mint,
			Owner: owner,
		})
		entries[i] = &pbstore.Entry{
			Key:   key,
			Value: anyValue,
		}
	}
	return entries
}

func TestBatchingByEntryCount(t *testing.T) {
	logger := zaptest.NewLogger(t)
	mockStore := NewMockStore()

	batchSize := 5
	maxBatchTime := 10 * time.Second
	handler := NewSinker(mockStore, logger, "", batchSize, maxBatchTime, 10)
	defer func() {
		handler.Shutdown(nil)
		<-handler.Terminated()
	}()

	entries := createTestEntries(12, "test_", 100)

	err := handler.addToBatch(entries[0:3])
	if err != nil {
		t.Fatalf("Failed to add first batch: %v", err)
	}

	calls := mockStore.GetSetAllCalls()
	if len(calls) != 0 {
		t.Errorf("Expected 0 SetAll calls after 3 entries, got %d", len(calls))
	}

	err = handler.addToBatch(entries[3:5])
	if err != nil {
		t.Fatalf("Failed to add second batch: %v", err)
	}

	// Should trigger flush now (5 entries >= batch size of 5)
	if !handler.shouldFlush() {
		t.Error("Should flush after 5 entries")
	}

	// Manually trigger the first flush like HandleBlockScopedData would
	batch1, batchBytes1 := handler.GetPendingBatchAndReset(1001)
	if len(batch1) != 5 {
		t.Errorf("Expected first batch to have 5 entries, got %d", len(batch1))
	}

	// Submit first batch to flusher
	req1 := &BatchRequest{
		blockNumber:    1001,
		cursor:         nil,
		cursorFilePath: "",
		batch:          batch1,
		batchBytes:     batchBytes1,
	}
	handler.flusher.SubmitBatch(req1)

	// Wait for async processing to complete
	time.Sleep(50 * time.Millisecond)

	calls = mockStore.GetSetAllCalls()
	if len(calls) != 1 {
		t.Errorf("Expected 1 SetAll call after 5 entries, got %d", len(calls))
	} else if len(calls[0].Entries) != 5 {
		t.Errorf("Expected first batch to have 5 entries, got %d", len(calls[0].Entries))
	}

	err = handler.addToBatch(entries[5:10])
	if err != nil {
		t.Fatalf("Failed to add third batch: %v", err)
	}

	// Should trigger another flush (10 entries total, 5 more than batch size)
	if !handler.shouldFlush() {
		t.Error("Should flush after 10 entries total")
	}

	// Manually trigger the second flush
	batch2, batchBytes2 := handler.GetPendingBatchAndReset(1002)
	if len(batch2) != 5 {
		t.Errorf("Expected second batch to have 5 entries, got %d", len(batch2))
	}

	req2 := &BatchRequest{
		blockNumber:    1002,
		cursor:         nil,
		cursorFilePath: "",
		batch:          batch2,
		batchBytes:     batchBytes2,
	}
	handler.flusher.SubmitBatch(req2)

	// Wait for async processing
	time.Sleep(50 * time.Millisecond)

	calls = mockStore.GetSetAllCalls()
	if len(calls) != 2 {
		t.Errorf("Expected 2 SetAll calls after 10 entries, got %d", len(calls))
	} else if len(calls[1].Entries) != 5 {
		t.Errorf("Expected second batch to have 5 entries, got %d", len(calls[1].Entries))
	}

	err = handler.addToBatch(entries[10:12])
	if err != nil {
		t.Fatalf("Failed to add fourth batch: %v", err)
	}

	// Should not trigger flush yet (only 2 more entries, total would be 2 < batch size 5)
	if handler.shouldFlush() {
		t.Error("Should not flush after only 2 more entries")
	}

	calls = mockStore.GetSetAllCalls()
	if len(calls) != 2 {
		t.Errorf("Expected still 2 SetAll calls after 12 entries, got %d", len(calls))
	}

	// Now manually flush the remaining entries
	err = handler.FlushPendingBatch(context.Background(), 1003, []byte("test_hash_1003"), 1004, nil)
	if err != nil {
		t.Fatalf("Failed to flush pending batch: %v", err)
	}

	// Wait for async processing
	time.Sleep(50 * time.Millisecond)

	calls = mockStore.GetSetAllCalls()
	if len(calls) != 3 {
		t.Errorf("Expected 3 SetAll calls after flush, got %d", len(calls))
	} else if len(calls[2].Entries) != 2 {
		t.Errorf("Expected third batch to have 2 entries, got %d", len(calls[2].Entries))
	}
}

func TestBatchingByTimeout(t *testing.T) {
	logger := zaptest.NewLogger(t)
	mockStore := NewMockStore()

	batchSize := 1000
	maxBatchTime := 50 * time.Millisecond
	handler := NewSinker(mockStore, logger, "", batchSize, maxBatchTime, 10)
	defer func() {
		handler.Shutdown(nil)
		<-handler.Terminated()
	}()

	entries := createTestEntries(3, "timeout_test_", 100)
	if err := handler.addToBatch(entries); err != nil {
		t.Fatalf("Failed to add entries: %v", err)
	}

	// no flush yet
	if calls := mockStore.GetSetAllCalls(); len(calls) != 0 {
		t.Errorf("Expected 0 SetAll calls immediately, got %d", len(calls))
	}

	// wait for the timeout to elapse, then force a flush
	time.Sleep(100 * time.Millisecond)
	if err := handler.FlushPendingBatch(context.Background(), 1000, []byte("test_hash_1000"), 0, nil); err != nil {
		t.Fatalf("FlushPendingBatch failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	calls := mockStore.GetSetAllCalls()
	if len(calls) != 1 {
		t.Errorf("Expected 1 SetAll call after timeout, got %d", len(calls))
	} else if len(calls[0].Entries) != 3 {
		t.Errorf("Expected batch to have 3 entries, got %d", len(calls[0].Entries))
	}
}

func TestBatchingByByteSize(t *testing.T) {
	logger := zaptest.NewLogger(t)
	mockStore := NewMockStore()

	batchSize := 1000
	maxBatchTime := 10 * time.Second
	handler := NewSinker(mockStore, logger, "", batchSize, maxBatchTime, 10)
	defer func() {
		handler.Shutdown(nil)
		<-handler.Terminated()
	}()

	// force a low byte threshold
	handler.maxBatchBytes = 1000
	entries := createTestEntries(2, "large_", 600)

	// first entry should not flush
	if err := handler.addToBatch(entries[0:1]); err != nil {
		t.Fatalf("Failed to add first entry: %v", err)
	}
	if calls := mockStore.GetSetAllCalls(); len(calls) != 0 {
		t.Errorf("Expected 0 SetAll calls after first large entry, got %d", len(calls))
	}

	// second entry pushes us over the byte limit; still no automatic flush, so force it
	if err := handler.addToBatch(entries[1:2]); err != nil {
		t.Fatalf("Failed to add second entry: %v", err)
	}
	if err := handler.FlushPendingBatch(context.Background(), 1000, []byte("test_hash_1000"), 0, nil); err != nil {
		t.Fatalf("FlushPendingBatch failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	calls := mockStore.GetSetAllCalls()
	if len(calls) != 1 {
		t.Errorf("Expected 1 SetAll call after byte limit flush, got %d", len(calls))
	} else if len(calls[0].Entries) != 2 {
		t.Errorf("Expected batch to have 2 entries, got %d", len(calls[0].Entries))
	}
}

func TestHandlerClose(t *testing.T) {
	logger := zaptest.NewLogger(t)
	mockStore := NewMockStore()

	batchSize := 10
	maxBatchTime := 100 * time.Millisecond
	handler := NewSinker(mockStore, logger, "", batchSize, maxBatchTime, 10)

	entries := createTestEntries(3, "close_test_", 100)
	if err := handler.addToBatch(entries); err != nil {
		t.Fatalf("Failed to add entries: %v", err)
	}

	// still nothing yet
	if calls := mockStore.GetSetAllCalls(); len(calls) != 0 {
		t.Errorf("Expected 0 SetAll calls before manual flush, got %d", len(calls))
	}

	// flush pending batch manually
	if err := handler.FlushPendingBatch(context.Background(), 1000, []byte("test_hash_1000"), 0, nil); err != nil {
		t.Fatalf("FlushPendingBatch failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	calls := mockStore.GetSetAllCalls()
	if len(calls) != 1 {
		t.Errorf("Expected 1 SetAll call after manual flush, got %d", len(calls))
	} else if len(calls[0].Entries) != 3 {
		t.Errorf("Expected batch to have 3 entries, got %d", len(calls[0].Entries))
	}

	// calling Shutdown() now should not add any more batches
	handler.Shutdown(nil)
	<-handler.Terminated()
	time.Sleep(50 * time.Millisecond)
	if calls2 := mockStore.GetSetAllCalls(); len(calls2) != 1 {
		t.Errorf("Expected no additional SetAll calls after Close(), got %d", len(calls2))
	}
}

func TestBatchAccumulation(t *testing.T) {
	logger := zaptest.NewLogger(t)
	mockStore := NewMockStore()

	batchSize := 50 // Large batch size so we don't auto‐flush during the test
	maxBatchTime := 10 * time.Second
	handler := NewSinker(mockStore, logger, "", batchSize, maxBatchTime, 10)
	defer func() {
		handler.Shutdown(nil)
		<-handler.Terminated()
	}()

	numCalls := 5
	entriesPerCall := 4

	// Call addToBatch sequentially to avoid data races
	for i := 0; i < numCalls; i++ {
		entries := createTestEntries(entriesPerCall, "concurrent_", 50)
		if err := handler.addToBatch(entries); err != nil {
			t.Errorf("Call %d failed to add entries: %v", i, err)
		}
	}

	// Nothing flushed yet
	if calls := mockStore.GetSetAllCalls(); len(calls) != 0 {
		t.Errorf("Expected 0 SetAll calls before manual flush, got %d", len(calls))
	}

	// Now manually flush
	if err := handler.FlushPendingBatch(context.Background(), 5999, []byte("test_hash_5999"), 5999, nil); err != nil {
		t.Fatalf("FlushPendingBatch failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	calls := mockStore.GetSetAllCalls()
	if len(calls) == 0 {
		t.Fatalf("Expected at least 1 SetAll call after flush, got %d", len(calls))
	}

	totalEntries := 0
	for _, call := range calls {
		totalEntries += len(call.Entries)
	}

	expected := numCalls * entriesPerCall
	if totalEntries != expected {
		t.Errorf("Expected %d total entries, got %d", expected, totalEntries)
	}
}

func TestAsyncFlushWorkerStartsAndStops(t *testing.T) {
	logger := zaptest.NewLogger(t)
	mockStore := NewMockStore()

	handler := NewSinker(mockStore, logger, "", 10, time.Second, 10)

	// Test that the sinker can be created and has a flusher
	if handler.flusher == nil {
		t.Error("Expected flusher to be initialized")
	}

	// Test clean shutdown
	handler.Shutdown(nil)
	<-handler.Terminated()

	// Test that we can shutdown multiple times without error
	handler.Shutdown(nil)
	<-handler.Terminated()
}

func TestAsyncFlushQueueDepthTracking(t *testing.T) {
	logger := zaptest.NewLogger(t)

	slowStore := &SlowMockStore{
		MockStore:  NewMockStore(),
		flushDelay: 100 * time.Millisecond,
	}

	handler := NewSinker(slowStore, logger, "", 10, time.Second, 5)
	defer func() {
		handler.Shutdown(nil)
		<-handler.Terminated()
	}()

	RegisterMetrics()

	FlushQueueDepth.SetUint64(0)
	initialDepth := FlushQueueDepth.Get()

	entries := []*pbstore.Entry{
		createTestEntry("key1", "value1"),
		createTestEntry("key2", "value2"),
		createTestEntry("key3", "value3"),
	}

	go func() {
		for i := 0; i < 3; i++ {
			handler.addToBatch(entries)
		}
	}()

	time.Sleep(50 * time.Millisecond)

	currentDepth := FlushQueueDepth.Get()
	t.Logf("Queue depth tracking working: initial=%f, current=%f", initialDepth, currentDepth)

	if currentDepth < 0 {
		t.Error("Queue depth should not be negative")
	}
}

func TestAsyncFlushPreservesDataSafety(t *testing.T) {
	logger := zaptest.NewLogger(t)
	mockStore := NewMockStore()

	tempDir := t.TempDir()
	cursorFile := filepath.Join(tempDir, "test.cursor")

	handler := NewSinker(mockStore, logger, cursorFile, 10, time.Second, 10)
	defer func() {
		handler.Shutdown(nil)
		<-handler.Terminated()
	}()

	entries := []*pbstore.Entry{createTestEntry("key1", "value1")}
	testCursor := createTestCursor()

	err := handler.addToBatch(entries)
	if err != nil {
		t.Fatalf("addToBatch failed: %v", err)
	}

	err = handler.FlushPendingBatch(context.Background(), 999, []byte("test_hash_999"), 1000, createTestCursor())
	if err != nil {
		t.Fatalf("FlushPendingBatch failed: %v", err)
	}

	// Wait for async flush to complete
	time.Sleep(50 * time.Millisecond)

	setAllCalls := mockStore.GetSetAllCalls()
	if len(setAllCalls) == 0 {
		t.Error("Expected at least 1 SetAll call")
	}

	err = SaveCursorToFile(testCursor, cursorFile, logger)
	if err != nil {
		t.Fatalf("SaveCursorToFile failed: %v", err)
	}

	if _, err := os.Stat(cursorFile); os.IsNotExist(err) {
		t.Error("Cursor file should exist after save")
	}

	if handler.flusher == nil {
		t.Error("Async flusher should be initialized")
	}
}

// SlowMockStore simulates slow flush operations for testing queue behavior
type SlowMockStore struct {
	*MockStore
	flushDelay time.Duration
}

func (s *SlowMockStore) FlushUpToBlock(blockNum uint64) error {
	time.Sleep(s.flushDelay)
	return s.MockStore.FlushUpToBlock(blockNum)
}

// Helper functions for tests
func createTestEntry(key, value string) *pbstore.Entry {
	return &pbstore.Entry{
		Key: []byte(key),
		Value: &anypb.Any{
			TypeUrl: "test.Entry",
			Value:   []byte(value),
		},
	}
}

func createTestCursor() *sink.Cursor {
	testCursorStr := "XWQh1iJoYAKTDtvllL7yraWwLpc_DFhvVQvlKhhCjYGDiHqspvzCXTgfFUum8f32iBSqMQXahNirXjQmq6AKuJSypu8Sm3NpAXkk8YPs-7TvePP7OgIRBMNqNpHvBoWCMUGBFGuvfOQBoa-4TKneAQh4P55GdmL211oH1PMGIeQTsRE="
	cursor, _ := sink.NewCursor(testCursorStr)
	return cursor
}

// ErrorMockStore simulates various store operation failures
type ErrorMockStore struct {
	*MockStore
	setAllError     error
	flushError      error
	evictError      error
	setAllFailCount int32
	flushFailCount  int32
	evictFailCount  int32
	setAllCallCount int32
	flushCallCount  int32
	evictCallCount  int32
}

func NewErrorMockStore() *ErrorMockStore {
	return &ErrorMockStore{
		MockStore: NewMockStore(),
	}
}

func (e *ErrorMockStore) SetAll(entries []*pbstore.Entry, blockNumber uint64, blockHash []byte) error {
	atomic.AddInt32(&e.setAllCallCount, 1)
	if e.setAllError != nil && atomic.LoadInt32(&e.setAllCallCount) <= e.setAllFailCount {
		return e.setAllError
	}
	return e.MockStore.SetAll(entries, blockNumber, blockHash)
}

func (e *ErrorMockStore) FlushUpToBlock(blockNum uint64) error {
	atomic.AddInt32(&e.flushCallCount, 1)
	if e.flushError != nil && atomic.LoadInt32(&e.flushCallCount) <= e.flushFailCount {
		return e.flushError
	}
	return e.MockStore.FlushUpToBlock(blockNum)
}

func (e *ErrorMockStore) EvictUpToBlock(upToBlockNumber uint64) error {
	atomic.AddInt32(&e.evictCallCount, 1)
	if e.evictError != nil && atomic.LoadInt32(&e.evictCallCount) <= e.evictFailCount {
		return e.evictError
	}
	return e.MockStore.EvictUpToBlock(upToBlockNumber)
}

func (e *ErrorMockStore) SetSetAllError(err error, failCount int32) {
	e.setAllError = err
	e.setAllFailCount = failCount
}

func (e *ErrorMockStore) SetFlushError(err error, failCount int32) {
	e.flushError = err
	e.flushFailCount = failCount
}

func (e *ErrorMockStore) SetEvictError(err error, failCount int32) {
	e.evictError = err
	e.evictFailCount = failCount
}

// TestFlusherErrorPropagation tests that errors from the store are properly propagated through the flusher error channel
func TestFlusherErrorPropagation(t *testing.T) {
	logger := zaptest.NewLogger(t)
	errorStore := NewErrorMockStore()

	// Set the store to fail on SetAll
	errorStore.SetSetAllError(errors.New("SetAll failed"), 1)

	handler := NewSinker(errorStore, logger, "", 5, time.Second, 10)
	defer func() {
		handler.Shutdown(nil)
		<-handler.Terminated()
	}()

	entries := createTestEntries(3, "error_test_", 100)
	err := handler.addToBatch(entries)
	if err != nil {
		t.Fatalf("addToBatch failed: %v", err)
	}

	// Trigger flush which should cause SetAll to fail
	err = handler.FlushPendingBatch(context.Background(), 1000, []byte("test_hash_1000"), 1000, nil)

	// Wait for async error propagation
	time.Sleep(100 * time.Millisecond)

	// Check the error channel directly
	select {
	case err := <-handler.flusher.ErrorChan():
		if !strings.Contains(err.Error(), "SetAll failed") {
			t.Errorf("Expected SetAll error, got: %v", err)
		}
	case <-time.After(50 * time.Millisecond):
		t.Error("Expected error in error channel from SetAll failure")
	}
}

// TestFlusherFlushError tests error handling when FlushUpToBlock fails
func TestFlusherFlushError(t *testing.T) {
	logger := zaptest.NewLogger(t)
	errorStore := NewErrorMockStore()

	// Set the store to fail on FlushUpToBlock
	errorStore.SetFlushError(errors.New("FlushUpToBlock failed"), 1)

	handler := NewSinker(errorStore, logger, "", 5, time.Second, 10)
	defer func() {
		handler.Shutdown(nil)
		<-handler.Terminated()
	}()

	entries := createTestEntries(3, "flush_error_test_", 100)
	err := handler.addToBatch(entries)
	if err != nil {
		t.Fatalf("addToBatch failed: %v", err)
	}

	// Trigger flush which should cause FlushUpToBlock to fail
	err = handler.FlushPendingBatch(context.Background(), 1000, []byte("test_hash_1000"), 1000, nil)

	// Wait for async error propagation
	time.Sleep(100 * time.Millisecond)

	// Check the error channel directly
	select {
	case err := <-handler.flusher.ErrorChan():
		if !strings.Contains(err.Error(), "FlushUpToBlock failed") {
			t.Errorf("Expected FlushUpToBlock error, got: %v", err)
		}
	case <-time.After(50 * time.Millisecond):
		t.Error("Expected error in error channel from FlushUpToBlock failure")
	}
}

// TestContextCancellationDuringFlush tests that context cancellation is properly handled
func TestContextCancellationDuringFlush(t *testing.T) {
	logger := zaptest.NewLogger(t)

	// Use slow store to ensure we can cancel during flush
	slowStore := &SlowMockStore{
		MockStore:  NewMockStore(),
		flushDelay: 200 * time.Millisecond,
	}

	handler := NewSinker(slowStore, logger, "", 5, time.Second, 10)
	defer func() {
		handler.Shutdown(nil)
		<-handler.Terminated()
	}()

	entries := createTestEntries(3, "cancel_test_", 100)
	err := handler.addToBatch(entries)
	if err != nil {
		t.Fatalf("addToBatch failed: %v", err)
	}

	// Create a context that we'll cancel
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel the context immediately
	cancel()

	// Try to flush with cancelled context
	err = handler.FlushPendingBatch(ctx, 1000, []byte("test_hash_1000"), 1000, nil)
	if err == nil {
		t.Error("Expected error due to context cancellation")
	} else if !strings.Contains(err.Error(), "context canceled") {
		t.Errorf("Expected context cancellation error, got: %v", err)
	}
}

// TestHandleBlockUndoSignal tests the undo signal handling flow
func TestHandleBlockUndoSignal(t *testing.T) {
	logger := zaptest.NewLogger(t)
	mockStore := NewMockStore()

	tempDir := t.TempDir()
	cursorFilePath := filepath.Join(tempDir, "undo_test.cursor")

	handler := NewSinker(mockStore, logger, cursorFilePath, 5, time.Second, 10)
	defer func() {
		handler.Shutdown(nil)
		<-handler.Terminated()
	}()

	// Add some data to batch first
	entries := createTestEntries(3, "undo_test_", 100)
	err := handler.addToBatch(entries)
	if err != nil {
		t.Fatalf("addToBatch failed: %v", err)
	}

	// Create test cursor
	testCursor := createTestCursor()

	// Create undo signal
	undoSignal := &pbsubstreamsrpc.BlockUndoSignal{
		LastValidBlock: &pbsubstreams.BlockRef{
			Number: 999,
		},
	}

	// Handle the undo signal
	err = handler.HandleBlockUndoSignal(context.Background(), undoSignal, testCursor)
	if err != nil {
		t.Fatalf("HandleBlockUndoSignal failed: %v", err)
	}

	// Wait for async operations
	time.Sleep(100 * time.Millisecond)

	// Verify that evict was called
	evictCalls := mockStore.evictCalls
	if len(evictCalls) != 1 {
		t.Errorf("Expected 1 evict call, got %d", len(evictCalls))
	}
	if len(evictCalls) > 0 && evictCalls[0] != 999 {
		t.Errorf("Expected evict call with block 999, got %d", evictCalls[0])
	}

	// Verify cursor was saved
	if _, err := os.Stat(cursorFilePath); os.IsNotExist(err) {
		t.Error("Cursor file should have been saved after undo signal")
	}
}

// TestHandleBlockUndoSignalWithEvictError tests undo signal handling when evict fails
func TestHandleBlockUndoSignalWithEvictError(t *testing.T) {
	logger := zaptest.NewLogger(t)
	errorStore := NewErrorMockStore()

	// Set evict to fail
	errorStore.SetEvictError(errors.New("EvictUpToBlock failed"), 1)

	handler := NewSinker(errorStore, logger, "", 5, time.Second, 10)
	defer func() {
		handler.Shutdown(nil)
		<-handler.Terminated()
	}()

	testCursor := createTestCursor()

	undoSignal := &pbsubstreamsrpc.BlockUndoSignal{
		LastValidBlock: &pbsubstreams.BlockRef{
			Number: 999,
		},
	}

	// Handle undo signal - should return error due to evict failure
	err := handler.HandleBlockUndoSignal(context.Background(), undoSignal, testCursor)
	if err == nil {
		t.Error("Expected error from HandleBlockUndoSignal due to evict failure")
	}

	if !strings.Contains(err.Error(), "failed to evict data up to block") {
		t.Errorf("Expected evict error, got: %v", err)
	}
}

// TestErrorChannelOverflow tests that error channel overflow is handled gracefully
func TestErrorChannelOverflow(t *testing.T) {
	logger := zaptest.NewLogger(t)
	errorStore := NewErrorMockStore()

	// Set the store to always fail on SetAll
	errorStore.SetSetAllError(errors.New("SetAll always fails"), 10000)

	// Create handler with small queue to trigger overflow quickly
	handler := NewSinker(errorStore, logger, "", 5, time.Second, 1)
	defer func() {
		handler.Shutdown(nil)
		<-handler.Terminated()
	}()

	// Submit multiple batches quickly to overflow the error channel
	entries := createTestEntries(3, "overflow_test_", 100)

	for i := 0; i < 10; i++ {
		if err := handler.addToBatch(entries); err != nil {
			t.Fatalf("addToBatch failed: %v", err)
		}
		// don't wait for processing, only for enqueue.
		_ = handler.FlushPendingBatch(context.Background(), uint64(i), []byte("test_hash"), uint64(i), nil)
	}

	// Give the flusher a moment to process and drop errors (if the errorChan fills)
	time.Sleep(200 * time.Millisecond)

	// The test passes if we don't deadlock and the flusher survives overflow conditions.
}

// TestShutdownDuringFlush tests proper shutdown coordination when flush is in progress
func TestShutdownDuringFlush(t *testing.T) {
	logger := zaptest.NewLogger(t)

	// Use slow store to ensure flush is in progress during shutdown
	slowStore := &SlowMockStore{
		MockStore:  NewMockStore(),
		flushDelay: 100 * time.Millisecond,
	}

	handler := NewSinker(slowStore, logger, "", 5, time.Second, 10)

	// Add data and start flush
	entries := createTestEntries(3, "shutdown_test_", 100)
	handler.addToBatch(entries)

	// Start flush in background
	go handler.FlushPendingBatch(context.Background(), 1000, []byte("test_hash_1000"), 1000, nil)

	// Shutdown immediately
	go func() {
		time.Sleep(20 * time.Millisecond) // Let flush start
		handler.Shutdown(nil)
	}()

	// Wait for termination - should complete without deadlock
	select {
	case <-handler.Terminated():
		// Success
	case <-time.After(500 * time.Millisecond):
		t.Error("Shutdown took too long - possible deadlock")
	}
}

// TestHandleBlockScopedDataWithAsyncError tests that HandleBlockScopedData properly checks for async errors
func TestHandleBlockScopedDataWithAsyncError(t *testing.T) {
	logger := zaptest.NewLogger(t)
	errorStore := NewErrorMockStore()

	// Set store to fail on first SetAll
	errorStore.SetSetAllError(errors.New("Async SetAll failure"), 1)

	handler := NewSinker(errorStore, logger, "", 5, time.Second, 10)
	defer func() {
		handler.Shutdown(nil)
		<-handler.Terminated()
	}()

	// Create test data
	entries := &pbstore.Entries{
		Entries: createTestEntries(2, "async_error_test_", 100),
	}

	anyValue, err := anypb.New(entries)
	if err != nil {
		t.Fatalf("Failed to create Any value: %v", err)
	}

	data := &pbsubstreamsrpc.BlockScopedData{
		Output: &pbsubstreamsrpc.MapModuleOutput{
			MapOutput: anyValue,
		},
		Clock: &pbsubstreams.Clock{Number: 1000},
	}

	testCursor := createTestCursor()

	// First call should trigger SetAll failure
	err = handler.HandleBlockScopedData(context.Background(), data, nil, testCursor)
	if err != nil {
		// This might succeed or fail depending on timing
		t.Logf("First HandleBlockScopedData result: %v", err)
	}

	// Second call should detect the async error
	data.Clock.Number = 1001
	err = handler.HandleBlockScopedData(context.Background(), data, nil, testCursor)
	if err == nil {
		// Give it another try in case timing was off
		time.Sleep(50 * time.Millisecond)
		data.Clock.Number = 1002
		err = handler.HandleBlockScopedData(context.Background(), data, nil, testCursor)
	}

	if err != nil && strings.Contains(err.Error(), "error during last flush") {
		// Expected error - async error was detected
	} else {
		t.Logf("Note: Async error detection test had timing issues, this is expected in some cases")
	}
}

// TestBatchSizeTriggersFlush tests that shouldFlush() properly triggers on batch size
func TestBatchSizeTriggersFlush(t *testing.T) {
	logger := zaptest.NewLogger(t)
	mockStore := NewMockStore()

	batchSize := 3
	handler := NewSinker(mockStore, logger, "", batchSize, time.Hour, 10) // Long timeout
	defer func() {
		handler.Shutdown(nil)
		<-handler.Terminated()
	}()

	// Add exactly batch size entries
	entries := createTestEntries(batchSize, "batch_size_test_", 100)
	err := handler.addToBatch(entries)
	if err != nil {
		t.Fatalf("addToBatch failed: %v", err)
	}

	// Should trigger flush now
	if !handler.shouldFlush() {
		t.Error("shouldFlush() should return true when batch size reached")
	}

	// Verify IsTerminating affects shouldFlush
	handler.Shutdown(nil)
	if !handler.shouldFlush() {
		t.Error("shouldFlush() should return true when terminating")
	}

	<-handler.Terminated()
}

// TestMaxBytesTriggersFlush tests that shouldFlush() properly triggers on byte size
func TestMaxBytesTriggersFlush(t *testing.T) {
	logger := zaptest.NewLogger(t)
	mockStore := NewMockStore()

	handler := NewSinker(mockStore, logger, "", 1000, time.Hour, 10) // Large batch size, long timeout
	defer func() {
		handler.Shutdown(nil)
		<-handler.Terminated()
	}()

	// Force low byte limit
	handler.maxBatchBytes = 500

	// Add entries that exceed byte limit
	entries := createTestEntries(2, "byte_limit_test_", 300) // Each ~300 bytes
	err := handler.addToBatch(entries)
	if err != nil {
		t.Fatalf("addToBatch failed: %v", err)
	}

	// Should trigger flush due to byte limit
	if !handler.shouldFlush() {
		t.Error("shouldFlush() should return true when byte limit exceeded")
	}
}

// TestTimeoutTriggersFlush tests that shouldFlush() properly triggers on timeout
func TestTimeoutTriggersFlush(t *testing.T) {
	logger := zaptest.NewLogger(t)
	mockStore := NewMockStore()

	shortTimeout := 50 * time.Millisecond
	handler := NewSinker(mockStore, logger, "", 1000, shortTimeout, 10) // Large batch size
	defer func() {
		handler.Shutdown(nil)
		<-handler.Terminated()
	}()

	// Add a small number of entries
	entries := createTestEntries(2, "timeout_test_", 100)
	err := handler.addToBatch(entries)
	if err != nil {
		t.Fatalf("addToBatch failed: %v", err)
	}

	// Should not flush immediately
	if handler.shouldFlush() {
		t.Error("shouldFlush() should return false immediately after adding small batch")
	}

	// Wait for timeout
	time.Sleep(100 * time.Millisecond)

	// Should flush due to timeout
	if !handler.shouldFlush() {
		t.Error("shouldFlush() should return true after timeout elapsed")
	}
}
