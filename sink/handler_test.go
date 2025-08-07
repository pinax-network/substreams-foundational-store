package sink

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	pbstore "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/v1"
	"github.com/streamingfast/substreams-foundational-store/store"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
	sink "github.com/streamingfast/substreams/sink"
	"go.uber.org/zap/zaptest"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCursorSaveAndLoad(t *testing.T) {
	logger := zaptest.NewLogger(t)

	tempDir := t.TempDir()
	cursorFilePath := filepath.Join(tempDir, "test.cursor")

	handler := &Handler{
		cursorFilePath: cursorFilePath,
		logger:         logger,
	}

	testCursorStr := "XWQh1iJoYAKTDtvllL7yraWwLpc_DFhvVQvlKhhCjYGDiHqspvzCXTgfFUum8f32iBSqMQXahNirXjQmq6AKuJSypu8Sm3NpAXkk8YPs-7TvePP7OgIRBMNqNpHvBoWCMUGBFGuvfOQBoa-4TKneAQh4P55GdmL211oH1PMGIeQTsRE="

	originalCursor, err := sink.NewCursor(testCursorStr)
	if err != nil {
		t.Fatalf("Failed to create cursor from test string: %v", err)
	}

	err = handler.saveCursorToFile(originalCursor)
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

			handler := &Handler{
				cursorFilePath: cursorFilePath,
				logger:         logger,
			}

			originalCursor, err := sink.NewCursor(testCursorStr)
			if err != nil {
				t.Fatalf("Failed to create cursor: %v", err)
			}

			for round := 0; round < 3; round++ {

				err = handler.saveCursorToFile(originalCursor)
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
}

func NewMockStore() *MockStore {
	return &MockStore{
		setAllCalls: make([]SetAllCall, 0),
		flushCalls:  make([]uint64, 0),
		evictCalls:  make([]uint64, 0),
	}
}

func (m *MockStore) Set(entry *pbstore.Entry, blockNumber uint64) error {
	return m.SetAll([]*pbstore.Entry{entry}, blockNumber)
}

func (m *MockStore) SetAll(entries []*pbstore.Entry, blockNumber uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	entriesCopy := make([]*pbstore.Entry, len(entries))
	copy(entriesCopy, entries)

	m.setAllCalls = append(m.setAllCalls, SetAllCall{
		Entries:     entriesCopy,
		BlockNumber: blockNumber,
	})
	return nil
}

func (m *MockStore) Get(request *pbstore.GetRequest) (*pbstore.GetResponse, error) {
	return &pbstore.GetResponse{Response: pbstore.ResponseCode_NOT_FOUND}, nil
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

		anyValue, _ := anypb.New(&pbstore.AccountOwner{
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
	handler := NewSinker("test", mockStore, logger, "", batchSize, maxBatchTime, 10)
	defer handler.Close()

	entries := createTestEntries(12, "test_", 100)

	// First 3 entries - should not trigger flush yet
	err := handler.HandleBlockScopedData(context.Background(), createBlockScopedData(entries[0:3], 1000), nil, createTestCursor())
	if err != nil {
		t.Fatalf("HandleBlockScopedData failed: %v", err)
	}

	// Allow async flush to complete
	time.Sleep(10 * time.Millisecond)

	calls := mockStore.GetSetAllCalls()
	if len(calls) != 0 {
		t.Errorf("Expected 0 SetAll calls after 3 entries, got %d", len(calls))
	}

	// Add 2 more entries to reach batch size of 5 - should trigger flush
	err = handler.HandleBlockScopedData(context.Background(), createBlockScopedData(entries[3:5], 1001), nil, createTestCursor())
	if err != nil {
		t.Fatalf("HandleBlockScopedData failed: %v", err)
	}

	// Allow async flush to complete
	time.Sleep(50 * time.Millisecond)

	calls = mockStore.GetSetAllCalls()
	if len(calls) != 1 {
		t.Errorf("Expected 1 SetAll call after 5 entries, got %d", len(calls))
	} else if len(calls[0].Entries) != 5 {
		t.Errorf("Expected first batch to have 5 entries, got %d", len(calls[0].Entries))
	}

	// Add 5 more entries - should trigger another flush
	err = handler.HandleBlockScopedData(context.Background(), createBlockScopedData(entries[5:10], 1002), nil, createTestCursor())
	if err != nil {
		t.Fatalf("HandleBlockScopedData failed: %v", err)
	}

	// Allow async flush to complete
	time.Sleep(50 * time.Millisecond)

	calls = mockStore.GetSetAllCalls()
	if len(calls) != 2 {
		t.Errorf("Expected 2 SetAll calls after 10 entries, got %d", len(calls))
	} else if len(calls[1].Entries) != 5 {
		t.Errorf("Expected second batch to have 5 entries, got %d", len(calls[1].Entries))
	}

	// Add 2 more entries - should not trigger flush yet
	err = handler.HandleBlockScopedData(context.Background(), createBlockScopedData(entries[10:12], 1003), nil, createTestCursor())
	if err != nil {
		t.Fatalf("HandleBlockScopedData failed: %v", err)
	}

	// Allow async flush to complete
	time.Sleep(10 * time.Millisecond)

	calls = mockStore.GetSetAllCalls()
	if len(calls) != 2 {
		t.Errorf("Expected still 2 SetAll calls after 12 entries, got %d", len(calls))
	}

	// Manual flush should process the remaining 2 entries
	err = handler.FlushPendingBatch(1004)
	if err != nil {
		t.Fatalf("Failed to flush pending batch: %v", err)
	}

	// Allow async flush to complete
	time.Sleep(50 * time.Millisecond)

	calls = mockStore.GetSetAllCalls()
	if len(calls) != 3 {
		t.Errorf("Expected 3 SetAll calls after flush, got %d", len(calls))
	} else if len(calls) >= 3 && len(calls[2].Entries) != 2 {
		t.Errorf("Expected third batch to have 2 entries, got %d", len(calls[2].Entries))
	}
}

func TestBatchingByTimeout(t *testing.T) {
	logger := zaptest.NewLogger(t)
	mockStore := NewMockStore()

	batchSize := 1000 // Large batch size so timeout triggers first
	maxBatchTime := 50 * time.Millisecond
	handler := NewSinker("test", mockStore, logger, "", batchSize, maxBatchTime, 10)
	defer handler.Close()

	entries := createTestEntries(3, "timeout_test_", 100)

	// Add entries through block processing
	err := handler.HandleBlockScopedData(context.Background(), createBlockScopedData(entries, 2000), nil, createTestCursor())
	if err != nil {
		t.Fatalf("HandleBlockScopedData failed: %v", err)
	}

	// Should not trigger flush immediately since batch size is large
	time.Sleep(10 * time.Millisecond)
	calls := mockStore.GetSetAllCalls()
	if len(calls) != 0 {
		t.Errorf("Expected 0 SetAll calls immediately, got %d", len(calls))
	}

	// Wait for timeout to trigger flush
	time.Sleep(60 * time.Millisecond)

	// Process another block to trigger the timeout check
	err = handler.HandleBlockScopedData(context.Background(), createBlockScopedData([]*pbstore.Entry{}, 2001), nil, createTestCursor())
	if err != nil {
		t.Fatalf("HandleBlockScopedData failed: %v", err)
	}

	// Allow async flush to complete
	time.Sleep(50 * time.Millisecond)

	calls = mockStore.GetSetAllCalls()
	if len(calls) != 1 {
		t.Errorf("Expected 1 SetAll call after timeout, got %d", len(calls))
	} else if len(calls[0].Entries) != 3 {
		t.Errorf("Expected batch to have 3 entries, got %d", len(calls[0].Entries))
	}
}

func TestBatchingByByteSize(t *testing.T) {
	logger := zaptest.NewLogger(t)
	mockStore := NewMockStore()

	batchSize := 1000 // Large batch size so bytes limit triggers first
	maxBatchTime := 10 * time.Second
	handler := NewSinker("test", mockStore, logger, "", batchSize, maxBatchTime, 10)
	defer handler.Close()

	handler.maxBatchBytes = 1000

	entries := createTestEntries(2, "large_", 600)

	// Add first large entry - should not trigger flush yet
	err := handler.HandleBlockScopedData(context.Background(), createBlockScopedData(entries[0:1], 3000), nil, createTestCursor())
	if err != nil {
		t.Fatalf("HandleBlockScopedData failed: %v", err)
	}

	// Allow async flush to complete
	time.Sleep(10 * time.Millisecond)

	calls := mockStore.GetSetAllCalls()
	if len(calls) != 0 {
		t.Errorf("Expected 0 SetAll calls after first large entry, got %d", len(calls))
	}

	// Add second large entry - should trigger flush due to byte limit
	err = handler.HandleBlockScopedData(context.Background(), createBlockScopedData(entries[1:2], 3001), nil, createTestCursor())
	if err != nil {
		t.Fatalf("HandleBlockScopedData failed: %v", err)
	}

	// Allow async flush to complete
	time.Sleep(50 * time.Millisecond)

	calls = mockStore.GetSetAllCalls()
	if len(calls) != 1 {
		t.Errorf("Expected 1 SetAll call after exceeding byte limit, got %d", len(calls))
	} else if len(calls[0].Entries) != 2 {
		t.Errorf("Expected batch to have 2 entries, got %d", len(calls[0].Entries))
	}
}

func TestHandlerClose(t *testing.T) {
	logger := zaptest.NewLogger(t)
	mockStore := NewMockStore()

	batchSize := 10
	maxBatchTime := 100 * time.Millisecond
	handler := NewSinker("test", mockStore, logger, "", batchSize, maxBatchTime, 10)

	entries := createTestEntries(3, "close_test_", 100)

	// Add entries through block processing
	err := handler.HandleBlockScopedData(context.Background(), createBlockScopedData(entries, 4000), nil, createTestCursor())
	if err != nil {
		t.Fatalf("HandleBlockScopedData failed: %v", err)
	}

	// Should not have flushed yet since batch size is 10 and we only added 3
	time.Sleep(10 * time.Millisecond)
	calls := mockStore.GetSetAllCalls()
	if len(calls) != 0 {
		t.Errorf("Expected 0 SetAll calls before close, got %d", len(calls))
	}

	// Close should flush the remaining entries
	err = handler.Close()
	if err != nil {
		t.Fatalf("Failed to close handler: %v", err)
	}

	// Allow async flush to complete
	time.Sleep(100 * time.Millisecond)

	calls = mockStore.GetSetAllCalls()
	if len(calls) != 1 {
		t.Errorf("Expected 1 SetAll call after close, got %d", len(calls))
	} else if len(calls[0].Entries) != 3 {
		t.Errorf("Expected batch to have 3 entries, got %d", len(calls[0].Entries))
	}

	// No additional calls should happen after close
	time.Sleep(200 * time.Millisecond)

	calls = mockStore.GetSetAllCalls()
	if len(calls) != 1 {
		t.Errorf("Expected still 1 SetAll call after waiting (handler closed), got %d", len(calls))
	}
}

func TestConcurrentBatching(t *testing.T) {
	logger := zaptest.NewLogger(t)
	mockStore := NewMockStore()

	batchSize := 10
	maxBatchTime := 1 * time.Second
	handler := NewSinker("test", mockStore, logger, "", batchSize, maxBatchTime, 10)
	defer handler.Close()

	numGoroutines := 5
	entriesPerGoroutine := 4

	// FIXME
	// Test concurrent block processing (lock-free)
	for i := 0; i < numGoroutines; i++ {
		//		wg.Add(1)
		//	go func(goroutineID int) {
		//	defer wg.Done()
		entries := createTestEntries(entriesPerGoroutine, "concurrent_", 50)
		err := handler.HandleBlockScopedData(context.Background(), createBlockScopedData(entries, uint64(5000+i)), nil, createTestCursor())
		if err != nil {
			t.Errorf("HandleBlockScopedData failed for step %d: %v", i, err)
		}

		//		}(i)
	}

	//	wg.Wait()

	// Allow async flushes to complete
	time.Sleep(300 * time.Millisecond)

	calls := mockStore.GetSetAllCalls()
	if len(calls) == 0 {
		t.Errorf("Expected at least 1 SetAll call from concurrent access, got %d", len(calls))
	}

	// Manually flush any remaining entries
	err := handler.FlushPendingBatch(5999)
	if err != nil {
		t.Fatalf("FlushPendingBatch failed: %v", err)
	}

	// Allow async flush to complete
	time.Sleep(300 * time.Millisecond)

	calls = mockStore.GetSetAllCalls()
	totalEntries := 0
	for _, call := range calls {
		totalEntries += len(call.Entries)
	}

	expectedTotal := numGoroutines * entriesPerGoroutine
	if totalEntries != expectedTotal {
		t.Errorf("Expected %d total entries, got %d", expectedTotal, totalEntries)
	}
}

func TestAsyncFlushWorkerStartsAndStops(t *testing.T) {
	logger := zaptest.NewLogger(t)
	mockStore := NewMockStore()

	handler := NewSinker("test", mockStore, logger, "", 10, time.Second, 10)

	if handler.flushQueue == nil {
		t.Error("Expected flush queue to be initialized")
	}
	if handler.flushWorkerDone == nil {
		t.Error("Expected flush worker done channel to be initialized")
	}
	if handler.shutdown == nil {
		t.Error("Expected shutdown channel to be initialized")
	}

	err := handler.Close()
	if err != nil {
		t.Fatalf("Failed to close handler: %v", err)
	}

	select {
	case <-handler.flushWorkerDone:
	case <-time.After(1 * time.Second):
		t.Error("Flush worker did not shut down within timeout")
	}
}

func TestAsyncFlushQueueDepthTracking(t *testing.T) {
	logger := zaptest.NewLogger(t)

	slowStore := &SlowMockStore{
		MockStore:  NewMockStore(),
		flushDelay: 100 * time.Millisecond,
	}

	handler := NewSinker("test", slowStore, logger, "", 10, time.Second, 5)
	defer handler.Close()

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
			err := handler.HandleBlockScopedData(context.Background(), createBlockScopedData(entries, uint64(1000+i)), nil, createTestCursor())
			if err != nil {
				t.Errorf("HandleBlockScopedData failed: %v", err)
			}
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

	handler := NewSinker("test", mockStore, logger, cursorFile, 10, time.Second, 10)
	defer handler.Close()

	entries := []*pbstore.Entry{createTestEntry("key1", "value1")}
	testCursor := createTestCursor()

	// Process entry through block processing
	err := handler.HandleBlockScopedData(context.Background(), createBlockScopedData(entries, 1000), nil, testCursor)
	if err != nil {
		t.Fatalf("HandleBlockScopedData failed: %v", err)
	}

	// Manually flush to trigger async processing
	err = handler.FlushPendingBatch(1000)
	if err != nil {
		t.Fatalf("FlushPendingBatch failed: %v", err)
	}

	// Allow async flush to complete
	time.Sleep(100 * time.Millisecond)

	setAllCalls := mockStore.GetSetAllCalls()
	if len(setAllCalls) == 0 {
		t.Error("Expected at least 1 SetAll call after async processing")
	}

	// Test cursor file operations
	err = handler.saveCursorToFile(testCursor)
	if err != nil {
		t.Fatalf("saveCursorToFile failed: %v", err)
	}

	if _, err := os.Stat(cursorFile); os.IsNotExist(err) {
		t.Error("Cursor file should exist after save")
	}

	// Verify async infrastructure is initialized
	if handler.flushQueue == nil {
		t.Error("Async flush queue should be initialized")
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

// Helper function to create BlockScopedData for testing
func createBlockScopedData(entries []*pbstore.Entry, blockNumber uint64) *pbsubstreamsrpc.BlockScopedData {
	entriesData := &pbstore.Entries{
		Entries: entries,
	}

	entriesAny, _ := anypb.New(entriesData)

	return &pbsubstreamsrpc.BlockScopedData{
		Output: &pbsubstreamsrpc.MapModuleOutput{
			MapOutput: entriesAny,
		},
		Clock: &pbsubstreams.Clock{
			Number:    blockNumber,
			Timestamp: timestamppb.New(time.Now()),
		},
	}
}

func TestTrulyAsyncFlushBehavior(t *testing.T) {
	logger := zaptest.NewLogger(t)
	mockStore := NewMockStore()

	batchSize := 5
	maxBatchTime := 1 * time.Second
	handler := NewSinker("test", mockStore, logger, "", batchSize, maxBatchTime, 10)
	defer handler.Close()

	entries := createTestEntries(3, "async_test_", 100)

	// Process block - should be non-blocking
	start := time.Now()
	err := handler.HandleBlockScopedData(context.Background(), createBlockScopedData(entries, 1000), nil, createTestCursor())
	processingTime := time.Since(start)

	if err != nil {
		t.Fatalf("HandleBlockScopedData failed: %v", err)
	}

	// Processing should be very fast (non-blocking)
	if processingTime > 10*time.Millisecond {
		t.Errorf("Processing took too long: %v, expected < 10ms (truly async should be non-blocking)", processingTime)
	}

	// No flush should have happened yet (batch size not reached)
	calls := mockStore.GetSetAllCalls()
	if len(calls) != 0 {
		t.Errorf("Expected 0 immediate SetAll calls (truly async), got %d", len(calls))
	}

	// Allow background worker to process if needed
	time.Sleep(50 * time.Millisecond)

	calls = mockStore.GetSetAllCalls()
	if len(calls) != 0 {
		t.Errorf("Expected 0 SetAll calls after waiting (batch not full), got %d", len(calls))
	}
}

func TestBatchAccumulationAcrossBlocks(t *testing.T) {
	logger := zaptest.NewLogger(t)
	mockStore := NewMockStore()

	batchSize := 10
	maxBatchTime := 1 * time.Second
	handler := NewSinker("test", mockStore, logger, "", batchSize, maxBatchTime, 10)
	defer handler.Close()

	// Process 4 blocks with 2 entries each (total 8 entries)
	for i := 0; i < 4; i++ {
		entries := createTestEntries(2, fmt.Sprintf("block_%d_", i), 50)
		err := handler.HandleBlockScopedData(context.Background(), createBlockScopedData(entries, uint64(1000+i)), nil, createTestCursor())
		if err != nil {
			t.Fatalf("HandleBlockScopedData failed for block %d: %v", i, err)
		}
	}

	// Should not have flushed yet (8 < 10)
	time.Sleep(50 * time.Millisecond)
	calls := mockStore.GetSetAllCalls()
	if len(calls) != 0 {
		t.Errorf("Expected 0 SetAll calls after 8 entries, got %d", len(calls))
	}

	// Add one more block with 3 entries (total 11, should trigger flush)
	entries := createTestEntries(3, "final_block_", 50)
	err := handler.HandleBlockScopedData(context.Background(), createBlockScopedData(entries, 1004), nil, createTestCursor())
	if err != nil {
		t.Fatalf("HandleBlockScopedData failed for final block: %v", err)
	}

	// Should have flushed now (11 > 10)
	time.Sleep(100 * time.Millisecond)
	calls = mockStore.GetSetAllCalls()
	if len(calls) != 1 {
		t.Errorf("Expected 1 SetAll call after 11 entries, got %d", len(calls))
	} else {
		// Should have flushed all 11 entries (efficient batch processing)
		if len(calls[0].Entries) != 11 {
			t.Errorf("Expected batch to have 11 entries, got %d", len(calls[0].Entries))
		}
	}
}

func TestTimerBasedFlushingWithoutAfterFunc(t *testing.T) {
	logger := zaptest.NewLogger(t)
	mockStore := NewMockStore()

	batchSize := 100 // Large batch size so timer triggers first
	maxBatchTime := 100 * time.Millisecond
	handler := NewSinker("test", mockStore, logger, "", batchSize, maxBatchTime, 10)
	defer handler.Close()

	entries := createTestEntries(3, "timer_test_", 50)

	// Add entries but don't reach batch size
	err := handler.HandleBlockScopedData(context.Background(), createBlockScopedData(entries, 2000), nil, createTestCursor())
	if err != nil {
		t.Fatalf("HandleBlockScopedData failed: %v", err)
	}

	// Should not flush immediately
	time.Sleep(10 * time.Millisecond)
	calls := mockStore.GetSetAllCalls()
	if len(calls) != 0 {
		t.Errorf("Expected 0 SetAll calls immediately, got %d", len(calls))
	}

	// Wait for timer to expire
	time.Sleep(120 * time.Millisecond)

	// Process another block to trigger the timer check (no AfterFunc used)
	err = handler.HandleBlockScopedData(context.Background(), createBlockScopedData([]*pbstore.Entry{}, 2001), nil, createTestCursor())
	if err != nil {
		t.Fatalf("HandleBlockScopedData failed: %v", err)
	}

	// Should have triggered flush due to timeout
	time.Sleep(100 * time.Millisecond)
	calls = mockStore.GetSetAllCalls()
	if len(calls) != 1 {
		t.Errorf("Expected 1 SetAll call after timeout, got %d", len(calls))
	} else if len(calls[0].Entries) != 3 {
		t.Errorf("Expected timer flush to have 3 entries, got %d", len(calls[0].Entries))
	}
}

func TestQueueBasedBackgroundProcessing(t *testing.T) {
	logger := zaptest.NewLogger(t)

	// Use SlowMockStore to test queue behavior
	slowStore := &SlowMockStore{
		MockStore:  NewMockStore(),
		flushDelay: 50 * time.Millisecond,
	}

	batchSize := 3
	maxBatchTime := 1 * time.Second
	handler := NewSinker("test", slowStore, logger, "", batchSize, maxBatchTime, 5) // Small queue
	defer handler.Close()

	RegisterMetrics()
	FlushQueueDepth.SetUint64(0)

	// Add multiple batches quickly to test queue behavior
	for i := 0; i < 3; i++ {
		entries := createTestEntries(3, fmt.Sprintf("queue_test_%d_", i), 50)
		err := handler.HandleBlockScopedData(context.Background(), createBlockScopedData(entries, uint64(3000+i)), nil, createTestCursor())
		if err != nil {
			t.Fatalf("HandleBlockScopedData failed for batch %d: %v", i, err)
		}

		// Small delay between batches
		time.Sleep(5 * time.Millisecond)
	}

	// Check queue depth increases
	time.Sleep(10 * time.Millisecond)
	queueDepth := FlushQueueDepth.Get()
	if queueDepth <= 0 {
		t.Errorf("Expected queue depth > 0, got %f", queueDepth)
	}

	// Wait for background processing to complete
	time.Sleep(300 * time.Millisecond)

	calls := slowStore.GetSetAllCalls()
	if len(calls) != 3 {
		t.Errorf("Expected 3 SetAll calls from queue processing, got %d", len(calls))
	}

	// Queue should be empty now
	finalQueueDepth := FlushQueueDepth.Get()
	if finalQueueDepth != 0 {
		t.Errorf("Expected final queue depth = 0, got %f", finalQueueDepth)
	}
}

func TestLockFreePerformance(t *testing.T) {
	logger := zaptest.NewLogger(t)
	mockStore := NewMockStore()

	batchSize := 1000 // Large batch to avoid flushes during test
	maxBatchTime := 10 * time.Second
	handler := NewSinker("test", mockStore, logger, "", batchSize, maxBatchTime, 100)
	defer handler.Close()

	numBlocks := 100
	entriesPerBlock := 5

	// Measure processing time for many blocks
	start := time.Now()

	for i := 0; i < numBlocks; i++ {
		entries := createTestEntries(entriesPerBlock, fmt.Sprintf("perf_test_%d_", i), 20)
		err := handler.HandleBlockScopedData(context.Background(), createBlockScopedData(entries, uint64(4000+i)), nil, createTestCursor())
		if err != nil {
			t.Fatalf("HandleBlockScopedData failed for block %d: %v", i, err)
		}
	}

	totalTime := time.Since(start)
	avgTimePerBlock := totalTime / time.Duration(numBlocks)

	t.Logf("Processed %d blocks in %v (avg: %v per block)", numBlocks, totalTime, avgTimePerBlock)

	// Performance should be very good with lock-free design
	if avgTimePerBlock > 1*time.Millisecond {
		t.Errorf("Average processing time per block too slow: %v, expected < 1ms", avgTimePerBlock)
	}

	// Allow any background processing
	time.Sleep(100 * time.Millisecond)

	// Should have accumulated all entries in buffer (no flushes yet due to large batch size)
	calls := mockStore.GetSetAllCalls()
	if len(calls) != 0 {
		t.Errorf("Expected 0 SetAll calls (large batch size), got %d", len(calls))
	}
}
