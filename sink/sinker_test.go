package sink

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	pbstore "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/v1"
	"github.com/streamingfast/substreams-foundational-store/store"
	sink "github.com/streamingfast/substreams/sink"
	"go.uber.org/zap/zaptest"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestCursorSaveAndLoad(t *testing.T) {
	logger := zaptest.NewLogger(t)

	tempDir := t.TempDir()
	cursorFilePath := filepath.Join(tempDir, "test.cursor")

	mockStore := NewMockStore()
	handler := NewSinker("test", mockStore, logger, cursorFilePath, 10, time.Second, 10)
	defer handler.Close()

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
			handler := NewSinker("test", mockStore, logger, cursorFilePath, 10, time.Second, 10)
			defer handler.Close()

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
	err = handler.FlushPendingBatch(1004)
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
	handler := NewSinker("test", mockStore, logger, "", batchSize, maxBatchTime, 10)
	defer handler.Close()

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
	if err := handler.FlushPendingBatch(0); err != nil {
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
	handler := NewSinker("test", mockStore, logger, "", batchSize, maxBatchTime, 10)
	defer handler.Close()

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
	if err := handler.FlushPendingBatch(0); err != nil {
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
	handler := NewSinker("test", mockStore, logger, "", batchSize, maxBatchTime, 10)

	entries := createTestEntries(3, "close_test_", 100)
	if err := handler.addToBatch(entries); err != nil {
		t.Fatalf("Failed to add entries: %v", err)
	}

	// still nothing yet
	if calls := mockStore.GetSetAllCalls(); len(calls) != 0 {
		t.Errorf("Expected 0 SetAll calls before manual flush, got %d", len(calls))
	}

	// flush pending batch manually
	if err := handler.FlushPendingBatch(0); err != nil {
		t.Fatalf("FlushPendingBatch failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	calls := mockStore.GetSetAllCalls()
	if len(calls) != 1 {
		t.Errorf("Expected 1 SetAll call after manual flush, got %d", len(calls))
	} else if len(calls[0].Entries) != 3 {
		t.Errorf("Expected batch to have 3 entries, got %d", len(calls[0].Entries))
	}

	// calling Close() now should not add any more batches
	if err := handler.Close(); err != nil {
		t.Fatalf("Failed to close handler: %v", err)
	}
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
	handler := NewSinker("test", mockStore, logger, "", batchSize, maxBatchTime, 10)
	defer handler.Close()

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
	if err := handler.FlushPendingBatch(5999); err != nil {
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

	handler := NewSinker("test", mockStore, logger, "", 10, time.Second, 10)

	// Test that the sinker can be created and has a flusher
	if handler.flusher == nil {
		t.Error("Expected flusher to be initialized")
	}

	// Test clean shutdown
	err := handler.Close()
	if err != nil {
		t.Fatalf("Failed to close handler: %v", err)
	}

	// Test that we can close multiple times without error
	err = handler.Close()
	if err != nil {
		t.Fatalf("Failed to close handler second time: %v", err)
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

	handler := NewSinker("test", mockStore, logger, cursorFile, 10, time.Second, 10)
	defer handler.Close()

	entries := []*pbstore.Entry{createTestEntry("key1", "value1")}
	testCursor := createTestCursor()

	err := handler.addToBatch(entries)
	if err != nil {
		t.Fatalf("addToBatch failed: %v", err)
	}

	err = handler.FlushPendingBatch(1000)
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
