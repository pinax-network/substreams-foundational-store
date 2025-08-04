package sink

import (
	"os"
	"path/filepath"
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
	logger := zaptest.NewLogger(t)

	tempDir := t.TempDir()
	emptyFilePath := filepath.Join(tempDir, "empty.cursor")

	err := os.WriteFile(emptyFilePath, []byte(""), 0644)
	if err != nil {
		t.Fatalf("Failed to create empty file: %v", err)
	}

	cursor := LoadCursorFromFile(logger, emptyFilePath)

	if cursor != nil {
		t.Errorf("Expected nil cursor for empty file, got: %v", cursor)
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

var _ store.ForkawareStore = (*MockStore)(nil)

type MockStore struct {
	mu           sync.Mutex
	setAllCalls  []SetAllCall
	flushCalls   []uint64
	evictCalls   []uint64
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
	handler := NewSinker("test", mockStore, logger, "", batchSize, maxBatchTime)
	defer handler.Close()
	
	entries := createTestEntries(12, "test_", 100)
	
	err := handler.addToBatch(entries[0:3], 1000)
	if err != nil {
		t.Fatalf("Failed to add first batch: %v", err)
	}
	
	calls := mockStore.GetSetAllCalls()
	if len(calls) != 0 {
		t.Errorf("Expected 0 SetAll calls after 3 entries, got %d", len(calls))
	}
	
	err = handler.addToBatch(entries[3:5], 1001)
	if err != nil {
		t.Fatalf("Failed to add second batch: %v", err)
	}
	
	calls = mockStore.GetSetAllCalls()
	if len(calls) != 1 {
		t.Errorf("Expected 1 SetAll call after 5 entries, got %d", len(calls))
	} else if len(calls[0].Entries) != 5 {
		t.Errorf("Expected first batch to have 5 entries, got %d", len(calls[0].Entries))
	}
	
	err = handler.addToBatch(entries[5:10], 1002)
	if err != nil {
		t.Fatalf("Failed to add third batch: %v", err)
	}
	
	calls = mockStore.GetSetAllCalls()
	if len(calls) != 2 {
		t.Errorf("Expected 2 SetAll calls after 10 entries, got %d", len(calls))
	} else if len(calls[1].Entries) != 5 {
		t.Errorf("Expected second batch to have 5 entries, got %d", len(calls[1].Entries))
	}
	
	err = handler.addToBatch(entries[10:12], 1003)
	if err != nil {
		t.Fatalf("Failed to add fourth batch: %v", err)
	}
	
	calls = mockStore.GetSetAllCalls()
	if len(calls) != 2 {
		t.Errorf("Expected still 2 SetAll calls after 12 entries, got %d", len(calls))
	}
	
	err = handler.FlushPendingBatch(1004)
	if err != nil {
		t.Fatalf("Failed to flush pending batch: %v", err)
	}
	
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
	handler := NewSinker("test", mockStore, logger, "", batchSize, maxBatchTime)
	defer handler.Close()
	
	entries := createTestEntries(3, "timeout_test_", 100)
	err := handler.addToBatch(entries, 2000)
	if err != nil {
		t.Fatalf("Failed to add entries: %v", err)
	}
	
	calls := mockStore.GetSetAllCalls()
	if len(calls) != 0 {
		t.Errorf("Expected 0 SetAll calls immediately, got %d", len(calls))
	}
	
	time.Sleep(100 * time.Millisecond)
	
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
	
	batchSize := 1000
	maxBatchTime := 10 * time.Second
	handler := NewSinker("test", mockStore, logger, "", batchSize, maxBatchTime)
	defer handler.Close()
	
	handler.maxBatchBytes = 1000
	
	entries := createTestEntries(2, "large_", 600)
	
	err := handler.addToBatch(entries[0:1], 3000)
	if err != nil {
		t.Fatalf("Failed to add first entry: %v", err)
	}
	
	calls := mockStore.GetSetAllCalls()
	if len(calls) != 0 {
		t.Errorf("Expected 0 SetAll calls after first large entry, got %d", len(calls))
	}
	
	err = handler.addToBatch(entries[1:2], 3001)
	if err != nil {
		t.Fatalf("Failed to add second entry: %v", err)
	}
	
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
	handler := NewSinker("test", mockStore, logger, "", batchSize, maxBatchTime)
	
	entries := createTestEntries(3, "close_test_", 100)
	err := handler.addToBatch(entries, 4000)
	if err != nil {
		t.Fatalf("Failed to add entries: %v", err)
	}
	
	calls := mockStore.GetSetAllCalls()
	if len(calls) != 0 {
		t.Errorf("Expected 0 SetAll calls before close, got %d", len(calls))
	}
	
	err = handler.Close()
	if err != nil {
		t.Fatalf("Failed to close handler: %v", err)
	}
	
	calls = mockStore.GetSetAllCalls()
	if len(calls) != 1 {
		t.Errorf("Expected 1 SetAll call after close, got %d", len(calls))
	} else if len(calls[0].Entries) != 3 {
		t.Errorf("Expected batch to have 3 entries, got %d", len(calls[0].Entries))
	}
	
	time.Sleep(200 * time.Millisecond)
	
	calls = mockStore.GetSetAllCalls()
	if len(calls) != 1 {
		t.Errorf("Expected still 1 SetAll call after waiting (timer should be stopped), got %d", len(calls))
	}
}

func TestConcurrentBatching(t *testing.T) {
	logger := zaptest.NewLogger(t)
	mockStore := NewMockStore()
	
	batchSize := 10
	maxBatchTime := 1 * time.Second
	handler := NewSinker("test", mockStore, logger, "", batchSize, maxBatchTime)
	defer handler.Close()
	
	var wg sync.WaitGroup
	numGoroutines := 5
	entriesPerGoroutine := 4
	
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			entries := createTestEntries(entriesPerGoroutine, "concurrent_", 50)
			err := handler.addToBatch(entries, uint64(5000+goroutineID))
			if err != nil {
				t.Errorf("Goroutine %d failed to add entries: %v", goroutineID, err)
			}
		}(i)
	}
	
	wg.Wait()
	
	calls := mockStore.GetSetAllCalls()
	if len(calls) == 0 {
		t.Errorf("Expected at least 1 SetAll call from concurrent access, got %d", len(calls))
	}
	
	handler.FlushPendingBatch(5999)
	
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
