package ForkAware

import (
	"testing"

	pbmodel "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/model/v1"
	pbservice "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/service/v1"
	"google.golang.org/protobuf/types/known/anypb"
)

type mockStoreCachedEntry struct {
	entry       *pbmodel.Entry
	blockNumber uint64
}

// mockStore is a simple in-memory implementation of the store.Store interface for testing
// It mimics basic fork-aware semantics based on block numbers only.
type mockStore struct {
	entries map[string]mockStoreCachedEntry
}

func newMockStore() *mockStore {
	return &mockStore{entries: make(map[string]mockStoreCachedEntry)}
}

func (m *mockStore) Set(entry *pbmodel.Entry, blockNumber uint64) error {
	m.entries[string(entry.Key)] = mockStoreCachedEntry{entry: entry, blockNumber: blockNumber}
	return nil
}

func (m *mockStore) SetAll(entries []*pbmodel.Entry, blockNumber uint64) error {
	for _, entry := range entries {
		m.entries[string(entry.Key)] = mockStoreCachedEntry{entry: entry, blockNumber: blockNumber}
	}
	return nil
}

func (m *mockStore) Get(request *pbservice.GetRequest) (*pbservice.GetResponse, error) {
	cached, ok := m.entries[string(request.Key)]
	resp := &pbservice.GetResponse{}
	if !ok || cached.blockNumber > request.BlockNumber {
		resp.Entry = &pbmodel.QueriedEntry{Code: pbmodel.ResponseCode_RESPONSE_CODE_NOT_FOUND}
		return resp, nil
	}
	resp.Entry = &pbmodel.QueriedEntry{
		Code:  pbmodel.ResponseCode_RESPONSE_CODE_FOUND,
		Entry: cached.entry,
	}
	return resp, nil
}

func (m *mockStore) GetAll(request *pbservice.GetAllRequest) (*pbservice.GetAllResponse, error) {
	entries := make([]*pbmodel.QueriedEntry, 0, len(request.Keys))
	for _, key := range request.Keys {
		cached, ok := m.entries[string(key)]
		if !ok || cached.blockNumber > request.BlockNumber {
			entries = append(entries, &pbmodel.QueriedEntry{Code: pbmodel.ResponseCode_RESPONSE_CODE_NOT_FOUND})
		} else {
			entries = append(entries, &pbmodel.QueriedEntry{
				Code:  pbmodel.ResponseCode_RESPONSE_CODE_FOUND,
				Entry: cached.entry,
			})
		}
	}
	return &pbservice.GetAllResponse{Entries: &pbmodel.QueriedEntries{Entries: entries}}, nil
}

func (m *mockStore) GetFirst(request *pbservice.GetFirstRequest) (*pbservice.GetResponse, error) {
	// Simple mock: try exact match; otherwise return the lexicographic next key >= requested
	if cached, ok := m.entries[string(request.Key)]; ok {
		return &pbservice.GetResponse{Entry: &pbmodel.QueriedEntry{Code: pbmodel.ResponseCode_RESPONSE_CODE_FOUND, Entry: cached.entry}}, nil
	}
	var bestKey string
	for k := range m.entries {
		if bestKey == "" {
			if k >= string(request.Key) {
				bestKey = k
			}
			continue
		}
		if k >= string(request.Key) && k < bestKey {
			bestKey = k
		}
	}
	if bestKey != "" {
		ce := m.entries[bestKey]
		return &pbservice.GetResponse{Entry: &pbmodel.QueriedEntry{Code: pbmodel.ResponseCode_RESPONSE_CODE_FOUND, Entry: ce.entry}}, nil
	}
	return &pbservice.GetResponse{Entry: &pbmodel.QueriedEntry{Code: pbmodel.ResponseCode_RESPONSE_CODE_NOT_FOUND}}, nil
}

func TestCacheStore(t *testing.T) {
	// Create a mock store
	mockStore := newMockStore()

	// Create a ForkAware store that wraps the mock store
	cacheStore := NewStore(mockStore)

	// Create some test entries
	entry1 := &pbmodel.Entry{Key: []byte("key1"), Value: &anypb.Any{TypeUrl: "test", Value: []byte("value1")}}
	entry2 := &pbmodel.Entry{Key: []byte("key2"), Value: &anypb.Any{TypeUrl: "test", Value: []byte("value2")}}
	entry3 := &pbmodel.Entry{Key: []byte("key3"), Value: &anypb.Any{TypeUrl: "test", Value: []byte("value3")}}

	// Set entries in the ForkAware store
	if err := cacheStore.Set(entry1, 100); err != nil {
		t.Fatalf("Failed to set entry1: %v", err)
	}
	if err := cacheStore.Set(entry2, 200); err != nil {
		t.Fatalf("Failed to set entry2: %v", err)
	}
	if err := cacheStore.Set(entry3, 300); err != nil {
		t.Fatalf("Failed to set entry3: %v", err)
	}

	// Verify that entries are in the ForkAware but not in the mock store (flushUpToBlock defaults to 0)
	if len(mockStore.entries) != 0 {
		t.Errorf("Expected 0 entries in mock store, got %d", len(mockStore.entries))
	}

	// Get entry1 from the ForkAware store
	resp1, err := cacheStore.Get(&pbservice.GetRequest{BlockNumber: 150, Key: []byte("key1")})
	if err != nil {
		t.Fatalf("Failed to get entry1: %v", err)
	}
	if resp1.Entry.Code != pbmodel.ResponseCode_RESPONSE_CODE_FOUND {
		t.Errorf("Expected FOUND response for entry1, got %v", resp1.Entry.Code)
	}

	// Flush entries with block numbers <= 200
	if err := cacheStore.FlushUpToBlock(200); err != nil {
		t.Fatalf("Failed to flush entries: %v", err)
	}

	// Verify that entry1 and entry2 are now in the mock store
	if len(mockStore.entries) != 2 {
		t.Errorf("Expected 2 entries in mock store, got %d", len(mockStore.entries))
	}

	// Verify that entry1 and entry2 are no longer in the ForkAware by checking that wrapped store's value is returned
	mockStore.entries["key1"] = mockStoreCachedEntry{blockNumber: 100, entry: &pbmodel.Entry{Key: []byte("key1"), Value: &anypb.Any{TypeUrl: "test", Value: []byte("modified1")}}}

	resp1, err = cacheStore.Get(&pbservice.GetRequest{BlockNumber: 150, Key: []byte("key1")})
	if err != nil {
		t.Fatalf("Failed to get entry1: %v", err)
	}
	if got := string(resp1.Entry.Entry.Value.Value); got != "modified1" {
		t.Errorf("Expected modified value for entry1, got %s", got)
	}

	// Verify that entry3 is still in the ForkAware
	resp3, err := cacheStore.Get(&pbservice.GetRequest{BlockNumber: 350, Key: []byte("key3")})
	if err != nil {
		t.Fatalf("Failed to get entry3: %v", err)
	}
	if resp3.Entry.Code != pbmodel.ResponseCode_RESPONSE_CODE_FOUND {
		t.Errorf("Expected FOUND response for entry3, got %v", resp3.Entry.Code)
	}
	if got := string(resp3.Entry.Entry.Value.Value); got != "value3" {
		t.Errorf("Expected original value for entry3, got %s", got)
	}
}

func TestForkAwareGetFirst_PrefersWrappedOverCache(t *testing.T) {
	ms := newMockStore()
	fa := NewStore(ms)

	// Put a value in cache for key "a"
	cacheEntry := &pbmodel.Entry{Key: []byte("a"), Value: &anypb.Any{TypeUrl: "t", Value: []byte("cache")}}
	if err := fa.Set(cacheEntry, 200); err != nil {
		t.Fatalf("set cache: %v", err)
	}

	// Put an older value in wrapped for the same key
	wrappedEntry := &pbmodel.Entry{Key: []byte("a"), Value: &anypb.Any{TypeUrl: "t", Value: []byte("wrapped-old")}}
	_ = ms.Set(wrappedEntry, 100)

	resp, err := fa.GetFirst(&pbservice.GetFirstRequest{Key: []byte("a")})
	if err != nil {
		t.Fatalf("GetFirst: %v", err)
	}
	if resp.Entry.Code != pbmodel.ResponseCode_RESPONSE_CODE_FOUND {
		t.Fatalf("expected FOUND, got %v", resp.Entry.Code)
	}
	if got := string(resp.Entry.Entry.Value.Value); got != "wrapped-old" {
		t.Fatalf("expected wrapped value, got %q", got)
	}
}

func TestForkAwareGetFirst_WrappedNotFound(t *testing.T) {
	ms := newMockStore()
	fa := NewStore(ms)

	// Only cache contains candidate >= key
	cacheEntry := &pbmodel.Entry{Key: []byte("b"), Value: &anypb.Any{TypeUrl: "t", Value: []byte("cache-b")}}
	if err := fa.Set(cacheEntry, 123); err != nil {
		t.Fatalf("set cache: %v", err)
	}

	resp, err := fa.GetFirst(&pbservice.GetFirstRequest{Key: []byte("a")})
	if err != nil {
		t.Fatalf("GetFirst: %v", err)
	}
	// Current ForkAware implementation delegates to wrapped store, so expect NOT_FOUND when wrapped has no match
	if resp.Entry.Code != pbmodel.ResponseCode_RESPONSE_CODE_NOT_FOUND {
		t.Fatalf("expected NOT_FOUND, got %v", resp.Entry.Code)
	}
}
