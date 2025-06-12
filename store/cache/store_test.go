package cache

import (
	"testing"

	pbstore "github.com/streamingfast/substreams-foundationnal-store/pb/store"
	"google.golang.org/protobuf/types/known/anypb"
)

// mockStore is a simple in-memory implementation of the store.Store interface for testing
type mockStore struct {
	entries map[string]*pbstore.Entry
}

func newMockStore() *mockStore {
	return &mockStore{
		entries: make(map[string]*pbstore.Entry),
	}
}

func (m *mockStore) Set(entry *pbstore.Entry) error {
	m.entries[string(entry.Key)] = entry
	return nil
}

func (m *mockStore) SetAll(entries []*pbstore.Entry) error {
	for _, entry := range entries {
		m.entries[string(entry.Key)] = entry
	}
	return nil
}

func (m *mockStore) Get(request *pbstore.GetRequest) (*pbstore.GetResponse, error) {
	entry, ok := m.entries[string(request.Key)]
	if !ok || entry.BlockNumber > request.BlockNumber {
		return &pbstore.GetResponse{
			Response: pbstore.ResponseCode_NOT_FOUND,
		}, nil
	}
	return &pbstore.GetResponse{
		Response: pbstore.ResponseCode_FOUND,
		Value:    entry.Value,
	}, nil
}

func (m *mockStore) GetAll(request *pbstore.GetAllRequest) (*pbstore.GetAllResponse, error) {
	response := &pbstore.GetAllResponse{
		Entries: make([]*pbstore.ResponseEntry, 0, len(request.Keys)),
	}
	for _, key := range request.Keys {
		entry, ok := m.entries[string(key)]
		if !ok || entry.BlockNumber > request.BlockNumber {
			response.Entries = append(response.Entries, &pbstore.ResponseEntry{
				Key: key,
				Response: &pbstore.GetResponse{
					Response: pbstore.ResponseCode_NOT_FOUND,
				},
			})
		} else {
			response.Entries = append(response.Entries, &pbstore.ResponseEntry{
				Key: key,
				Response: &pbstore.GetResponse{
					Response: pbstore.ResponseCode_FOUND,
					Value:    entry.Value,
				},
			})
		}
	}
	return response, nil
}

func TestCacheStore(t *testing.T) {
	// Create a mock store
	mockStore := newMockStore()

	// Create a cache store that wraps the mock store
	cacheStore := NewStore(mockStore)

	// Create some test entries
	entry1 := &pbstore.Entry{
		BlockNumber: 100,
		Key:         []byte("key1"),
		Value:       &anypb.Any{TypeUrl: "test", Value: []byte("value1")},
	}
	entry2 := &pbstore.Entry{
		BlockNumber: 200,
		Key:         []byte("key2"),
		Value:       &anypb.Any{TypeUrl: "test", Value: []byte("value2")},
	}
	entry3 := &pbstore.Entry{
		BlockNumber: 300,
		Key:         []byte("key3"),
		Value:       &anypb.Any{TypeUrl: "test", Value: []byte("value3")},
	}

	// Set entries in the cache store
	if err := cacheStore.Set(entry1); err != nil {
		t.Fatalf("Failed to set entry1: %v", err)
	}
	if err := cacheStore.Set(entry2); err != nil {
		t.Fatalf("Failed to set entry2: %v", err)
	}
	if err := cacheStore.Set(entry3); err != nil {
		t.Fatalf("Failed to set entry3: %v", err)
	}

	// Verify that entries are in the cache but not in the mock store
	// (since flushUpToBlock is 0 by default)
	if len(mockStore.entries) != 0 {
		t.Errorf("Expected 0 entries in mock store, got %d", len(mockStore.entries))
	}

	// Get entry1 from the cache store
	resp1, err := cacheStore.Get(&pbstore.GetRequest{
		BlockNumber: 150,
		Key:         []byte("key1"),
	})
	if err != nil {
		t.Fatalf("Failed to get entry1: %v", err)
	}
	if resp1.Response != pbstore.ResponseCode_FOUND {
		t.Errorf("Expected FOUND response for entry1, got %v", resp1.Response)
	}

	// Flush entries with block numbers <= 200
	if err := cacheStore.FlushUpToBlock(200); err != nil {
		t.Fatalf("Failed to flush entries: %v", err)
	}

	// Verify that entry1 and entry2 are now in the mock store
	if len(mockStore.entries) != 2 {
		t.Errorf("Expected 2 entries in mock store, got %d", len(mockStore.entries))
	}

	// Verify that entry1 and entry2 are no longer in the cache
	// by checking if the mock store is used for retrieval
	mockStore.entries["key1"] = &pbstore.Entry{
		BlockNumber: 100,
		Key:         []byte("key1"),
		Value:       &anypb.Any{TypeUrl: "test", Value: []byte("modified1")},
	}

	resp1, err = cacheStore.Get(&pbstore.GetRequest{
		BlockNumber: 150,
		Key:         []byte("key1"),
	})
	if err != nil {
		t.Fatalf("Failed to get entry1: %v", err)
	}
	if string(resp1.Value.Value) != "modified1" {
		t.Errorf("Expected modified value for entry1, got %s", string(resp1.Value.Value))
	}

	// Verify that entry3 is still in the cache
	resp3, err := cacheStore.Get(&pbstore.GetRequest{
		BlockNumber: 350,
		Key:         []byte("key3"),
	})
	if err != nil {
		t.Fatalf("Failed to get entry3: %v", err)
	}
	if resp3.Response != pbstore.ResponseCode_FOUND {
		t.Errorf("Expected FOUND response for entry3, got %v", resp3.Response)
	}
	if string(resp3.Value.Value) != "value3" {
		t.Errorf("Expected original value for entry3, got %s", string(resp3.Value.Value))
	}
}
