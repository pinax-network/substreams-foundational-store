package badger_time_traversal

import (
	"testing"

	pbtest "github.com/streamingfast/substreams-foundational-store/internal/pb/test"
	pbmodel "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/model/v2"
	pbservice "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/service/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetAllFirst_Basic_BadgerTimeTraversal(t *testing.T) {
	ts := setupTestStore(t)
	defer ts.cleanup()

	// Prepare two entries at different blocks to ensure block logic doesn't break first semantics
	owner1 := createAccountOwner("owner-1")
	entry1, err := createEntry(100, []byte("k1"), owner1, ts.typeURL)
	require.NoError(t, err)
	require.NoError(t, ts.store.Set(entry1, 100))

	owner2 := createAccountOwner("owner-2")
	entry2, err := createEntry(150, []byte("k2"), owner2, ts.typeURL)
	require.NoError(t, err)
	require.NoError(t, ts.store.Set(entry2, 150))

	req := &pbservice.GetRequest{
		BlockNumber: 200,
		BlockHash:   []byte("test_block_hash"),
		Keys: []*pbmodel.Key{
			{Bytes: []byte("k1")},
			{Bytes: []byte("missing")},
			{Bytes: []byte("k2")},
		},
	}

	resp, err := ts.store.GetFirst(req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, true, resp.BlockReached)
	require.Equal(t, 3, len(resp.Entries.Entries))

	// k1
	assert.Equal(t, pbmodel.ResponseCode_RESPONSE_CODE_FOUND, resp.Entries.Entries[0].Code)
	got1 := &pbtest.TestAccountOwner{}
	require.NoError(t, resp.Entries.Entries[0].Entry.Value.UnmarshalTo(got1))
	assert.Equal(t, owner1.Owner, got1.Owner)

	// missing
	assert.Equal(t, pbmodel.ResponseCode_RESPONSE_CODE_NOT_FOUND, resp.Entries.Entries[1].Code)

	// k2
	assert.Equal(t, pbmodel.ResponseCode_RESPONSE_CODE_FOUND, resp.Entries.Entries[2].Code)
	got2 := &pbtest.TestAccountOwner{}
	require.NoError(t, resp.Entries.Entries[2].Entry.Value.UnmarshalTo(got2))
	assert.Equal(t, owner2.Owner, got2.Owner)
}

// New test: when a key has multiple values across blocks, only the first (oldest) entry is returned
func TestGetAllFirst_MultipleValues_ReturnsOldest_BadgerTimeTraversal(t *testing.T) {
	ts := setupTestStore(t)
	defer ts.cleanup()

	// k1 has two values at different blocks; oldest is owner-A, then owner-A2
	ownerA := createAccountOwner("owner-A")
	entryA1, err := createEntry(50, []byte("k1"), ownerA, ts.typeURL)
	require.NoError(t, err)
	require.NoError(t, ts.store.Set(entryA1, 50))

	ownerA2 := createAccountOwner("owner-A2")
	entryA2, err := createEntry(200, []byte("k1"), ownerA2, ts.typeURL)
	require.NoError(t, err)
	require.NoError(t, ts.store.Set(entryA2, 200))

	// k2 also has two values; oldest is owner-B
	ownerB := createAccountOwner("owner-B")
	entryB1, err := createEntry(10, []byte("k2"), ownerB, ts.typeURL)
	require.NoError(t, err)
	require.NoError(t, ts.store.Set(entryB1, 10))

	ownerB2 := createAccountOwner("owner-B2")
	entryB2, err := createEntry(500, []byte("k2"), ownerB2, ts.typeURL)
	require.NoError(t, err)
	require.NoError(t, ts.store.Set(entryB2, 500))

	req := &pbservice.GetRequest{
		BlockNumber: 999, // after all writes
		BlockHash:   []byte("test_block_hash"),
		Keys: []*pbmodel.Key{
			{Bytes: []byte("k1")},
			{Bytes: []byte("k2")},
		},
	}

	resp, err := ts.store.GetFirst(req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, 2, len(resp.Entries.Entries))

	// For k1, expect oldest value owner-A
	got1 := &pbtest.TestAccountOwner{}
	require.NoError(t, resp.Entries.Entries[0].Entry.Value.UnmarshalTo(got1))
	assert.Equal(t, ownerA.Owner, got1.Owner)

	// For k2, expect oldest value owner-B
	got2 := &pbtest.TestAccountOwner{}
	require.NoError(t, resp.Entries.Entries[1].Entry.Value.UnmarshalTo(got2))
	assert.Equal(t, ownerB.Owner, got2.Owner)
}
