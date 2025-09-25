package main

import (
	"fmt"
	"log"

	pbstore "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/v1"
	"github.com/streamingfast/substreams-foundational-store/store"
	"github.com/streamingfast/substreams-foundational-store/store/postgres_time_traversal"
	"google.golang.org/protobuf/types/known/anypb"
)

func main() {
	// Test DSN - modify as needed for your postgres setup
	dsnString := "postgres://localhost:5432/test_db?sslmode=disable"
	dsn, err := store.ParseDSN(dsnString)
	if err != nil {
		log.Fatalf("Failed to parse DSN: %v", err)
	}

	// Create postgres time traversal store
	typeUrl := "type.googleapis.com/test.TestMessage"
	pgStore, err := postgres_time_traversal.NewStore(dsn, typeUrl)
	if err != nil {
		log.Fatalf("Failed to create postgres time traversal store: %v", err)
	}
	defer pgStore.Close()

	// Test data
	testKey := []byte("test_key_1")
	testValue1 := []byte("value_at_block_100")
	testValue2 := []byte("value_at_block_200")
	testValue3 := []byte("value_at_block_300")

	fmt.Println("Testing Postgres Time Traversal Store...")

	// Store entries at different block numbers
	fmt.Println("1. Storing entries at different block numbers...")

	entry1 := &pbstore.Entry{
		Key: testKey,
		Value: &anypb.Any{
			TypeUrl: typeUrl,
			Value:   testValue1,
		},
	}
	err = pgStore.Set(entry1, 100)
	if err != nil {
		log.Fatalf("Failed to set entry at block 100: %v", err)
	}

	entry2 := &pbstore.Entry{
		Key: testKey,
		Value: &anypb.Any{
			TypeUrl: typeUrl,
			Value:   testValue2,
		},
	}
	err = pgStore.Set(entry2, 200)
	if err != nil {
		log.Fatalf("Failed to set entry at block 200: %v", err)
	}

	entry3 := &pbstore.Entry{
		Key: testKey,
		Value: &anypb.Any{
			TypeUrl: typeUrl,
			Value:   testValue3,
		},
	}
	err = pgStore.Set(entry3, 300)
	if err != nil {
		log.Fatalf("Failed to set entry at block 300: %v", err)
	}

	fmt.Println("   ✓ Successfully stored entries at blocks 100, 200, 300")

	// Test time traversal queries
	fmt.Println("2. Testing time traversal queries...")

	// Query at block 150 - should get value from block 100
	request1 := &pbstore.GetRequest{
		Key:         testKey,
		BlockNumber: 150,
	}
	resp1, err := pgStore.Get(request1)
	if err != nil {
		log.Fatalf("Failed to get entry at block 150: %v", err)
	}
	if resp1.Response != pbstore.ResponseCode_RESPONSE_CODE_FOUND {
		log.Fatalf("Expected entry to be found at block 150")
	}
	if string(resp1.Value.Value) != string(testValue1) {
		log.Fatalf("Expected value from block 100, got: %s", string(resp1.Value.Value))
	}
	fmt.Printf("   ✓ Block 150 query returned value from block 100: %s\n", string(resp1.Value.Value))

	// Query at block 250 - should get value from block 200
	request2 := &pbstore.GetRequest{
		Key:         testKey,
		BlockNumber: 250,
	}
	resp2, err := pgStore.Get(request2)
	if err != nil {
		log.Fatalf("Failed to get entry at block 250: %v", err)
	}
	if resp2.Response != pbstore.ResponseCode_RESPONSE_CODE_FOUND {
		log.Fatalf("Expected entry to be found at block 250")
	}
	if string(resp2.Value.Value) != string(testValue2) {
		log.Fatalf("Expected value from block 200, got: %s", string(resp2.Value.Value))
	}
	fmt.Printf("   ✓ Block 250 query returned value from block 200: %s\n", string(resp2.Value.Value))

	// Query at block 350 - should get value from block 300
	request3 := &pbstore.GetRequest{
		Key:         testKey,
		BlockNumber: 350,
	}
	resp3, err := pgStore.Get(request3)
	if err != nil {
		log.Fatalf("Failed to get entry at block 350: %v", err)
	}
	if resp3.Response != pbstore.ResponseCode_RESPONSE_CODE_FOUND {
		log.Fatalf("Expected entry to be found at block 350")
	}
	if string(resp3.Value.Value) != string(testValue3) {
		log.Fatalf("Expected value from block 300, got: %s", string(resp3.Value.Value))
	}
	fmt.Printf("   ✓ Block 350 query returned value from block 300: %s\n", string(resp3.Value.Value))

	// Query at block 50 - should not find anything
	request4 := &pbstore.GetRequest{
		Key:         testKey,
		BlockNumber: 50,
	}
	resp4, err := pgStore.Get(request4)
	if err != nil {
		log.Fatalf("Failed to query at block 50: %v", err)
	}
	if resp4.Response != pbstore.ResponseCode_RESPONSE_CODE_NOT_FOUND {
		log.Fatalf("Expected no entry to be found at block 50")
	}
	fmt.Println("   ✓ Block 50 query correctly returned NOT_FOUND")

	// Test key-only operations
	fmt.Println("3. Testing key-only operations...")

	keyOnlyEntry, err := pgStore.GetKeyOnly(testKey, 250)
	if err != nil {
		log.Fatalf("Failed to get key-only entry at block 250: %v", err)
	}
	if keyOnlyEntry == nil {
		log.Fatalf("Expected key-only entry to be found at block 250")
	}
	if keyOnlyEntry.BlockNumber != 200 {
		log.Fatalf("Expected key-only entry from block 200, got block %d", keyOnlyEntry.BlockNumber)
	}
	fmt.Printf("   ✓ Key-only query at block 250 returned entry from block %d\n", keyOnlyEntry.BlockNumber)

	fmt.Println("\n🎉 All postgres time traversal tests passed!")
}
