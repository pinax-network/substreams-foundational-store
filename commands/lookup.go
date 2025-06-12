package commands

import (
	"encoding/binary"
	"fmt"

	badgerdb "github.com/dgraph-io/badger/v3"
	"github.com/mr-tron/base58"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/streamingfast/substreams-foundationnal-store/store"
	"github.com/streamingfast/substreams-foundationnal-store/store/badger"
	"google.golang.org/protobuf/types/known/anypb"
)

var (
	lookupTypeUrl   string
	lookupDSN       string
	lookupPrefixStr string
)

// LookupResult represents a key-value pair with block number
type LookupResult struct {
	Key         []byte
	BlockNumber uint64
	Value       *anypb.Any
}

// LookupCmd represents the lookup command
var LookupCmd = &cobra.Command{
	Use:   "lookup",
	Short: "Lookup keys with a prefix in a Badger store",
	Long: `Lookup keys with a prefix in a Badger store and display the results.
This command only works with Badger stores.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if lookupPrefixStr == "" {
			return fmt.Errorf("prefix is required")
		}

		// Parse the DSN
		dsn, err := store.ParseDSN(lookupDSN)
		if err != nil {
			return fmt.Errorf("failed to parse DSN: %w", err)
		}

		// Ensure we're using a Badger store
		if dsn.Driver() != "badger" {
			return fmt.Errorf("this tool only works with Badger stores. Got: %s", dsn.Driver())
		}

		// Create the Badger store
		badgerStore, err := badger.NewStore(dsn, lookupTypeUrl)
		if err != nil {
			return fmt.Errorf("failed to create Badger store: %w", err)
		}
		defer badgerStore.Close()

		fmt.Printf("Connected to Badger store at %s\n", dsn.Database)
		fmt.Printf("Looking up prefix: %s\n", lookupPrefixStr)

		// Decode the prefix from base58 if it's encoded
		prefixBytes, err := base58.Decode(lookupPrefixStr)
		if err != nil {
			fmt.Printf("Warning: Failed to decode prefix as base58, using raw prefix: %s\n", err)
			prefixBytes = []byte(lookupPrefixStr)
		}

		// Perform the prefix lookup
		results, err := performPrefixLookup(badgerStore, prefixBytes)
		if err != nil {
			return fmt.Errorf("failed to lookup prefix: %w", err)
		}

		// Display the results
		fmt.Printf("Found %d results:\n", len(results))
		for i, result := range results {
			fmt.Printf("%d. Key: %s\n", i+1, base58.Encode(result.Key))
			fmt.Printf("   Block Number: %d\n", result.BlockNumber)
			fmt.Printf("   Value Size: %d bytes\n", len(result.Value.Value))
			fmt.Printf("   Raw Input Key: %s\n", prefixBytes)
			fmt.Printf("   Raw Found Key: %s\n", result.Key)
			fmt.Println()
		}

		return nil
	},
}

// performPrefixLookup performs a prefix lookup in the Badger store
func performPrefixLookup(s *badger.Store, prefix []byte) ([]LookupResult, error) {
	var results []LookupResult

	// Get the underlying Badger DB
	db := s.GetDB()
	typeUrl := s.GetTypeURL()

	// Create a read-only transaction
	err := db.View(func(txn *badgerdb.Txn) error {
		// Create an iterator with default options
		opts := badgerdb.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		// Seek to the prefix
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()

			// Copy the key since it's only valid within this iteration
			keyCopy := append([]byte{}, key...)

			// Get the value
			var valueCopy []byte
			err := item.Value(func(val []byte) error {
				// Copy the value since it's only valid within this function
				valueCopy = append([]byte{}, val...)
				return nil
			})
			if err != nil {
				return err
			}

			// Ensure we have at least 8 bytes for the block number
			if len(valueCopy) < 8 {
				return fmt.Errorf("invalid stored value: expected at least 8 bytes for block number")
			}

			// Extract the block number from the first 8 bytes
			blockNumber := binary.BigEndian.Uint64(valueCopy[:8])

			// The actual value is everything after the first 8 bytes
			actualValue := valueCopy[8:]

			// Add to results
			results = append(results, LookupResult{
				Key:         keyCopy,
				BlockNumber: blockNumber,
				Value: &anypb.Any{
					TypeUrl: typeUrl,
					Value:   actualValue,
				},
			})
		}
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to lookup prefix: %w", err)
	}

	return results, nil
}

func init() {
	LookupCmd.Flags().StringVar(&lookupTypeUrl, "type-url", "AccountOwner", "Type URL for the stored values")
	LookupCmd.Flags().StringVar(&lookupDSN, "dsn", "badger:///tmp/badger-db", "Data Source Name (DSN) for the Badger store")
	LookupCmd.Flags().StringVar(&lookupPrefixStr, "prefix", "", "Prefix to lookup in the Badger store")
	LookupCmd.MarkFlagRequired("prefix")

	viper.BindPFlag("lookup.type_url", LookupCmd.Flags().Lookup("type-url"))
	viper.BindPFlag("lookup.dsn", LookupCmd.Flags().Lookup("dsn"))
	viper.BindPFlag("lookup.prefix", LookupCmd.Flags().Lookup("prefix"))
}
