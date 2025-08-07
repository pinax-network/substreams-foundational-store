package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	pbstore "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/v1"
	storelib "github.com/streamingfast/substreams-foundational-store/store"
	badgerstore "github.com/streamingfast/substreams-foundational-store/store/badger"
	pgstore "github.com/streamingfast/substreams-foundational-store/store/postgres"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// LoaderCmd represents the loader command
var LoaderCmd = &cobra.Command{
	Use:   "loader",
	Short: "Load data into a foundational-store",
	Long: `Load data from a CSV file into either PostgreSQL or Badger backends.

Example DSNs:
  - PostgreSQL: postgres://localhost:5432/postgres?sslmode=disable&schemaName=magic2
  - Badger: badger:///path/to/database`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Get flag values
		loaderFilePath, _ := cmd.Flags().GetString("file")
		loaderDSN, _ := cmd.Flags().GetString("dsn")
		batchSize, _ := cmd.Flags().GetInt("batch-size")

		// Parse DSN and create store
		dsn, err := storelib.ParseDSN(loaderDSN)
		if err != nil {
			return fmt.Errorf("failed to parse DSN: %w", err)
		}

		var dataStore storelib.Store
		typeURL := "type.googleapis.com/AccountOwner"

		switch dsn.Driver() {
		case "postgres":
			dataStore, err = pgstore.NewStore(dsn, typeURL)
			if err != nil {
				return fmt.Errorf("failed to create postgres foundational-store: %w", err)
			}
		case "badger":
			dataStore, err = badgerstore.NewStore(dsn, typeURL)
			if err != nil {
				return fmt.Errorf("failed to create badger foundational-store: %w", err)
			}
		default:
			return fmt.Errorf("unsupported driver: %s", dsn.Driver())
		}

		return LoadCSVIntoStore(dataStore, loaderFilePath, batchSize)
	},
}

func batchInsert(store storelib.Store, entries []*pbstore.Entry, blockNumber uint64) error {
	// Use the foundational-store's SetAll method to insert all entries
	err := store.SetAll(entries, blockNumber)
	if err != nil {
		return fmt.Errorf("failed to insert batch: %w", err)
	}

	return nil
}

func LoadCSVIntoStore(dataStore storelib.Store, csvFilePath string, batchSize int) error {
	if csvFilePath == "" {
		return fmt.Errorf("CSV file path is empty")
	}

	if _, err := os.Stat(csvFilePath); os.IsNotExist(err) {
		return fmt.Errorf("CSV file does not exist: %s", csvFilePath)
	}

	file, err := os.Open(csvFilePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	_, err = reader.Read()
	if err != nil {
		return fmt.Errorf("failed to read the CSV header: %w", err)
	}

	count := 0
	var storeEntries []*pbstore.Entry
	var blockNumber uint64

	for {
		record, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				// Insert remaining batch
				if len(storeEntries) > 0 {
					err = batchInsert(dataStore, storeEntries, blockNumber)
					if err != nil {
						return fmt.Errorf("failed to insert final batch: %w", err)
					}
				}
				fmt.Println("Goodbye!")
				break
			}
			return fmt.Errorf("failed to read CSV record: %w", err)
		}

		blockNumber, _ = strconv.ParseUint(record[0], 10, 64)
		deleted, _ := strconv.ParseBool(record[3])
		if deleted {
			continue
		}

		Account := record[4]
		MintAddress := record[6]
		Owner := record[7]

		accountOwner := &pbstore.AccountOwner{
			Mint:  storelib.MustBase58Decode(MintAddress),
			Owner: storelib.MustBase58Decode(Owner),
		}

		data, err := proto.Marshal(accountOwner)
		if err != nil {
			return fmt.Errorf("failed to marshal proto: %w", err)
		}

		anyValue := &anypb.Any{
			TypeUrl: "type.googleapis.com/AccountOwner",
			Value:   data,
		}

		entry := &pbstore.Entry{
			Key:   storelib.MustBase58Decode(Account),
			Value: anyValue,
		}

		storeEntries = append(storeEntries, entry)

		// Batch insert every batchSize (N) entries
		if len(storeEntries) >= batchSize {
			err := batchInsert(dataStore, storeEntries, blockNumber)
			if err != nil {
				return fmt.Errorf("failed to insert batch: %w", err)
			}
			storeEntries = []*pbstore.Entry{}
			count += batchSize
			if count > 0 && count%250000 == 0 {
				fmt.Printf("Inserted %d entries\n", count)
			}
		}
	}

	fmt.Printf("Successfully loaded data from %s\n", csvFilePath)
	return nil
}

func init() {
	LoaderCmd.Flags().String("file", "/Users/cbillett/t/clickhouse-exports/initialized_accounts.csv", "Path to the CSV file")
	LoaderCmd.Flags().String("dsn", "postgres://localhost:5432/postgres?sslmode=disable&schemaName=magic", "DSN connection string")
	LoaderCmd.Flags().Int("batch-size", 1000, "Number of entries to batch together for insertion")

	viper.BindPFlag("loader.file", LoaderCmd.Flags().Lookup("file"))
	viper.BindPFlag("loader.dsn", LoaderCmd.Flags().Lookup("dsn"))
	viper.BindPFlag("loader.batch_size", LoaderCmd.Flags().Lookup("batch-size"))
}
