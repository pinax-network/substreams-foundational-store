package commands

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/streamingfast/substreams-foundationnal-store/pb/store"
	storelib "github.com/streamingfast/substreams-foundationnal-store/store"
	badgerstore "github.com/streamingfast/substreams-foundationnal-store/store/badger"
	pgstore "github.com/streamingfast/substreams-foundationnal-store/store/postgres"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

var (
	loaderFilePath string
	loaderDSN      string
)

// LoaderCmd represents the loader command
var LoaderCmd = &cobra.Command{
	Use:   "loader",
	Short: "Load data into a store",
	Long: `Load data from a CSV file into either PostgreSQL or Badger backends.

Example DSNs:
  - PostgreSQL: postgres://localhost:5432/postgres?sslmode=disable&schemaName=magic2
  - Badger: badger:///path/to/database`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Step 1: Open the file
		file, err := os.Open(loaderFilePath)
		if err != nil {
			return fmt.Errorf("failed to open file: %w", err)
		}
		defer file.Close()

		reader := csv.NewReader(file)

		// Step 2: Skip the header row
		_, err = reader.Read() // Read the first row, which contains column headers
		if err != nil {
			return fmt.Errorf("failed to read the CSV header: %w", err)
		}

		// Step 3: Connect to the database using the appropriate store implementation
		dsn, err := storelib.ParseDSN(loaderDSN)
		if err != nil {
			return fmt.Errorf("failed to parse DSN: %w", err)
		}

		// Create a new store with the AccountOwner type URL based on the driver
		var dataStore storelib.Store
		typeURL := "type.googleapis.com/AccountOwner"

		switch dsn.Driver() {
		case "postgres":
			dataStore, err = pgstore.NewStore(dsn, typeURL)
			if err != nil {
				return fmt.Errorf("failed to create postgres store: %w", err)
			}
		case "badger":
			dataStore, err = badgerstore.NewStore(dsn, typeURL)
			if err != nil {
				return fmt.Errorf("failed to create badger store: %w", err)
			}
		default:
			return fmt.Errorf("unsupported driver: %s", dsn.Driver())
		}

		count := 0
		var storeEntries []*store.Entry
		for {
			record, err := reader.Read()
			if err != nil {
				if err.Error() == "EOF" { // Detect end of file
					err = batchInsert(dataStore, storeEntries)
					if err != nil {
						return fmt.Errorf("failed to insert batch: %w", err)
					}
					fmt.Println("Goodbye!")
					break
				}
				return fmt.Errorf("failed to read CSV record: %w", err)
			}
			// Parse block number to use in the store.Entry
			blockNumber, _ := strconv.ParseUint(record[0], 10, 64)
			deleted, _ := strconv.ParseBool(record[3])
			if deleted {
				continue
			}
			Account := record[4]
			MintAddress := record[6]
			Owner := record[7]

			accountOwner := &store.AccountOwner{
				Mint:  storelib.MustBase58Decode(MintAddress),
				Owner: storelib.MustBase58Decode(Owner),
			}

			// Marshal the AccountOwner proto message
			data, err := proto.Marshal(accountOwner)
			if err != nil {
				return fmt.Errorf("failed to marshal proto: %w", err)
			}

			// Create an Any proto message to wrap the AccountOwner
			anyValue := &anypb.Any{
				TypeUrl: "type.googleapis.com/AccountOwner",
				Value:   data,
			}

			// Create a store.Entry
			entry := &store.Entry{
				BlockNumber: blockNumber,
				Key:         storelib.MustBase58Decode(Account),
				Value:       anyValue,
			}

			storeEntries = append(storeEntries, entry)

			if len(storeEntries) >= 1000 {
				err := batchInsert(dataStore, storeEntries)
				if err != nil {
					return fmt.Errorf("failed to insert batch: %w", err)
				}
				storeEntries = []*store.Entry{}
				count += 1000
				if count > 0 && count%250000 == 0 {
					fmt.Printf("Inserted %d entries\n", count)
				}
			}
		}

		return nil
	},
}

func batchInsert(store storelib.Store, entries []*store.Entry) error {
	// Use the store's SetAll method to insert all entries
	err := store.SetAll(entries)
	if err != nil {
		return fmt.Errorf("failed to insert batch: %w", err)
	}

	return nil
}

func init() {
	LoaderCmd.Flags().StringVar(&loaderFilePath, "file", "/Users/cbillett/t/clickhouse-exports/initialized_accounts.csv", "Path to the CSV file")
	LoaderCmd.Flags().StringVar(&loaderDSN, "dsn", "postgres://localhost:5432/postgres?sslmode=disable&schemaName=magic", "DSN connection string")

	viper.BindPFlag("loader.file", LoaderCmd.Flags().Lookup("file"))
	viper.BindPFlag("loader.dsn", LoaderCmd.Flags().Lookup("dsn"))
}
