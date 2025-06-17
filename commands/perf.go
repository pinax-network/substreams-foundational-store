package commands

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/gob"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"github.com/mr-tron/base58"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	pbStore "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/v1"
	"github.com/streamingfast/substreams-foundational-store/store"
	"github.com/streamingfast/substreams-foundational-store/store/badger"
	"github.com/streamingfast/substreams-foundational-store/store/postgres"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// PerfCmd represents the perf command
var PerfCmd = &cobra.Command{
	Use:   "perf",
	Short: "Performance testing for the foundational-store",
	Long: `Performance testing for the foundational-store. This command can test the performance of
various foundational-store implementations (PostgreSQL, Badger) with different configurations.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Println("args:", os.Args[1:])

		// Get flag values
		perfTypeUrl, _ := cmd.Flags().GetString("type-url")
		perfNumAccounts, _ := cmd.Flags().GetInt("num-accounts")
		perfNumWorkers, _ := cmd.Flags().GetInt("num-workers")
		perfNumClients, _ := cmd.Flags().GetInt("num-clients")
		perfDuration, _ := cmd.Flags().GetDuration("duration")
		perfRunConcurrent, _ := cmd.Flags().GetBool("run-concurrent")
		perfDSN, _ := cmd.Flags().GetString("dsn")
		perfAccountFile, _ := cmd.Flags().GetString("account-file")

		// Log all command-line flags and their values
		fmt.Printf("Running with configuration:\n")
		fmt.Printf("- type-url: %s\n", perfTypeUrl)
		fmt.Printf("- num-accounts: %d\n", perfNumAccounts)
		fmt.Printf("- num-workers: %d\n", perfNumWorkers)
		fmt.Printf("- num-clients: %d\n", perfNumClients)
		fmt.Printf("- duration: %s\n", perfDuration)
		fmt.Printf("- run-concurrent: %v\n", perfRunConcurrent)
		fmt.Printf("- dsn: %s\n", perfDSN)
		fmt.Printf("- account-file: %s\n", perfAccountFile)

		// Open the file for reading
		file, err := os.Open(perfAccountFile)
		if err != nil {
			return fmt.Errorf("failed to open file: %w", err)
		}
		defer file.Close()

		// Create a decoder
		var accounts []string
		decoder := gob.NewDecoder(file)
		err = decoder.Decode(&accounts)
		if err != nil {
			return fmt.Errorf("failed to decode accounts: %w", err)
		}

		fmt.Println("Accounts loaded:", len(accounts))

		dsn, err := store.ParseDSN(perfDSN)
		if err != nil {
			return fmt.Errorf("failed to parse DSN: %w", err)
		}

		var storeInstance store.Store

		// Create the appropriate foundational-store based on the DSN driver
		switch dsn.Driver() {
		case "postgres":
			pgStore, err := postgres.NewStore(dsn, perfTypeUrl)
			if err != nil {
				return fmt.Errorf("failed to create postgres foundational-store: %w", err)
			}
			storeInstance = pgStore
			fmt.Println("Connected to postgres")
		case "badger":
			// For badger foundational-store, use the DSN directly
			badgerStore, err := badger.NewStore(dsn, perfTypeUrl, badger.WithNumWorkers(perfNumWorkers))
			if err != nil {
				return fmt.Errorf("failed to create badger foundational-store: %w", err)
			}
			storeInstance = badgerStore
			fmt.Println("Using badger foundational-store with DSN:", dsn, "and", perfNumWorkers, "workers")
		default:
			return fmt.Errorf("unsupported foundational-store driver: %s", dsn.Driver())
		}

		err = runSingleQuery(accounts, storeInstance)
		if err != nil {
			return fmt.Errorf("failed to run single query: %w", err)
		}

		err = runMultiAccountQuery(perfNumAccounts, accounts, storeInstance, perfTypeUrl)
		if err != nil {
			return fmt.Errorf("failed to run multi query: %w", err)
		}

		if perfRunConcurrent {
			fmt.Println("Running concurrent multi-account queries test...")
			err = runConcurrentMultiAccountQueries(perfNumClients, perfNumAccounts, accounts, storeInstance, perfTypeUrl, perfDuration)
			if err != nil {
				return fmt.Errorf("failed to run concurrent multi-account queries: %w", err)
			}
		}

		fmt.Println("Goodbye!")
		return nil
	},
}

// generateRandomSolanaAddress generates a random Solana-like address
func generateRandomSolanaAddress() string {
	// Solana addresses are 32 bytes
	randomBytes := make([]byte, 32)
	_, err := cryptorand.Read(randomBytes)
	if err != nil {
		// If we can't generate random bytes, fallback to a deterministic approach
		for i := 0; i < 32; i++ {
			randomBytes[i] = byte(rand.Intn(256))
		}
	}

	// Encode the random bytes as base58
	return base58.Encode(randomBytes)
}

// runRandomDataInsertion runs as a goroutine to insert random data using SetAll
func runRandomDataInsertion(ctx context.Context, storeInstance store.Store, typeUrl string) {
	// Create a new random seed for this goroutine
	localRand := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Track statistics
	insertsPerformed := 0
	totalInsertTime := time.Duration(0)

	// Start the timer for the overall operation
	overallStart := time.Now()

	// Run inserts until the context is done
	for {
		select {
		case <-ctx.Done():
			// Test duration reached, print summary and exit
			fmt.Printf("Random data insertion completed after %d inserts in %s\n",
				insertsPerformed, time.Since(overallStart))
			if insertsPerformed > 0 {
				fmt.Printf("Average insert time: %s\n",
					totalInsertTime/time.Duration(insertsPerformed))
			}
			return
		default:
			// Generate random entries for insertion
			numEntries := localRand.Intn(500) + 1 // Insert 1-500 entries at a time
			entries := make([]*pbStore.Entry, numEntries)

			for i := 0; i < numEntries; i++ {
				// Generate a random account address
				randomAccount := generateRandomSolanaAddress()

				// Generate random mint and owner addresses
				randomMint := generateRandomSolanaAddress()
				randomOwner := generateRandomSolanaAddress()

				// Create an AccountOwner instance
				accountOwner := &pbStore.AccountOwner{
					Mint:  store.MustBase58Decode(randomMint),
					Owner: store.MustBase58Decode(randomOwner),
				}

				// Marshal the AccountOwner proto message
				data, err := proto.Marshal(accountOwner)
				if err != nil {
					fmt.Printf("Failed to marshal proto: %s\n", err)
					continue
				}

				// Create an Any proto message to wrap the AccountOwner
				anyValue := &anypb.Any{
					TypeUrl: typeUrl,
					Value:   data,
				}

				// Create a foundational-store.Entry
				entries[i] = &pbStore.Entry{
					Key:   store.MustBase58Decode(randomAccount),
					Value: anyValue,
				}
			}

			// Insert the entries using SetAll
			insertStart := time.Now()
			err := storeInstance.SetAll(entries, uint64(localRand.Intn(1000)))
			insertTime := time.Since(insertStart)
			totalInsertTime += insertTime

			if err != nil {
				fmt.Printf("Failed to insert entries: %s\n", err)
				continue
			}

			insertsPerformed++

			// Print progress every 10 inserts
			if insertsPerformed%10 == 0 {
				fmt.Printf("Completed %d inserts, last insert time: %s, inserted %d entries, average insert time %s\n",
					insertsPerformed, insertTime, numEntries, totalInsertTime/time.Duration(insertsPerformed))
			}

			// Sleep a short time to avoid overwhelming the foundational-store
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func runSingleQuery(accounts []string, storeInstance store.Store) error {
	rand.Seed(time.Now().UnixNano())
	randomIndex := rand.Intn(len(accounts))
	randomAddress := accounts[randomIndex]
	fmt.Printf("Random address: %s\n", randomAddress)

	start := time.Now()
	response, err := storeInstance.Get(&pbStore.GetRequest{
		BlockNumber: 400000000,
		OmitDeleted: false,
		Key:         store.MustBase58Decode(randomAddress),
	})
	if err != nil {
		return fmt.Errorf("failed to get account: %w", err)
	}
	fmt.Println("Query time:", time.Since(start))
	fmt.Println(response)

	return nil
}

func runMultiAccountQuery(size int, accounts []string, storeInstance store.Store, typeUrl string) error {
	fmt.Printf("Running multi-account query with %d accounts\n", size)

	// Initialize random number generator
	rand.Seed(time.Now().UnixNano())

	selectedAccounts := make([]string, 0, size)
	accountsCopy := make([]string, len(accounts))
	copy(accountsCopy, accounts)

	for i := 0; i < size && i < len(accountsCopy); i++ {
		randomIndex := rand.Intn(len(accountsCopy))
		selectedAccounts = append(selectedAccounts, accountsCopy[randomIndex])
		accountsCopy[randomIndex] = accountsCopy[len(accountsCopy)-1]
		accountsCopy = accountsCopy[:len(accountsCopy)-1]
	}

	// Convert account strings to byte arrays
	accountsBytes := make([][]byte, len(selectedAccounts))
	for i, s := range selectedAccounts {
		accountsBytes[i] = store.MustBase58Decode(s)
	}

	// Make the GetAll call
	queryStart := time.Now()
	response, err := storeInstance.GetAll(&pbStore.GetAllRequest{
		BlockNumber: 400000000,
		OmitDeleted: false,
		Keys:        accountsBytes,
	})
	queryTime := time.Since(queryStart)
	fmt.Printf("Query time: %s\n", queryTime)

	if err != nil {
		return fmt.Errorf("failed to get accounts: %w", err)
	}

	for i, entry := range response.Entries {
		if entry.Response.Response == pbStore.ResponseCode_NOT_FOUND {
			fmt.Printf("Account %s not found\n", base58.Encode(entry.Key))
			continue
		}
		ao := &pbStore.AccountOwner{}
		err := entry.Response.Value.UnmarshalTo(ao)
		if err != nil {
			fmt.Printf("Failed to unmarshal AccountOwner: %s\n", err)
			continue
		}
		fmt.Printf("Account %s: owner:%s mint:%s\n", base58.Encode(entry.Key), base58.Encode(ao.Owner), base58.Encode(ao.Mint))

		if i >= 10 {
			break
		}
	}

	return nil
}

type queryResult struct {
	queryTime  time.Duration
	foundCount int
}

// runConcurrentMultiAccountQueries simulates multiple clients making concurrent requests
// for multiple accounts over a specified duration
func runConcurrentMultiAccountQueries(numClients, accountsPerQuery int, accounts []string, storeInstance store.Store, typeUrl string, duration time.Duration) error {
	fmt.Printf("Running concurrent multi-account queries with %d clients, %d accounts per query, for %s\n",
		numClients, accountsPerQuery, duration)

	// Create a context with timeout for the test duration
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	// Create channels for collecting results
	resultsChan := make(chan *queryResult) // Buffer to avoid blocking
	errorsChan := make(chan error, 1000)   // Buffer to avoid blocking

	// Start the client goroutines
	for i := 0; i < numClients; i++ {
		go func(clientID int) {
			// Create a new random seed for this client
			localRand := rand.New(rand.NewSource(time.Now().UnixNano() + int64(clientID)))

			// Track statistics for this client
			queriesPerformed := 0

			// Run queries until the context is done
			for {
				select {
				case <-ctx.Done():
					// Test duration reached, exit
					return
				default:
					// Select random accounts for this query
					selectedAccounts := make([]string, 0, accountsPerQuery)
					accountsCopy := make([]string, len(accounts))
					copy(accountsCopy, accounts)

					for j := 0; j < accountsPerQuery && j < len(accountsCopy); j++ {
						randomIndex := localRand.Intn(len(accountsCopy))
						selectedAccounts = append(selectedAccounts, accountsCopy[randomIndex])
						accountsCopy[randomIndex] = accountsCopy[len(accountsCopy)-1]
						accountsCopy = accountsCopy[:len(accountsCopy)-1]
					}

					// Convert account strings to byte arrays
					accountsBytes := make([][]byte, len(selectedAccounts))
					for j, s := range selectedAccounts {
						accountsBytes[j] = store.MustBase58Decode(s)
					}

					// Make the GetAll call
					queryStart := time.Now()
					resp, err := storeInstance.GetAll(&pbStore.GetAllRequest{
						BlockNumber: 400000000,
						OmitDeleted: false,
						Keys:        accountsBytes,
					})
					queryTime := time.Since(queryStart)

					// Send results to channels
					if err != nil {
						errorsChan <- fmt.Errorf("client %d: failed to get accounts: %w", clientID, err)
					} else {
						foundCount := 0
						for _, entry := range resp.Entries {
							if entry.Response.Response == pbStore.ResponseCode_FOUND {
								foundCount++
							}
						}
						if foundCount != len(selectedAccounts) {
							errorsChan <- fmt.Errorf("client %d: found %d accounts, expected %d", clientID, foundCount, len(selectedAccounts))
						}
						resultsChan <- &queryResult{
							queryTime:  queryTime,
							foundCount: foundCount,
						}
					}

					queriesPerformed++

					// Sleep a short time to avoid overwhelming the foundational-store
					time.Sleep(100 * time.Millisecond)
				}
			}
		}(i)
	}

	// Collect and report results
	var totalQueries int64
	var totalErrors int64
	var totalQueryTime time.Duration
	var minQueryTime time.Duration = time.Hour // Initialize to a large value
	var maxQueryTime time.Duration
	var foundCount int
	var statMutex sync.Mutex

	// Start a goroutine to collect results
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Print current stats every second
				if totalQueries > 0 {
					statMutex.Lock()
					fmt.Printf("Progress: %d queries, %d errors, avg time: %s, min: %s, max: %s found!: %d\n",
						totalQueries, totalErrors,
						totalQueryTime/time.Duration(totalQueries),
						minQueryTime,
						maxQueryTime,
						foundCount,
					)
					totalQueries = 0
					totalQueryTime = 0
					minQueryTime = time.Hour
					maxQueryTime = 0
					foundCount = 0
					statMutex.Unlock()
				}
			case queryRes := <-resultsChan:
				statMutex.Lock()
				foundCount += queryRes.foundCount
				totalQueries++
				totalQueryTime += queryRes.queryTime
				if queryRes.queryTime < minQueryTime {
					minQueryTime = queryRes.queryTime
				}
				if queryRes.queryTime > maxQueryTime {
					maxQueryTime = queryRes.queryTime
				}
				statMutex.Unlock()
			case err := <-errorsChan:
				totalErrors++
				fmt.Printf("Error: %s\n", err)
			}
		}
	}()

	// Wait for the test duration
	<-ctx.Done()

	// Print final results
	fmt.Println("\nConcurrent multi-account queries test completed")

	return nil
}

func init() {
	PerfCmd.Flags().String("type-url", "patate/poils", "Type URL for the stored values")
	PerfCmd.Flags().Int("num-accounts", 4000, "Number of accounts to fetch in multi-account query")
	PerfCmd.Flags().Int("num-workers", 10, "Number of workers for parallel operations in badger foundational-store")
	PerfCmd.Flags().Int("num-clients", 20, "Number of concurrent clients for parallel GetAll operations")
	PerfCmd.Flags().Duration("duration", 60*time.Second, "Duration to run the concurrent GetAll test")
	PerfCmd.Flags().Bool("run-concurrent", false, "Run concurrent multi-account queries test")
	PerfCmd.Flags().String("dsn", "postgres://localhost:5432/postgres?sslmode=disable&schemaName=magic", "Data Source Name (DSN) for the foundational-store")
	PerfCmd.Flags().String("account-file", "/Users/cbillett/t/clickhouse-exports/account.bin", "Path to the account.bin file")

	viper.BindPFlag("perf.type_url", PerfCmd.Flags().Lookup("type-url"))
	viper.BindPFlag("perf.num_accounts", PerfCmd.Flags().Lookup("num-accounts"))
	viper.BindPFlag("perf.num_workers", PerfCmd.Flags().Lookup("num-workers"))
	viper.BindPFlag("perf.num_clients", PerfCmd.Flags().Lookup("num-clients"))
	viper.BindPFlag("perf.duration", PerfCmd.Flags().Lookup("duration"))
	viper.BindPFlag("perf.run_concurrent", PerfCmd.Flags().Lookup("run-concurrent"))
	viper.BindPFlag("perf.dsn", PerfCmd.Flags().Lookup("dsn"))
	viper.BindPFlag("perf.account_file", PerfCmd.Flags().Lookup("account-file"))
}
