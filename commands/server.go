package commands

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/streamingfast/substreams-foundationnal-store/server"
	"github.com/streamingfast/substreams-foundationnal-store/store"
	"github.com/streamingfast/substreams-foundationnal-store/store/badger"
	"github.com/streamingfast/substreams-foundationnal-store/store/postgres"
)

var (
	serverAddr    string
	serverDSN     string
	serverTypeUrl string
	serverWorkers int
)

// ServerCmd represents the server command
var ServerCmd = &cobra.Command{
	Use:   "server",
	Short: "Start the gRPC server",
	Long: `Start the gRPC server that provides access to the store.
The server supports various store implementations (PostgreSQL, Badger) with different configurations.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if serverDSN == "" {
			return fmt.Errorf("DSN is required")
		}

		if serverTypeUrl == "" {
			return fmt.Errorf("Type URL is required")
		}

		// Parse the DSN
		dsn, err := store.ParseDSN(serverDSN)
		if err != nil {
			return fmt.Errorf("failed to parse DSN: %w", err)
		}

		// Create the store based on the DSN driver
		var storeImpl store.Store
		var badgerStore *badger.Store

		switch dsn.Driver() {
		case "badger":
			badgerStore, err = badger.NewStore(dsn, serverTypeUrl,
				badger.WithNumWorkers(serverWorkers),
			)
			if err != nil {
				return fmt.Errorf("failed to create Badger store: %w", err)
			}
			storeImpl = badgerStore
		case "postgres":
			pgStore, err := postgres.NewStore(dsn, serverTypeUrl)
			if err != nil {
				return fmt.Errorf("failed to create Postgres store: %w", err)
			}
			storeImpl = pgStore
		default:
			return fmt.Errorf("unsupported store driver: %s", dsn.Driver())
		}

		// Ensure we close the Badger store when we're done
		if badgerStore != nil {
			defer badgerStore.Close()
		}

		// Create a channel to listen for interrupt signals
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

		// Start the gRPC server in a goroutine
		errCh := make(chan error, 1)
		go func() {
			fmt.Printf("Starting gRPC server on %s\n", serverAddr)
			errCh <- server.Serve(serverAddr, storeImpl)
		}()

		// Wait for an interrupt signal or an error from the server
		select {
		case <-sigCh:
			fmt.Println("Received interrupt signal, shutting down...")
			return nil
		case err := <-errCh:
			return fmt.Errorf("server error: %w", err)
		}
	},
}

func init() {
	ServerCmd.Flags().StringVar(&serverAddr, "addr", ":50051", "Address to listen on")
	ServerCmd.Flags().StringVar(&serverDSN, "dsn", "", "DSN for the store (e.g. badger:///path/to/db or postgres://user:pass@host:port/dbname)")
	ServerCmd.Flags().StringVar(&serverTypeUrl, "type-url", "", "Type URL for the stored values")
	ServerCmd.Flags().IntVar(&serverWorkers, "workers", 10, "Number of workers for parallel operations")

	ServerCmd.MarkFlagRequired("dsn")
	ServerCmd.MarkFlagRequired("type-url")

	viper.BindPFlag("server.addr", ServerCmd.Flags().Lookup("addr"))
	viper.BindPFlag("server.dsn", ServerCmd.Flags().Lookup("dsn"))
	viper.BindPFlag("server.type_url", ServerCmd.Flags().Lookup("type-url"))
	viper.BindPFlag("server.workers", ServerCmd.Flags().Lookup("workers"))
}
