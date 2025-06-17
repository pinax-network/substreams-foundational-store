package commands

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/streamingfast/logging"
	"github.com/streamingfast/substreams-foundational-store/server"
	"github.com/streamingfast/substreams-foundational-store/sink"
	"github.com/streamingfast/substreams-foundational-store/store"
	"github.com/streamingfast/substreams-foundational-store/store/ForkAware"
	"github.com/streamingfast/substreams-foundational-store/store/badger"
	"github.com/streamingfast/substreams-foundational-store/store/postgres"

	subsink "github.com/streamingfast/substreams-sink"
	"go.uber.org/zap"
)

// ServerCmd represents the server command
var ServerCmd = &cobra.Command{
	Use:   "server",
	Short: "Start the gRPC server",
	Long: `Start the gRPC server that provides access to the foundational-store.
The server supports various foundational-store implementations (PostgreSQL, Badger) with different configurations.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Initialize logger
		zlog, tracer := logging.ApplicationLogger("server", "info")

		// Get flag values
		serverDSN, _ := cmd.Flags().GetString("dsn")
		serverTypeUrl, _ := cmd.Flags().GetString("type-url")
		serverAddr, _ := cmd.Flags().GetString("addr")
		serverWorkers, _ := cmd.Flags().GetInt("workers")
		substreamsEndpoint, _ := cmd.Flags().GetString("substreams-endpoint")
		manifestPath, _ := cmd.Flags().GetString("manifest-path")
		outputModuleName, _ := cmd.Flags().GetString("output-module-name")
		startBlock, _ := cmd.Flags().GetString("start-block")
		stopBlock, _ := cmd.Flags().GetString("stop-block")
		outputType, _ := cmd.Flags().GetString("output-type")
		cursorFilePath, _ := cmd.Flags().GetString("cursor-file-path")

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

		// Create the foundational-store based on the DSN driver
		var baseStore store.Store
		var badgerStore *badger.Store

		switch dsn.Driver() {
		case "badger":
			badgerStore, err = badger.NewStore(dsn, serverTypeUrl,
				badger.WithNumWorkers(serverWorkers),
			)
			if err != nil {
				return fmt.Errorf("failed to create Badger foundational-store: %w", err)
			}
			baseStore = badgerStore
		case "postgres":
			pgStore, err := postgres.NewStore(dsn, serverTypeUrl)
			if err != nil {
				return fmt.Errorf("failed to create Postgres foundational-store: %w", err)
			}
			baseStore = pgStore
		default:
			return fmt.Errorf("unsupported foundational-store driver: %s", dsn.Driver())
		}

		// Wrap the foundational-store with a ForkAware foundational-store
		storeImpl := ForkAware.NewStore(baseStore)

		// Ensure we close the Badger foundational-store when we're done
		if badgerStore != nil {
			defer badgerStore.Close()
		}

		// Create a channel to listen for interrupt signals
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

		// Calculate the block range from start-block and stop-block
		blockRange := ""
		if startBlock != "" {
			blockRange = startBlock
		}
		blockRange += ":"
		if stopBlock != "0" {
			blockRange += stopBlock
		}

		// Create a substreams sink using Viper configuration
		fmt.Println("new from viper")
		substreamsClient, err := subsink.NewFromViper(
			cmd,
			outputType,
			substreamsEndpoint,
			manifestPath,
			outputModuleName,
			blockRange,
			zlog,
			tracer, // tracer is nil
		)
		if err != nil {
			return fmt.Errorf("failed to create substreams sink: %w", err)
		}

		// Create a handler for the substreams sink
		fmt.Println("new from sink")
		handler := sink.NewSinker(serverTypeUrl, storeImpl, zlog, cursorFilePath)

		// Load cursor from file if it exists
		cursor := sink.LoadCursorFromFile(zlog, cursorFilePath)
		if cursor != nil {
			zlog.Info("Loaded cursor from file, will resume from saved position")
		} else {
			zlog.Info("No cursor file found, will start from the beginning")
		}

		// Start the gRPC server in a goroutine
		errCh := make(chan error, 1)
		go func() {
			fmt.Printf("Starting gRPC server on %s\n", serverAddr)
			errCh <- server.Serve(serverAddr, storeImpl)
		}()

		// Start the substreams sink in a goroutine
		sinkerDone := make(chan struct{})
		fmt.Println("new from RUN")
		go func() {
			substreamsClient.OnTerminating(func(err error) {
				zlog.Error("sinker terminating", zap.Error(err))
				close(sinkerDone)
			})
			substreamsClient.Run(cmd.Context(), cursor, handler)
		}()

		// Wait for an interrupt signal or an error from the server
		select {
		case <-sigCh:
			fmt.Println("Received interrupt signal, shutting down...")
			substreamsClient.Shutdown(nil)
			return nil
		case err := <-errCh:
			substreamsClient.Shutdown(err)
			return fmt.Errorf("server error: %w", err)
		case <-sinkerDone:
			fmt.Println("Sinker done")
			return nil
		}

	},
}

func init() {
	subsink.AddFlagsToSet(ServerCmd.Flags())

	ServerCmd.Flags().String("addr", ":50051", "Address to listen on")
	ServerCmd.Flags().String("dsn", "", "DSN for the foundational-store (e.g. badger:///path/to/db or postgres://user:pass@host:port/dbname)")
	ServerCmd.Flags().String("type-url", "", "Type URL for the stored values")
	ServerCmd.Flags().Int("workers", 10, "Number of workers for parallel operations")
	ServerCmd.Flags().String("substreams-endpoint", "", "Substreams endpoint")
	ServerCmd.Flags().String("manifest-path", "", "Path to the manifest file")
	ServerCmd.Flags().String("output-module-name", "", "Name of the output module")
	ServerCmd.Flags().String("start-block", "", "Start block")
	ServerCmd.Flags().String("stop-block", "0", "Stop block")
	// ServerCmd.Flags().String("network", "", "Network")
	ServerCmd.Flags().String("output-type", "", "Output type")
	ServerCmd.Flags().String("cursor-file-path", "", "Path to the cursor file")

	// ServerCmd.Flags().String(subsink.FlagAPITokenEnvvar, "", "name of env var that contains the token")
	// ServerCmd.Flags().String(subsink.FlagAPIKeyEnvvar, "", "name of env var that contains the key")
	// ServerCmd.Flags().Bool(subsink.FlagInsecure, false, "runs insecurely")
	// ServerCmd.Flags().Bool(subsink.FlagPlaintext, false, "uses plain text")

	ServerCmd.MarkFlagRequired("dsn")
	ServerCmd.MarkFlagRequired("type-url")

	viper.BindPFlag("server.addr", ServerCmd.Flags().Lookup("addr"))
	viper.BindPFlag("server.dsn", ServerCmd.Flags().Lookup("dsn"))
	viper.BindPFlag("server.type_url", ServerCmd.Flags().Lookup("type-url"))
	viper.BindPFlag("server.workers", ServerCmd.Flags().Lookup("workers"))
	viper.BindPFlag("substreams.endpoint", ServerCmd.Flags().Lookup("substreams-endpoint"))
	viper.BindPFlag("substreams.manifest_path", ServerCmd.Flags().Lookup("manifest-path"))
	viper.BindPFlag("substreams.output_module_name", ServerCmd.Flags().Lookup("output-module-name"))
	viper.BindPFlag("substreams.start_block", ServerCmd.Flags().Lookup("start-block"))
	viper.BindPFlag("substreams.stop_block", ServerCmd.Flags().Lookup("stop-block"))
	viper.BindPFlag("substreams.network", ServerCmd.Flags().Lookup("network"))
	viper.BindPFlag("substreams.output_type", ServerCmd.Flags().Lookup("output-type"))
	viper.BindPFlag("server.cursor_file_path", ServerCmd.Flags().Lookup("cursor-file-path"))
}
