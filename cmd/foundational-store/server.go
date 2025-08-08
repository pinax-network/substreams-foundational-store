package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/streamingfast/logging"
	"github.com/streamingfast/substreams-foundational-store/server"
	"github.com/streamingfast/substreams-foundational-store/sink"
	"github.com/streamingfast/substreams-foundational-store/store"
	"github.com/streamingfast/substreams-foundational-store/store/ForkAware"
	"github.com/streamingfast/substreams-foundational-store/store/badger"
	"github.com/streamingfast/substreams-foundational-store/store/postgres"

	subsink "github.com/streamingfast/substreams/sink"
)

// ServerCmd represents the server command
var ServerCmd = &cobra.Command{
	Use:   "server",
	Short: "Start the gRPC server",
	Long: `Start the gRPC server that provides access to the foundational-store.
The server supports various foundational-store implementations (PostgreSQL, Badger) with different configurations.`,
	RunE: serverCmdE,
}

func serverCmdE(cmd *cobra.Command, args []string) error {
	// Initialize logger
	zlog, tracer := logging.ApplicationLogger("server", "info")

	// Initialize metrics
	sink.RegisterMetrics()

	// Get flag values
	serverDSN, _ := cmd.Flags().GetString("dsn")
	serverTypeUrl, _ := cmd.Flags().GetString("type-url")
	serverAddr, _ := cmd.Flags().GetString("addr")
	serverWorkers, _ := cmd.Flags().GetInt("workers")
	manifestPath, _ := cmd.Flags().GetString("manifest-path")
	outputModuleName, _ := cmd.Flags().GetString("output-module-name")
	cursorFilePath, _ := cmd.Flags().GetString("cursor-file-path")
	batchSize, _ := cmd.Flags().GetInt("batch-size")
	maxBatchTime, _ := cmd.Flags().GetDuration("max-batch-time")
	flushQueueSize, _ := cmd.Flags().GetInt("flush-queue-size")

	if serverDSN == "" {
		return fmt.Errorf("dsn is required")
	}

	if serverTypeUrl == "" {
		return fmt.Errorf("type URL is required")
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
			badger.WithLogger(zlog),
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

	// Load cursor from file if it exists and set it in the command flags so subsink.NewFromViper can use it
	cursor := sink.LoadCursorFromFile(zlog, cursorFilePath)
	if cursor != nil {
		zlog.Info("Loaded cursor from file, will resume from saved position")
	} else {
		zlog.Info("No cursor file found, will start from the beginning")
	}

	// Create a substreams sink using Viper configuration
	substreamsClient, err := subsink.NewFromViper(
		cmd,
		"",
		manifestPath,
		outputModuleName,
		"substreams-foundational-store",
		zlog,
		tracer, // tracer is nil
	)
	if err != nil {
		return fmt.Errorf("failed to create substreams sink: %w", err)
	}

	// Create a sinker for the substreams sink
	sinker := sink.NewSinker(serverTypeUrl, storeImpl, zlog, cursorFilePath, batchSize, maxBatchTime, flushQueueSize)

	// Start the gRPC server in a goroutine
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Serve(serverAddr, storeImpl, zlog)
	}()

	// Start periodic database stats logging
	dbStatsTicker := time.NewTicker(15 * time.Second)
	go func() {
		for range dbStatsTicker.C {
			sink.LogDatabaseStats(zlog)
		}
	}()
	defer dbStatsTicker.Stop()

	// Start the substreams sink in a goroutine
	go func() {
		substreamsClient.OnTerminating(func(err error) {
			sinker.Shutdown(err)
		})
		substreamsClient.Run(cmd.Context(), cursor, sinker)
	}()
	// ensure we catch any shutdown on sinker to always close substreamsclient too
	sinker.OnTerminating(func(err error) {
		substreamsClient.Shutdown(err)
	})
	// Wait for an interrupt signal or an error from the server
	select {
	case <-sigCh:
		zlog.Info("received interrupt signal, shutting down...")
		// Clean shutdown with timer cleanup and batch flush
		substreamsClient.Shutdown(nil)
		sinker.Shutdown(fmt.Errorf("received shutdown signal"))
	case err := <-errCh:
		substreamsClient.Shutdown(err)
		sinker.Shutdown(fmt.Errorf("server error: %w", err))
		err = fmt.Errorf("server error: %w", err)
	case <-sinker.Terminating():
	}
	<-sinker.Terminated() // final batch flush process
	return nil
}

func init() {
	subsink.AddFlagsToSet(ServerCmd.Flags())

	ServerCmd.Flags().String("addr", ":50051", "Address to listen on")
	ServerCmd.Flags().String("dsn", "", "DSN for the foundational-store (e.g. badger:///path/to/db or postgres://user:pass@host:port/dbname)")
	ServerCmd.Flags().String("type-url", "", "Type URL for the stored values")
	ServerCmd.Flags().Int("workers", 10, "Number of workers for parallel operations")
	ServerCmd.Flags().String("manifest-path", "", "Path to the manifest file")
	ServerCmd.Flags().String("output-module-name", "", "Name of the output module")
	ServerCmd.Flags().String("cursor-file-path", "state.cursor", "Path to the cursor file")
	ServerCmd.Flags().Int("batch-size", 1000, "Number of entries to batch for insertion")
	ServerCmd.Flags().Duration("max-batch-time", 30*time.Second, "Maximum time to wait before flushing a partial batch")
	ServerCmd.Flags().Int("flush-queue-size", 100, "Size of the async flush queue buffer")

	ServerCmd.MarkFlagRequired("dsn")
	ServerCmd.MarkFlagRequired("type-url")

	viper.BindPFlag("server.addr", ServerCmd.Flags().Lookup("addr"))
	viper.BindPFlag("server.dsn", ServerCmd.Flags().Lookup("dsn"))
	viper.BindPFlag("server.type_url", ServerCmd.Flags().Lookup("type-url"))
	viper.BindPFlag("server.workers", ServerCmd.Flags().Lookup("workers"))
	viper.BindPFlag("substreams.manifest_path", ServerCmd.Flags().Lookup("manifest-path"))
	viper.BindPFlag("substreams.output_module_name", ServerCmd.Flags().Lookup("output-module-name"))
	viper.BindPFlag("server.cursor_file_path", ServerCmd.Flags().Lookup("cursor-file-path"))
	viper.BindPFlag("server.batch_size", ServerCmd.Flags().Lookup("batch-size"))
	viper.BindPFlag("server.max_batch_time", ServerCmd.Flags().Lookup("max-batch-time"))
	viper.BindPFlag("server.flush_queue_size", ServerCmd.Flags().Lookup("flush-queue-size"))

	viper.BindPFlag("endpoint", ServerCmd.Flags().Lookup("endpoint"))
	viper.BindPFlag("start-block", ServerCmd.Flags().Lookup("start-block"))
	viper.BindPFlag("stop-block", ServerCmd.Flags().Lookup("stop-block"))
	viper.BindPFlag("development-mode", ServerCmd.Flags().Lookup("development-mode"))
	viper.BindPFlag("plaintext", ServerCmd.Flags().Lookup("plaintext"))
}
