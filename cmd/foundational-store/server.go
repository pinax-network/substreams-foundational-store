package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/streamingfast/cli"
	"github.com/streamingfast/logging"
	"github.com/streamingfast/substreams-foundational-store/server"
	"github.com/streamingfast/substreams-foundational-store/sink"
	"github.com/streamingfast/substreams-foundational-store/store"
	"github.com/streamingfast/substreams-foundational-store/store/ForkAware"
	"github.com/streamingfast/substreams-foundational-store/store/badger"
	"github.com/streamingfast/substreams-foundational-store/store/postgres"
	"go.uber.org/zap"

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
	// sink.RegisterMetrics()

	// Get flag values
	serverDSN, _ := cmd.Flags().GetString("dsn")
	serverTypeUrl, _ := cmd.Flags().GetString("type-url")
	serverAddr, _ := cmd.Flags().GetString("addr")
	serverWorkers, _ := cmd.Flags().GetInt("workers")
	manifestPath, _ := cmd.Flags().GetString("manifest-path")
	outputModuleName, _ := cmd.Flags().GetString("output-module-name")
	cursorFilePath, _ := cmd.Flags().GetString("cursor-file-path")

	if serverDSN == "" {
		return fmt.Errorf("dsn is required")
	}

	if serverTypeUrl == "" {
		return fmt.Errorf("type URL is required")
	}

	if !strings.HasPrefix(serverTypeUrl, "type.googleapis.com/") {
		serverTypeUrl = "type.googleapis.com/" + serverTypeUrl
	}

	// Parse the DSN
	dsn, err := store.ParseDSN(serverDSN)
	if err != nil {
		return fmt.Errorf("failed to parse DSN: %w", err)
	}

	// Create the foundational-store based on the DSN driver
	var baseStore store.Store

	switch dsn.Driver() {
	case "badger":
		badgerStore, err := badger.NewStore(dsn, serverTypeUrl,
			badger.WithNumWorkers(serverWorkers),
			badger.WithLogger(zlog),
		)
		if err != nil {
			return fmt.Errorf("failed to create Badger foundational-store: %w", err)
		}
		defer badgerStore.Close()
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

	// Load cursor from file if it exists and set it in the command flags so subsink.NewFromViper can use it
	cursor := sink.LoadCursorFromFile(zlog, cursorFilePath)
	if cursor != nil {
		zlog.Info("loaded cursor from file, will resume from saved position")
	} else {
		zlog.Info("no cursor file found, will start from the beginning")
	}

	app := cli.NewApplication(cmd.Context())

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

	// Start periodic database stats logging
	dbStatsTicker := time.NewTicker(15 * time.Second)
	// logger := zlog.Named("stats").With(zap.Bool("keep", false))

	// go func() {
	// 	for range dbStatsTicker.C {
	// 		sink.LogDatabaseStats(logger)
	// 	}
	// }()
	defer dbStatsTicker.Stop()

	sinker := sink.NewSinker(storeImpl, zlog, cursorFilePath)
	server := server.NewStoreServer(storeImpl, sinker.HeadBlock, zlog)

	app.SuperviseAndStartUsing(sinker.Shutter, func() {
		substreamsClient.Run(cmd.Context(), cursor, sinker)
		sinker.Shutdown(substreamsClient.Err())
	})

	app.SuperviseAndStartUsing(server, func() {
		server.Run(serverAddr)
	})

	appErr := app.WaitForTermination(zlog, 5*time.Second, 15*time.Second)
	if appErr != nil {
		zlog.Error("application error", zap.Error(appErr))
		zlog.Core().Sync()
		os.Exit(1)
	}

	// TODO: Double check, probably not require
	<-sinker.Terminated() // final batch flush process
	return nil
}

func init() {
	subsink.AddFlagsToSet(ServerCmd.Flags())

	ServerCmd.Flags().String("addr", ":50051", "Address to listen on")
	ServerCmd.Flags().String("dsn", "", "DSN for the foundational-store (e.g. badger:///path/to/db or postgres://user:pass@host:port/dbname)")
	ServerCmd.Flags().String("type-url", "", "any.Any type URL are stripped at storage, this needs to be the domain specific type URL like 'sf.substreams.spl-initialized-account.v1.AccountOwner', used by the server to reconstruct the correct any.Any value at retrieval time")
	ServerCmd.Flags().Int("workers", 10, "Number of workers for parallel operations")
	ServerCmd.Flags().String("manifest-path", "", "Path to the manifest file")
	ServerCmd.Flags().String("output-module-name", "", "Name of the output module")
	ServerCmd.Flags().String("cursor-file-path", "state.cursor", "Path to the cursor file")
	ServerCmd.Flags().Int("batch-size", 1, "Number of entries to batch for insertion")
	ServerCmd.Flags().Duration("max-batch-time", 30*time.Second, "Maximum time to wait before flushing a partial batch")
	ServerCmd.Flags().Int("flush-queue-size", 3, "Size of the async flush queue buffer")

	ServerCmd.MarkFlagRequired("dsn")
	ServerCmd.MarkFlagRequired("type-url")

	viper.BindPFlag("server.addr", ServerCmd.Flags().Lookup("addr"))
	viper.BindPFlag("server.dsn", ServerCmd.Flags().Lookup("dsn"))
	viper.BindPFlag("server.type_url", ServerCmd.Flags().Lookup("type-url"))
	viper.BindPFlag("server.workers", ServerCmd.Flags().Lookup("workers"))
	viper.BindPFlag("substreams.manifest_path", ServerCmd.Flags().Lookup("manifest-path"))
	viper.BindPFlag("substreams.output_module_name", ServerCmd.Flags().Lookup("output-module-name"))
	viper.BindPFlag("server.cursor_file_path", ServerCmd.Flags().Lookup("cursor-file-path"))

	viper.BindPFlag("endpoint", ServerCmd.Flags().Lookup("endpoint"))
	viper.BindPFlag("start-block", ServerCmd.Flags().Lookup("start-block"))
	viper.BindPFlag("stop-block", ServerCmd.Flags().Lookup("stop-block"))
	viper.BindPFlag("development-mode", ServerCmd.Flags().Lookup("development-mode"))
	viper.BindPFlag("plaintext", ServerCmd.Flags().Lookup("plaintext"))
}
