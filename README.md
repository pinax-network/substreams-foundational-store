# Substreams Foundational Store

A high-performance, multi-backend key-value storage system designed for [Substreams](https://github.com/streamingfast/substreams) data ingestion and serving. The foundational store provides a unified interface to persist and query time-series blockchain data with fork-awareness and efficient batch processing.

## Architecture

The foundational store consists of three main components:

- **Sink**: Ingests streaming data from Substreams, handles batching, flushing, and fork reorganizations
- **Store**: Provides a unified interface for multiple storage backends (Badger, PostgreSQL)
- **Server**: Exposes a gRPC API for data retrieval with high-performance querying

### Key Features

- **Fork-aware storage**: Handles blockchain reorganizations by maintaining versioned data and automatic rollback capabilities
- **Multiple backends**: Support for embedded Badger database and PostgreSQL for different scale requirements
- **Batch processing**: Efficient bulk insertion with configurable batch sizes and time-based flushing
- **Async flushing**: Non-blocking write operations with configurable queue depth for optimal throughput
- **gRPC API**: High-performance data serving with Get/GetAll operations
- **Metrics**: Built-in Prometheus metrics for monitoring performance and health

## Quick Start

### Installation

Build from source:
```bash
git clone https://github.com/streamingfast/substreams-foundational-store
cd substreams-foundational-store
go build -o foundational-store ./cmd/foundational-store
```

### Running the Server

Start a server with Badger backend:
```bash
./foundational-store server \
  --dsn "badger:///path/to/data" \
  --type-url "type.googleapis.com/your.message.Type" \
  --manifest-path "path/to/substreams.yaml" \
  --output-module-name "your_output_module" \
  --endpoint "mainnet.eth.streamingfast.io:443"
```

Start a server with PostgreSQL backend:
```bash
./foundational-store server \
  --dsn "postgres://user:pass@localhost:5432/dbname" \
  --type-url "type.googleapis.com/your.message.Type" \
  --manifest-path "path/to/substreams.yaml" \
  --output-module-name "your_output_module" \
  --endpoint "mainnet.eth.streamingfast.io:443"
```

## Storage Backends

### Badger

High-performance embedded key-value store, ideal for single-node deployments:

```bash
--dsn "badger:///path/to/database"
```

### PostgreSQL

Enterprise-grade relational database for distributed deployments:

```bash
--dsn "postgres://user:password@host:port/database?sslmode=require"
```

## Configuration

```bash
foundational-store --help
account-list   Extract accounts from a CSV file and save them to a binary file
completion     Generate autocompletion scripts
get            Get a value from the foundational-store using gRPC
getall         Get multiple values from the foundational-store using gRPC
loader         Load data into a foundational-store
lookup         Lookup keys with a prefix in a Badger foundational-store
perf           Performance testing for the foundational-store
server         Start the gRPC server
```
```bash
foundational-store server --help
Start the gRPC server that provides access to the foundational-store.
Supports PostgreSQL, Badger.


Flags:
  --addr string               Address to listen on (default ":50051")
  --dsn string                DSN for the store (e.g. badger:///... or postgres://...)
  --type-url string           Protobuf type URL for stored values
  --manifest-path string      Path to Substreams manifest
  --output-module-name string Name of the output module
  --batch-size int            Number of entries per batch (default 1000)
  --max-batch-time duration   Max wait before flushing batch (default 30s)
  --flush-queue-size int      Async flush queue buffer size (default 3)
  --workers int               Number of parallel workers (default 10)
  --cursor-file-path string   Path to cursor file (default "state.cursor")
  --prometheus-addr string    Prometheus metrics address (default "localhost:9102")
  --start-block string        Starting block
  --stop-block string         Stop block (default "0")
  --undo-buffer-size int      Number of blocks kept buffered for forks
  ... (other flags for headers, API keys, retries, insecure mode, etc.)
```
```bash
foundational-store loader --help
Usage:
  foundational-store loader [flags]

Flags:
  --file string      Path to CSV file (default "/path/to/initialized_accounts.csv")
  --dsn string       DSN connection string (Postgres/Badger)
  --batch-size int   Batch size for insertion (default 1000)
```
```bash
foundational-store perf --help
Usage:
  foundational-store perf [flags]

Flags:
  --account-file string   Path to account.bin file
  --dsn string            DSN for the store
  --duration duration     Duration of the test (default 1m)
  --num-accounts int      Number of accounts in multi-account query (default 4000)
  --num-clients int       Concurrent clients (default 20)
  --num-workers int       Workers for Badger backend (default 10)
  --run-concurrent        Run concurrent multi-account queries
  --type-url string       Type URL for stored values
```

## Data Model

### Entry Structure

Data is stored as key-value pairs with block-level versioning:

```protobuf
message Entry {
  bytes key = 2;
  google.protobuf.Any value = 4;
}

message Entries {
  repeated Entry entries = 1;
}
```

### API Operations

#### Get Request
```protobuf
message GetRequest {
  uint64 block_number = 1;
  bool omit_deleted = 3;
  bytes key = 4;
}
```
#### GetAll Request
```protobuf
message GetAllRequest {
  uint64 block_number = 1;
  bool omit_deleted = 3;
  repeated bytes keys = 4;
}
```

#### Response Codes
- `FOUND`: Key exists at specified block
- `NOT_FOUND`: Key doesn't exist
- `NOT_FOUND_FINALIZE`: Key deleted after finality
- `NOT_FOUND_BLOCK_NOT_REACHED`: Block not yet processed

## Fork Handling

The foundational store maintains fork-awareness through:

1. **Versioned Storage**: Each entry is tagged with block number and hash
2. **Automatic Rollback**: Undo signals trigger data eviction up to reorganization point  
3. **LIB Tracking**: Uses Last Irreversible Block for finality decisions
4. **Cursor Management**: Persistent state for resuming from correct position

## Performance Tuning

### Batch Configuration

Optimize for your workload:

```bash
# High throughput, larger batches
--batch-size 5000 --max-batch-time 60s --flush-queue-size 5

# Low latency, smaller batches  
--batch-size 500 --max-batch-time 10s --flush-queue-size 2
```

### Backend-Specific Tuning

**Badger:**
- Adjust `--workers` based on CPU cores
- Use SSD storage for better performance
- Monitor memory usage for large datasets

**PostgreSQL:**
- Configure connection pooling
- Tune `shared_buffers` and `work_mem`
- Use appropriate indexes for query patterns

## Monitoring

### Prometheus Metrics

Built-in metrics available on `--prometheus-addr` (default: `localhost:9102`):

- `foundational_store_blocks_processed_total`: Blocks processed counter
- `foundational_store_entries_processed_total`: Entries processed counter
- `foundational_store_batch_flush_duration_seconds`: Flush operation latency
- `foundational_store_grpc_requests_total`: gRPC request counters
- `foundational_store_cursor_save_errors_total`: Cursor save error counter

### Health Checks

Monitor service health through:
- gRPC reflection for service discovery
- Cursor file updates for ingestion progress
- Prometheus `/metrics` endpoint availability

## Development

### Building

```bash
# Build binary
go build -o foundational-store ./cmd/foundational-store

# Run tests
go test ./...

# Generate protobuf
buf generate
```

### Docker

```bash
# Build image
docker build -t foundational-store .

# Run container
docker run -p 50051:50051 foundational-store server --dsn="badger:///data" --type-url="your.type"
```

### Testing

The project includes comprehensive tests:

```bash
# Run all tests
go test ./...

# Test specific backend
go test ./store/badger/...
go test ./store/postgres/...

# Test with race detection
go test -race ./...
```

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Related Projects

- [Substreams](https://github.com/streamingfast/substreams) - Real-time blockchain data processing
- [Firehose](https://github.com/streamingfast/firehose) - Blockchain data extraction protocol
