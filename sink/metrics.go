package sink

import (
	"runtime"
	"syscall"

	"github.com/streamingfast/dmetrics"
	"go.uber.org/zap"
)

func RegisterMetrics() {
	Metrics.Register()
}

var Metrics = dmetrics.NewSet()

var EntriesProcessed = Metrics.NewCounter("foundational_store_entries_processed_total", "Total number of entries processed and stored")
var EntriesPerBlock = Metrics.NewHistogram("foundational_store_entries_per_block", "Number of entries processed per block")

var StoreSetAllDuration = Metrics.NewHistogram("foundational_store_setall_duration_seconds", "Time taken for SetAll operations")
var StoreFlushDuration = Metrics.NewHistogram("foundational_store_flush_operation_duration_seconds", "Time taken for flush operations")
var StoreEvictDuration = Metrics.NewHistogram("foundational_store_evict_duration_seconds", "Time taken for evict operations")

var DiskSpaceUsed = Metrics.NewGauge("foundational_store_disk_space_used_bytes", "Disk space used by the foundational store in bytes")
var DiskSpaceFree = Metrics.NewGauge("foundational_store_disk_space_free_bytes", "Free disk space available in bytes")
var DiskSpaceTotal = Metrics.NewGauge("foundational_store_disk_space_total_bytes", "Total disk space in bytes")
var MemoryUsage = Metrics.NewGauge("foundational_store_memory_usage_bytes", "Memory usage in bytes")

var BadgerStoreSize = Metrics.NewGauge("foundational_store_badger_size_bytes", "Size of Badger database in bytes")
var BadgerFlushDuration = Metrics.NewHistogram("foundational_store_badger_flush_duration_seconds", "Time taken for BadgerDB flush operations")
var BadgerFlushCount = Metrics.NewCounter("foundational_store_badger_flush_total", "Total number of BadgerDB flush operations")
var BadgerFlushErrors = Metrics.NewCounter("foundational_store_badger_flush_errors_total", "Number of BadgerDB flush errors")

var GRPCGetDuration = Metrics.NewHistogram("foundational_store_grpc_get_duration_seconds", "Time taken for gRPC Get requests")
var GRPCGetCount = Metrics.NewCounter("foundational_store_grpc_get_total", "Total number of gRPC Get requests")
var GRPCGetAllDuration = Metrics.NewHistogram("foundational_store_grpc_getall_duration_seconds", "Time taken for gRPC GetAll requests")
var GRPCGetAllCount = Metrics.NewCounter("foundational_store_grpc_getall_total", "Total number of gRPC GetAll requests")

// Database operation metrics
var DatabaseKeysProcessed = Metrics.NewCounter("foundational_store_db_keys_processed_total", "Total number of keys processed by database")
var DatabaseGetHits = Metrics.NewCounter("foundational_store_db_get_hits_total", "Number of successful database get operations")
var DatabaseGetMisses = Metrics.NewCounter("foundational_store_db_get_misses_total", "Number of database get operations that returned no data")
var DatabaseGetErrors = Metrics.NewCounter("foundational_store_db_get_errors_total", "Number of database get operation errors")
var DatabaseSetOperations = Metrics.NewCounter("foundational_store_db_set_operations_total", "Total number of database set operations")
var DatabaseSetErrors = Metrics.NewCounter("foundational_store_db_set_errors_total", "Number of database set operation errors")

// Badger specific metrics
var BadgerLSMSize = Metrics.NewGauge("foundational_store_badger_lsm_size_bytes", "Size of Badger LSM tree in bytes")
var BadgerVLogSize = Metrics.NewGauge("foundational_store_badger_vlog_size_bytes", "Size of Badger value log in bytes")
var BadgerPendingWrites = Metrics.NewGauge("foundational_store_badger_pending_writes", "Number of pending writes in Badger")
var BadgerMemTableSize = Metrics.NewGauge("foundational_store_badger_memtable_size_bytes", "Size of Badger memtables in bytes")

var PostgresConnections = Metrics.NewGauge("foundational_store_postgres_connections", "Number of active PostgreSQL connections")

var CursorSaveErrors = Metrics.NewCounter("foundational_store_cursor_save_errors_total", "Number of errors saving cursor to file")

func GetDiskUsage(path string) (total, free, used uint64, err error) {
	var stat syscall.Statfs_t
	err = syscall.Statfs(path, &stat)
	if err != nil {
		return 0, 0, 0, err
	}

	total = stat.Blocks * uint64(stat.Bsize)
	free = stat.Bavail * uint64(stat.Bsize)
	used = total - free

	return total, free, used, nil
}

func UpdateDiskMetrics(path string) {
	total, free, used, err := GetDiskUsage(path)
	if err != nil {
		// Don't fail the entire op if metrics fail
		return
	}

	DiskSpaceTotal.SetUint64(total)
	DiskSpaceFree.SetUint64(free)
	DiskSpaceUsed.SetUint64(used)
}

func RecordBlockProcessing(entriesCount int, blockNum uint64) {
	EntriesProcessed.AddInt(entriesCount)
	EntriesPerBlock.ObserveInt64(int64(entriesCount))

	// Update memory metrics every 50 blocks to avoid overhead
	if blockNum%50 == 0 {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		MemoryUsage.SetUint64(m.Alloc)
	}
}

func LogDatabaseStats(logger *zap.Logger) {
	keysProcessed := uint64(DatabaseKeysProcessed.Get())
	getHits := uint64(DatabaseGetHits.Get())
	getMisses := uint64(DatabaseGetMisses.Get())
	getErrors := uint64(DatabaseGetErrors.Get())
	setOps := uint64(DatabaseSetOperations.Get())
	setErrors := uint64(DatabaseSetErrors.Get())

	flushCount := uint64(BadgerFlushCount.Get())
	flushErrors := uint64(BadgerFlushErrors.Get())

	badgerSize := uint64(BadgerStoreSize.Get())
	lsmSize := uint64(BadgerLSMSize.Get())
	vlogSize := uint64(BadgerVLogSize.Get())

	totalGets := getHits + getMisses
	hitRate := float64(0)
	if totalGets > 0 {
		hitRate = float64(getHits) / float64(totalGets) * 100
	}

	logger.Info("database stats",
		zap.Uint64("keys_processed_total", keysProcessed),
		zap.Uint64("get_hits", getHits),
		zap.Uint64("get_misses", getMisses),
		zap.Float64("cache_hit_rate_percent", hitRate),
		zap.Uint64("get_errors", getErrors),
		zap.Uint64("set_operations", setOps),
		zap.Uint64("set_errors", setErrors),
		zap.Uint64("badger_flushes", flushCount),
		zap.Uint64("badger_flush_errors", flushErrors),
		zap.Uint64("badger_total_size_bytes", badgerSize),
		zap.Uint64("badger_lsm_size_bytes", lsmSize),
		zap.Uint64("badger_vlog_size_bytes", vlogSize),
	)
}
