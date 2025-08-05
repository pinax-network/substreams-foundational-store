package sink

import (
	"runtime"
	"syscall"

	"github.com/streamingfast/dmetrics"
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
