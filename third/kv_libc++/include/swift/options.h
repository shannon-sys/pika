// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef SHANNON_DB_INCLUDE_OPTIONS_H_
#define SHANNON_DB_INCLUDE_OPTIONS_H_

#include <stddef.h>
#include <vector>
#include <string>
#include "swift/env.h"
#include "swift/compaction_filter.h"
#include "swift/comparator.h"
#include "swift/table.h"
#include "swift/advanced_options.h"

namespace shannon {

class Snapshot;
struct ColumnFamilyDescriptor;
class ColumnFamilyHandle;
struct Options;
struct DbPath;

enum ManualCompactionType {
	KCompactSelfRepairRemoveExtraSSTFile,
	KCompactSelfRepairRemoveGCSSTFile,
	KCompactSelfRepairRemoveCorruptionSSTFile,
};

enum CompressionType : unsigned char {
    // NOTE: do not change the values of existing entries, as these are
    // part of the persistent format on disk.
    kNoCompression = 0x0,
    kSnappyCompression = 0x1,
    kZlibCompression = 0x2,
    kBZip2Compression = 0x3,
    kLZ4Compression = 0x5,
    kXpressCompression = 0x6,
    kZSTD = 0x7,

    // Only use kZSTDNotFinalCompression if you have to use ZSTD lib older than
    // 0.8.0 or consider a possibility of downgrading the service or copying
    // the database files to another service running with an older version of
    // RocksdDB that doesn't have kZSTD. Otherwise, you should use kZSTD. We will
    // eventually remove the option from the public API.
    kZSTDNotFinalCompression = 0x40,

    // kDisableCompressionOption is used to disable some compression options.
    kDisableCompressionOption = 0xff
};

struct DBOptions {
	DBOptions* OldDefaults(int rocksdb_major_version = 4,
				int rocksdb_minor_version = 6);
	DBOptions* OptimizeForSmallDb();
	bool create_if_missing = false;
	bool create_missing_column_families = false;
	bool error_if_exists = false;
	bool paranoid_checks = true;

	//std::shared_ptr<RateLimiter> rate_limiter = nullptr;
	//std::shared_ptr<SstFileManager> sst_file_manager = nullptr;
	std::shared_ptr<Logger> info_log = nullptr;
    Env* env = Env::Default();
	int max_open_files = -1;
	int max_file_opening_threads = 16;
	uint64_t max_total_wal_size = 0;
	//std::shared_ptr<Statistics> statistics = nullptr;
	bool use_fsync = false;
	std::vector<DbPath> db_paths;
	std::string db_log_dir = "";
	std::string wal_dir = "";
	uint64_t delete_obsolete_files_period_micros = 6ULL * 60 * 60 * 1000000;
	int max_background_jobs = 2;
	int base_background_compactions = -1;
	int max_background_compactions = -1;
	uint32_t max_subcompactions = 1;
	int max_background_flushes = -1;
	size_t max_log_file_size = 0;
	size_t log_file_time_to_roll = 0;
	size_t keep_log_file_num = 1000;
	size_t recycle_log_file_num = 0;
	uint64_t max_manifest_file_size=  1024 * 1024 * 1024;
	int table_cache_numshardbits = 6;
	uint64_t WAL_ttl_seconds = 0;
	uint64_t WAL_size_limit_MB = 0;
	size_t manifest_preallocation_size = 4 * 1024 * 1024;
	bool allow_mmap_reads = false;
	bool allow_mmap_writes = false;
	bool use_direct_reads = false;
	bool use_direct_io_for_flush_and_compaction = false;
	bool allow_fallocate = true;
	bool is_fd_close_on_exec = true;
	bool skip_log_error_on_recovery = false;
	unsigned int stats_dump_period_sec = 600;
	bool advise_random_on_open = true;
	size_t db_write_buffer_size = 0;
//	std::shared_ptr<WriteBufferManager> write_buffer_manager = nullptr;
	enum AccessHint {
		NONE,
		NORMAL,
		SEQUENTIAL,
		WILLNEED
	};
	AccessHint access_hint_on_compaction_start = NORMAL;
	bool new_table_reader_for_compaction_inputs = false;
	size_t compaction_readahead_size = 0;
	size_t random_access_max_buffer_size = 1024 * 1024;
	size_t writable_file_max_buffer_size = 1024 * 1024;
	bool use_adaptive_mutex = false;
	DBOptions(){}
	//explicit DBOptions(const Options& options);
//	void Dump(Logger* log) const;
	uint64_t bytes_per_sync = 0;
	uint64_t wal_bytes_per_sync = 0;
//	std::vector<std::shared_ptr<EventListener>> listeners;
	bool enable_thread_tracking = false;
	uint64_t delayed_write_rate = 0;
	bool enable_pipelined_write = false;
	bool allow_concurrent_memtable_write = true;
	bool enable_write_thread_adaptive_yield = true;
	uint64_t write_thread_max_yield_usec = 100;
	uint64_t write_thread_slow_yield_usec = 3;
	bool skip_stats_update_on_db_open = false;
//	WALRecoveryMode wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;
	bool allow_2pc = false;
	//std::shared_ptr<Cache> row_cache = nullptr;
	bool fail_if_options_file_error = false;
	bool dump_malloc_stats = false;
	bool avoid_flush_during_recovery = false;
	bool avoid_flush_during_shutdown = false;
	bool allow_ingest_behind = false;
	bool preserve_deletes = false;
	bool two_write_queues = false;
	bool manual_wal_flush = false;
};
/*
struct DBOptions {
  // -------------------
  // Parameters that affect behavior

  // If true, the database will be created if it is missing.
  // Default: false
  bool create_if_missing;

  // If true, an error is raised if the database already exists.
  // Default: false
  bool error_if_exists;

  // -------------------
  // Parameters that affect performance

  // Amount of cache to use in memory
  //
  // Default: 1GB
  size_t cache_size;
  bool paranoid_checks = true;
  Env* env = Env::Default();
};
*/
struct AdvancedColumnFamilyOptions {
    size_t cache_size = 1024;
	int max_write_buffer_number = 2;
	int min_write_buffer_number_to_merge = 1;
	int max_write_buffer_number_to_maintain = 0;
	bool inplace_update_support = false;
	size_t inplace_update_num_locks = 10000;
	//UpdateStatus (*inplace_callback)(char* existing_value,
	//				uint32_t* existing_value_size,
	//				Slice delta_value,
	//				std::string* merged_value) = nullptr; double memtable_prefix_bloom_size_ratio = 0.0;
	size_t memtable_huge_page_size = 0;
	// std::shared_ptr<const SliceTransform> memtable_insert_with_hint_prefix_extractor = nullptr;
	uint32_t bloom_locality = 0;
	size_t arena_block_size = 0;
	// std::vector<CompressionType> compression_per_level;
	int num_levels = 7;
	int level0_slowdown_writes_trigger = 20;
	int level0_stop_writes_trigger = 36;
	uint64_t target_file_size_base = 64 * 1048576;
	int target_file_size_multiplier = 1;
	bool level_compaction_dynamic_level_bytes = false;
	double max_bytes_for_level_multiplier = 10;
	// std::vector<int> max_bytes_for_level_multiplier_additional = std::vector<int>(num_levels, 1);
	uint64_t max_compaction_bytes = 0;
	uint64_t soft_pending_compaction_bytes_limit = 64 * 1073741824ull;
	uint64_t hard_pending_compaction_bytes_limit = 256 * 1073741824ull;
	// CompactionStyle compaction_style = kCompactionStyleLevel;
	// CompactionPri compaction_pri = kByCompensatedSize;
	// CompactionOptionsUniversal compaction_options_universal;
	// CompactionOptionsFIFO compaction_options_fifio;
	uint64_t max_sequential_skip_in_iterations = 8;
	// std::shared_ptr<MemTableRepFactory> memtable_factory = std::shared_ptr<SkipListFactory>(new SkipListFactory);
	// typedef std::vector<std::shared_ptr<TablePropertiesCollectorFactor>> TablePropertiesCollectorFactories;
	// TablePropertieCollectorFactories table_properties_collector_factories;
	size_t max_successive_merges = 0;
	bool optimize_filters_for_hits = false;
	bool paranoid_file_checks = false;
	bool force_consistency_checks = false;
	bool report_bg_io_stats = false;
	uint64_t ttl = 0;
	AdvancedColumnFamilyOptions() { }
	explicit AdvancedColumnFamilyOptions(const Options& options) {  }
	int max_mem_compaction_level;
	double soft_rate_limit=  0.0;
	double hard_rate_limit = 0.0;
	unsigned int rate_limit_delay_max_milliseconds = 100;
	bool purge_redundant_kvs_while_flush = true;
};

//
struct ColumnFamilyOptions : public AdvancedColumnFamilyOptions {
	ColumnFamilyOptions* OldDefaults(int rocksdb_major_version = 4,
					int rocksdb_minor_version = 6);
	ColumnFamilyOptions* OptimizeForSmallDb();
	ColumnFamilyOptions* OptimizeForPointLookup(uint64_t block_cache_size_mb);
	ColumnFamilyOptions* OptimizeLevelStyleCompaction(
		uint64_t memtable_memory_budget = 512 * 1024 * 1024);
	ColumnFamilyOptions* OptimizeUniversalStyleCompaction(
		uint64_t memtable_memory_budget = 512 * 1024 * 1024);
	const Comparator* comparator = BytewiseComparator();
	// std::shared_ptr<MergeOperator> merge_operator = nullptr;
	// const CompactionFilter* compaction_filter = nullptr;
	std::shared_ptr<CompactionFilterFactory> compaction_filter_factory = nullptr;
	size_t write_buffer_size = 64 << 20;
	CompressionType compression;
	// CompressionType bottommost_compression = kDisableCompressionOption;
	// CompressionOptions bottommost_compression_opts;
	// CompressionOptions compression_opts;
	// int level0_file_num_compaction_trigger = 4;
	// std::shared_ptr<const SliceTransform> prefix_extractor = nullptr;
	uint64_t max_bytes_for_level_base = 256 * 1048576;
	bool disable_auto_compactions = false;
	std::shared_ptr<TableFactory> table_factory;
	std::vector<DbPath> cf_paths;
	ColumnFamilyOptions(){ }
	explicit ColumnFamilyOptions(const Options& options) {  }
	// void Dump(Logger* log) const;
};

// Options to control the behavior of a database (passed to DB::Open)
struct Options : public DBOptions, public ColumnFamilyOptions {
  // Create an Options object with default values for all fields.
  Options() : DBOptions(), ColumnFamilyOptions() {  };
  Options(const DBOptions& db_options,
          const ColumnFamilyOptions& column_family_options)
      : DBOptions(db_options), ColumnFamilyOptions(column_family_options) {}
};
// Options that control read operations
struct ReadOptions {
  // If true, all data read from underlying storage will be
  // verified against corr
  // Default: false
  bool verify_checksums;

  // Should the data read for this iteration be cached in memory?
  // Callers may wish to set this field to false for bulk scans.
  // Default: true
  bool fill_cache;

  // If "snapshot" is non-NULL, read as of the supplied snapshot
  // (which must belong to the DB that is being read and which must
  // not have been released).  If "snapshot" is NULL, use an impliicit
  // snapshot of the state at the beginning of this read operation.
  // Default: NULL
  const Snapshot* snapshot;

  ReadOptions()
      : verify_checksums(false),
        fill_cache(true),
        snapshot(NULL) {
  }
};

// Options that control write operations
struct WriteOptions {
  // Default: true
  bool sync;
  // Should the data write for this iteration be cached in memory?
  // Default: true
  bool fill_cache;

  WriteOptions()
      : sync(true),
        fill_cache(true) {
  }
};

struct DbPath {
	std::string path;
	uint64_t target_size;
	DbPath() : target_size(0) {}
	DbPath(const std::string& p, uint64_t t) : path(p), target_size(t) {}
};


// For level based compaction, we can configure if we want to skip/force
// bottommost level compaction.
enum class BottommostLevelCompaction {
    // Skip bottommost level compaction
    kSkip,
    // Only compact bottommost level if there is a compaction filter
    // This is the default option
    kIfHaveCompactionFilter,
    // Always compact bottommost level
    kForce,
};
struct CompactRangeOptions {
    // If true, no other compaction will run at the same time as this
    // manual compaction
    bool exclusive_manual_compaction = true;
    // If true, compacted files will be moved to the minimum level capable
    // of holding the data or given level (specified non-negative target level).
    bool change_level = false;
    // If change_level is true and target_level have non-negative value, compacted
    // files will be moved to target_level.
    int target_level = -1;
    // Compaction outputs will be placed in options.db_paths[target_path_id].
    // Behavior is undefined if target_path_id is out of range.
    uint32_t target_path_id = 0;
    // By default level based compaction will only compact the bottommost level
    // if there is a compaction filter
    BottommostLevelCompaction bottommost_level_compaction =
        BottommostLevelCompaction::kIfHaveCompactionFilter;
    // If true, will execute immediately even if doing so would cause the DB to
    // enter write stall mode. Otherwise, it'll sleep until load is low enough.
    bool allow_write_stall = false;
    // If > 0, it will replace the option in the DBOptions for this compaction.
    uint32_t max_subcompactions = 0;
};

};  // namespace shannon

#endif  // SHANNON_DB_INCLUDE_OPTIONS_H_
