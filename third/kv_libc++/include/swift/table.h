// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Currently we support two types of tables: plain table and block-based table.
//   1. Block-based table: this is the default table type that we inherited from
//      LevelDB, which was designed for storing data in hard disk or flash
//      device.
//   2. Plain table: it is one of shannon's SST file format optimized
//      for low query latency on pure-memory or really low-latency media.
//
// A tutorial of shannon table formats is available here:
//   https://github.com/facebook/shannon/wiki/A-Tutorial-of-shannon-SST-formats
//
// Example code is also available
//   https://github.com/facebook/shannon/wiki/A-Tutorial-of-shannon-SST-formats#wiki-examples

#pragma once
#include <memory>
#include <string>
#include <unordered_map>
#include <iostream>

#include "swift/cache.h"
#include "swift/env.h"
#include "swift/iterator.h"
//#include "swift/options.h"
#include "swift/status.h"
#include "swift/filter_policy.h"

using namespace std;

namespace shannon {

// -- Block-based Table
struct EnvOptions;
struct Options;

using std::unique_ptr;

enum ChecksumType : char {
  kNoChecksum = 0x0,
  kCRC32c = 0x1,
  kxxHash = 0x2,
};

struct TableFactory {
};

struct BlockBasedTableOptions {
    bool cache_index_and_filter_blocks = false;

    // If cache_index_and_filter_blocks is enabled, cache index and filter
    // blocks with gith priority. If set to true, depending on implementation of
    // block cache, index and filter blocks may be less likely to be evicted
    // than data blocks.
    bool cache_index_and_filter_blocks_with_high_priority = false;

    // The index type that will be used for this table.
    enum IndexType : char {
        // A space efficient index block that is optimized for
        // binary-search-based index.
        kBinarySearch,

        // The hash index, if enabled, will do the hash lookup when
        // Options prefix_extractor is provided.
        kHashSearch,

        // A two-level index implementation. Both levels are binary search indexes.
        kTwoLevelIndexSearch,
    };

    IndexType index_type = kBinarySearch;

    // This option is now deprecated. No matter what value it is st to,
    // it will behave as if hash_index_allow_collision = true.
    bool hash_index_allow_collision = true;

    // Use the specified checksum type. Newly created table files will be
    // protected with this checksum type. Old tale files will still be readable,
    // even though they have different checksum type.
    // CheckSumType checkSUM = KCRC32c;

    // Disable block cache, If this is set to true,
    // then no block cache should be used, and the block_cache should
    // point to a nullptr object.
    bool no_block_cache = false;

    // If non-NULL use the specified cache for blocks.
    // If NULL, rocksdb will automatically create and use an 8MB internal cache.
    std::shared_ptr<Cache> block_cache = nullptr;

    // If non-NULL use the specified cache for pages read from device
    // IF NULL, no page cache is used
    //
    // Approximate size of user data packed per block. Note that the
    // block size specified here corresponds to uncompressed data. The
    // actual size of the unit read from disk may be smaller if
    // compression is enabled. This parameter can be changed dynamically.
    size_t block_size = 4 * 1024;

    // This is used to close a block before it reaches the configured
    // 'block_size'. If the percentage of free space in the current block is less
    // than this specified number and adding a new record to the block will
    // exceed the configured block size, then this block will be closed and the
    // new record will be written to the next block.
    int block_size_deviation = 10;

    // Number of keys between restart points for delta encoding of keys.
    // This parameter can be changed dynamically. Most clients should
    // leave this parameter alonee. The minimum value allowed is 1. Any smaller
    // value will be sliently overwritten with 1
    int block_restart_interval = 16;

    // Same as block_restart interval but used for the index block.
    int index_block_restart_interval = 1;

    // Block size for partitioned metadata. Currently applied to indexes when
    // kTwoLevelIndexSearch is used and to filters when partition_filters is used
    // Note: Since in the current implementation the filters and index partitions
    // are aligned, and index/filter block is created when either index or filter
    // block size reches the specified limit.
    // Note: this limit is currently applied to only index blocks; afilter
    // partition is cut right after an index block is cut
    // TODO(myabandeh): remove the note above when filter partitions are cut
    // separately
    uint64_t metadata_block_size = 4096;

    // Note: currently this option requires kTwoLevelIndexSearch to be set as
    // well.
    // TODO(myabandeh): remove the note above once the limitation is lifted
    // Use partitioned full filters for each SST file. This options is
    // incompatibile with block-based filter.
    bool partition_filters = false;

    // Use delta encoding to compress keys in blocks.
    // ReadOptions::pin_data requires this option to be disabled.
    //
    //Default： true
    bool use_delta_encoding = true;
    // If non-nullptr, use the specified filter policy to reduce disk reads.
    // Many applications will benefit from passing the result of
    // NewBloomFilterPolicy() here.
    std::shared_ptr<const FilterPolicy> filter_policy = nullptr;

    // If true, place whole keys in the filter (not just prefixes).
    // This must generally be true for gets to be efficient.
    bool whole_key_filtering = true;

    // Verify that decompressing the compressed block gives back the input. This
    // is a verification mode that we use to detect bugs in compression
    // algorithms.
    bool verify_compression = false;

  // If used, For every data block we load into memory, we will create a bitmap
  // of size ((block_size / `read_amp_bytes_per_bit`) / 8) bytes. This bitmap
  // will be used to figure out the percentage we actually read of the blocks.
  //
  // When this feature is used Tickers::READ_AMP_ESTIMATE_USEFUL_BYTES and
  // Tickers::READ_AMP_TOTAL_READ_BYTES can be used to calculate the
  // read amplification using this formula
  // (READ_AMP_TOTAL_READ_BYTES / READ_AMP_ESTIMATE_USEFUL_BYTES)
  //
  // value  =>  memory usage (percentage of loaded blocks memory)
  // 1      =>  12.50 %
  // 2      =>  06.25 %
  // 4      =>  03.12 %
  // 8      =>  01.56 %
  // 16     =>  00.78 %
  //
  // Note: This number must be a power of 2, if not it will be sanitized
  // to be the next lowest power of 2, for example a value of 7 will be
  // treated as 4, a value of 19 will be treated as 16.
  //
  // Default: 0 (disabled)
  uint32_t read_amp_bytes_per_bit = 0;

  // We currently have three versions:
  // 0 -- This version is currently written out by all RocksDB's versions by
  // default.  Can be read by really old RocksDB's. Doesn't support changing
  // checksum (default is CRC32).
  // 1 -- Can be read by RocksDB's versions since 3.0. Supports non-default
  // checksum, like xxHash. It is written by RocksDB when
  // BlockBasedTableOptions::checksum is something other than kCRC32c. (version
  // 0 is silently upconverted)
  // 2 -- Can be read by RocksDB's versions since 3.10. Changes the way we
  // encode compressed blocks with LZ4, BZip2 and Zlib compression. If you
  // don't plan to run RocksDB before version 3.10, you should probably use
  // this.
  // This option only affects newly written tables. When reading exising tables,
  // the information about version is read from the footer.
  uint32_t format_version = 2;

};

// Table Properties that are specific to block-based table properties.
struct BlockBasedTablePropertyNames {
  // value of this propertis is a fixed int32 number.
  static const std::string kIndexType;
  // value is "1" for true and "0" for false.
  static const std::string kWholeKeyFiltering;
  // value is "1" for true and "0" for false.
  static const std::string kPrefixFiltering;
};


#ifndef shannon_LITE

enum EncodingType : char {
  kPlain,
  kPrefix,
};

struct PlainTablePropertyNames {
  static const std::string kEncodingType;
  static const std::string kBloomVersion;
  static const std::string kNumBloomBlocks;
};

const uint32_t kPlainTableVariableLength = 0;
#endif

extern TableFactory* NewBlockBasedTableFactory(
        const BlockBasedTableOptions& table_options = BlockBasedTableOptions());

}  // namespace shannon
