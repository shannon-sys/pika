//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_REDIS_H_
#define SRC_REDIS_H_

#include <string>
#include <memory>
#include <vector>

#include "swift/shannon_db.h"
#include "swift/status.h"
#include "swift/slice.h"

#include "src/lock_mgr.h"
#include "src/mutex_impl.h"
#include "blackwidow/blackwidow.h"

#define LLONG_MAX 9223372036854775807
#define LLONG_MIN -9223372036854775807
namespace blackwidow {
using Status = shannon::Status;
using Slice = shannon::Slice;
class Redis {
 public:
  Redis();
  virtual ~Redis();

  shannon::DB* get_db() {
    return db_;
  }

  // Common Commands
  virtual Status Open(const BlackwidowOptions& bw_options,
                      const std::string& db_path) = 0;
  virtual Status CompactRange(const shannon::Slice* begin,
                              const shannon::Slice* end) = 0;
  virtual Status GetProperty(const std::string& property, uint64_t* out) = 0;
  virtual Status ScanKeyNum(uint64_t* num) = 0;
  virtual Status ScanKeys(const std::string& pattern,
                          std::vector<std::string>* keys) = 0;

  // Keys Commands
  virtual Status Expire(const Slice& key, int32_t ttl) = 0;
  virtual Status Del(const Slice& key) = 0;
  virtual bool Scan(const std::string& start_key,
                    const std::string& pattern,
                    std::vector<std::string>* keys,
                    int64_t* count,
                    std::string* next_key) = 0;
  virtual Status Expireat(const Slice& key,
                          int32_t timestamp) = 0;
  virtual Status Persist(const Slice& key) = 0;
  virtual Status TTL(const Slice& key, int64_t* timestamp) = 0;
  virtual Status DoDelKey(){
    return Status::OK();
  };
  virtual  Status AddDelKey(BlackWidow * bw,const string & str) = 0;
 protected:
  LockMgr* lock_mgr_;
  shannon::DB* db_;
  std::string default_device_name_ = "/dev/kvdev0";
  shannon::WriteOptions default_write_options_;
  shannon::ReadOptions default_read_options_;
  shannon::CompactRangeOptions default_compact_range_options_;

  // For Scan
  slash::Mutex scan_cursors_mutex_;
  BlackWidow::LRU<std::string, std::string> scan_cursors_store_;

  Status GetScanStartPoint(const Slice& key, const Slice& pattern, int64_t cursor, std::string* start_point);
  Status StoreScanNextPoint(const Slice& key, const Slice& pattern, int64_t cursor, const std::string& next_point);
};

}  //  namespace blackwidow
#endif  //  SRC_REDIS_H_
