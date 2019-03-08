// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_BINLOG_H_
#define PIKA_BINLOG_H_

#include <cstdio>
#include <list>
#include <string>
#include <deque>
#include <pthread.h>

#ifndef __STDC_FORMAT_MACROS
# define __STDC_FORMAT_MACROS
# include <inttypes.h>
#endif

#include "slash/include/slash_status.h"
#include "slash/include/slash_mutex.h"
#include "slash/include/env.h"
#include "include/pika_define.h"
#include "blackwidow/blackwidow.h"
#include "blackwidow/backupable.h"
#include "swift/shannon_db.h"

using slash::Status;
using slash::Slice;

class Version;

class Binlog {
 public:
  Binlog();
  ~Binlog();

  void Lock()         { mutex_.Lock(); }
  void Unlock()       { mutex_.Unlock(); }

  Status Put(const BinlogItem& item);


  Status GetProducerStatus(uint64_t* pro_offset, uint64_t* logic_id = NULL);
  /*
   * Set Producer pro_num and pro_offset with lock
   */
  Status SetProducerStatus(uint64_t offset);

  // Double master used
  Status GetDoubleRecvInfo(uint64_t* double_offset);

  Status SetDoubleRecvInfo(uint64_t double_offset);

  Status GetNewLogIterator(uint64_t offset, shannon::LogIterator** log_iter);

  void AddDB(shannon::DB* db) {
    dbs_.push_back(db);
  }
  std::shared_ptr<blackwidow::BlackWidow> db_;
 private:
  Version* version_;

  slash::Mutex mutex_;
  std::string default_device_name_ = "/dev/kvdev0";
  std::string binlog_path_;

  uint64_t offset_;
  uint64_t double_offset_;

  std::vector<shannon::DB*> dbs_;
  // No copying allowed
  Binlog(const Binlog&);
  void operator=(const Binlog&);
};

class Version {
 public:
  Version(shannon::DB* db, std::vector<shannon::ColumnFamilyHandle*> handle);
  Version();
  ~Version();

  Status Init();

  // RWLock should be held when access members.
  Status StableSave();

  uint64_t offset_;
  uint64_t logic_id_;

  // Double master used
  uint64_t double_master_recv_offset_;

  pthread_rwlock_t rwlock_;

  void debug() {
    slash::RWLock(&rwlock_, false);
    printf ("Current offset %lu\n", offset_);
  }

 private:
  // No copying allowed;
  Version(const Version&);
  void operator=(const Version&);
};

#endif
