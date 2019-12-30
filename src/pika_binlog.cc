// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_binlog.h"

#include <iostream>
#include <string>
#include <stdint.h>
#include <signal.h>
#include <unistd.h>
#include <sys/time.h>
#include "blackwidow/blackwidow.h"
#include "coding.h"
#include <glog/logging.h>
#include <sys/syscall.h>
#include "slash/include/slash_mutex.h"
#include "swift/shannon_db.h"
#include "swift/log_iter.h"

using slash::RWLock;

/*
 * Version
 */
Version::Version(shannon::DB* db, std::vector<shannon::ColumnFamilyHandle*> handles)
  : offset_(0),
    double_master_recv_offset_(0) {
    assert(db != NULL);
    pthread_rwlock_init(&rwlock_, NULL);
}

Version::Version()
  : offset_(0),
    double_master_recv_offset_(0) {
  pthread_rwlock_init(&rwlock_, NULL);
}

Version::~Version() {
  StableSave();
  pthread_rwlock_destroy(&rwlock_);
}

Status Version::StableSave() {
  std::string value;
  /*
  value.resize(8 + 8 + 8);
  char *p = const_cast<char *>(value.data());

  memcpy(p, &offset_, sizeof(uint64_t));
  p += 8;
  memcpy(p, &logic_id_, sizeof(uint64_t));
  p += 8;
  memcpy(p, &double_master_recv_offset_, sizeof(uint64_t));
  p += 8;
  shannon::Status s = db_->Put(shannon::WriteOptions(), handles_[0], "manifest", value);
  if (s.ok()) {
      return Status::OK();
  }
  */
  return Status::Corruption("error!");
}

Status Version::Init() {
  shannon::Status s;
  std::string value;
  /*
  s = db_->Get(shannon::ReadOptions(), handles_[0], "manifest", &value);
  if (s.ok()) {
    const char* p = value.data();
    memcpy((char*)(&offset_), p, sizeof(uint64_t));
    memcpy((char*)(&logic_id_), p + 8, sizeof(uint64_t));
    memcpy((char*)(&double_master_recv_offset_), p + 16, sizeof(uint64_t));
    return Status::OK();
  } else {
    return Status::Corruption("version init error");
  }
  */
  return Status::OK();
}

Binlog::Binlog() {
  version_ = new Version();
}
Binlog::~Binlog() {
  delete version_;
}

Status Binlog::GetNewLogIterator(uint64_t offset, shannon::LogIterator** log_iter) {
  if (log_iter == NULL) {
    return Status::InvalidArgument("log_iter cannot be empty");
  }
  shannon::Status s = shannon::NewLogIterator(default_device_name_, offset, log_iter);
  if (!s.ok()) {
      return Status::IOError("log iterator create failed");
  }
  return Status::OK();
}

Status Binlog::GetProducerStatus(uint64_t* pro_offset, uint64_t* logic_id) {
  slash::RWLock(&(version_->rwlock_), false);
  shannon::Status s = shannon::GetSequenceNumber(default_device_name_, pro_offset);

  if (s.ok()) {
    // version_->offset_ = pro_offset;
  } else {
    return Status::IOError("get offset failed");
  }
  if (logic_id != NULL) {
    *logic_id = version_->logic_id_;
  }
  return Status::OK();
}

Status Binlog::GetDoubleRecvInfo(uint64_t* double_offset) {
  slash::RWLock(&(version_->rwlock_), false);

  *double_offset = version_->double_master_recv_offset_;

  return Status::OK();
}

Status Binlog::SetDoubleRecvInfo(uint64_t double_offset) {
  slash::RWLock(&(version_->rwlock_), true);

  version_->double_master_recv_offset_ = double_offset;

  version_->StableSave();

  return Status::OK();
}

Status Binlog::Put(const BinlogItem& item) {
  const shannon::Slice key(item.key());
  const shannon::Slice value(item.value());
  switch(item.optype()) {
    case shannon::KEY_ADD:
      db_->LogCmdAdd(key, value, item.db(), item.cf());
    break;
    case shannon::KEY_DELETE:
      db_->LogCmdDelete(key, item.db(), item.cf());
    break;
    case shannon::DB_CREATE:
      db_->LogCmdCreateDB(value.ToString(), item.db());
    break;
    case shannon::DB_DELETE:
      db_->LogCmdDeleteDB(item.db());
    break;
    default:
      stringstream ss;
      ss<<item.optype();
      return Status::NotSupported(ss.str());
  }
  return Status::OK();
}

Status Binlog::SetProducerStatus(uint64_t offset) {
  slash::MutexLock l(&mutex_);
  shannon::Status s = shannon::SetSequenceNumber(default_device_name_, offset);
  if (s.ok()) {
    return Status::OK();
  }
  return Status::InvalidArgument("offset error!");
}

