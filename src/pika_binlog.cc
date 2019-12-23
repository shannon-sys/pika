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
Version::Version(shannon::DB* db)
  : db_(db),
    pro_num_(0),
    pro_offset_(0),
    double_master_recv_offset_(0) {
    assert(db != NULL);
    pthread_rwlock_init(&rwlock_, NULL);
}

Version::Version()
  : db_(NULL),
    pro_num_(0),
    pro_offset_(0),
    double_master_recv_offset_(0) {
  pthread_rwlock_init(&rwlock_, NULL);
}

Version::~Version() {
  StableSave();
  pthread_rwlock_destroy(&rwlock_);
}

Status Version::StableSave() {
  std::string value;
  value.append(reinterpret_cast<char*>(&pro_num_), sizeof(pro_num_));
  value.append(reinterpret_cast<char*>(&pro_offset_), sizeof(pro_offset_));
  value.append(reinterpret_cast<char*>(&logic_id_), sizeof(logic_id_));
  shannon::Status s = db_->Put(shannon::WriteOptions(), "manifest", value);
  if (s.ok()) {
      return Status::OK();
  }
  return Status::Corruption("error!");
}

Status Version::Init() {
  shannon::Status s;
  std::string value;
  s = db_->Get(shannon::ReadOptions(), "manifest", &value);
  if (s.ok()) {
    const char* p = value.data();
    memcpy(reinterpret_cast<char*>(&pro_num_), p, sizeof(pro_num_));
    p += sizeof(pro_num_);
    memcpy(reinterpret_cast<char*>(&pro_offset_), p, sizeof(pro_offset_));
    p += sizeof(pro_offset_);
    memcpy((char*)(&logic_id_), p, sizeof(logic_id_));
    return Status::OK();
  } else {
    return Status::Corruption("version init error");
  }
  return Status::OK();
}

Binlog::Binlog(const std::string& device_path, const std::string& binlog_path, const int file_size) :
    consumer_num_(0),
    version_(NULL),
    pro_num_(0),
    pool_(NULL),
    exit_all_consume_(false),
    device_path_(device_path),
    binlog_path_(binlog_path),
    file_size_(file_size){
  shannon::Options options;
  options.create_if_missing = true;
  shannon::Status s = shannon::DB::Open(options, binlog_path_, device_path_, &db_);
  if (!s.ok()) {
    LOG(FATAL) << "Binlog: open failed!";
    return;
  }
  version_ = new Version(db_);
  if (!version_->Init().ok()) {
    if (!version_->StableSave().ok())  {
      delete db_;
      LOG(FATAL) << "Binlog: open versionfile error";
      return;
    }
  }
  pro_num_ = version_->pro_num_;
}

Binlog::~Binlog() {
  delete version_;
}

Status Binlog::GetProducerStatus(uint32_t* filenum, uint64_t* pro_offset, uint64_t* logic_id) {
  slash::RWLock(&(version_->rwlock_), false);

  *filenum = version_->pro_num_;
  *pro_offset = version_->pro_offset_;
  if (logic_id != NULL) {
    *logic_id = version_->logic_id_;
  }
  return Status::OK();
}

Status Binlog::GetDoubleRecvInfo(uint32_t* double_filenum, uint64_t* double_offset) {
  slash::RWLock(&(version_->rwlock_), false);

  *double_offset = version_->double_master_recv_offset_;

  return Status::OK();
}

Status Binlog::SetDoubleRecvInfo(uint32_t double_filenum, uint64_t double_offset) {
  slash::RWLock(&(version_->rwlock_), true);

  version_->double_master_recv_offset_ = double_offset;

  version_->StableSave();

  return Status::OK();
}

Status Binlog::Put(const std::string &item) {
  return Put(item.c_str(), item.size());
}

Status Binlog::Put(const char* item, int len) {
  Status s;

  /* Check to roll log file */
  uint64_t filesize = queue_file_size_;
  if (filesize > file_size_) {
    queue_file_size_ = 0;
    // switch next file
    pro_num_ ++;
    {
      slash::RWLock(&(version_->rwlock_), true);
      version_->pro_offset_ = 0;
      version_->pro_num_ = pro_num_;
      version_->StableSave();
    }
    InitLogFile();
  }

  int pro_offset;
  s = Produce(Slice(item, len), &pro_offset);
  if (s.ok()) {
    slash::RWLock(&(version_->rwlock_), true);
    version_->pro_offset_ = pro_offset;
    version_->logic_id_ ++;
    version_->StableSave();
  }
  return s;
}

Status Binlog::Produce(const Slice &item, int *temp_pro_offset) {
  Status s;
  const char *ptr = item.data();
  size_t left = item.size();
  bool begin = true;

  if (version_ == NULL) {
    LOG(FATAL) << "version file is not exits!";
    return Status::Corruption("version file is not exists");
  }
  *temp_pro_offset = version_->pro_offset_;
  do {
    const int leftover = static_cast<int>(kBlockSize) - block_offset_;
    assert(leftover >= 0);
    if (static_cast<size_t>(leftover) < kHeaderSize) {
      if (leftover > 0) {
        *temp_pro_offset += leftover;
      }
      block_offset_ = 0;
    }

    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
    const size_t fragment_length = (left < avail) ? left : avail;
    RecordType type;
    const bool end = (left == fragment_length);
    if (begin && end) {
      type = kFullType;
    } else if(begin) {
      type = kFirstType;
    } else if(end) {
      type = kLastType;
    } else {
      type = kMiddleType;
    }

    s = EmitPhysicalRecord(type, ptr, fragment_length, temp_pro_offset);
    ptr += fragment_length;
    left -= fragment_length;
    begin = false;
  } while (s.ok() && left > 0);
}

static const bool kLittleEndian = (__BYTE_ORDER == __LITTLE_ENDIAN);
void EncodeBigFixed32(char* buf, int32_t n) {
  if (kLittleEndian) {
    buf[0] = (n >> 24) & 0xff;
    buf[1] = (n >> 16) & 0xff;
    buf[2] = (n >> 8) & 0xff;
    buf[3] = n & 0xff;
  } else {
    memcpy(buf, &n, sizeof(n));
  }
}

void EncodeBigFixed64(char* buf, int64_t n) {
  if (kLittleEndian) {
    buf[0] = (n >> 56) & 0xff;
    buf[1] = (n >> 48) & 0xff;
    buf[2] = (n >> 40) & 0xff;
    buf[3] = (n >> 32) & 0xff;
    buf[4] = (n >> 24) & 0xff;
    buf[5] = (n >> 16) & 0xff;
    buf[6] = (n >> 8) & 0xff;
    buf[7] = n & 0xff;
  } else {
    memcpy(buf, &n, sizeof(n));
  }
}

Status Binlog::EmitPhysicalRecord(RecordType t, const char *ptr, size_t n, int *temp_pro_offset) {
    assert(n <= 0xffffff);
    assert(block_offset_ + kHeaderSize + n <= kBlockSize);

    char buf[kHeaderSize];

    char key[8];
    std::string value;
    uint64_t now ;
    struct timeval tv;
    gettimeofday(&tv, NULL);
    now = tv.tv_sec;
    buf[0] = static_cast<char>(n & 0xff);
    buf[1] = static_cast<char>((n & 0xff00) >> 8);
    buf[2] = static_cast<char>(n >> 16);
    buf[3] = static_cast<char>(now & 0xff);
    buf[4] = static_cast<char>((now & 0xff00) >> 8);
    buf[5] = static_cast<char>((now & 0xff0000) >> 16);
    buf[6] = static_cast<char>((now & 0xff000000) >> 24);
    buf[7] = static_cast<char>(t);

    EncodeBigFixed32(key, pro_num_);
    EncodeBigFixed32(key + 4, *temp_pro_offset);
    value.append(buf, kHeaderSize);
    value.append(ptr, n);
    shannon::Status s = db_->Put(default_write_options_, shannon::Slice(key, 8), value);
    if (s.ok()) {
      block_offset_ += static_cast<int>(kHeaderSize + n);
      *temp_pro_offset += kHeaderSize + n;
      return Status::OK();
    }
    return Status::Corruption("");
}

Status Binlog::SetProducerStatus(uint32_t filenum, uint64_t offset) {
  slash::MutexLock l(&mutex_);
  return Status::InvalidArgument("offset error!");
}

void Binlog::InitLogFile() {
  assert(queue_file_size_ >= 0);
  uint64_t filesize = queue_file_size_;
  block_offset_ = filesize % kBlockSize;
}

