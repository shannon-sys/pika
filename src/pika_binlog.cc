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
#include "write_batch_internal.h"

#include <glog/logging.h>
#include <sys/syscall.h>

#include "slash/include/slash_mutex.h"

using slash::RWLock;

std::string NewFileName(const std::string name, const uint32_t current) {
  char buf[256];
  snprintf(buf, sizeof(buf), "%s%u", name.c_str(), current);
  return std::string(buf);
}

/*
 * Version
 */
Version::Version(shannon::DB* db, std::vector<shannon::ColumnFamilyHandle*> handles)
  : pro_num_(0),
    pro_offset_(0),
    double_master_recv_num_(0),
    double_master_recv_offset_(0),
    db_(db),
    handles_(handles) {
    assert(db != NULL);
    pthread_rwlock_init(&rwlock_, NULL);
}

Version::~Version() {
  StableSave();
  pthread_rwlock_destroy(&rwlock_);
}

Status Version::StableSave() {
  std::string value;
  value.resize(4 + 8 + 8 + 4 + 8);
  char *p = const_cast<char *>(value.data());

  memcpy(p, &pro_num_, sizeof(uint32_t));
  p += 4;
  memcpy(p, &pro_offset_, sizeof(uint64_t));
  p += 8;
  memcpy(p, &logic_id_, sizeof(uint64_t));
  p += 8;
  memcpy(p, &double_master_recv_num_, sizeof(uint32_t));
  p += 4;
  memcpy(p, &double_master_recv_offset_, sizeof(uint64_t));
  p += 8;
  shannon::Status s = db_->Put(shannon::WriteOptions(), handles_[0], "manifest", value);
  if (s.ok()) {
      return Status::OK();
  }
  return Status::Corruption("error!");
}

Status Version::Init() {
  shannon::Status s;
  std::string value;
  s = db_->Get(shannon::ReadOptions(), handles_[0], "manifest", &value);
  if (s.ok()) {
    const char* p = value.data();
    memcpy((char*)(&pro_num_), p, sizeof(uint32_t));
    memcpy((char*)(&pro_offset_), p + 4, sizeof(uint64_t));
    memcpy((char*)(&logic_id_), p + 12, sizeof(uint64_t));
    memcpy((char*)(&double_master_recv_num_), p + 20, sizeof(uint32_t));
    memcpy((char*)(&double_master_recv_offset_), p + 24, sizeof(uint64_t));
    return Status::OK();
  } else {
    return Status::Corruption("version init error");
  }
}

/*
 * Binlog
 */
Binlog::Binlog(const std::string& binlog_path, const int file_size) :
    consumer_num_(0),
    version_(NULL),
    queue_(NULL),
    versionfile_(NULL),
    pro_num_(0),
    pool_(NULL),
    exit_all_consume_(false),
    binlog_path_(binlog_path),
    file_size_(file_size) {
  InitLogFile();
}

Binlog::~Binlog() {
  delete version_;
  delete versionfile_;

  delete queue_;
}

shannon::Status Binlog::Open(const blackwidow::BlackwidowOptions& bw_options) {
  shannon::Status s;
  shannon::Options ops(bw_options.options);
  s = shannon::DB::Open(ops, binlog_path_, default_device_name_, &db_);
  if (s.ok()) {
      shannon::ColumnFamilyHandle* cf_binlog;
      shannon::ColumnFamilyOptions cfo;
      s = db_->CreateColumnFamily(cfo, "binlog_cf", &cf_binlog);
      if (!s.ok()) {
          return s;
      }
      delete cf_binlog;
      delete db_;
  }
  shannon::DBOptions db_ops(bw_options.options);
  shannon::ColumnFamilyOptions cf_ops(bw_options.options);
  std::vector<shannon::ColumnFamilyDescriptor> column_families;
  // main cf
  column_families.push_back(shannon::ColumnFamilyDescriptor(
              shannon::kDefaultColumnFamilyName, cf_ops));
  // data cf
  column_families.push_back(shannon::ColumnFamilyDescriptor(
              "binlog_cf", shannon::ColumnFamilyOptions()));
  s = shannon::DB::Open(db_ops, binlog_path_, default_device_name_, column_families, &handles_, &db_);
  if (s.ok()) {
    std::string value;
    shannon::Status s = db_->Get(shannon::ReadOptions(), handles_[0], "manifest", &value);
    if (s.ok()) {
      version_ = new Version(db_, handles_);
      version_->Init();
      pro_num_ = version_->pro_num_;
      // Debug
      // version->debug();
    } else if(s.IsNotFound()) {
      version_ = new Version(db_, handles_);
      version_->StableSave();
    } else {
      delete db_;
      return s;
    }
  }
  return s;
}

void Binlog::InitLogFile() {
  // assert(queue_ != NULL);
  assert(queue_file_size_ != 0);

  uint64_t filesize = queue_file_size_;  // queue_->Filesize();
  block_offset_ = filesize % kBlockSize;
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

  *double_filenum = version_->double_master_recv_num_;
  *double_offset = version_->double_master_recv_offset_;

  return Status::OK();
}

Status Binlog::SetDoubleRecvInfo(uint32_t double_filenum, uint64_t double_offset) {
  slash::RWLock(&(version_->rwlock_), true);

  version_->double_master_recv_num_ = double_filenum;
  version_->double_master_recv_offset_ = double_offset;

  version_->StableSave();

  return Status::OK();
}

// Note: mutex lock should be held
Status Binlog::Put(const std::string &item) {
  return Put(item.c_str(), item.size());
}

// Note: mutex lock should be held
Status Binlog::Put(const char* item, int len) {
  Status s;

  uint64_t filesize = queue_file_size_;
  if (filesize > file_size_) {
    queue_file_size_ = 0;
    char key[4], value[8];
    // put previous pro_num modify time
    blackwidow::EncodeBigFixed32(key, pro_num_);
    blackwidow::EncodeBigFixed64(value, static_cast<int64_t>(time(NULL)));
    db_->Put(shannon::WriteOptions(), handles_[0],
            shannon::Slice(key, sizeof(uint32_t)), shannon::Slice(value, sizeof(int64_t)));
    pro_num_ ++;
    // put current pro_num modify time
    blackwidow::EncodeBigFixed32(key, pro_num_);
    blackwidow::EncodeBigFixed32(value, static_cast<int64_t>(time(NULL)));
    db_->Put(shannon::WriteOptions(), handles_[0],
            shannon::Slice(key, sizeof(uint32_t)), shannon::Slice(value, sizeof(int64_t)));
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
    version_->logic_id_++;
    version_->StableSave();
    queue_file_size_ = pro_offset;
  }

  return s;
}

Status Binlog::EmitPhysicalRecord(RecordType t, const char *ptr, size_t n, int *temp_pro_offset) {
    Status s;
    std::string value;
    assert(n <= 0xffffff);
    assert(block_offset_ + kHeaderSize + n <= kBlockSize);

    char buf[kHeaderSize];
    char key[8];
    uint64_t now;
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

    blackwidow::EncodeBigFixed32(key, pro_num_);
    blackwidow::EncodeBigFixed32(key + 4, static_cast<uint32_t>(*temp_pro_offset));
    value.append(buf, kHeaderSize);
    value.append(ptr, n);
    shannon::Status status = db_->Put(shannon::WriteOptions(), handles_[1], shannon::Slice(key, 8), value);
    block_offset_ += static_cast<int>(kHeaderSize + n);

    *temp_pro_offset += kHeaderSize + n;
    if (status.ok()) {
        return Status::OK();
    } else {
        return Status::Corruption("version init error");
    }
    return s;
}

Status Binlog::Produce(const Slice &item, int *temp_pro_offset) {
  Status s;
  const char *ptr = item.data();
  size_t left = item.size();
  bool begin = true;

  if (version_ == NULL) {
      std::cout<<"version_ pro_offset is null!"<<std::endl;
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
    } else if (begin) {
      type = kFirstType;
    } else if (end) {
      type = kLastType;
    } else {
      type = kMiddleType;
    }

    s = EmitPhysicalRecord(type, ptr, fragment_length, temp_pro_offset);
    ptr += fragment_length;
    left -= fragment_length;
    begin = false;
  } while (s.ok() && left > 0);

  return s;
}

Status Binlog::AppendBlank(slash::WritableFile *file, uint64_t len) {
  if (len < kHeaderSize) {
    return Status::OK();
  }
  return Status::OK();
}

Status Binlog::SetProducerStatus(uint32_t pro_num, uint64_t pro_offset) {
  slash::MutexLock l(&mutex_);

  // offset smaller than the first header
  if (pro_offset < 4) {
    pro_offset = 0;
  }

  std::vector<shannon::Iterator*> iterators;
  shannon::Status s = db_->NewIterators(
          shannon::ReadOptions(), handles_, &iterators);
  if (s.ok()) {
      // delete write2file0
      char key[4];
      blackwidow::EncodeBigFixed32(key, 0);
      shannon::Iterator* iter = iterators[1];
      shannon::WriteBatch write_batch;
      shannon::Slice iter_key;
      for (iter->Seek(shannon::Slice(key, 4));
           iter->Valid() && (iter_key = iter->key()).starts_with(shannon::Slice(key, 4));
           iter->Next()) {
          write_batch.Delete(handles_[1], iter_key);
          if (shannon::WriteBatchInternal::Count(&write_batch) >= 800) {
              s = db_->Write(shannon::WriteOptions(), &write_batch);
              assert(s.ok());
              write_batch.Clear();
          }
      }
      if (shannon::WriteBatchInternal::Count(&write_batch) > 0) {
          s = db_->Write(shannon::WriteOptions(), &write_batch);
          assert(s.ok());
          write_batch.Clear();
      }

      // delete write2file+pro_num
      blackwidow::EncodeBigFixed32(key, pro_num);
      for (iter->Seek(shannon::Slice(key, 4));
           iter->Valid() && (iter_key = iter->key()).starts_with(shannon::Slice(key, 4));
           iter->Next()) {
          write_batch.Delete(handles_[1], iter_key);
          if (shannon::WriteBatchInternal::Count(&write_batch) >= 800) {
              s = db_->Write(shannon::WriteOptions(), &write_batch);
              assert(s.ok());
              write_batch.Clear();
          }
      }
      write_batch.Delete(handles_[0], shannon::Slice(key, 4));
      if (shannon::WriteBatchInternal::Count(&write_batch) > 0) {
          s = db_->Write(shannon::WriteOptions(), &write_batch);
          assert(s.ok());
          write_batch.Clear();
      }
  }
  for (auto iter : iterators) {
      delete iter;
  }

  pro_num_ = pro_num;

  {
    slash::RWLock(&(version_->rwlock_), true);
    version_->pro_num_ = pro_num;
    version_->pro_offset_ = pro_offset;
    version_->StableSave();
  }

  InitLogFile();
  return Status::OK();
}
