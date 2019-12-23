// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_binlog_sender_thread.h"

#include <glog/logging.h>
#include <poll.h>

#include "include/pika_server.h"
#include "include/pika_define.h"
#include "include/pika_binlog_sender_thread.h"
#include "include/pika_master_conn.h"
#include "include/rate_limiter.h"
#include "pink/include/redis_cli.h"
#include "coding.h"
#include <sys/syscall.h>
#include "swift/log_iter.h"

extern PikaServer* g_pika_server;
extern RateLimiter* g_rate_limiter;
extern void EncodeBigFixed32(char* buf, int32_t n);
extern void EncodeBigFixed64(char* buf, int64_t n);

PikaBinlogSenderThread::PikaBinlogSenderThread(const std::string &ip, int port,
                                               int64_t sid,
                                               shannon::DB* db,
                                               uint32_t filenum,
                                               uint64_t con_offset)
    :
      filenum_(filenum),
      con_offset_(con_offset),
      db_(db),
      backing_store_(new char[kBlockSize]),
      buffer_(),
      ip_(ip),
      port_(port),
      sid_(sid),
      timeout_ms_(35000) {
  cli_ = pink::NewRedisCli();
  last_record_offset_ = con_offset % kBlockSize;
  set_thread_name("BinlogSender");
}

PikaBinlogSenderThread::~PikaBinlogSenderThread() {
  StopThread();
  delete cli_;
  delete[] backing_store_;
  // delete queue_;
  LOG(INFO) << "a BinlogSender thread " << thread_id() << " exit!";
}

int PikaBinlogSenderThread::trim() {
  slash::Status s;
  uint64_t start_block = (con_offset_ / kBlockSize) * kBlockSize;
  uint64_t block_offset = con_offset_ % kBlockSize;
  uint64_t ret = 0;
  uint64_t res = 0;
  bool is_error = false;

  while (true) {
    if (res >= block_offset) {
      con_offset_ = start_block + res;
      break;
    }
    ret = get_next(is_error);
    if (is_error == true) {
      return -1;
    }
    res += ret;
  }
  last_record_offset_ = con_offset_ % kBlockSize;
  return 0;
}

uint64_t PikaBinlogSenderThread::get_next(bool &is_error) {
  uint64_t offset = 0;
  slash::Status s;
  shannon::Status status;
  is_error = false;
  char key[8];

  while (true) {
    EncodeBigFixed32(key, filenum_);
    EncodeBigFixed64(key + 4, static_cast<uint32_t>(con_offset_));
    status = db_->Get(shannon::ReadOptions(), shannon::Slice(key, 8), &s_buffer_);
    if (!status.ok()) {
      is_error = true;
    }

    const char* header = s_buffer_.data();
    const uint32_t a = static_cast<uint32_t>(header[0]) & 0xff;
    const uint32_t b = static_cast<uint32_t>(header[1]) & 0xff;
    const uint32_t c = static_cast<uint32_t>(header[2]) & 0xff;
    const unsigned int type = header[7];
    const uint32_t length = a | (b << 8) | (c << 16);

    if (type == kFullType) {
      buffer_ = Slice(s_buffer_.data() + 8, s_buffer_.size() - 8);
      offset += kHeaderSize + length;
      break;
    } else if (type == kFirstType) {
      buffer_ = Slice(s_buffer_.data() + 8, s_buffer_.size() - 8);
      offset += kHeaderSize + length;
      break;
    } else if (type == kMiddleType) {
      buffer_ = Slice(s_buffer_.data() + 8, s_buffer_.size() - 8);
      offset += kHeaderSize + length;
      break;
    } else if (type == kLastType) {
      buffer_ = Slice(s_buffer_.data() + 8, s_buffer_.size() - 8);
      offset += kHeaderSize + length;
      break;
    } else {
      is_error = true;
      break;
    }
  }
  return offset;
}

unsigned int PikaBinlogSenderThread::ReadPhysicalRecord(slash::Slice *result) {
  slash::Status s;
  shannon::Status status;
  if (kBlockSize - last_record_offset_ <= kHeaderSize) {
    con_offset_ += (kBlockSize - last_record_offset_);
    last_record_offset_ = 0;
  }
  buffer_.clear();
  char key[8];
  EncodeBigFixed32(key, filenum_);
  EncodeBigFixed32(key + 4, static_cast<uint32_t>(con_offset_));
  status = db_->Get(shannon::ReadOptions(), shannon::Slice(key, sizeof(key)), &s_buffer_);
  if (status.IsNotFound()) {
    return kEof;
  } else if(!status.ok()) {
    return kBadRecord;
  }

  const char* header = s_buffer_.data();
  const uint32_t a = static_cast<uint32_t>(header[0]) & 0xff;
  const uint32_t b = static_cast<uint32_t>(header[1]) & 0xff;
  const uint32_t c = static_cast<uint32_t>(header[2]) & 0xff;
  const unsigned int type = header[7];
  const uint32_t length = a | (b << 8) | (c << 16);
  if (type == kZeroType || length == 0) {
    buffer_.clear();
    return kOldRecord;
  }

  buffer_.clear();
  buffer_ = Slice(s_buffer_.data() + 8, s_buffer_.size() - 8);
  *result = slash::Slice(buffer_.data(), buffer_.size());
  last_record_offset_ += kHeaderSize + length;
  if (result->size() == length) {
    con_offset_ += (kHeaderSize + length);
  }
  return type;
}

long get_time() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000000 + tv.tv_usec;
}

Status PikaBinlogSenderThread::Consume(std::string &scratch) {
  Status s;

  slash::Slice fragment;
  while (true) {
    const unsigned int record_type = ReadPhysicalRecord(&fragment);

    switch (record_type) {
      case kFullType:
        scratch = std::string(fragment.data(), fragment.size());
        s = Status::OK();
        break;
      case kFirstType:
        scratch.assign(fragment.data(), fragment.size());
        s = Status::NotFound("Middle Status");
        break;
      case kMiddleType:
        scratch.append(fragment.data(), fragment.size());
        s = Status::NotFound("Middle Status");
        break;
      case kLastType:
        scratch.append(fragment.data(), fragment.size());
        s = Status::OK();
        break;
      case kEof:
        return Status::EndFile("Eof");
      case kBadRecord:
        return Status::IOError("Data Corruption");
      case kOldRecord:
        return Status::EndFile("Eof");
      default:
        return Status::IOError("Unknow reason");
    }
    if (s.ok()) {
      break;
    }
  }
  return Status::OK();
}

// Get a whole message;
// the status will be OK, IOError or Corruption;
Status PikaBinlogSenderThread::Parse(std::string &scratch) {
  scratch.clear();
  Status s;
  uint32_t pro_num;
  uint64_t pro_offset;

  Binlog* logger = g_pika_server->logger_;
  while (!should_stop()) {
    logger->GetProducerStatus(&pro_num, &pro_offset);
    if (filenum_ == pro_num && con_offset_ == pro_offset) {
      DLOG(INFO) << "BinlogSender Parse no new msg, con_offset " << con_offset_;
      usleep(10000);
      continue;
    }

    //DLOG(INFO) << "BinlogSender start Parse a msg filenum_" << filenum_ << ", con_offset " << con_offset_;
    s = Consume(scratch);

    if (s.IsEndFile()) {
      char key[8];
      std::string value;
      EncodeBigFixed32(key, filenum_ + 1);
      EncodeBigFixed32(key + 4, static_cast<uint32_t>(0));
      shannon::Status status = db_->Get(shannon::ReadOptions(), shannon::Slice(key, 8), &value);
      if (status.ok()) {
        DLOG(INFO) << "BinlogSender roll to new binlog" << (filenum_);
        filenum_ ++;
        con_offset_ = 0;
        last_record_offset_ = 0;
      } else {
        usleep(10000);
      }
    }
    if (s.ok()) {
      break;
    }
  }

  if (should_stop()) {
    return Status::Corruption("should exit");
  }
  return s;
}

// When we encount
void* PikaBinlogSenderThread::ThreadMain() {
  Status s, result;
  bool last_send_flag = true;
  std::string header, scratch, transfer;
  scratch.reserve(1024 * 1024);
  while (!should_stop()) {
    sleep(2);
    // 1. Connect to slave
    result = cli_->Connect(ip_, port_, "");
    LOG(INFO) << "BinlogSender Connect slave(" << ip_ << ":" << port_ << ") " << result.ToString();

    // 2. Auth
    if (result.ok()) {
      cli_->set_send_timeout(timeout_ms_);
      // Auth sid
      std::string auth_cmd;
      pink::RedisCmdArgsType argv;
      argv.push_back("auth");
      argv.push_back(std::to_string(sid_));
      pink::SerializeRedisCommand(argv, &auth_cmd);
      header.clear();
      slash::PutFixed16(&header, TransferOperate::kTypeAuth);
      slash::PutFixed32(&header, auth_cmd.size());
      transfer = header + auth_cmd;
      result = cli_->Send(&transfer);
      if (!result.ok()) {
        LOG(WARNING) << "BinlogSender send slave(" << ip_ << ":" << port_ << ") failed,  " << result.ToString();
        break;
      }
      while (true) {
        // 3. Should Parse new msg;
        if (last_send_flag) {
          s = Parse(scratch);
          //DLOG(INFO) << "BinlogSender Parse, return " << s.ToString();

          if (s.IsCorruption()) {     // should exit
            LOG(WARNING) << "BinlogSender Parse failed, will exit, error: " << s.ToString();
            //close(sockfd_);
            break;
          } else if (s.IsIOError()) {
            LOG(WARNING) << "BinlogSender Parse error, " << s.ToString();
            continue;
          }
        }

        /* 这个地方是用来判断该条binlog信息是否是属于另一个主机的，如果属于，
         * 那么就不发送该条信息 */
        BinlogItem binlog_item;
        PikaBinlogTransverter::BinlogDecode(BinlogType::TypeFirst,
                                            scratch,
                                            &binlog_item);
        // If this binlog from the peer-master, can not resend to the peer-master
        if (binlog_item.server_id() == g_pika_server->DoubleMasterSid()
          && ip_ == g_pika_server->master_ip()
          && port_ == (g_pika_server->master_port() + 1000)) {
          continue;
        }

        // 4. After successful parse, we send msg;
        header.clear();
        slash::PutFixed16(&header, TransferOperate::kTypeBinlog);
        slash::PutFixed32(&header, scratch.size());
        transfer = header + scratch;
        result = cli_->Send(&transfer);
        if (result.ok()) {
          last_send_flag = true;
        } else {
          last_send_flag = false;
          LOG(WARNING) << "BinlogSender send slave(" << ip_ << ":" << port_ << ") failed,  " << result.ToString();
          break;
        }
      }
    }

    // error
    cli_->Close();
    sleep(1);
  }
  return NULL;
}
