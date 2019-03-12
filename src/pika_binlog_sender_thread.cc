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

PikaBinlogSenderThread::PikaBinlogSenderThread(const std::string &ip, int port,
                                               int64_t sid,
                                               uint64_t con_offset,
                                               shannon::LogIterator* log_iter)
    :
      statistics_speed_(0),
      statistics_count_(0),
      speed_(0),
      count_(0),
      last_time_(0),
      cur_time_(0),
      statistics_frequency_(400000),
      cmd_size_(0),
      con_offset_(con_offset),
      buffer_(),
      ip_(ip),
      port_(port),
      sid_(sid),
      timeout_ms_(35000),
      log_iter_(log_iter) {
  cli_ = pink::NewRedisCli();
  port_ = port;
  set_thread_name("BinlogSender");
}

PikaBinlogSenderThread::~PikaBinlogSenderThread() {
  StopThread();
  delete log_iter_;
  delete cli_;
  // delete queue_;
  LOG(INFO) << "a BinlogSender thread " << thread_id() << " exit!";
}

int PikaBinlogSenderThread::trim() {
  return 0;
}

uint64_t PikaBinlogSenderThread::get_next(bool &is_error) {
  return 0;
}

unsigned int PikaBinlogSenderThread::ReadPhysicalRecord(slash::Slice *result) {
  return 0;
}

long get_time() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000000 + tv.tv_usec;
}

Status PikaBinlogSenderThread::Consume(std::string &scratch) {
  log_iter_->Next();
  if (log_iter_->Valid()) {
    shannon::Slice key = log_iter_->key();
    if (!log_iter_->status().ok()) {
      return Status::IOError("");
    }
    shannon::Slice value = log_iter_->value();
    if (!log_iter_->status().ok()) {
      return Status::IOError("");
    }
    shannon::LogOpType op_type = log_iter_->optype();
    uint64_t timestamp = log_iter_->timestamp();

    int32_t db_index = log_iter_->db();
    int32_t cf_index = log_iter_->cf();
    shannon::DB* db = g_pika_server->logger_->db_->GetDBByIndex(db_index);
    if (db == NULL) {
      stringstream ss;
      ss<<"db:"<<db_index<<" not exists";
      return Status::IOError(ss.str());
    }
    shannon::Slice db_name(db->GetName());
    const shannon::ColumnFamilyHandle* cf_handle = db->GetColumnFamilyHandle(cf_index);
    if (cf_handle == NULL) {
      stringstream ss;
      ss<<"cf:"<<cf_index<<" not exists";
      return Status::IOError(ss.str());
    }
    shannon::Slice cf_name(cf_handle->GetName());
    scratch = PikaBinlogTransverter::BinlogEncode(key, value, op_type,
                                                  timestamp,
                                                  db_name,
                                                  cf_name);
    con_offset_ ++;
    cmd_size_ = key.size() + value.size();
  } else if (log_iter_->status().IsCorruption()) {
    return Status::Corruption(log_iter_->status().ToString());
  } else {
    return Status::IOError("valid failed");
  }
  return Status::OK();
}

// Get a whole message;
// the status will be OK, IOError or Corruption;
Status PikaBinlogSenderThread::Parse(std::string &scratch) {
  scratch.clear();
  Status s;
  uint64_t pro_offset = 0;

  Binlog* logger = g_pika_server->logger_;
  while (!should_stop()) {
    logger->GetProducerStatus(&pro_offset);
    if (con_offset_ == pro_offset) {
      DLOG(INFO) << "BinlogSender Parse no new msg, con_offset " << con_offset_;
      usleep(10000);
      continue;
    }

    //DLOG(INFO) << "BinlogSender start Parse a msg filenum_" << filenum_ << ", con_offset " << con_offset_;
    s = Consume(scratch);
    if (s.ok()) {
      break;
    }
  }

  if (should_stop()) {
    return Status::Corruption("should exit");
  }
  return s;
}

void PikaBinlogSenderThread::CalcInfo() {
  cur_time_ = get_time();
  if (cur_time_ >= last_time_ + statistics_frequency_) {
    speed_ = statistics_speed_ * 1000000.0 / statistics_frequency_;
    count_ = statistics_count_ * 1000000.0 / statistics_frequency_;
    last_time_ = cur_time_;
    statistics_speed_ = 0;
    statistics_count_ = 0;
  }
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
        /*BinlogItem binlog_item;
        PikaBinlogTransverter::BinlogDecode(BinlogType::TypeFirst,
                                            scratch,
                                            &binlog_item);
        If this binlog from the peer-master, can not resend to the peer-master
        if (binlog_item.server_id() == g_pika_server->DoubleMasterSid()
          && ip_ == g_pika_server->master_ip()
          && port_ == (g_pika_server->master_port() + 1000)) {
          continue;
        }*/

        // 4. After successful parse, we send msg;
        header.clear();
        slash::PutFixed16(&header, TransferOperate::kTypeBinlog);
        slash::PutFixed32(&header, scratch.size());
        transfer = header + scratch;
        result = cli_->Send(&transfer);
        if (result.ok()) {
          last_send_flag = true;
          statistics_speed_ += transfer.size();
          statistics_count_ += 1;
          CalcInfo();
          if (g_pika_server->GetSlowestSlaveSid() == sid_) {
            g_rate_limiter->acquire(cmd_size_);
          }
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
