// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_BINLOG_SENDER_THREAD_H_
#define PIKA_BINLOG_SENDER_THREAD_H_

#include "pink/include/pink_thread.h"
#include "pink/include/pink_cli.h"
#include "slash/include/slash_slice.h"
#include "slash/include/slash_status.h"
#include "slash/include/env.h"
#include "slash/include/slash_mutex.h"
#include "swift/shannon_db.h"
#include "swift/log_iter.h"

using slash::Status;
using slash::Slice;

class PikaBinlogSenderThread : public pink::Thread {
 public:

  PikaBinlogSenderThread(const std::string &ip, int port,
                         int64_t sid,
                         uint64_t con_offset,
                         shannon::LogIterator* log_iter);

  virtual ~PikaBinlogSenderThread();

  uint64_t con_offset() {
    return con_offset_;
  }

  int trim();

  int speed_size() {
    return speed_;
  }

  int speed_count() {
    return count_;
  }

  uint64_t offset() {
    return con_offset_;
  }

 private:
  uint64_t get_next(bool &is_error);
  Status Parse(std::string &scratch);
  Status Consume(std::string &scratch);
  unsigned int ReadPhysicalRecord(slash::Slice *fragment);
  void CalcInfo();

  int statistics_speed_, statistics_count_;
  int speed_;
  int count_;
  long last_time_, cur_time_, statistics_frequency_;
  int cmd_size_;
  uint64_t con_offset_;
  Slice buffer_;
  std::string ip_;
  int port_;
  int64_t sid_;
  int timeout_ms_;
  shannon::LogIterator* log_iter_;
  pink::PinkCli *cli_;
  virtual void* ThreadMain();
};

#endif
