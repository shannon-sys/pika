// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_TRYSYNC_THREAD_H_
#define PIKA_TRYSYNC_THREAD_H_

#include "pink/include/pink_thread.h"
#include "pink/include/redis_cli.h"
#include <mutex>
#include <thread>

class PikaTrysyncThread : public pink::Thread {
 public:
  PikaTrysyncThread() {
    cli_ = pink::NewRedisCli();
    cli_->set_connect_timeout(1500);
    set_thread_name("TrysyncThread");
	};
  virtual ~PikaTrysyncThread();

 private:
  std::mutex mutex_sst_file;
  std::unordered_map<std::string, std::thread> map_sst_file;

  int completion_count_ = 0;
  int sockfd_;
  int64_t sid_;
  pink::PinkCli *cli_;

  void ReadSstToKV(const std::string sst_name);
  bool ReceiveFiles();
  bool Send(std::string lip);
  bool RecvProc();
  void PrepareRsync();
  bool TryUpdateMasterOffset();

  virtual void* ThreadMain();

};

#endif
