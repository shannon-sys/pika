// Copyright (c) 2018 Shannon Authors. All rights reserved.
// Use of this source code is governed by a BSD style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
#ifndef INCLUDE_RATE_LIMITER_H_
#define INCLUDE_RATE_LIMITER_H_

#include <iostream>
#include <atomic>
#include <mutex>
#include <condition_variable>

class RateLimiter {
 public:
  RateLimiter() {
    tokens_ = 0;
  }
  void acquire();
  void acquire(int permits);
  void add(int permits);
  void set(int threshold);
  void init();
  void init(int bucket_size);
  int bucket_size();
  int tokens();
  void Lock();
  void UnLock();
 private:
  int bucket_size_ = 1024*1024*1024;  // 1G
  int tokens_;
  std::condition_variable cv_;
  std::condition_variable cv_full_;
  std::mutex mtx_;
  std::mutex mtx_full_;
  std::mutex mtx_lock_;
  std::mutex mtx_add_;
};

#endif
