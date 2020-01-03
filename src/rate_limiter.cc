// Copyright (c) 2018 Shannon Authors. All rights reserved.
// Use of this source code is governed by a BSD style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//

#include "include/rate_limiter.h"
#include <unistd.h>

void RateLimiter::acquire() {
  acquire(1);
}

void RateLimiter::acquire(int permits) {
  mtx_.lock();
  if (tokens_ <= permits) {
    tokens_ = 0;
  } else {
    tokens_ -= permits;
  }
  mtx_.unlock();
  cv_full_.notify_one();
}

void RateLimiter::set(int threshold) {
  tokens_ = threshold;
}

void RateLimiter::add(int permits) {
  mtx_add_.lock();
  while (tokens_ + permits > bucket_size_) {
    std::unique_lock<std::mutex> lock(mtx_full_);
    cv_full_.wait(lock);
  }
  mtx_add_.unlock();
  mtx_.lock();
  tokens_ += permits;
  mtx_.unlock();
  // cv_.notify_all();
}

void RateLimiter::init() {
  tokens_ = 0;
  cv_full_.notify_one();
}

void RateLimiter::init(int bucket_size) {
  bucket_size_ = bucket_size;
  tokens_ = 0;
}

int RateLimiter::bucket_size() {
  return bucket_size_;
}

int RateLimiter::tokens() {
  return tokens_;
}

void RateLimiter::Lock() {
  mtx_lock_.lock();
}

void RateLimiter::UnLock() {
  mtx_lock_.unlock();
}
