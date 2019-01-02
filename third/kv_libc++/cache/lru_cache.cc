//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "cache/lru_cache.h"
#include "swift/cache.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <iostream>

//#include "util/mutexlock.h"

namespace shannon {

/*std::shared_ptr<Cache> NewLRUCache(size_t capacity, int num_shard_bits,
                                   bool strict_capacity_limit,
                                   double high_pri_pool_ratio) {

  return NULL;
}*/
std::shared_ptr<Cache> NewLRUCache(size_t capacity, int num_shard_bits,
                bool strict_capacity_limit,
                double high_pri_pool_ratio) {
    if (num_shard_bits >= 20) {
        return nullptr;
    }
    if (high_pri_pool_ratio < 0.0 || high_pri_pool_ratio > 1.0) {
        return nullptr;
    }
    if (num_shard_bits < 0) {
        num_shard_bits = 1024;//GetDefaultCacheShardBits(capacity);
    }
    return std::make_shared<LRUCache>(capacity, num_shard_bits,
                                    strict_capacity_limit, high_pri_pool_ratio);
}

LRUCache::LRUCache(size_t capacity, int num_shard_bits,
                   bool strict_capacity_limit, double high_pri_pool_ratio)
    : ShardedCache(capacity, num_shard_bits, strict_capacity_limit) {
    }

LRUCache::~LRUCache() {

}

CacheShard* LRUCache::GetShard(int shard) {
    return nullptr;
}
const CacheShard* LRUCache::GetShard(int shard) const {
    return nullptr;
}

void* LRUCache::Value(Handle* handle) {
    return nullptr;
}

size_t LRUCache::GetCharge(Handle* handle) const {
    return 0;
}

uint32_t LRUCache::GetHash(Handle* handle) const {
    return 0;
}

void LRUCache::DisownData() {

}

size_t LRUCache::TEST_GetLRUSize() {
    return 0;
}

}  // namespace shannon
