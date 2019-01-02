//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_LISTS_META_VALUE_FORMAT_H_
#define SRC_LISTS_META_VALUE_FORMAT_H_

#include <string>

#include "src/base_value_format.h"

namespace blackwidow {


class ListsMetaValue : public InternalValue {
 public:
  explicit ListsMetaValue(const Slice& user_value) :
    InternalValue(user_value), index_(0), count_(0) {
  }

  int64_t get_fact_index(int64_t index) {
    if (index < 0 || index >= count_)
        return 0;
    char *dst = const_cast<char*>(user_value_.data() +
        kDefaultValuePrefixLength);
    return DecodeFixed64(dst + (index) * 8);
  }

  virtual size_t AppendTimestampAndVersion() override {
    char* dst = const_cast<char*>(user_value_.data());
    dst += sizeof(int32_t) * 2;
    EncodeFixed32(dst, version_);
    dst += sizeof(int32_t);
    EncodeFixed32(dst, timestamp_);
    return 2 * sizeof(int32_t);
  }

  virtual size_t AppendCount() {
    char *dst = const_cast<char*>(user_value_.data() + sizeof(int32_t));
    EncodeFixed32(dst, count_);
    return sizeof(int32_t);
  }

  virtual size_t AppendCurrentIndex() {
    char* dst = const_cast<char*>(user_value_.data());
    EncodeFixed64(dst + 4 * sizeof(int32_t), index_);
    return 3 * sizeof(int32_t);
  }

  size_t AppendLogIndex() {
      char* dst = const_cast<char*>(user_value_.data());
      EncodeFixed64(dst, log_index_);
      return sizeof(int32_t);
  }

  static const size_t kDefaultValueSuffixLength = sizeof(int32_t) * 2 +
      sizeof(int64_t) * 2;
  static const size_t kDefaultValuePrefixLength = sizeof(int32_t) * 4 +
      sizeof(int64_t);

  static const int32_t kListsMetaValueMinCacheSize = 8096;

  virtual const Slice Encode() override {
    AppendLogIndex();
    AppendCount();
    AppendTimestampAndVersion();
    AppendCurrentIndex();
    return Slice(user_value_.data(), user_value_.size());
  }

  int32_t UpdateVersion() {
    int64_t unix_time;
    shannon::Env::Default()->GetCurrentTime(&unix_time);
    if (version_ >= static_cast<int32_t>(unix_time)) {
      version_++;
    } else {
      version_ = static_cast<int32_t>(unix_time);
    }
    return version_;
  }

  void ModifyCacheMoveLeft(int index) {
      char *dst = const_cast<char*>(user_value_.data() + kDefaultValuePrefixLength);
      memcpy(dst, dst + index * sizeof(int64_t), (count_ - index) * sizeof(int64_t));
  }

  int32_t calc_cache_size(int32_t count) {
      int32_t cache_size = (count * 8) << 1;
      int32_t bit = 1 << 31;

      while (bit != static_cast<int32_t>(0xffffffff) && ((cache_size & bit) == 0)) {
          bit >>= 1;
      }
      cache_size = cache_size & bit;
      if (cache_size < kListsMetaValueMinCacheSize) {
          return kListsMetaValueMinCacheSize;
      }
      return cache_size;
  }

  void ModifyCacheMoveRight(int index) {
      if ((index + count_) * sizeof(uint64_t) +
            kDefaultValuePrefixLength > user_value_.size()) {
      }
      int64_t *dst = reinterpret_cast<int64_t*>(reinterpret_cast<void*>(
                  const_cast<char*>(user_value_.data() + kDefaultValuePrefixLength)));
      int64_t *dst_p = reinterpret_cast<int64_t*>(dst) + count_ - 1;
      int64_t *res_p = dst_p + index;
      while (dst_p != dst) {
          *dst_p = *res_p;
          res_p = res_p - 1;
          dst_p = dst_p - 1;
      }
  }

  void ModifyCacheMoveRightForBlock(uint32_t i, uint32_t j, uint32_t step) {
      if ((j + step) * sizeof(uint64_t) +
              kDefaultValuePrefixLength > user_value_.size()) {
          // user_value_.resize((user_value.size() - kDefaultValuePrefixLength) * 2 + kDefaultValuePrefixLength);
      }
      int64_t *dst = reinterpret_cast<int64_t*>(reinterpret_cast<void*>(
                  const_cast<char*>(user_value_.data() + kDefaultValuePrefixLength)));
      uint32_t index = 0;
      uint32_t count = j - i;
      int64_t *dst_p = reinterpret_cast<int64_t*>(dst) + j - 1 + step;
      int64_t *res_p = reinterpret_cast<int64_t*>(dst) + j - 1;
      while (index < count) {
          *dst_p = *res_p;
          index ++;
          dst_p --;
          res_p --;
      }
  }

  void ModifyCacheMoveLeftForBlock(uint32_t i, uint32_t j, uint32_t step) {
      if (step > i) {
          step = i;
      }
      int64_t *rst = reinterpret_cast<int64_t*>(reinterpret_cast<void*>(const_cast<char*>(user_value_.data() +
                  kDefaultValuePrefixLength)));
      rst += i;
      int64_t *dst = rst - step;
      // memcpy(dst, rst, (j-i));
      for (; i < j; i ++) {
          *dst = *rst;
          dst --;
          rst --;
      }
  }

  void ModifyCacheIndex(uint32_t index, uint64_t value) {
      if (index * sizeof(uint64_t) +
              kDefaultValuePrefixLength >= user_value_.size()) {
          // user_value_.resize((user_value.size() - kDefaultValuePrefixLength) * 2 + kDefaultValuePrefixLength);
      }
      uint64_t *dst = reinterpret_cast<uint64_t*>(reinterpret_cast<void*>(
                  const_cast<char*>(user_value_.data() +
             kDefaultValuePrefixLength)));
      *(dst + index) = value;
  }

  void ModifyCount(int32_t count) {
      char *dst = const_cast<char*>(user_value_.data());
      if (count < 0 && count_ + count > count_) {
        count_ = 0;
      } else {
        count_ += count;
      }
      EncodeFixed32(dst, count_);
      int32_t expected_cache_size = calc_cache_size(count);
      expected_cache_size = calc_cache_size(count);
      if (expected_cache_size + kDefaultValuePrefixLength < user_value_.size()) {
          // user_value_.resize(expected_cache_size);
      }
  }

  void ModifyIndex(uint64_t index) {
      uint64_t *dst = reinterpret_cast<uint64_t*>(reinterpret_cast<void*>(
                  const_cast<char*>(user_value_.data() +
                      sizeof(int32_t) * 4)));
      index_ += index;
      *dst = index_;
  }

  uint64_t index() {
      return index_;
  }

  uint32_t count() {
      return count_;
  }

  void ModifyLogToIndex(uint32_t index) {
      log_index_ = index;
      EncodeFixed32(const_cast<char *>(user_value_.data()), log_index_);
  }

  void ModifyLogIndex(uint32_t count) {
      log_index_ += count;
      EncodeFixed32(const_cast<char *>(user_value_.data()), log_index_);
  }

  void set_log_index(uint32_t index) {
      log_index_ = index;
  }

  uint32_t log_index() {
      return log_index_;
  }

 private:
  uint64_t index_;
  uint32_t count_;
  uint32_t log_index_;
};

class ParsedListsMetaValue : public ParsedInternalValue {
 public:
  // Use this constructor after shannon::DB::Get();
  explicit ParsedListsMetaValue(std::string* internal_value_str) :
    ParsedInternalValue(internal_value_str), count_(0), index_(0) {
    assert(internal_value_str->size() >= kListsMetaValuePrefixLength);
    if (internal_value_str->size() >= kListsMetaValuePrefixLength) {
      log_index_ = DecodeFixed32(internal_value_str->data());
      user_value_ = Slice(internal_value_str->data(), sizeof(int32_t));
      version_ = DecodeFixed32(internal_value_str->data() + sizeof(int32_t) * 2);
      timestamp_ = DecodeFixed32(internal_value_str->data() + sizeof(int32_t) * 3);
      index_ = DecodeFixed64(internal_value_str->data() + sizeof(int32_t) * 4);
    }
    count_ = DecodeFixed32(internal_value_str->data() + sizeof(int32_t));
  }

  // Use this constructor in shannon::CompactionFilter::Filter();
  explicit ParsedListsMetaValue(const Slice& internal_value_slice) :
    ParsedInternalValue(internal_value_slice), count_(0), index_(0) {
    assert(internal_value_slice.size() >= kDefaultValueVirutalPrefixLength + kListsMetaValuePrefixLength);
    if (internal_value_slice.size() >= kListsMetaValuePrefixLength) {
      log_index_ = DecodeFixed32(internal_value_slice.data());
      user_value_ = Slice(internal_value_slice.data(), sizeof(int32_t));
      version_ = DecodeFixed32(internal_value_slice.data() + sizeof(int32_t) * 2);
      timestamp_ = DecodeFixed32(internal_value_slice.data() + sizeof(int32_t) * 3);
      index_ = DecodeFixed64(internal_value_slice.data() + sizeof(int32_t) * 4);
    }
    count_ = DecodeFixed32(internal_value_slice.data() + sizeof(int32_t));
    // cache_ = Slice(internal_value_str.data(), calc_cache_size());
  }

  int64_t get_fact_index(int64_t index) {
    if (index < 0 || index >= count_)
        return 0;
    char *dst = const_cast<char*>(value_->data() + kDefaultValuePrefixLength);
    return DecodeFixed64(dst + (index) * 8);
  }

  uint32_t get_raw_index(uint64_t index) {
      uint64_t *dst = reinterpret_cast<uint64_t*>(reinterpret_cast<void*>(
               const_cast<char*>(value_->data() + kDefaultValuePrefixLength)));
      for (uint32_t i = 0; i < count_; i ++) {
          if (dst[i] == index)
              return i;
      }
      return 0xffffffff;
  }

  int32_t calc_cache_size(int32_t count) {
      int32_t cache_size = (count * 8) << 1;
      int32_t bit = 1 << 31;

      while (bit != static_cast<int32_t>(0xffffffff) && ((cache_size & bit) == 0)) {
          bit >>= 1;
      }
      cache_size = cache_size & bit;
      if (cache_size < 8096) {
          return 8096;
      }
      return cache_size;
  }

  virtual void StripSuffix() override {
    if (value_ != nullptr) {
      // value_->erase(value_->size() - kListsMetaValueSuffixLength,
      // kListsMetaValueSuffixLength);
    }
  }

  virtual void SetVersionToValue() override {
    if (value_ != nullptr) {
      char* dst = const_cast<char*>(value_->data()) + sizeof(int32_t) * 2;
      EncodeFixed32(dst, version_);
    }
  }

  virtual void SetTimestampToValue() override {
    if (value_ != nullptr) {
      char* dst = const_cast<char*>(value_->data()) + sizeof(int32_t) * 3;
      EncodeFixed32(dst, timestamp_);
    }
  }

  static const size_t kListsMetaValuePrefixLength =
                          4 * sizeof(int32_t) + 1 * sizeof(int64_t);
  static const size_t kDefaultValuePrefixLength = sizeof(int32_t) * 4 +
                        sizeof(int64_t);

  static const size_t kListsMetaValueMinCacheSize = 8096;

  int32_t InitialMetaValue() {
    this->init_cache();
    this->set_count(0);
    this->set_timestamp(0);
    this->set_current_index(0);
    this->set_log_index(0);
    return this->UpdateVersion();
  }

  uint32_t count() {
    return count_;
  }

  uint64_t index() {
    return index_;
  }

  void init_cache() {
      value_->resize(kDefaultValuePrefixLength + kListsMetaValueMinCacheSize);
      // memset(const_cast<char *>(value_->data()), 0, sizeof(int32_t));
  }

  void set_count(uint32_t count) {
    count_ = count;
    if (value_ != nullptr) {
      char* dst = const_cast<char*>(value_->data() + sizeof(int32_t));
      EncodeFixed32(dst, count_);
    }
  }

  void set_current_index(uint64_t current_index) {
    index_ = current_index;
    if (value_ != nullptr) {
      char* dst = const_cast<char*>(value_->data());
      EncodeFixed64(dst + sizeof(int32_t) * 4, current_index);
    }
  }

  void ModifyCount(int32_t delta) {
    char *dst = const_cast<char*>(value_->data());
    if (delta < 0 && count_ + delta > count_) {
        count_ = 0;
    } else {
        count_ += delta;
    }
    EncodeFixed32(dst + sizeof(int32_t), count_);
    int32_t expected_cache_size = calc_cache_size(count_);
    if (expected_cache_size + kDefaultValuePrefixLength >= value_->size()) {
        value_->resize(expected_cache_size + kDefaultValuePrefixLength);
    }
  }

  void ModifyIndex(uint64_t index) {
    char *dst = const_cast<char*>(value_->data() + sizeof(int32_t) * 4);
    index_ += index;
    EncodeFixed32(dst, index_);
  }

  void ModifyCacheMoveLeft(int index) {
    char *dst = const_cast<char*>(value_->data() + kDefaultValuePrefixLength);
    memcpy(dst, dst + index * sizeof(int64_t), (count_ - index) * sizeof(int64_t));
  }

  void ModifyCacheMoveRight(int index) {
    if ((index + count_) * sizeof(int64_t) + kDefaultValuePrefixLength >= value_->size()) {
        value_->resize((value_->size() - kDefaultValuePrefixLength) * 2 + kDefaultValuePrefixLength);
    }
    int64_t *dst = reinterpret_cast<int64_t*>(reinterpret_cast<void*>(
                const_cast<char*>(value_->data() + kDefaultValuePrefixLength)));
    int64_t *res_p = reinterpret_cast<int64_t*>(dst) + count_ - 1;
    int64_t *dst_p = res_p + index;
    uint32_t i = 0;
    while (i < count_) {
        *dst_p = *res_p;
        res_p = res_p - 1;
        dst_p = dst_p - 1;
        i ++;
    }
  }

  void ModifyCacheMoveRightForBlock(uint32_t i, uint32_t j, uint32_t step) {
      if ((j + step) * sizeof(uint64_t) + kDefaultValuePrefixLength >= value_->size()) {
          value_->resize((value_->size() - kDefaultValuePrefixLength) * 2
            + kDefaultValuePrefixLength);
      }
      int64_t *dst = reinterpret_cast<int64_t*>(reinterpret_cast<void*>(
               const_cast<char*>(value_->data() + kDefaultValuePrefixLength)));
      uint32_t index = 0;
      uint32_t count = j - i;
      int64_t *dst_p = reinterpret_cast<int64_t*>(dst) + j - 1 + step;
      int64_t *res_p = reinterpret_cast<int64_t*>(dst) + j - 1;
      while (index < count) {
          *dst_p = *res_p;
          dst_p --;
          res_p --;
          index ++;
      }
  }

  void ModifyCacheMoveLeftForBlock(uint32_t i, uint32_t j, uint32_t step) {
      if (step > i) {
          step = i;
      }
      int64_t *rst = reinterpret_cast<int64_t*>(reinterpret_cast<void*>(
                  const_cast<char*>(value_->data() + kDefaultValuePrefixLength)));
      rst += i;
      int64_t *dst = rst - step;
      for (; i < j; i ++) {
          *dst = *rst;
          dst ++;
          rst ++;
      }
  }

  void ModifyCacheIndex(uint32_t index, uint64_t value) {
      if (index * sizeof(uint64_t) + kDefaultValuePrefixLength >= value_->size()) {
          value_->resize((value_->size() - kDefaultValuePrefixLength) * 2 +
                  kDefaultValuePrefixLength);
      }
      uint64_t *dst = reinterpret_cast<uint64_t*>(reinterpret_cast<void*>(const_cast<char*>(value_->data() +
                      kDefaultValuePrefixLength)));
      *(dst + index) = value;
  }

  uint32_t SortOutCache() {
      uint32_t count = 0;
      uint64_t *dst = reinterpret_cast<uint64_t*>(reinterpret_cast<void*>(
                  const_cast<char*>(value_->data() + kDefaultValuePrefixLength)));
      uint32_t i = 0, j = 0;
      while (i < count_) {
          if (dst[i] == 0xffffffffffffffff) {
              i ++;
              count ++;
              continue;
          }
          dst[j] = dst[i];
          j ++;
          i ++;
      }
      return count;
  }

  int32_t UpdateVersion() {
    int64_t unix_time;
    shannon::Env::Default()->GetCurrentTime(&unix_time);
    if (version_ >= static_cast<int32_t>(unix_time)) {
      version_++;
    } else {
      version_ = static_cast<int32_t>(unix_time);
    }
    SetVersionToValue();
    return version_;
  }

  static const char LIST_META_VALUE_INSERT = 0x01;
  static const char LIST_META_VALUE_DEL = 0x02;
  static const char LIST_META_VALUE_LPUSH = 0x03;
  static const char LIST_META_VALUE_RPUSH = 0x04;
  static const char LIST_META_VALUE_RPOPLPUSH = 0x05;

  bool ExecuteCmd(const char *buf_cmd) {
      if (buf_cmd == NULL)
          return false;
      char type = buf_cmd[0];
      uint32_t index = DecodeFixed32(buf_cmd+1);
      uint32_t count;
      int64_t right_index;
      switch (type) {
        case LIST_META_VALUE_INSERT:
          index = DecodeFixed32(buf_cmd + 1);
          this->ModifyCacheMoveRightForBlock(index, this->count_, 1);
          this->ModifyCacheIndex(index, this->index_);
          this->ModifyIndex(1);
          this->ModifyCount(1);
        break;
        case LIST_META_VALUE_DEL:
          index = DecodeFixed32(buf_cmd + sizeof(char));
          count = DecodeFixed32(buf_cmd + sizeof(char) + sizeof(int32_t));
          this->ModifyCacheMoveLeftForBlock(index + count, this->count_, count);
          this->ModifyCount(-count);
        break;
        case LIST_META_VALUE_LPUSH:
          index = DecodeFixed32(buf_cmd + 1);
          this->ModifyCacheMoveRight(index);
          this->ModifyCount(index);
          for (uint32_t i = 1; i <= index; i ++) {
            this->ModifyCacheIndex(index - i, this->index_);
            this->ModifyIndex(1);
          }
        break;
        case LIST_META_VALUE_RPUSH:
          index = DecodeFixed32(buf_cmd + 1);
          for (uint32_t i = 0; i < index; i ++) {
            this->ModifyCacheIndex(this->count_, this->index_);
            this->ModifyIndex(1);
            this->ModifyCount(1);
          }
        break;
        case LIST_META_VALUE_RPOPLPUSH:
          right_index = this->get_fact_index(this->count_ - 1);
          this->ModifyCacheMoveRightForBlock(0, this->count_ - 1, 1);
          this->ModifyCacheIndex(0, right_index);
        break;
        default:
            return false;
        break;
      }
      return true;
  }

  void ModifyLogToIndex(uint32_t index) {
      log_index_ = index;
      EncodeFixed32(const_cast<char *>(value_->data()), log_index_);
  }

  void ModifyLogIndex(uint32_t count) {
      log_index_ += count;
      EncodeFixed32(const_cast<char *>(value_->data()), log_index_);
  }

  int32_t log_index() {
      return log_index_;
  }

  void set_log_index(int32_t log_index) {
      log_index_ = log_index;
      EncodeFixed32(const_cast<char *>(value_->data()), log_index_);
  }
 private:
  int32_t log_index_;
  uint32_t count_;
  uint64_t index_;
};

}  //  namespace blackwidow
#endif  //  SRC_LISTS_META_VALUE_FORMAT_H_

