// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <stdlib.h>
#include <errno.h>
#include <iostream>
#include "kv_impl.h"
#include "swift/iterator.h"
#include "venice_kv.h"
#include "venice_ioctl.h"

namespace shannon {

class KVIter: public Iterator {
 public:
  KVIter(KVImpl* db, int iter_index, int cf_index, uint64_t timestamp)
      : db_(db),
        index_(iter_index),
        cf_index_(cf_index),
        timestamp_(timestamp),
        valid_(false),
        prefix_length_(0) {
  }

  virtual ~KVIter();

  virtual bool Valid() const { return valid_; }
  virtual Slice key();
  virtual Slice value();
  virtual Status status() const {
      return status_;
  }

  virtual void Next();
  virtual void Prev();
  virtual void Seek(const Slice& target);
  virtual void SeekToFirst();
  virtual void SeekToLast();
  virtual void SeekForPrev(const Slice& target);
  virtual void SetPrefix(const Slice& prefix);
 private:
  KVImpl* db_;
  int index_;
  int cf_index_;
  uint64_t timestamp_;

  Status status_;
  std::string saved_key_;
  std::string saved_value_;
  int direction_;
  bool valid_;
  char prefix_[256];
  int prefix_length_;

  // No copying allowed
  KVIter(const KVIter&);
  void operator=(const KVIter&);
  bool IncreaseOne(char *prefix, int prefix_length);

};

KVIter::~KVIter() {
  struct cf_iterator iter;
  int ret = 0;

  iter.db_index = db_->db_;
  iter.cf_index = cf_index_;
  iter.timestamp = timestamp_;
  iter.iter_index = index_;
  ret = ioctl(db_->fd_, IOCTL_DESTROY_ITERATOR, &iter);
  if (ret < 0) {
    status_ = Status::IOError("ioctl destroy_iterator failed!!!\n");
  }
  status_ = Status();
}

void KVIter::Next() {
  struct kvdb_iter_move_option move;
  int ret;

  move.iter.db_index = db_->db_;
  move.iter.timestamp = timestamp_;
  move.iter.iter_index = index_;
  move.iter.cf_index = cf_index_;
  move.move_direction = MOVE_NEXT;
  ret = ioctl(db_->fd_, IOCTL_ITERATOR_MOVE, &move);
  valid_ = move.iter.valid_key == 0 ? false : true;
  if (ret < 0) {
    valid_ = false;
    status_ = Status::IOError("Next Failed", strerror(errno));
    return;
  }
  /* prefix iterator */
  if (this->prefix_length_ > 0) {
      Slice slice_key = this->key();
      if (slice_key.size() < this->prefix_length_
        || strncmp(this->prefix_, slice_key.data(), this->prefix_length_) != 0) {
          valid_ = false;
      }
  }
  status_ = Status();
}

void KVIter::Prev() {
  struct kvdb_iter_move_option move;
  int ret;

  status_ = Status();
  move.iter.db_index = db_->db_;
  move.iter.timestamp = timestamp_;
  move.iter.iter_index = index_;
  move.iter.cf_index = cf_index_;
  move.move_direction = MOVE_PREV;
  ret = ioctl(db_->fd_, IOCTL_ITERATOR_MOVE, &move);
  valid_ = move.iter.valid_key == 0 ? false : true;
  if (ret < 0) {
    valid_ = false;
    status_ = Status::IOError("Prev Failed", strerror(errno));
    return;
  }
  /* prefix iterator */
  if (this->prefix_length_ > 0) {
      Slice slice_key = this->key();
      if (slice_key.size() < this->prefix_length_
        || strncmp(this->prefix_, slice_key.data(), this->prefix_length_) != 0) {
          valid_ = false;
      }
  }
  status_ = Status();
}

void KVIter::Seek(const Slice& target) {
  struct kvdb_iter_seek_option seek;
  int ret;

  status_ = Status();
  seek.iter.db_index = db_->db_;
  seek.iter.timestamp = timestamp_;
  seek.iter.iter_index = index_;
  seek.iter.cf_index = cf_index_;
  seek.seek_type = SEEK_KEY;
  seek.key_len = target.size();
  memcpy(seek.key, target.data(),
  target.size() < MAX_KEY_SIZE ? target.size() : MAX_KEY_SIZE);
  ret = ioctl(db_->fd_, IOCTL_ITERATOR_SEEK, &seek);
  valid_ = seek.iter.valid_key == 0 ? false : true;
  if (ret < 0) {
    status_ = Status::IOError("Seek Key Failed", strerror(errno));
    return;
  }
}

void KVIter::SeekToFirst() {
  struct kvdb_iter_seek_option seek;
  int ret;

  /* use prefix */
  if (this->prefix_length_ > 0) {
      this->Seek(prefix_);
      if (this->Valid()) {
          Slice slice_key = this->key();
          if (slice_key.size() >= prefix_length_ &&
              strncmp(prefix_, slice_key.data(), prefix_length_) == 0) {
              valid_ = true;
              return;
          }
      }
      valid_ = false;
      return;
  }
  status_ = Status();
  direction_ = MOVE_NEXT;
  seek.iter.db_index = db_->db_;
  seek.iter.timestamp = timestamp_;
  seek.iter.iter_index = index_;
  seek.iter.cf_index = cf_index_;
  seek.seek_type = SEEK_FIRST;
  ret = ioctl(db_->fd_, IOCTL_ITERATOR_SEEK, &seek);
  valid_ = seek.iter.valid_key == 0 ? false : true;
  if (ret < 0) {
    status_ = Status::IOError("Seek Failed", strerror(errno));
    return;
  }
}

bool KVIter::IncreaseOne(char *prefix, int prefix_length) {
    int i;
    bool is_overflow = false;
    if (prefix == NULL) {
        return false;
    }
    i = prefix_length - 1;
    while (i >= 0) {
        if (prefix[i] != (char)-1) {
            prefix[i] += 1;
            break;
        }
        i --;
    }
    if (i < 0) {
        return true;
    }
    while(++i < prefix_length) {
        prefix[i] = 0;
    }
    return false;
}

void KVIter::SeekToLast() {
  struct kvdb_iter_seek_option seek;
  int ret;
  bool is_overflow;
  char prefix[256];

  /* use prefix */
  if (this->prefix_length_ > 0) {
      strncpy(prefix, prefix_, prefix_length_);
      is_overflow = this->IncreaseOne(prefix, this->prefix_length_);
      prefix[this->prefix_length_] = 0;
      if (is_overflow) {
          int temp_length = this->prefix_length_;
          this->prefix_length_ = 0;
          this->SeekToLast();
          this->prefix_length_ = temp_length;
      } else {
          this->SeekForPrev(Slice(prefix));
      }
      if (this->Valid()) {
          Slice slice_key = this->key();
          if (slice_key.size() >= this->prefix_length_ &&
              strncmp(this->prefix_, slice_key.data(), this->prefix_length_) == 0) {
              valid_ = true;
              return;
          }
      }
      valid_ = false;
      return;
  }
  status_ = Status();
  direction_ = MOVE_NEXT;
  seek.iter.db_index = db_->db_;
  seek.iter.timestamp = timestamp_;
  seek.iter.iter_index = index_;
  seek.iter.cf_index = cf_index_;
  seek.seek_type = SEEK_LAST;
  ret = ioctl(db_->fd_, IOCTL_ITERATOR_SEEK, &seek);
  valid_ = seek.iter.valid_key == 0 ? false : true;
  if (ret < 0) {
    status_ = Status::IOError("Seek Failed", strerror(errno));
    return;
  }
}

void KVIter::SeekForPrev(const Slice& target) {
    struct kvdb_iter_seek_option seek;
    int ret;

    status_ = Status();
    seek.iter.db_index = db_->db_;
    seek.iter.timestamp = timestamp_;
    seek.iter.iter_index = index_;
    seek.iter.cf_index = cf_index_;
    seek.seek_type = SEEK_FOR_PREV;
    if (target.size() >= 256) {
        status_ = Status::InvalidArgument("key length is too longer.");
        return;
    }
    strncpy(seek.key, target.data(), target.size());
    seek.key_len = target.size();
    ret = ioctl(db_->fd_, IOCTL_ITERATOR_SEEK, &seek);
    valid_ = seek.iter.valid_key == 0 ? false : true;
    if (ret < 0) {
        status_ = Status::IOError("Seek For Prev Failed.", strerror(errno));
    }
}

void KVIter::SetPrefix(const Slice& prefix) {
    if (prefix.size() >= 256) {
        return;
    }
    strncpy(this->prefix_, prefix.data(), prefix.size());
    this->prefix_length_ = prefix.size();
}

Slice KVIter::key() {
  struct kvdb_iter_get_option get;
  int ret;

  memset(&get, 0, sizeof(struct kvdb_iter_get_option));
  status_ = Status();
  saved_key_.clear();
  get.iter.db_index = db_->db_;
  get.iter.timestamp = timestamp_;
  get.iter.iter_index = index_;
  get.iter.cf_index = cf_index_;
  get.get_type = ITER_GET_KEY;
  get.key = (char *) malloc(MAX_KEY_SIZE);
  get.key_buf_len = MAX_KEY_SIZE;
  ret = ioctl(db_->fd_, IOCTL_ITERATOR_GET, &get);
  if (ret < 0) {
    status_ = Status::IOError("Iter Get key Failed", strerror(errno));
  }
  else {
    saved_key_.assign(get.key, get.key_len);
  }
  free(get.key);
  return saved_key_;
}

Slice KVIter::value() {
  struct kvdb_iter_get_option get;
  int ret;

  status_ = Status();
  saved_value_.clear();
  get.iter.db_index = db_->db_;
  get.iter.timestamp = timestamp_;
  get.iter.iter_index = index_;
  get.iter.cf_index = cf_index_;
  get.get_type = ITER_GET_VALUE;
  get.value = (char *) malloc(MAX_VALUE_SIZE);
  get.value_buf_len = MAX_VALUE_SIZE;
  ret = ioctl(db_->fd_, IOCTL_ITERATOR_GET, &get);
  if (ret < 0) {
    status_ = Status::IOError("Iter Get Value Failed", strerror(errno));
  }
  else {
    saved_value_.assign(get.value, get.value_len);
  }
  free(get.value);
  return saved_value_;
}

Iterator* NewDBIterator(
    KVImpl* db, int iter_index, int cf_index, uint64_t timestamp) {
  return new KVIter(db, iter_index, cf_index, timestamp);
}

}  // namespace shannon
