// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef SHANNON_DB_INCLUDE_ITERATOR_H_
#define SHANNON_DB_INCLUDE_ITERATOR_H_

#include "swift/slice.h"
#include "swift/status.h"

namespace shannon {

class Iterator {
 public:
  Iterator() {};
  virtual ~Iterator() {};

  // An iterator is either positioned at a key/value pair, or
  // not valid.  This method returns true iff the iterator is valid.
  virtual bool Valid() const = 0;

  // Position at the first key in the source.  The iterator is Valid()
  // after this call iff the source is not empty.
  virtual void SeekToFirst() = 0;

  // Position at the last key in the source.  The iterator is
  // Valid() after this call iff the source is not empty.
  virtual void SeekToLast() = 0;

  // Position at the first key in the source that at or past target
  // The iterator is Valid() after this call iff the source contains
  // an entry that comes at or past target.
  virtual void Seek(const Slice& target) = 0;

  // Moves to the next entry in the source.  After this call, Valid() is
  // true iff the iterator was not positioned at the last entry in the source.
  // REQUIRES: Valid()
  virtual void Next() = 0;

  // Moves to the previous entry in the source.  After this call, Valid() is
  // true iff the iterator was not positioned at the first entry in source.
  // REQUIRES: Valid()
  virtual void Prev() = 0;

  // Return the key for the current entry.  The underlying storage for
  // the returned slice is valid only until the next modification of
  // the iterator.
  // REQUIRES: Valid()
  virtual Slice key() = 0;

  // Return the value for the current entry.  The underlying storage for
  // the returned slice is valid only until the next modification of
  // the iterator.
  // REQUIRES: !AtEnd() && !AtStart()
  virtual Slice value() = 0;

  // If an error has occurred, return it.  Else return an ok status.
  virtual Status status() const = 0;

  // Position at the last key in the source that at or before target.
  // The iterator is Valid() after this call iff the source contains
  // and entry that comes at or before target.
  virtual void SeekForPrev(const Slice& target) = 0;

  //
  virtual void SetPrefix(const Slice &prefix) = 0;
 private:
  // No copying allowed
  Iterator(const Iterator&);
  void operator=(const Iterator&);
};
}  // namespace shannon

#endif  // SHANNON_DB_INCLUDE_ITERATOR_H_
