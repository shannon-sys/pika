// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_BINLOG_TRANSVERTER_H_
#define PIKA_BINLOG_TRANSVERTER_H_

#include <iostream>
#include <vector>
#include <glog/logging.h>
#include "slash/include/slash_coding.h"
#include "swift/shannon_db.h"
#include "swift/log_iter.h"
/*
 * ***********************************************Type First Binlog Item Format***********************************************
 * | <Type> | <Create Time> | <Server Id> | <Binlog Logic Id> | <File Num> | <Offset> | <Content Length> |      <Content>     |
 *  2 Bytes      4 Bytes        4 Bytes          8 Bytes         4 Bytes     8 Bytes         4 Bytes      content length Bytes
 *
 */
enum BinlogType {
  TypeFirst = 1,
};

class BinlogItem {
  public:
    BinlogItem() :
        timestamp_(0) {
      key_ = "";
      value_ = "";
    }

    friend class PikaBinlogTransverter;

  const std::string& key()              const;
  const std::string& value()            const;
  shannon::LogOpType optype()           const;
  uint64_t timestamp()                  const;
  int32_t db()                          const;
  int32_t cf()                          const;
  std::string ToString()                const;

  void set_key(std::string key);
  void set_value(std::string value);
  void set_optype(shannon::LogOpType optype);
  void set_timestamp(uint64_t timestamp);
  void set_db(int32_t db);
  void set_cf(int32_t cf);

  private:
    std::string key_ = "";
    std::string value_ = "";
    shannon::LogOpType optype_;
    uint64_t timestamp_;
    int32_t db_ = 0;
    int32_t cf_ = 0;
};

class BinlogItem2 {
  public:
    BinlogItem2() :
        exec_time_(0),
        server_id_(0),
        logic_id_(0),
        filenum_(0),
        offset_(0),
        content_("") {}

    friend class PikaBinlogTransverter;

    uint32_t exec_time()   const;
    uint32_t server_id()   const;
    uint64_t logic_id()    const;
    uint32_t filenum()     const;
    uint64_t offset()      const;
    std::string content()  const;
    std::string ToString() const;

    void set_exec_time(uint32_t exec_time);
    void set_server_id(uint32_t server_id);
    void set_logic_id(uint64_t logic_id);
    void set_filenum(uint32_t filenum);
    void set_offset(uint64_t offset);

  private:
    uint32_t exec_time_;
    uint32_t server_id_;
    uint64_t logic_id_;
    uint32_t filenum_;
    uint64_t offset_;
    std::string content_;
    std::vector<std::string> extends_;
};

class PikaBinlogTransverter {
  public:
    PikaBinlogTransverter() {};

    static std::string BinlogEncode(shannon::Slice& key,
                                    shannon::Slice& value,
                                    shannon::LogOpType optype,
                                    uint64_t timestamp,
                                    int32_t db_index,
                                    int32_t cf_index);

    static bool BinlogDecode(BinlogItem* binlog_item,
                             std::string& str);

    static bool BinlogDecode(std::string* key,
                             std::string* value,
                             shannon::LogOpType* optype,
                             uint64_t* timestamp,
                             int32_t* db_index,
                             int32_t* cf_index,
                             std::string& str);

    static std::string BinlogEncode(BinlogType type,
                                    uint32_t exec_time,
                                    uint32_t server_id,
                                    uint64_t logic_id,
                                    uint32_t filenum,
                                    uint64_t offset,
                                    const std::string& content,
                                    const std::vector<std::string>& extends) {
        return "";
    }

    static bool BinlogDecode(BinlogType type,
                             const std::string& binlog,
                             BinlogItem* binlog_item) {
        return false;
    }

};

#endif
