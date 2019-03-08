// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_binlog_transverter.h"

uint32_t BinlogItem2::exec_time() const {
  return exec_time_;
}

uint32_t BinlogItem2::server_id() const {
  return server_id_;
}

uint64_t BinlogItem2::logic_id() const {
  return logic_id_;
}

uint32_t BinlogItem2::filenum() const {
  return filenum_;
}

uint64_t BinlogItem2::offset() const {
  return offset_;
}

std::string BinlogItem2::content() const {
  return content_;
}

void BinlogItem2::set_exec_time(uint32_t exec_time) {
  exec_time_ = exec_time;
}

void BinlogItem2::set_server_id(uint32_t server_id) {
  server_id_ = server_id;
}

void BinlogItem2::set_logic_id(uint64_t logic_id) {
  logic_id_ = logic_id;
}

void BinlogItem2::set_filenum(uint32_t filenum) {
  filenum_ = filenum;
}

void BinlogItem2::set_offset(uint64_t offset) {
  offset_ = offset;
}

std::string BinlogItem2::ToString() const {
  std::string str;
  str.append("exec_time: "  + std::to_string(exec_time_));
  str.append(",server_id: " + std::to_string(server_id_));
  str.append(",logic_id: "  + std::to_string(logic_id_));
  str.append(",filenum: "   + std::to_string(filenum_));
  str.append(",offset: "    + std::to_string(offset_));
  str.append("\ncontent: ");
  for (size_t idx = 0; idx < content_.size(); ++idx) {
    if (content_[idx] == '\n') {
      str.append("\\n");
    } else if (content_[idx] == '\r') {
      str.append("\\r");
    } else {
      str.append(1, content_[idx]);
    }
  }
  str.append("\n");
  return str;
}

const std::string& BinlogItem::key() const {
  return key_;
}

const std::string& BinlogItem::value() const {
  return value_;
}

shannon::LogOpType BinlogItem::optype() const {
  return optype_;
}

uint64_t BinlogItem::timestamp() const {
  return timestamp_;
}

const std::string& BinlogItem::db() const {
  return db_;
}

const std::string& BinlogItem::cf() const {
  return cf_;
}

void BinlogItem::set_key(std::string key) {
  key_ = key;
}

void BinlogItem::set_value(std::string value) {
  value_ = value;
}

void BinlogItem::set_optype(shannon::LogOpType optype) {
  optype_ = optype;
}

void BinlogItem::set_timestamp(uint64_t timestamp) {
  timestamp_ = timestamp;
}

void BinlogItem::set_db(std::string db) {
  db_ = db;
}

void BinlogItem::set_cf(std::string cf) {
  cf_ = cf;
}

std::string BinlogItem::ToString() const {
  std::string str;
  str.append("key: " + key_);
  str.append(",value: " + value_);
  str.append(",optype: " + std::to_string(optype_));
  str.append(",timestamp: " + std::to_string(timestamp_));
  str.append(",db: " + db_);
  str.append(",cf: " + cf_);
  return str;
}

std::string PikaBinlogTransverter::BinlogEncode(shannon::Slice& key,
                                                shannon::Slice& value,
                                                shannon::LogOpType optype,
                                                uint64_t timestamp,
                                                shannon::Slice& db_name,
                                                shannon::Slice& cf_name) {
  std::string str;
  size_t key_size = key.size();
  size_t value_size = value.size();
  size_t db_name_size = db_name.size();
  size_t cf_name_size = cf_name.size();
  size_t offset = 0;

  str.resize(2 * sizeof(size_t) + key.size() + value.size() + sizeof(optype) +
             sizeof(timestamp) + 2 * sizeof(size_t) + db_name_size + cf_name_size);
  char *data = const_cast<char*>(str.data());
  memcpy(data + offset, &key_size, sizeof(key_size));
  offset += sizeof(size_t);
  memcpy(data + offset, &value_size, sizeof(value_size));
  offset += sizeof(size_t);
  memcpy(data + offset, key.data(), key.size());
  offset += key.size();
  memcpy(data + offset, value.data(), value.size());
  offset += value.size();
  memcpy(data + offset, &optype, sizeof(optype));
  offset += sizeof(optype);
  memcpy(data + offset, &timestamp, sizeof(timestamp));
  offset += sizeof(timestamp);
  memcpy(data + offset, &db_name_size, sizeof(db_name_size));
  offset += sizeof(size_t);
  memcpy(data + offset, &cf_name_size, sizeof(cf_name_size));
  offset += sizeof(size_t);
  memcpy(data + offset, db_name.data(), db_name.size());
  offset += db_name.size();
  memcpy(data + offset, cf_name.data(), cf_name.size());
  offset += cf_name.size();
  return str;
}

bool PikaBinlogTransverter::BinlogDecode(BinlogItem* binlog_item,
                                         std::string& str) {
  std::string key, value;
  shannon::LogOpType optype;
  uint64_t timestamp;
  std::string db, cf;
  bool flag = BinlogDecode(&key, &value, &optype, &timestamp, &db, &cf, str);
  if (flag) {
    binlog_item->set_key(key);
    binlog_item->set_value(value);
    binlog_item->set_optype(optype);
    binlog_item->set_timestamp(timestamp);
    binlog_item->set_db(db);
    binlog_item->set_cf(cf);
  }
  return flag;
}

bool PikaBinlogTransverter::BinlogDecode(std::string* key,
                                         std::string* value,
                                         shannon::LogOpType* optype,
                                         uint64_t* timestamp,
                                         std::string* db_name,
                                         std::string* cf_name,
                                         std::string& str) {
  size_t key_size, value_size, db_name_size, cf_name_size;
  size_t offset = 0;
  if (str.size() <= (sizeof(size_t) * 4 + sizeof(shannon::LogOpType) +
                     sizeof(uint64_t))) {
    return false;
  }
  memcpy(&key_size, str.data() + offset, sizeof(key_size));
  offset += sizeof(key_size);
  memcpy(&value_size, str.data() + offset, sizeof(value_size));
  offset += sizeof(value_size);
  if (key != NULL) {
    key->clear();
    key->assign(str.data() + offset, key_size);
  }
  offset += key_size;
  if (value != NULL) {
    value->clear();
    value->assign(str.data() + offset, value_size);
  }
  offset += value_size;
  memcpy(optype, str.data() + offset, sizeof(shannon::LogOpType));
  offset += sizeof(shannon::LogOpType);
  memcpy(timestamp, str.data() + offset, sizeof(uint64_t));
  offset += sizeof(uint64_t);
  memcpy(&db_name_size, str.data() + offset, sizeof(size_t));
  offset += sizeof(size_t);
  memcpy(&cf_name_size, str.data() + offset, sizeof(size_t));
  offset += sizeof(size_t);
  if (db_name != NULL) {
    db_name->clear();
    db_name->assign(str.data() + offset, db_name_size);
  }
  offset += db_name_size;
  if (cf_name != NULL) {
    cf_name->clear();
    cf_name->assign(str.data() + offset, cf_name_size);
  }
  offset += cf_name_size;
  return true;
}
