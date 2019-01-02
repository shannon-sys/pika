//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.


#include <memory>
#include <map>
#include <unordered_map>
#include <sys/time.h>

#include "blackwidow/util.h"
#include "src/redis_lists.h"
#include "src/lists_filter.h"
#include "src/lists_log.h"
#include "src/scope_record_lock.h"
#include "src/scope_snapshot.h"
using namespace std;
std::unordered_map<std::string, std::string*> meta_infos_list_;
namespace blackwidow {

const shannon::Comparator* ListsDataKeyComparator() {
  static ListsDataKeyComparatorImpl ldkc;
  return &ldkc;
}


RedisLists::~RedisLists() {
  std::vector<shannon::ColumnFamilyHandle*> tmp_handles = handles_;
  handles_.clear();
  for (auto handle : tmp_handles) {
    delete handle;
  }
  for (std::unordered_map<std::string, std::string*>::iterator iter = meta_infos_list_.begin();
       iter != meta_infos_list_.end();
       ++ iter) {
      delete iter->second;
  }
  meta_infos_list_.clear();
}

Status RedisLists::Open(const BlackwidowOptions& bw_options,
                        const std::string& db_path) {
  shannon::Options ops(bw_options.options);
  Status s = shannon::DB::Open(ops, db_path, default_device_name_, &db_);
  if (s.ok()) {
    // Create column family
    shannon::ColumnFamilyHandle* cf;
    shannon::ColumnFamilyHandle* cf_timeout;
    shannon::ColumnFamilyHandle* cf_log;
    shannon::ColumnFamilyHandle* cf_index;
    shannon::ColumnFamilyOptions cfo;
    cfo.comparator = ListsDataKeyComparator();
    s = db_->CreateColumnFamily(cfo, "data_cf", &cf);
    if (!s.ok()) {
      return s;
    }
    s = db_->CreateColumnFamily(cfo, "log_cf", &cf_log);
    if (!s.ok()) {
        delete cf;
        return s;
    }
    s = db_->CreateColumnFamily(cfo, "index_cf", &cf_index);
    if (!s.ok()) {
        delete cf;
        delete cf_log;
        return s;
    }
    s = db_->CreateColumnFamily(shannon::ColumnFamilyOptions(), "timeout_cf", &cf_timeout);
    if (!s.ok()) {
      return s;
    }
    // Close DB
    delete cf_index;
    delete cf_log;
    delete cf;
    delete cf_timeout;
    delete db_;
  }    // Close DB

  // Open
  shannon::DBOptions db_ops(bw_options.options);
  shannon::ColumnFamilyOptions meta_cf_ops(bw_options.options);
  shannon::ColumnFamilyOptions data_cf_ops(bw_options.options);
  shannon::ColumnFamilyOptions log_cf_ops(bw_options.options);
  shannon::ColumnFamilyOptions index_cf_ops(bw_options.options);
  shannon::ColumnFamilyOptions timeout_cf_ops(bw_options.options);

  meta_cf_ops.compaction_filter_factory = std::make_shared<ListsMetaFilterFactory>();
  data_cf_ops.compaction_filter_factory = std::make_shared<ListsDataFilterFactory>(&db_, &handles_);
  data_cf_ops.comparator = ListsDataKeyComparator();

  //use the bloom filter policy to reduce disk reads
  shannon::BlockBasedTableOptions table_ops(bw_options.table_options);
  table_ops.filter_policy.reset(shannon::NewBloomFilterPolicy(10, true));
  shannon::BlockBasedTableOptions meta_cf_table_ops(table_ops);
  shannon::BlockBasedTableOptions data_cf_table_ops(table_ops);
  shannon::BlockBasedTableOptions log_cf_table_ops(table_ops);
  shannon::BlockBasedTableOptions index_cf_table_ops(table_ops);
  shannon::BlockBasedTableOptions timeout_cf_table_ops(table_ops);

  if (!bw_options.share_block_cache && bw_options.block_cache_size > 0) {
    meta_cf_table_ops.block_cache = shannon::NewLRUCache(bw_options.block_cache_size);
    data_cf_table_ops.block_cache = shannon::NewLRUCache(bw_options.block_cache_size);
    log_cf_table_ops.block_cache = shannon::NewLRUCache(bw_options.block_cache_size);
    index_cf_table_ops.block_cache = shannon::NewLRUCache(bw_options.block_cache_size);
    timeout_cf_table_ops.block_cache = shannon::NewLRUCache(bw_options.block_cache_size);
  }
  meta_cf_ops.table_factory.reset(shannon::NewBlockBasedTableFactory(meta_cf_table_ops));
  data_cf_ops.table_factory.reset(shannon::NewBlockBasedTableFactory(data_cf_table_ops));
  log_cf_ops.table_factory.reset(shannon::NewBlockBasedTableFactory(log_cf_table_ops));
  index_cf_ops.table_factory.reset(shannon::NewBlockBasedTableFactory(index_cf_table_ops));
  timeout_cf_ops.table_factory.reset(shannon::NewBlockBasedTableFactory(timeout_cf_table_ops));

  std::vector<shannon::ColumnFamilyDescriptor> column_families;
  // Meta CF
  column_families.push_back(shannon::ColumnFamilyDescriptor(
      shannon::kDefaultColumnFamilyName, meta_cf_ops));
  // Data CF
  column_families.push_back(shannon::ColumnFamilyDescriptor(
      "data_cf", data_cf_ops));
  // Log CF
  column_families.push_back(shannon::ColumnFamilyDescriptor(
      "log_cf", log_cf_ops));
  // Index CF
  column_families.push_back(shannon::ColumnFamilyDescriptor(
      "index_cf", index_cf_ops));
  // log TimeOut
  column_families.push_back(shannon::ColumnFamilyDescriptor(
      "timeout_cf", shannon::ColumnFamilyOptions()));

  lists_log_count_ = bw_options.lists_log_count;
  s = shannon::DB::Open(db_ops, db_path, default_device_name_, column_families, &handles_, &db_);
  if (s.ok()) {
      // Read meta info to cache
      std::vector<shannon::Iterator*> iters;
      Status status = db_->NewIterators(shannon::ReadOptions(), handles_, &iters);
      shannon::Iterator* iter = iters[0];
      shannon::Iterator* iter_log = iters[2];
      shannon::Iterator* iter_count = iters[1];
      uint32_t count = 0;
      for (iter_count->SeekToFirst();
           iter_count->Valid();
           iter_count->Next()) {
          count ++;
      }
      std::cout<<"count:"<<count<<std::endl;
      for (iter->SeekToFirst();
           iter->Valid();
           iter->Next()) {
          Slice slice_key = iter->key();
          Slice slice_value = iter->value();
          std::string *meta_value = new std::string();
          meta_value->resize(slice_value.size());
          memcpy(const_cast<char *>(meta_value->data()), slice_value.data(), slice_value.size());
          ParsedListsMetaValue parsed_lists_meta_value(meta_value);
          if (parsed_lists_meta_value.IsStale()) {
              delete meta_value;
              continue;
          }
          std::string v;
          uint32_t log_count = 0, log_index_max = 0;
          std::string seek_key;
          shannon::WriteBatch batch;
          int32_t version = parsed_lists_meta_value.version();
          seek_key.resize(slice_key.size() + sizeof(int32_t));
          memcpy(const_cast<char*>(seek_key.data()), slice_key.data(), slice_key.size());
          EncodeFixed32(const_cast<char*>(seek_key.data() + slice_key.size()), version);
          Slice temp_key = seek_key;
          status = db_->Get(shannon::ReadOptions(), handles_[3], slice_key, &v);
          if (status.ok()) {
              log_index_max = DecodeFixed32(v.data());
          }
          for (iter_log->Seek(seek_key);
               iter_log->Valid() && temp_key.starts_with(seek_key)
               && log_count < log_index_max;
               iter_log->Next()) {
              temp_key = iter_log->key();
              Slice value = iter_log->value();
              parsed_lists_meta_value.ExecuteCmd(value.data());
              log_count ++;
          }
          if (log_count > 0 && log_count == log_index_max) {
              batch.Put(handles_[0], slice_key, *meta_value);
              char buf[4];
              EncodeFixed32(buf, 0);
              batch.Put(handles_[3], slice_key, Slice(buf, sizeof(int32_t)));
              s = db_->Write(shannon::WriteOptions(), &batch);
              batch.Clear();
          }
          meta_infos_list_.insert(make_pair(string(slice_key.data()), meta_value));
      }
      for (auto iter : iters) {
          delete iter;
      }
  }
  return s;
}

Status RedisLists::CompactRange(const shannon::Slice* begin,
                                 const shannon::Slice* end) {
  Status s = db_->CompactRange(default_compact_range_options_,
      handles_[0], begin, end);
  if (!s.ok()) {
    return s;
  }
  return db_->CompactRange(default_compact_range_options_,
      handles_[1], begin, end);
}

Status RedisLists::GetProperty(const std::string& property, uint64_t* out) {
  std::string value;
  db_->GetProperty(handles_[0], property, &value);
  *out = std::strtoull(value.c_str(), NULL, 10);
  db_->GetProperty(handles_[1], property, &value);
  *out += std::strtoull(value.c_str(), NULL, 10);
  return Status::OK();
}

Status RedisLists::ScanKeyNum(uint64_t* num) {

  uint64_t count = 0;
  shannon::ReadOptions iterator_options;
  const shannon::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;

  shannon::Iterator* iter = db_->NewIterator(iterator_options, handles_[0]);
  for (iter->SeekToFirst();
       iter->Valid();
       iter->Next()) {
    ParsedListsMetaValue parsed_lists_meta_value(iter->value());
    if (!parsed_lists_meta_value.IsStale()
      && parsed_lists_meta_value.count() != 0) {
      count++;
    }
  }
  *num = count;
  delete iter;
  return Status::OK();
}

Status RedisLists::ScanKeys(const std::string& pattern,
                              std::vector<std::string>* keys) {
  std::string key;
  shannon::ReadOptions iterator_options;
  const shannon::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;

  shannon::Iterator* iter = db_->NewIterator(iterator_options, handles_[0]);

  for (iter->SeekToFirst();
       iter->Valid();
       iter->Next()) {
    ParsedListsMetaValue parsed_lists_meta_value(iter->value());
    if (!parsed_lists_meta_value.IsStale()
      && parsed_lists_meta_value.count() != 0) {
      key = iter->key().ToString();
      if (StringMatch(pattern.data(), pattern.size(), key.data(), key.size(), 0)) {
        keys->push_back(key);
      }
    }
  }
  delete iter;
  return Status::OK();
}

void show_meta_info() {
    uint64_t total_size = 0;
    uint32_t count = 0;
    for (std::unordered_map<std::string, std::string*>::iterator iter = meta_infos_list_.begin();
         iter != meta_infos_list_.end();
         ++ iter) {
        total_size += iter->first.size();
        total_size += iter->second->size();
        ++ count;
    }
    std::cout<<"count:"<<count<<"total size:"<<total_size<<"B "<<(total_size/1024)<<"k "<<(total_size/1024/1024)<<"M"<<std::endl;
}

Status RedisLists::LIndex(const Slice& key, int64_t index, std::string* element) {
  shannon::ReadOptions read_options;
  const shannon::Snapshot* snapshot;

  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  Status s;
  std::string meta_value;
  std::unordered_map<std::string, std::string*>::iterator meta_info =
      meta_infos_list_.find(key.data());
  // show_meta_info();
  if (meta_info != meta_infos_list_.end()) {
    ParsedListsMetaValue parsed_lists_meta_value(meta_info->second);
    int32_t version = parsed_lists_meta_value.version();
    if (parsed_lists_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_lists_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      std::string tmp_element;
      int64_t target_index = index >= 0 ? index :
            parsed_lists_meta_value.count() + index;
      if (target_index >= 0
        && target_index < parsed_lists_meta_value.count()) {
        target_index = parsed_lists_meta_value.get_fact_index(target_index);
        ListsDataKey lists_data_key(key, version, target_index);
        s = db_->Get(read_options, handles_[1], lists_data_key.Encode(), &tmp_element);
        if (s.ok()) {
          *element = tmp_element;
        }
      } else {
        return Status::NotFound();
      }
    }
  }
  return s;
}

Status RedisLists::LInsert(const Slice& key,
                           const BeforeOrAfter& before_or_after,
                           const std::string& pivot,
                           const std::string& value,
                           int64_t* ret) {
  *ret = 0;
  shannon::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);
  std::string *meta_value;
  Status s;
  std::unordered_map<std::string, std::string*>::iterator meta_info =
      meta_infos_list_.find(key.data());
  if (meta_info != meta_infos_list_.end()) {
    meta_value = new std::string();
    meta_value->resize(meta_info->second->size());
    memcpy(const_cast<char *>(meta_value->data()), meta_info->second->data(), meta_info->second->size());
    ParsedListsMetaValue parsed_lists_meta_value(meta_value);
    if (parsed_lists_meta_value.IsStale()) {
      *ret = 0;
      delete meta_value;
      return Status::NotFound("Stale");
    } else if (parsed_lists_meta_value.count() == 0) {
      *ret = 0;
      return Status::NotFound();
    } else {
      bool find_pivot = false;
      uint32_t pivot_index = 0;
      uint64_t index = 0;
      uint32_t version = parsed_lists_meta_value.version();
      shannon::Iterator* iter = db_->NewIterator(default_read_options_, handles_[1]);
      ListsDataKey lists_data_key(key, version, 0);
      for (iter->Seek(lists_data_key.Encode());
           iter->Valid() && index < parsed_lists_meta_value.count();
           index ++) {
          if (strcmp(iter->value().ToString().data(), pivot.data()) == 0) {
              find_pivot = true;
              ParsedListsDataKey parsed_lists_data_key(iter->key());
              pivot_index = parsed_lists_data_key.index();
              pivot_index = parsed_lists_meta_value.get_raw_index(pivot_index);
              break;
          }
          iter->Next();
      }
      delete iter;
      if (!find_pivot || pivot_index == 0xffffffff) {
        *ret = -1;
        delete meta_value;
        return Status::NotFound();
      } else {
        if (before_or_after == After) {
            pivot_index += 1;
        }
        uint32_t count = parsed_lists_meta_value.count();
        parsed_lists_meta_value.ModifyCacheMoveRightForBlock(pivot_index, count, 1);
        uint64_t target_index = parsed_lists_meta_value.index();
        ListsDataKey lists_target_key(key, version, target_index);
        batch.Put(handles_[1], lists_target_key.Encode(), value);
        parsed_lists_meta_value.ModifyCacheIndex(pivot_index, target_index);
        parsed_lists_meta_value.ModifyIndex(1);
        parsed_lists_meta_value.ModifyCount(1);

        int32_t log_index = parsed_lists_meta_value.log_index();
        if (log_index >= lists_log_count_) {
            batch.Put(handles_[0], key, *meta_value);
            parsed_lists_meta_value.ModifyLogToIndex(0);
            char buf[4];
            EncodeFixed32(buf, 0);
            batch.Put(handles_[3], key, Slice(buf, sizeof(int32_t)));
        } else {
            char buf[4];
            ListsMetaKeyLog lists_meta_key_log(key, version, log_index);
            ListsMetaValueLog lists_meta_value_log(ParsedListsMetaValue::LIST_META_VALUE_INSERT, pivot_index);
            parsed_lists_meta_value.ModifyLogIndex(1);
            batch.Put(handles_[2], lists_meta_key_log.Encode(), lists_meta_value_log.Encode());
            EncodeFixed32(buf, parsed_lists_meta_value.log_index());
            batch.Put(handles_[3], key, Slice(buf, sizeof(int32_t)));
        }
        *ret = parsed_lists_meta_value.count();
        s = db_->Write(default_write_options_, &batch);
        if (s.ok()) {
            delete meta_info->second;
            meta_info->second = meta_value;
        } else {
            delete meta_value;
        }
      }
    }
  } else {
    *ret = 0;
  }
  return s;
}

Status RedisLists::LLen(const Slice& key, uint64_t* len) {
  *len = 0;
  std::unordered_map<std::string, std::string*>::iterator meta_info =
      meta_infos_list_.find(key.data());
  Status s;
  if (meta_info != meta_infos_list_.end()) {
    ParsedListsMetaValue parsed_lists_meta_value(meta_info->second);
    if (parsed_lists_meta_value.IsStale()) {
      *len = 0;
      return Status::NotFound("Stale");
    } else if (parsed_lists_meta_value.count() == 0) {
      *len = 0;
      return Status::NotFound();
    } else {
      *len = parsed_lists_meta_value.count();
      return s;
    }
  }
  return s;
}

Status RedisLists::LPop(const Slice& key, std::string* element) {
  shannon::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);
  std::string *meta_value;
  std::unordered_map<std::string, std::string*>::iterator meta_info =
      meta_infos_list_.find(key.data());
  Status s;
  if (meta_info != meta_infos_list_.end()) {
    meta_value = new std::string();
    meta_value->resize(meta_info->second->size());
    memcpy(const_cast<char*>(meta_value->data()), meta_info->second->data(), meta_info->second->size());
    ParsedListsMetaValue parsed_lists_meta_value(meta_value);
    if (parsed_lists_meta_value.IsStale()) {
      delete meta_value;
      return Status::NotFound("Stale");
    } else if (parsed_lists_meta_value.count() == 0) {
      delete meta_value;
      return Status::NotFound();
    } else {
      int32_t version = parsed_lists_meta_value.version();
      uint64_t first_node_index = parsed_lists_meta_value.get_fact_index(0);
      ListsDataKey lists_data_key(key, version, first_node_index);
      s = db_->Get(default_read_options_, handles_[1], lists_data_key.Encode(), element);
      if (s.ok()) {
        batch.Delete(handles_[1], lists_data_key.Encode());
        parsed_lists_meta_value.ModifyCacheMoveLeft(1);
        parsed_lists_meta_value.ModifyCount(-1);
        int32_t log_index = parsed_lists_meta_value.log_index();
        if (log_index >= lists_log_count_) {
            batch.Put(handles_[0], key, *meta_value);
            parsed_lists_meta_value.ModifyLogToIndex(0);
            char buf[4];
            EncodeFixed32(buf, 0);
            batch.Put(handles_[3], key, Slice(buf, 4));
        } else {
            parsed_lists_meta_value.ModifyLogIndex(1);
            ListsMetaKeyLog lists_meta_key_log(key, version, log_index);
            ListsMetaValueLog lists_meta_value_log(ParsedListsMetaValue::LIST_META_VALUE_DEL, 0);
            batch.Put(handles_[2], lists_meta_key_log.Encode(), lists_meta_value_log.Encode());
            char buf[4];
            EncodeFixed32(buf, parsed_lists_meta_value.log_index());
            batch.Put(handles_[3], key, Slice(buf, sizeof(int32_t)));
        }
        s = db_->Write(default_write_options_, &batch);
        if (s.ok()) {
            delete meta_info->second;
            meta_info->second = meta_value;
        } else {
            delete meta_value;
        }
        return s;
      } else {
        return s;
      }
    }
  }
  return s;
}

Status RedisLists::LPush(const Slice& key,
                         const std::vector<std::string>& values,
                         uint64_t* ret) {
  *ret = 0;
  shannon::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);

  uint64_t index = 0;
  int32_t version = 0;
  int32_t log_index;
  Status s;
  std::string *meta_value;
  bool is_stale = false;
  std::unordered_map<std::string, std::string*>::iterator meta_info = meta_infos_list_.find(key.data());
  if (meta_info != meta_infos_list_.end()) {
    meta_value = new string();
    meta_value->resize(meta_info->second->size());
    memcpy(const_cast<char*>(meta_value->data()), meta_info->second->data(), meta_value->size());
    ParsedListsMetaValue parsed_lists_meta_value(meta_value);
    log_index = parsed_lists_meta_value.log_index();
    if (parsed_lists_meta_value.IsStale()) {
      version = parsed_lists_meta_value.InitialMetaValue();
      is_stale = true;
    } else {
      version = parsed_lists_meta_value.version();
    }
    parsed_lists_meta_value.ModifyCacheMoveRight(values.size());
    uint64_t raw_index = values.size() - 1;
    for (const auto& value : values) {
      index = parsed_lists_meta_value.index();
      parsed_lists_meta_value.ModifyIndex(1);
      parsed_lists_meta_value.ModifyCacheIndex(raw_index --, index);
      parsed_lists_meta_value.ModifyCount(1);
      ListsDataKey lists_data_key(key, version, index);
      batch.Put(handles_[1], lists_data_key.Encode(), value);
    }
    if (is_stale || log_index >= lists_log_count_) {
        batch.Put(handles_[0], key, *meta_value);
        parsed_lists_meta_value.ModifyLogToIndex(0);
        char buf[4];
        EncodeFixed32(buf, 0);
        batch.Put(handles_[3], key, Slice(buf, sizeof(int32_t)));
    } else {
        char buf[4];
        int32_t version = parsed_lists_meta_value.version();
        int32_t log_index = parsed_lists_meta_value.log_index();
        parsed_lists_meta_value.ModifyLogIndex(1);
        ListsMetaKeyLog lists_meta_key_log(key, version, log_index);
        ListsMetaValueLog lists_meta_value_log(ParsedListsMetaValue::LIST_META_VALUE_LPUSH, values.size());
        batch.Put(handles_[2], lists_meta_key_log.Encode(), lists_meta_value_log.Encode());
        EncodeFixed32(buf, parsed_lists_meta_value.log_index());
        batch.Put(handles_[3], key, Slice(buf, sizeof(int32_t)));
    }
    *ret = parsed_lists_meta_value.count();
  } else {
    meta_value = new std::string();
    meta_value->resize(16+8+8096);
    EncodeFixed32(const_cast<char *>(meta_value->data() + sizeof(int32_t)), 0);
    ListsMetaValue lists_meta_value(Slice(meta_value->data(), meta_value->size()));
    version = lists_meta_value.UpdateVersion();
    lists_meta_value.ModifyLogToIndex(0);
    uint64_t raw_index = values.size() - 1;
    for (const auto& value : values) {
      index = lists_meta_value.index();
      lists_meta_value.ModifyIndex(1);
      lists_meta_value.ModifyCacheIndex(raw_index --, index);
      lists_meta_value.ModifyCount(1);
      ListsDataKey lists_data_key(key, version, index);
      batch.Put(handles_[1], lists_data_key.Encode(), value);
    }
    batch.Put(handles_[0], key, lists_meta_value.Encode());
    *ret = lists_meta_value.count();
    log_index = lists_meta_value.log_index();
  }
  s = db_->Write(default_write_options_, &batch);
  if (s.ok()) {
      if (meta_info != meta_infos_list_.end()) {
          delete meta_info->second;
          meta_info->second = meta_value;
      } else {
          meta_infos_list_.insert(make_pair(key.data(), meta_value));
      }
  } else {
      delete meta_value;
  }
  return s;
}

Status RedisLists::LPushx(const Slice& key, const Slice& value, uint64_t* len) {
  *len = 0;
  shannon::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);

  std::string *meta_value;
  std::unordered_map<std::string, std::string*>::iterator meta_info =
      meta_infos_list_.find(key.data());
  Status s;
  if (meta_info != meta_infos_list_.end()) {
    meta_value = new std::string();
    meta_value->resize(meta_info->second->size());
    memcpy(const_cast<char *>(meta_value->data()), meta_info->second->data(), meta_info->second->size());
    ParsedListsMetaValue parsed_lists_meta_value(meta_value);
    if (parsed_lists_meta_value.IsStale()) {
      delete meta_value;
      return Status::NotFound("Stale");
    } else if (parsed_lists_meta_value.count() == 0) {
      delete meta_value;
      return Status::NotFound();
    } else {
      uint32_t version = parsed_lists_meta_value.version();
      uint64_t index = parsed_lists_meta_value.index();
      parsed_lists_meta_value.ModifyCacheMoveRight(1);
      parsed_lists_meta_value.ModifyIndex(1);
      parsed_lists_meta_value.ModifyCacheIndex(0, index);
      parsed_lists_meta_value.ModifyCount(1);
      ListsDataKey lists_data_key(key, version, index);
      batch.Put(handles_[1], lists_data_key.Encode(), value);
      int32_t log_index = parsed_lists_meta_value.log_index();
      if (log_index >= lists_log_count_) {
          batch.Put(handles_[0], key, *meta_value);
          parsed_lists_meta_value.ModifyLogToIndex(0);
          char buf[4];
          EncodeFixed32(buf, 0);
          batch.Put(handles_[3], key, Slice(buf, sizeof(int32_t)));
      } else {
          parsed_lists_meta_value.ModifyLogIndex(1);
          ListsMetaKeyLog lists_meta_key_log(key, version, log_index);
          ListsMetaValueLog lists_meta_value_log(ParsedListsMetaValue::LIST_META_VALUE_LPUSH, 1);
          batch.Put(handles_[2], lists_meta_key_log.Encode(), lists_meta_value_log.Encode());
          char buf[4];
          EncodeFixed32(buf, parsed_lists_meta_value.log_index());
          batch.Put(handles_[3], key, Slice(buf, sizeof(int32_t)));
      }
      *len = parsed_lists_meta_value.count();
      s = db_->Write(default_write_options_, &batch);
      if (s.ok()) {
          delete meta_info->second;
          meta_info->second = meta_value;
      } else {
          delete meta_value;
      }
      return s;
    }
  } else {
      s = Status::NotFound();
  }
  return s;
}

Status RedisLists::LRange(const Slice& key, int64_t start, int64_t stop,
                          std::vector<std::string>* ret) {
  shannon::ReadOptions read_options;
  const shannon::Snapshot* snapshot;

  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;

  Status s;
  std::unordered_map<std::string, std::string*>::iterator meta_info;
  meta_info = meta_infos_list_.find(key.data());
  if (meta_info != meta_infos_list_.end()) {
    ParsedListsMetaValue parsed_lists_meta_value(meta_info->second);
    if (parsed_lists_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_lists_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      int32_t version = parsed_lists_meta_value.version();
      uint64_t origin_left_index = 0;//parsed_lists_meta_value.left_index() + 1;
      uint64_t origin_right_index = (uint64_t)parsed_lists_meta_value.count() - 1;
      uint64_t sublist_left_index = start >= 0 ?
                                    origin_left_index + start :
                                    origin_right_index + start + 1;
      uint64_t sublist_right_index = stop >= 0 ?
                                    origin_left_index + stop :
                                    origin_right_index + stop + 1;

      if (sublist_left_index > sublist_right_index
        || sublist_left_index > origin_right_index
        || sublist_right_index < origin_left_index) {
        return Status::OK();
      } else {
        if (sublist_left_index < origin_left_index) {
          sublist_left_index = origin_left_index;
        }
        if (sublist_right_index > origin_right_index) {
          sublist_right_index = origin_right_index;
        }
        uint64_t current_index = sublist_left_index;
        for (; current_index <= sublist_right_index; current_index ++) {
            ListsDataKey data_key(key, version, parsed_lists_meta_value.get_fact_index(current_index));
            std::string data_value;
            s = db_->Get(read_options, handles_[1], data_key.Encode(), &data_value);
            if (s.ok()) {
                ret->push_back(data_value);
            }
        }
        return Status::OK();
      }
    }
  } else {
    return Status::NotFound();
  }
}

Status RedisLists::LRem(const Slice& key, int64_t count,
                        const Slice& value, uint64_t* ret) {
  *ret = 0;
  shannon::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);
  std::string *meta_value;
  std::unordered_map<std::string, std::string*>::iterator meta_info =
      meta_infos_list_.find(key.data());
  Status s;
  if (meta_info != meta_infos_list_.end()) {
    meta_value = new std::string();
    meta_value->resize(meta_info->second->size());
    memcpy(const_cast<char *>(meta_value->data()), meta_info->second->data(), meta_info->second->size());
    ParsedListsMetaValue parsed_lists_meta_value(meta_value);
    if (parsed_lists_meta_value.IsStale()) {
      *ret = 0;
      delete meta_value;
      return Status::NotFound("Stale");
    } else if (parsed_lists_meta_value.count() == 0) {
      *ret = 0;
      delete meta_value;
      return Status::NotFound();
    } else {
      uint64_t current_index;
      std::vector<uint64_t> target_index;
      std::vector<uint64_t> delete_index;
      uint64_t rest = (count < 0) ? -count : count;
      uint32_t version = parsed_lists_meta_value.version();
      uint64_t start_index = 0;
      uint64_t stop_index = parsed_lists_meta_value.count() - 1;
      ListsDataKey start_data_key(key, version, start_index);
      ListsDataKey stop_data_key(key, version, stop_index);
      if (count >= 0) {
        for (current_index = start_index;
             current_index <= stop_index && (!count || rest != 0);
             ++ current_index) {
          std::string data_value;
          ListsDataKey lists_data_key(key, version, parsed_lists_meta_value.get_fact_index(current_index));
          s = db_->Get(default_read_options_, handles_[1], lists_data_key.Encode(), &data_value);
          if (s.ok()) {
            if (strcmp(data_value.data(), value.data()) == 0) {

                target_index.push_back(current_index);
                if (count != 0) {
                    rest--;
                }
            }
          }
        }
      } else {
        current_index = stop_index;
        for (;
             current_index >= start_index && (!count || rest != 0);
             current_index--) {
          std::string data_value;
          ListsDataKey lists_data_key(key, version, parsed_lists_meta_value.get_fact_index(current_index));
          s = db_->Get(default_read_options_, handles_[1], lists_data_key.Encode(), &data_value);
          if (s.ok()) {
            if (strcmp(data_value.data(), value.data()) == 0) {
                target_index.push_back(current_index);
                if (count != 0) {
                    rest--;
                }
            }
          }
        }
      }
      if (target_index.empty()) {
        *ret = 0;
        delete meta_value;
        return Status::NotFound();
      } else {
        rest = target_index.size();
        for (uint64_t i : target_index) {
            ListsDataKey lists_data_key(key, version, parsed_lists_meta_value.get_fact_index(i));
            batch.Delete(handles_[1], lists_data_key.Encode());
        }
        for (int32_t index = target_index.size() - 1; index >= 0; index --) {
            uint64_t i = target_index[index];
            parsed_lists_meta_value.ModifyCacheIndex(i, 0xffffffffffffffff);
            int32_t log_index = parsed_lists_meta_value.log_index();
            parsed_lists_meta_value.ModifyLogIndex(1);
            ListsMetaKeyLog lists_meta_key_log(key, version, log_index);
            ListsMetaValueLog lists_meta_value_log(ParsedListsMetaValue::LIST_META_VALUE_DEL, i);
            batch.Put(handles_[2], lists_meta_key_log.Encode(), lists_meta_value_log.Encode());
        }

        uint32_t count = parsed_lists_meta_value.SortOutCache();
        parsed_lists_meta_value.ModifyCount(-count);
        int32_t log_index = parsed_lists_meta_value.log_index();
        if (log_index >= lists_log_count_) {
            batch.Put(handles_[0], key, *meta_value);
            parsed_lists_meta_value.ModifyLogToIndex(0);
            char buf[4];
            EncodeFixed32(buf, 0);
            batch.Put(handles_[3], key, Slice(buf, sizeof(int32_t)));
        } else {
            char buf[4];
            EncodeFixed32(buf, parsed_lists_meta_value.log_index());
            batch.Put(handles_[3], key, Slice(buf, sizeof(int32_t)));
        }
        *ret = target_index.size();
        s = db_->Write(default_write_options_, &batch);
        if (s.ok()) {
            delete meta_info->second;
            meta_info->second = meta_value;
        } else {
            delete meta_value;
        }
        return s;
      }
    }
    delete meta_value;
  } else {
      s = Status::NotFound();
  }
  return s;
}

Status RedisLists::LSet(const Slice& key, int64_t index, const Slice& value) {
  ScopeRecordLock l(lock_mgr_, key);
  std::string meta_value;
  std::unordered_map<std::string, std::string*>::iterator meta_info =
      meta_infos_list_.find(key.data());
  Status s;
  if (meta_info != meta_infos_list_.end()) {
    ParsedListsMetaValue parsed_lists_meta_value(meta_info->second);
    if (parsed_lists_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_lists_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      uint32_t version = parsed_lists_meta_value.version();
      int64_t target_index = index >= 0 ? index
            : parsed_lists_meta_value.count() + index;
      if (target_index < 0
        || target_index >= parsed_lists_meta_value.count()) {
        return Status::Corruption("index out of range");
      }
      target_index = parsed_lists_meta_value.get_fact_index(target_index);
      ListsDataKey lists_data_key(key, version, target_index);
      if (parsed_lists_meta_value.timestamp() != 0 ) {
        char str[sizeof(int32_t)+key.size() +1];
        EncodeFixed32(str,parsed_lists_meta_value.timestamp());
        memcpy(str + sizeof(int32_t) , key.data(),key.size());
        db_->Put(default_write_options_,handles_[4], {str,sizeof(int32_t)+key.size()}, "1" );
      }
      return db_->Put(default_write_options_, handles_[1],
                      lists_data_key.Encode(), value);
    }
  } else {
      s = Status::NotFound();
  }
  return s;
}

Status RedisLists::LTrim(const Slice& key, int64_t start, int64_t stop) {

  shannon::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);

  std::string *meta_value = NULL;
  std::unordered_map<std::string, std::string*>::iterator meta_info =
      meta_infos_list_.find(key.data());
  Status s;
  if (meta_info != meta_infos_list_.end()) {
    meta_value = new std::string();
    meta_value->resize(meta_info->second->size());
    memcpy(const_cast<char *>(meta_value->data()), meta_info->second->data(), meta_info->second->size());
    ParsedListsMetaValue parsed_lists_meta_value(meta_value);
    int32_t version = parsed_lists_meta_value.version();
    if (parsed_lists_meta_value.IsStale()) {
      delete meta_value;
      return Status::NotFound("Stale");
    } else if (parsed_lists_meta_value.count() == 0) {
      delete meta_value;
      return Status::NotFound();
    } else {
      uint64_t origin_left_index = 0;
      uint64_t origin_right_index = parsed_lists_meta_value.count() - 1;
      uint64_t sublist_left_index  = start >= 0 ? start :
                                     origin_right_index + start + 1;
      uint64_t sublist_right_index = stop >= 0 ? stop :
                                     origin_right_index + stop + 1;
      int32_t log_index = parsed_lists_meta_value.log_index();
      if (sublist_left_index == origin_left_index
       && sublist_right_index == origin_right_index) {
          return Status::OK();
      } else if (sublist_left_index > sublist_right_index
        || sublist_left_index > origin_right_index
        || sublist_right_index < origin_left_index) {
        // delete all
        for (uint64_t idx = origin_left_index; idx <= origin_right_index; idx++) {
          ListsDataKey lists_data_key(key, version, parsed_lists_meta_value.get_fact_index(idx));
          batch.Delete(handles_[1], lists_data_key.Encode());
        }
        parsed_lists_meta_value.InitialMetaValue();
        batch.Put(handles_[0], key, *meta_value);
        char buf[4];
        EncodeFixed32(buf, 0);
        batch.Put(handles_[3], key, Slice(buf, sizeof(int32_t)));
      } else {
        if (sublist_left_index < origin_left_index) {
          sublist_left_index = origin_left_index;
        }

        if (sublist_right_index > origin_right_index) {
          sublist_right_index = origin_right_index;
        }

        uint64_t delete_node_num = (sublist_left_index - origin_left_index)
          + (origin_right_index - sublist_right_index);

        char buf[4];
        for (uint64_t idx = origin_right_index; idx > sublist_right_index; --idx) {
          ListsDataKey lists_data_key(key, version, parsed_lists_meta_value.get_fact_index(idx));
          batch.Delete(handles_[1], lists_data_key.Encode());
        }
        if (sublist_right_index < origin_right_index) {
            log_index = parsed_lists_meta_value.log_index();
            if (log_index < lists_log_count_) {
                parsed_lists_meta_value.ModifyLogIndex(1);
                ListsMetaKeyLog lists_meta_key_log(key, version, log_index);
                ListsMetaValueLog lists_meta_value_log(ParsedListsMetaValue::LIST_META_VALUE_DEL,
                        sublist_right_index + 1, (origin_right_index - sublist_right_index));
                batch.Put(handles_[2], lists_meta_key_log.Encode(), lists_meta_value_log.Encode());
            } else {
                batch.Put(handles_[0], key, *meta_value);
                parsed_lists_meta_value.ModifyLogToIndex(0);
                char buf[4];
                EncodeFixed32(buf, 0);
                batch.Put(handles_[3], key, Slice(buf, 0));
            }
        }
        for (uint64_t idx = origin_left_index; idx < sublist_left_index; ++idx) {
          ListsDataKey lists_data_key(key, version, parsed_lists_meta_value.get_fact_index(idx));
          batch.Delete(handles_[1], lists_data_key.Encode());
        }
        if (sublist_left_index > origin_left_index) {
            log_index = parsed_lists_meta_value.log_index();
            if (log_index < lists_log_count_) {
                parsed_lists_meta_value.ModifyLogIndex(1);
                ListsMetaKeyLog lists_meta_key_log(key, version, log_index);
                ListsMetaValueLog lists_meta_value_log(ParsedListsMetaValue::LIST_META_VALUE_DEL,
                        origin_left_index, (sublist_left_index - origin_left_index));
                batch.Put(handles_[2], lists_meta_key_log.Encode(), lists_meta_value_log.Encode());
                EncodeFixed32(buf, parsed_lists_meta_value.log_index());
                batch.Put(handles_[3], key, Slice(buf, sizeof(int32_t)));
            } else {
                batch.Put(handles_[0], key, *meta_value);
                parsed_lists_meta_value.ModifyLogToIndex(0);
                char buf[4];
                EncodeFixed32(buf, 0);
                batch.Put(handles_[3], key, Slice(buf, 0));
            }
        }
        parsed_lists_meta_value.ModifyCacheMoveLeft(sublist_left_index-origin_left_index);
        parsed_lists_meta_value.ModifyCount(-delete_node_num);
      }
    }
  } else {
    return Status::NotFound();
  }
  s = db_->Write(default_write_options_, &batch);
  if (s.ok()) {
      delete meta_info->second;
      meta_info->second = meta_value;
  } else {
      delete meta_value;
  }
  return s;
}

Status RedisLists::RPop(const Slice& key, std::string* element) {
  shannon::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);

  std::string *meta_value;
  std::unordered_map<std::string, std::string*>::iterator meta_info =
      meta_infos_list_.find(key.data());
  Status s;
  if (meta_info != meta_infos_list_.end()) {
    meta_value = new std::string();
    meta_value->resize(meta_info->second->size());
    memcpy(const_cast<char*>(meta_value->data()), meta_info->second->data(), meta_info->second->size());
    ParsedListsMetaValue parsed_lists_meta_value(meta_value);
    if (parsed_lists_meta_value.IsStale()) {
      delete meta_value;
      return Status::NotFound("Stale");
    } else if (parsed_lists_meta_value.count() == 0) {
      delete meta_value;
      return Status::NotFound();
    } else {
      int32_t version = parsed_lists_meta_value.version();
      uint64_t last_node_index = parsed_lists_meta_value.count() - 1;
      last_node_index = parsed_lists_meta_value.get_fact_index(last_node_index);
      ListsDataKey lists_data_key(key, version, last_node_index);
      s = db_->Get(default_read_options_, handles_[1], lists_data_key.Encode(), element);
      if (s.ok()) {
        batch.Delete(handles_[1], lists_data_key.Encode());
        parsed_lists_meta_value.ModifyCount(-1);
        int32_t log_index = parsed_lists_meta_value.log_index();
        if (log_index < lists_log_count_) {
            parsed_lists_meta_value.ModifyLogIndex(1);
            ListsMetaKeyLog lists_meta_key_log(key, version, log_index);
            ListsMetaValueLog lists_meta_value_log(ParsedListsMetaValue::LIST_META_VALUE_DEL,
                    parsed_lists_meta_value.count());
            batch.Put(handles_[2], lists_meta_key_log.Encode(), lists_meta_value_log.Encode());
            char buf[4];
            EncodeFixed32(buf, parsed_lists_meta_value.log_index());
            batch.Put(handles_[3], key, Slice(buf, sizeof(int32_t)));
        } else {
            batch.Put(handles_[0], key, *meta_value);
            parsed_lists_meta_value.ModifyLogToIndex(0);
            char buf[4];
            EncodeFixed32(buf, 0);
            batch.Put(handles_[3], key, Slice(buf, sizeof(int32_t)));
        }
        s = db_->Write(default_write_options_, &batch);
        if (s.ok()) {
            delete meta_info->second;
            meta_info->second = meta_value;
        } else {
            delete meta_value;
        }
      } else {
        delete meta_value;
        return s;
      }
    }
  } else {
      s = Status::NotFound();
  }
  return s;
}

Status RedisLists::RPoplpush(const Slice& source,
                             const Slice& destination,
                             std::string* element) {
  element->clear();
  Status s;
  shannon::WriteBatch batch;
  std::string *meta_value;
  std::unordered_map<std::string, std::string*>::iterator meta_info;
  MultiScopeRecordLock l(lock_mgr_, {source.ToString(), destination.ToString()});
  if (!source.compare(destination)) {
    meta_info = meta_infos_list_.find(source.data());
    if (meta_info != meta_infos_list_.end()) {
      meta_value = new std::string();
      meta_value->resize(meta_info->second->size());
      memcpy(const_cast<char *>(meta_value->data()), meta_info->second->data(), meta_info->second->size());
      ParsedListsMetaValue parsed_lists_meta_value(meta_value);
      if (parsed_lists_meta_value.IsStale()) {
        delete meta_value;
        return Status::NotFound("Stale");
      } else if (parsed_lists_meta_value.count() == 0) {
        delete meta_value;
        return Status::NotFound();
      } else {
        int32_t version = parsed_lists_meta_value.version();
        uint32_t count = parsed_lists_meta_value.count();
        uint64_t right_index = parsed_lists_meta_value.get_fact_index(count - 1);
        std::string target;
        parsed_lists_meta_value.ModifyCacheMoveRightForBlock(0, count - 1, 1);
        parsed_lists_meta_value.ModifyCacheIndex(0, right_index);

        int32_t log_index = parsed_lists_meta_value.log_index();
        if (log_index >= lists_log_count_) {
            batch.Put(handles_[0], source, *meta_value);
            parsed_lists_meta_value.ModifyLogToIndex(0);
            char buf[4];
            EncodeFixed32(buf, 0);
            batch.Put(handles_[3], source, Slice(buf, sizeof(int32_t)));
        } else {
            parsed_lists_meta_value.ModifyLogIndex(1);
            ListsMetaKeyLog lists_meta_key_log(source, version, log_index);
            ListsMetaValueLog lists_meta_value_log(ParsedListsMetaValue::LIST_META_VALUE_RPOPLPUSH, 0);
            batch.Put(handles_[2], lists_meta_key_log.Encode(), lists_meta_value_log.Encode());
            char buf[4];
            EncodeFixed32(buf, parsed_lists_meta_value.log_index());
            batch.Put(handles_[3], source, Slice(buf, sizeof(int32_t)));
        }
        // batch.Put(handles_[0], source, *meta_value);
        ListsDataKey lists_data_key(source, version, right_index);
        s = db_->Get(default_read_options_, handles_[1], lists_data_key.Encode(), &target);
        if (s.ok()) {
            *element = target;
        }
        s = db_->Write(default_write_options_, &batch);
        if (s.ok()) {
            delete meta_info->second;
            meta_info->second = meta_value;
        } else {
            delete meta_value;
        }
        return s;
      }
    } else {
      return s;
    }
  }

  int32_t version;
  std::string target;
  std::string *source_meta_value;
  std::unordered_map<std::string, std::string*>::iterator meta_info2;
  meta_info = meta_infos_list_.find(source.data());
  if (meta_info != meta_infos_list_.end()) {
    source_meta_value = new std::string();
    source_meta_value->resize(meta_info->second->size());
    memcpy(const_cast<char *>(source_meta_value->data()), meta_info->second->data(), meta_info->second->size());
    ParsedListsMetaValue parsed_lists_meta_value(source_meta_value);
    if (parsed_lists_meta_value.IsStale()) {
      delete source_meta_value;
      return Status::NotFound("Stale");
    } else if(parsed_lists_meta_value.count() == 0) {
      delete source_meta_value;
      return Status::NotFound();
    } else {
      version = parsed_lists_meta_value.version();
      uint64_t last_node_index = parsed_lists_meta_value.count() - 1;
      last_node_index = parsed_lists_meta_value.get_fact_index(last_node_index);
      ListsDataKey lists_data_key(source, version, last_node_index);
      s = db_->Get(default_read_options_, handles_[1], lists_data_key.Encode(), &target);
      if (s.ok()) {
        batch.Delete(handles_[1], lists_data_key.Encode());
        parsed_lists_meta_value.ModifyCount(-1);
        // batch.Put(handles_[0], source, *source_meta_value);
        int32_t log_index = parsed_lists_meta_value.log_index();
        if (log_index >= lists_log_count_) {
            batch.Put(handles_[0], source, *source_meta_value);
            parsed_lists_meta_value.ModifyLogToIndex(0);
            char buf[4];
            EncodeFixed32(buf, 0);
            batch.Put(handles_[3], source, Slice(buf, sizeof(int32_t)));
        } else {
            parsed_lists_meta_value.ModifyLogIndex(1);
            ListsMetaKeyLog lists_meta_key_log(source, version, log_index);
            ListsMetaValueLog lists_meta_value_log(ParsedListsMetaValue::LIST_META_VALUE_DEL, parsed_lists_meta_value.count());
            batch.Put(handles_[2], lists_meta_key_log.Encode(), lists_meta_value_log.Encode());
            char buf[4];
            EncodeFixed32(buf, parsed_lists_meta_value.log_index());
            batch.Put(handles_[3], source, Slice(buf, sizeof(int32_t)));
        }
      } else {
        delete source_meta_value;
        return s;
      }
    }
  } else {
    return s;
  }

  std::string *destination_meta_value;
  bool is_stale = false;
  meta_info2 = meta_infos_list_.find(destination.data());
  if (meta_info2 != meta_infos_list_.end()) {
    destination_meta_value = new std::string();
    destination_meta_value->resize(meta_info2->second->size());
    memcpy(const_cast<char *>(destination_meta_value->data()), meta_info2->second->data(), meta_info2->second->size());
    ParsedListsMetaValue parsed_lists_meta_value(destination_meta_value);
    if (parsed_lists_meta_value.IsStale()) {
      version = parsed_lists_meta_value.InitialMetaValue();
      parsed_lists_meta_value.ModifyCacheIndex(0, 0);
      is_stale = true;
    } else {
      version = parsed_lists_meta_value.version();
    }
    uint64_t target_index = parsed_lists_meta_value.index();
    ListsDataKey lists_data_key(destination, version, target_index);
    batch.Put(handles_[1], lists_data_key.Encode(), target);
    parsed_lists_meta_value.ModifyCacheMoveRight(1);
    parsed_lists_meta_value.ModifyCacheIndex(0, target_index);
    parsed_lists_meta_value.ModifyIndex(1);
    parsed_lists_meta_value.ModifyCount(1);
    int32_t log_index = parsed_lists_meta_value.log_index();
    if (is_stale || log_index < lists_log_count_) {
        batch.Put(handles_[0], destination, *destination_meta_value);
        parsed_lists_meta_value.ModifyLogToIndex(0);
        char buf[4];
        EncodeFixed32(buf, 0);
        batch.Put(handles_[3], destination, Slice(buf, sizeof(int32_t)));
    } else {
        parsed_lists_meta_value.ModifyLogIndex(1);
        ListsMetaKeyLog lists_meta_key_log(destination, version, log_index);
        ListsMetaValueLog lists_meta_value_log(ParsedListsMetaValue::LIST_META_VALUE_LPUSH, 1);
        batch.Put(handles_[2], lists_meta_key_log.Encode(), lists_meta_value_log.Encode());
        char buf[4];
        EncodeFixed32(buf, parsed_lists_meta_value.log_index());
        batch.Put(handles_[3], destination, Slice(buf, sizeof(int32_t)));
    }
  } else {
    destination_meta_value = new std::string();
    destination_meta_value->resize(16 + 8 + 8096);
    ListsMetaValue lists_meta_value(Slice(destination_meta_value->data(), destination_meta_value->size()));
    lists_meta_value.ModifyLogToIndex(0);
    version = lists_meta_value.UpdateVersion();
    uint64_t target_index = lists_meta_value.index();
    ListsDataKey lists_data_key(destination, version, target_index);
    batch.Put(handles_[1], lists_data_key.Encode(), target);
    lists_meta_value.ModifyCacheIndex(0, target_index);
    lists_meta_value.ModifyIndex(1);
    lists_meta_value.ModifyCount(1);
    batch.Put(handles_[0], destination, lists_meta_value.Encode());
  }

  s = db_->Write(default_write_options_, &batch);
  if (s.ok()) {
    *element = target;
    delete meta_info->second;
    meta_info->second = source_meta_value;
    if (meta_info2 != meta_infos_list_.end()) {
        delete meta_info2->second;
        meta_info2->second = destination_meta_value;
    } else {
        meta_infos_list_.insert(make_pair(destination.data(), destination_meta_value));
    }
  } else {
      delete source_meta_value;
      delete destination_meta_value;
  }
  return s;
}

Status RedisLists::RPush(const Slice& key,
                         const std::vector<std::string>& values,
                         uint64_t* ret) {
  *ret = 0;
  shannon::WriteBatch batch;

  uint64_t index = 0;
  int32_t version = 0;
  uint32_t count = 0;
  bool is_stale = false;
  std::string *meta_value;
  Status s;
  std::unordered_map<std::string, std::string*>::iterator meta_info = meta_infos_list_.find(key.data());
  if (meta_info != meta_infos_list_.end()) {
    meta_value = new std::string();
    meta_value->resize(meta_info->second->size());
    memcpy(const_cast<char*>(meta_value->data()), meta_info->second->data(), meta_info->second->size());
    ParsedListsMetaValue parsed_lists_meta_value(meta_value);
    if (parsed_lists_meta_value.IsStale()) {
      version = parsed_lists_meta_value.InitialMetaValue();
      is_stale = true;
    } else {
      version = parsed_lists_meta_value.version();
    }
    for (const auto& value : values) {
      count = parsed_lists_meta_value.count();
      index = parsed_lists_meta_value.index();
      parsed_lists_meta_value.ModifyCount(1);
      parsed_lists_meta_value.ModifyIndex(1);
      parsed_lists_meta_value.ModifyCacheIndex(count, index);
      ListsDataKey lists_data_key(key, version, index);
      batch.Put(handles_[1], lists_data_key.Encode(), value);
    }
    int32_t log_index = parsed_lists_meta_value.log_index();
    if (is_stale || log_index >= lists_log_count_) {
        batch.Put(handles_[0], key, *meta_value);
        parsed_lists_meta_value.ModifyLogToIndex(0);
        char buf[4];
        EncodeFixed32(buf, 0);
        batch.Put(handles_[3], key, Slice(buf, sizeof(int32_t)));
    } else {
        char buf[4];
        ListsMetaKeyLog lists_meta_key_log(key, parsed_lists_meta_value.version(), log_index);
        ListsMetaValueLog lists_meta_value_log(ParsedListsMetaValue::LIST_META_VALUE_RPUSH, values.size());
        parsed_lists_meta_value.ModifyLogIndex(1);
        batch.Put(handles_[2], lists_meta_key_log.Encode(), lists_meta_value_log.Encode());
        EncodeFixed32(buf, parsed_lists_meta_value.log_index());
        batch.Put(handles_[3], key, Slice(buf, sizeof(int32_t)));
    }
    *ret = parsed_lists_meta_value.count();
  } else {
    meta_value = new std::string();
    meta_value->resize(16+8+8096);
    ListsMetaValue lists_meta_value(Slice(meta_value->data(), meta_value->size()));
    version = lists_meta_value.UpdateVersion();
    for (auto value : values) {
      index = lists_meta_value.count();
      lists_meta_value.ModifyIndex(1);
      lists_meta_value.ModifyCacheIndex(index, index);
      lists_meta_value.ModifyCount(1);
      ListsDataKey lists_data_key(key, version, index);
      batch.Put(handles_[1], lists_data_key.Encode(), value);
    }
    batch.Put(handles_[0], key, lists_meta_value.Encode());
    *ret = lists_meta_value.count();
  }
  s = db_->Write(default_write_options_, &batch);
  if (s.ok()) {
      if (meta_info != meta_infos_list_.end()) {
          delete meta_info->second;
          meta_info->second = meta_value;
      } else {
          meta_infos_list_.insert(make_pair(key.data(), meta_value));
      }
  } else {
      delete meta_value;
  }
  return s;
}

Status RedisLists::RPushx(const Slice& key, const Slice& value, uint64_t* len) {
  *len = 0;
  shannon::WriteBatch batch;

  ScopeRecordLock l(lock_mgr_, key);
  std::string *meta_value;
  std::unordered_map<std::string, std::string*>::iterator meta_info =
      meta_infos_list_.find(key.data());
  Status s;
  if (meta_info != meta_infos_list_.end()) {
    meta_value = new std::string();
    meta_value->resize(meta_info->second->size());
    memcpy(const_cast<char *>(meta_value->data()), meta_info->second->data(), meta_info->second->size());
    ParsedListsMetaValue parsed_lists_meta_value(meta_value);
    if (parsed_lists_meta_value.IsStale()) {
      delete meta_value;
      return Status::NotFound("Stale");
    } else if (parsed_lists_meta_value.count() == 0) {
      delete meta_value;
      return Status::NotFound();
    } else {
      uint32_t version = parsed_lists_meta_value.version();
      uint64_t index = parsed_lists_meta_value.index();
      uint32_t count = parsed_lists_meta_value.count();
      parsed_lists_meta_value.ModifyIndex(1);
      parsed_lists_meta_value.ModifyCount(1);
      parsed_lists_meta_value.ModifyCacheIndex(count, index);
      ListsDataKey lists_data_key(key, version, index);
      int32_t log_index = parsed_lists_meta_value.log_index();
      if (log_index >= lists_log_count_) {
        batch.Put(handles_[0], key, *meta_value);
        parsed_lists_meta_value.ModifyLogToIndex(0);
        char buf[4];
        EncodeFixed32(buf, 0);
        batch.Put(handles_[3], key, Slice(buf, sizeof(int32_t)));
      } else {
        char buf[4];
        ListsMetaKeyLog lists_meta_key_log(key, version, log_index);
        ListsMetaValueLog lists_meta_value_log(ParsedListsMetaValue::LIST_META_VALUE_RPUSH, 1);
        parsed_lists_meta_value.ModifyLogIndex(1);
        batch.Put(handles_[2], lists_meta_key_log.Encode(), lists_meta_value_log.Encode());
        EncodeFixed32(buf, parsed_lists_meta_value.log_index());
        batch.Put(handles_[3], key, Slice(buf, sizeof(int32_t)));
      }
      batch.Put(handles_[1], lists_data_key.Encode(), value);
      *len = parsed_lists_meta_value.count();
      s = db_->Write(default_write_options_, &batch);
      if (s.ok()) {
          delete meta_info->second;
          meta_info->second = meta_value;
      } else {
          delete meta_value;
      }
      return s;
    }
  } else {
      s = Status::NotFound();
  }
  return s;
}

uint64_t get_time() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000000 + tv.tv_usec;
}
Status RedisLists::Expire(const Slice& key, int32_t ttl) {
  ScopeRecordLock l(lock_mgr_, key);
  Status s;
  std::unordered_map<std::string, std::string*>::iterator meta_info;
  meta_info = meta_infos_list_.find(key.data());
  if (meta_info != meta_infos_list_.end()) {
    ParsedListsMetaValue parsed_lists_meta_value(meta_info->second);
    if (parsed_lists_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    }
    uint32_t old_count = parsed_lists_meta_value.count();
    int32_t old_version = parsed_lists_meta_value.version();
    int32_t old_timestamp = parsed_lists_meta_value.timestamp();
    uint64_t old_index = parsed_lists_meta_value.index();
    if (ttl > 0) {
      parsed_lists_meta_value.SetRelativeTimestamp(ttl);
      s = db_->Put(default_write_options_, handles_[0], key, *meta_info->second);
      if (parsed_lists_meta_value.timestamp() != 0 ) {
        char str[sizeof(int32_t)+key.size() +1];
        EncodeFixed32(str,parsed_lists_meta_value.timestamp());
        memcpy(str + sizeof(int32_t) , key.data(),key.size());
        db_->Put(default_write_options_,handles_[4], {str,sizeof(int32_t)+key.size()}, "1" );
      }
    } else {
      parsed_lists_meta_value.InitialMetaValue();
      s = db_->Put(default_write_options_, handles_[0], key, *meta_info->second);
    }
    if (!s.ok()) {
        parsed_lists_meta_value.set_count(old_count);
        parsed_lists_meta_value.set_version(old_version);
        parsed_lists_meta_value.set_timestamp(old_timestamp);
        parsed_lists_meta_value.set_current_index(old_index);
    }
  } else {
      s = Status::NotFound();
  }
  return s;
}

Status RedisLists::Del(const Slice& key) {
  std::string *meta_value;
  Status s;
  ScopeRecordLock l(lock_mgr_, key);
  std::unordered_map<std::string, std::string*>::iterator meta_info =
      meta_infos_list_.find(key.data());
  if (meta_info != meta_infos_list_.end()) {
    meta_value = new std::string();
    meta_value->resize(meta_info->second->size());
    memcpy(const_cast<char *>(meta_value->data()), meta_info->second->data(), meta_info->second->size());
    ParsedListsMetaValue parsed_lists_meta_value(meta_value);
    if (parsed_lists_meta_value.IsStale()) {
      delete meta_value;
      return Status::NotFound("Stale");
    } else if (parsed_lists_meta_value.count() == 0) {
      delete meta_value;
      return Status::NotFound();
    } else {
      parsed_lists_meta_value.InitialMetaValue();
      s = db_->Put(default_write_options_, handles_[0], key, *meta_value);
    }
    if (s.ok()) {
        delete meta_info->second;
        meta_info->second = meta_value;
    } else {
        delete meta_value;
    }
  } else {
      s = Status::NotFound();
  }
  return s;
}

Status RedisLists::AddDelKey(BlackWidow *bw,const string & str){
  return bw->AddDelKey(db_,str,handles_[1]);
};

bool RedisLists::Scan(const std::string& start_key,
                      const std::string& pattern,
                      std::vector<std::string>* keys,
                      int64_t* count,
                      std::string* next_key) {
  std::string meta_key;
  bool is_finish = true;
  shannon::ReadOptions iterator_options;
  const shannon::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;

  shannon::Iterator* it = db_->NewIterator(iterator_options, handles_[0]);

  it->Seek(start_key);
  while (it->Valid() && (*count) > 0) {
    ParsedListsMetaValue parsed_lists_meta_value(it->value());
    if (parsed_lists_meta_value.IsStale()
      || parsed_lists_meta_value.count() == 0) {
      it->Next();
      continue;
    } else {
      meta_key = it->key().ToString();
      if (StringMatch(pattern.data(), pattern.size(),
                         meta_key.data(), meta_key.size(), 0)) {
        keys->push_back(meta_key);
      }
      (*count)--;
      it->Next();
    }
  }

  std::string prefix = isTailWildcard(pattern) ?
    pattern.substr(0, pattern.size() - 1) : "";
  if (it->Valid()
    && (it->key().compare(prefix) <= 0 || it->key().starts_with(prefix))) {
    *next_key = it->key().ToString();
    is_finish = false;
  } else {
    *next_key = "";
  }
  delete it;
  return is_finish;
}

Status RedisLists::Expireat(const Slice& key, int32_t timestamp) {
  ScopeRecordLock l(lock_mgr_, key);
  Status s;
  std::unordered_map<std::string, std::string*>::iterator meta_info = meta_infos_list_.find(key.data());
  if (meta_info != meta_infos_list_.end()) {
    ParsedListsMetaValue parsed_lists_meta_value(meta_info->second);
    if (parsed_lists_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else {
      int32_t old_timestamp = parsed_lists_meta_value.timestamp();
      parsed_lists_meta_value.set_timestamp(timestamp);
      if (parsed_lists_meta_value.timestamp() != 0 ) {
        char str[sizeof(int32_t)+key.size() +1];
        EncodeFixed32(str,parsed_lists_meta_value.timestamp());
        memcpy(str + sizeof(int32_t) , key.data(),key.size());
        db_->Put(default_write_options_,handles_[4], {str,sizeof(int32_t)+key.size()}, "1" );
      }
      s = db_->Put(default_write_options_, handles_[0], key, *meta_info->second);
      if (!s.ok()) {
          parsed_lists_meta_value.set_timestamp(old_timestamp);
      }
    }
  }
  return s;
}

Status RedisLists::Persist(const Slice& key) {
  ScopeRecordLock l(lock_mgr_, key);
  Status s;
  std::unordered_map<std::string, std::string*>::iterator meta_info = meta_infos_list_.find(key.data());
  if (meta_info != meta_infos_list_.end()) {
    ParsedListsMetaValue parsed_lists_meta_value(meta_info->second);
    if (parsed_lists_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else {
      int32_t old_timestamp = parsed_lists_meta_value.timestamp();
      if (old_timestamp == 0) {
        return Status::NotFound("Not have an associated timeout");
      } else {
        parsed_lists_meta_value.set_timestamp(0);
        s = db_->Put(default_write_options_, handles_[0], key, *meta_info->second);
        if (!s.ok()) {
            parsed_lists_meta_value.set_timestamp(old_timestamp);
        }
      }
    }
  } else {
      s = Status::NotFound();
  }
  return s;
}

Status RedisLists::TTL(const Slice& key, int64_t* timestamp) {
  Status s;
  std::unordered_map<std::string, std::string*>::iterator meta_info =
      meta_infos_list_.find(key.data());
  if (meta_info != meta_infos_list_.end()) {
    ParsedListsMetaValue parsed_lists_meta_value(meta_info->second);
    if (parsed_lists_meta_value.IsStale()) {
      *timestamp = -2;
      return Status::NotFound("Stale");
    } else {
      *timestamp = parsed_lists_meta_value.timestamp();
      if (*timestamp == 0) {
        *timestamp = -1;
      } else {
        int64_t curtime;
        shannon::Env::Default()->GetCurrentTime(&curtime);
        *timestamp = *timestamp - curtime > 0 ? *timestamp - curtime : -1;
      }
    }
  } else {
    *timestamp = -2;
    s = Status::NotFound();
  }
  return s;
}

void RedisLists::ScanDatabase() {

  shannon::ReadOptions iterator_options;
  const shannon::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;
  int32_t current_time = time(NULL);

  printf("\n***************List Meta Data***************\n");
  auto meta_iter = db_->NewIterator(iterator_options, handles_[0]);
  for (meta_iter->SeekToFirst();
       meta_iter->Valid();
       meta_iter->Next()) {
    ParsedListsMetaValue parsed_lists_meta_value(meta_iter->value());
    int32_t survival_time = 0;
    if (parsed_lists_meta_value.timestamp() != 0) {
      survival_time = parsed_lists_meta_value.timestamp() - current_time > 0 ?
        parsed_lists_meta_value.timestamp() - current_time : -1;
    }

    printf("[key : %-30s] [count : %-10lu] [left index : %-10lu] [right index : %-10lu] [timestamp : %-10d] [version : %d] [survival_time : %d]\n",
           meta_iter->key().ToString().c_str(),
           parsed_lists_meta_value.count(),
           reinterpret_cast<uint64_t>(0UL),
           reinterpret_cast<uint64_t>(0UL),
           // parsed_lists_meta_value.left_index(),
           // parsed_lists_meta_value.right_index(),
           parsed_lists_meta_value.timestamp(),
           parsed_lists_meta_value.version(),
           survival_time);
  }
  delete meta_iter;

  printf("\n***************List Node Data***************\n");
  auto data_iter = db_->NewIterator(iterator_options, handles_[1]);
  for (data_iter->SeekToFirst();
       data_iter->Valid();
       data_iter->Next()) {
    ParsedListsDataKey parsed_lists_data_key(data_iter->key());
    printf("[key : %-30s] [index : %-10lu] [data : %-20s] [version : %d]\n",
           parsed_lists_data_key.key().ToString().c_str(),
           parsed_lists_data_key.index(),
           data_iter->value().ToString().c_str(),
           parsed_lists_data_key.version());
  }
  delete data_iter;
}

Status RedisLists::DelTimeout(BlackWidow * bw,std::string * key) {
  Status s = Status::OK();
  shannon::Iterator *iter = db_->NewIterator(shannon::ReadOptions(), handles_[4]);
  iter->SeekToFirst();
  if (!iter->Valid()) {
    *key = "";
    delete iter;
    return s;
  }
  Slice slice_key = iter->key().data();
  int64_t cur_meta_timestamp_ = DecodeFixed32(slice_key.data());
  int64_t unix_time;
  shannon::Env::Default()->GetCurrentTime(&unix_time);
  if (cur_meta_timestamp_ > 0 && cur_meta_timestamp_ < static_cast<int32_t>(unix_time))
  {
   key->resize(iter->key().size()-sizeof(int32_t));
   memcpy(const_cast<char *>(key->data()),slice_key.data()+sizeof(int32_t),iter->key().size()-sizeof(int32_t));
    s = RealDelTimeout(bw,key);
    if (s.ok()) {
      s = db_->Delete(shannon::WriteOptions(), handles_[4], iter->key());
    }
  }
  else  *key = "";
  delete iter;
  return s;
}

Status RedisLists::RealDelTimeout(BlackWidow * bw,std::string * key) {
    Status s = Status::OK();
    ScopeRecordLock l(lock_mgr_, *key);
    std::string meta_value;
    std::unordered_map<std::string, std::string *>::iterator meta_info =
        meta_infos_list_.find(*key);
        cout << *key ;
    if (meta_info != meta_infos_list_.end()) {
      meta_value.resize(meta_info->second->size());
      memcpy(const_cast<char *>(meta_value.data()), meta_info->second->data(), meta_info->second->size());
      ParsedListsMetaValue parsed_lists_meta_value(&meta_value);
      int64_t unix_time;
      shannon::Env::Default()->GetCurrentTime(&unix_time);
      if (parsed_lists_meta_value.timestamp() < static_cast<int32_t>(unix_time))
      {
        AddDelKey(bw, *key);
        s = db_->Delete(shannon::WriteOptions(), handles_[0], *key);
        delete meta_info->second;
        meta_infos_list_.erase(*key);
      }
    }
    return s;
}
}   //  namespace blackwidow

