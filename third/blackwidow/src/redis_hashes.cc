//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "src/redis_hashes.h"

#include <memory>

#include "blackwidow/util.h"
#include "src/base_filter.h"
#include "src/scope_record_lock.h"
#include "src/scope_snapshot.h"

namespace blackwidow {
std::unordered_map<std::string, std::string*> meta_infos_hashes_;
RedisHashes::~RedisHashes() {
  std::vector<shannon::ColumnFamilyHandle*> tmp_handles = handles_;
  handles_.clear();
  for (auto handle : tmp_handles) {
    delete handle;
  }

  for (std::unordered_map<std::string, std::string*>::iterator iter =
        meta_infos_hashes_.begin();
        iter != meta_infos_hashes_.end();
        ++ iter) {
      delete iter->second;
  }
  meta_infos_hashes_.clear();
}

Status RedisHashes::Open(const BlackwidowOptions& bw_options,
                         const std::string& db_path) {
  shannon::Options ops(bw_options.options);
  Status s = shannon::DB::Open(ops, db_path, default_device_name_, &db_);
  if (s.ok()) {
    // create column family
    shannon::ColumnFamilyHandle* cf;
    shannon::ColumnFamilyHandle* tcf;
    s = db_->CreateColumnFamily(shannon::ColumnFamilyOptions(),
        "data_cf", &cf);
    if (!s.ok()) {
      return s;
    }
    s = db_->CreateColumnFamily(shannon::ColumnFamilyOptions(), "timeout_cf", &tcf);
    if (!s.ok()) {
      return s;
    }
    // close DB
    delete tcf;
    delete cf;
    delete db_;
  }

  // Open
  shannon::DBOptions db_ops(bw_options.options);
  shannon::ColumnFamilyOptions meta_cf_ops(bw_options.options);
  shannon::ColumnFamilyOptions data_cf_ops(bw_options.options);
  shannon::ColumnFamilyOptions timeout_cf_ops(bw_options.options);
  meta_cf_ops.compaction_filter_factory =
    std::make_shared<HashesMetaFilterFactory>();
  data_cf_ops.compaction_filter_factory =
    std::make_shared<HashesDataFilterFactory>(&db_, &handles_);

  //use the bloom filter policy to reduce disk reads
  shannon::BlockBasedTableOptions table_ops(bw_options.table_options);
  table_ops.filter_policy.reset(shannon::NewBloomFilterPolicy(10, true));
  shannon::BlockBasedTableOptions meta_cf_table_ops(table_ops);
  shannon::BlockBasedTableOptions data_cf_table_ops(table_ops);
  shannon::BlockBasedTableOptions timeout_cf_table_ops(table_ops);
  if (!bw_options.share_block_cache && bw_options.block_cache_size > 0) {
    meta_cf_table_ops.block_cache = shannon::NewLRUCache(bw_options.block_cache_size);
    data_cf_table_ops.block_cache = shannon::NewLRUCache(bw_options.block_cache_size);
    timeout_cf_table_ops.block_cache = shannon::NewLRUCache(bw_options.block_cache_size);
  }
  meta_cf_ops.table_factory.reset(shannon::NewBlockBasedTableFactory(meta_cf_table_ops));
  data_cf_ops.table_factory.reset(shannon::NewBlockBasedTableFactory(data_cf_table_ops));
  timeout_cf_ops.table_factory.reset(shannon::NewBlockBasedTableFactory(timeout_cf_table_ops));
  std::vector<shannon::ColumnFamilyDescriptor> column_families;
  // Meta CF
  column_families.push_back(shannon::ColumnFamilyDescriptor(
      shannon::kDefaultColumnFamilyName, meta_cf_ops));
  // Data CF
  column_families.push_back(shannon::ColumnFamilyDescriptor(
      "data_cf", data_cf_ops));
  column_families.push_back(shannon::ColumnFamilyDescriptor(
      "timeout_cf", shannon::ColumnFamilyOptions()));
  s = shannon::DB::Open(db_ops, db_path, default_device_name_, column_families, &handles_, &db_);
  if (s.ok()) {
      shannon::Iterator* iter = db_->NewIterator(shannon::ReadOptions(), handles_[0]);
      for (iter->SeekToFirst();
           iter->Valid();
           iter->Next()) {
          std::string *meta_value = new std::string();
          Slice slice_key = iter->key();
          Slice slice_value = iter->value();
          meta_value->resize(slice_value.size());
          memcpy(const_cast<char *>(meta_value->data()), slice_value.data(), slice_value.size());
          meta_infos_hashes_.insert(make_pair(slice_key.data(), meta_value));
      }
      delete iter;
  }
  return s;
}

Status RedisHashes::CompactRange(const shannon::Slice* begin,
                                 const shannon::Slice* end) {
  Status s = db_->CompactRange(default_compact_range_options_,
      handles_[0], begin, end);
  if (!s.ok()) {
    return s;
  }
  return db_->CompactRange(default_compact_range_options_,
      handles_[1], begin, end);
}

Status RedisHashes::GetProperty(const std::string& property, uint64_t* out) {
  std::string value;
  db_->GetProperty(handles_[0], property, &value);
  *out = std::strtoull(value.c_str(), NULL, 10);
  db_->GetProperty(handles_[1], property, &value);
  *out += std::strtoull(value.c_str(), NULL, 10);
  return Status::OK();
}

Status RedisHashes::ScanKeyNum(uint64_t* num) {

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
    ParsedHashesMetaValue parsed_hashes_meta_value(iter->value());
    if (!parsed_hashes_meta_value.IsStale()
      && parsed_hashes_meta_value.count() != 0) {
      count++;
    }
  }
  *num = count;
  delete iter;
  return Status::OK();
}

Status RedisHashes::ScanKeys(const std::string& pattern,
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
    ParsedHashesMetaValue parsed_hashes_meta_value(iter->value());
    if (!parsed_hashes_meta_value.IsStale()
      && parsed_hashes_meta_value.count() != 0) {
      key = iter->key().ToString();
      if (StringMatch(pattern.data(), pattern.size(), key.data(), key.size(), 0)) {
        keys->push_back(key);
      }
    }
  }
  delete iter;
  return Status::OK();
}

Status RedisHashes::HDel(const Slice& key,
                         const std::vector<std::string>& fields,
                         int32_t* ret) {
  std::vector<std::string> filtered_fields;
  std::unordered_set<std::string> field_set;
  for (auto iter = fields.begin(); iter != fields.end(); ++iter) {
    std::string field = *iter;
    if (field_set.find(field) == field_set.end()) {
      field_set.insert(field);
      filtered_fields.push_back(*iter);
    }
  }

  shannon::WriteBatch batch;
  shannon::ReadOptions read_options;
  const shannon::Snapshot* snapshot;

  std::string *meta_value;
  int32_t del_cnt = 0;
  int32_t version = 0;
  ScopeRecordLock l(lock_mgr_, key);
  Status s;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  std::unordered_map<std::string, std::string*>::iterator meta_info =
      meta_infos_hashes_.find(key.data());
  if (meta_info != meta_infos_hashes_.end()) {
    meta_value = new std::string();
    meta_value->resize(meta_info->second->size());
    memcpy(const_cast<char *>(meta_value->data()), meta_info->second->data(), meta_info->second->size());
    ParsedHashesMetaValue parsed_hashes_meta_value(meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      *ret = 0;
      delete meta_value;
      return Status::OK();
    } else {
      std::string data_value;
      version = parsed_hashes_meta_value.version();
      int32_t hlen = parsed_hashes_meta_value.count();
      for (const auto& field : filtered_fields) {
        HashesDataKey hashes_data_key(key, version, field);
        s = db_->Get(read_options, handles_[1],
                hashes_data_key.Encode(), &data_value);
        if (s.ok()) {
          del_cnt++;
          batch.Delete(handles_[1], hashes_data_key.Encode());
        } else if (s.IsNotFound()) {
          continue;
        } else {
          delete meta_value;
          return s;
        }
      }
      *ret = del_cnt;
      hlen -= del_cnt;
      parsed_hashes_meta_value.set_count(hlen);
      batch.Put(handles_[0], key, *meta_value);
    }
  } else {
    *ret = 0;
    return Status::OK();
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

Status RedisHashes::HExists(const Slice& key, const Slice& field) {
  std::string value;
  return HGet(key, field, &value);
}

Status RedisHashes::HGet(const Slice& key, const Slice& field,
                         std::string* value) {
  std::string meta_value;
  int32_t version = 0;
  shannon::ReadOptions read_options;
  const shannon::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  Status s;
  std::unordered_map<std::string, std::string*>::iterator meta_info =
      meta_infos_hashes_.find(key.data());
  if (meta_info != meta_infos_hashes_.end()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(meta_info->second);
    if (parsed_hashes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else {
      version = parsed_hashes_meta_value.version();
      HashesDataKey data_key(key, version, field);
      s = db_->Get(read_options, handles_[1], data_key.Encode(), value);
    }
  } else {
      s = Status::NotFound();
  }
  return s;
}

Status RedisHashes::HGetall(const Slice& key,
                            std::vector<FieldValue>* fvs) {
  shannon::ReadOptions read_options;
  const shannon::Snapshot* snapshot;

  std::string meta_value;
  int32_t version = 0;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  Status s = db_->Get(read_options, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else {
      version = parsed_hashes_meta_value.version();
      HashesDataKey hashes_data_key(key, version, "");
      Slice prefix = hashes_data_key.Encode();
      auto iter = db_->NewIterator(read_options, handles_[1]);
      for (iter->Seek(prefix);
           iter->Valid() && iter->key().starts_with(prefix);
           iter->Next()) {
        ParsedHashesDataKey parsed_hashes_data_key(iter->key());
        fvs->push_back({parsed_hashes_data_key.field().ToString(),
                iter->value().ToString()});
      }
      delete iter;
    }
  }
  return s;
}

Status RedisHashes::HIncrby(const Slice& key, const Slice& field, int64_t value,
                            int64_t* ret) {
  *ret = 0;
  shannon::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);

  int32_t version = 0;
  std::string old_value;
  std::string *meta_value;
  Status s;
  std::unordered_map<std::string, std::string*>::iterator meta_info =
      meta_infos_hashes_.find(key.data());

  if (meta_info != meta_infos_hashes_.end()) {
    meta_value = new std::string();
    meta_value->resize(meta_info->second->size());
    memcpy(const_cast<char *>(meta_value->data()), meta_info->second->data(), meta_info->second->size());
    ParsedHashesMetaValue parsed_hashes_meta_value(meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      version = parsed_hashes_meta_value.UpdateVersion();
      parsed_hashes_meta_value.set_count(1);
      parsed_hashes_meta_value.set_timestamp(0);
      batch.Put(handles_[0], key, *meta_value);
      HashesDataKey hashes_data_key(key, version, field);
      char buf[32];
      Int64ToStr(buf, 32, value);
      batch.Put(handles_[1], hashes_data_key.Encode(), buf);
      *ret = value;
    } else {
      version = parsed_hashes_meta_value.version();
      HashesDataKey hashes_data_key(key, version, field);
      s = db_->Get(default_read_options_, handles_[1],
              hashes_data_key.Encode(), &old_value);
      if (s.ok()) {
        int64_t ival = 0;
        if (!StrToInt64(old_value.data(), old_value.size(), &ival)) {
          return Status::Corruption("hash value is not an integer");
        }
        if ((value >= 0 && LLONG_MAX - value < ival) ||
          (value < 0 && LLONG_MIN - value > ival)) {
          return Status::InvalidArgument("Overflow");
        }
        *ret = ival + value;
        char buf[32];
        Int64ToStr(buf, 32, *ret);
        batch.Put(handles_[1], hashes_data_key.Encode(), buf);
      } else if (s.IsNotFound()) {
        char buf[32];
        Int64ToStr(buf, 32, value);
        parsed_hashes_meta_value.ModifyCount(1);
        batch.Put(handles_[0], key, *meta_value);
        batch.Put(handles_[1], hashes_data_key.Encode(), buf);
        *ret = value;
      } else {
        return s;
      }
    }
  } else {
    meta_value = new std::string();
    meta_value->resize(12);
    EncodeFixed32(const_cast<char *>(meta_value->data()), 1);
    HashesMetaValue hashes_meta_value(std::string(meta_value->data(), meta_value->size()));
    version = hashes_meta_value.UpdateVersion();
    batch.Put(handles_[0], key, hashes_meta_value.Encode());
    HashesDataKey hashes_data_key(key, version, field);

    char buf[32];
    Int64ToStr(buf, 32, value);
    batch.Put(handles_[1], hashes_data_key.Encode(), buf);
    *ret = value;
  }
  s = db_->Write(default_write_options_, &batch);
  if (s.ok()) {
    if (meta_info != meta_infos_hashes_.end()) {
        delete meta_info->second;
        meta_info->second = meta_value;
    } else {
        meta_infos_hashes_.insert(make_pair(key.data(), meta_value));
    }
  } else {
    delete meta_value;
  }
  return s;
}

Status RedisHashes::HIncrbyfloat(const Slice& key, const Slice& field,
                                 const Slice& by, std::string* new_value) {
  new_value->clear();
  shannon::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);

  int32_t version = 0;
  std::string *meta_value;
  std::string old_value_str;
  Status s;
  long double long_double_by;

  if (StrToLongDouble(by.data(), by.size(), &long_double_by) == -1) {
    return Status::Corruption("value is not a vaild float");
  }

  std::unordered_map<std::string, std::string*>::iterator meta_info =
      meta_infos_hashes_.find(key.data());
  if (meta_info != meta_infos_hashes_.end()) {
    meta_value = new std::string();
    meta_value->resize(meta_info->second->size());
    ParsedHashesMetaValue parsed_hashes_meta_value(meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      version = parsed_hashes_meta_value.UpdateVersion();
      parsed_hashes_meta_value.set_count(1);
      parsed_hashes_meta_value.set_timestamp(0);
      batch.Put(handles_[0], key, *meta_value);
      HashesDataKey hashes_data_key(key, version, field);

      LongDoubleToStr(long_double_by, new_value);
      batch.Put(handles_[1], hashes_data_key.Encode(), *new_value);
    } else {
      version = parsed_hashes_meta_value.version();
      HashesDataKey hashes_data_key(key, version, field);
      s = db_->Get(default_read_options_, handles_[1],
              hashes_data_key.Encode(), &old_value_str);
      if (s.ok()) {
        long double total;
        long double old_value;
        if (StrToLongDouble(old_value_str.data(),
                    old_value_str.size(), &old_value) == -1) {
          return Status::Corruption("value is not a vaild float");
        }

        total = old_value + long_double_by;
        if (LongDoubleToStr(total, new_value) == -1) {
          return Status::InvalidArgument("Overflow");
        }
        batch.Put(handles_[1], hashes_data_key.Encode(), *new_value);
      } else if (s.IsNotFound()) {
        LongDoubleToStr(long_double_by, new_value);
        parsed_hashes_meta_value.ModifyCount(1);
        batch.Put(handles_[0], key, *meta_value);
        batch.Put(handles_[1], hashes_data_key.Encode(), *new_value);
      } else {
        return s;
      }
    }
  } else {
    meta_value = new std::string();
    meta_value->resize(12);
    EncodeFixed32(const_cast<char *>(meta_value->data()), 1);
    HashesMetaValue hashes_meta_value(std::string(meta_value->data(), meta_value->size()));
    version = hashes_meta_value.UpdateVersion();
    batch.Put(handles_[0], key, hashes_meta_value.Encode());

    HashesDataKey hashes_data_key(key, version, field);
    LongDoubleToStr(long_double_by, new_value);
    batch.Put(handles_[1], hashes_data_key.Encode(), *new_value);
  }
  s = db_->Write(default_write_options_, &batch);
  if (s.ok()) {
      if (meta_info != meta_infos_hashes_.end()) {
          delete meta_info->second;
          meta_info->second = meta_value;
      } else {
          meta_infos_hashes_.insert(make_pair(key.data(), meta_value));
      }
  } else {
      delete meta_value;
  }
  return s;
}

Status RedisHashes::HKeys(const Slice& key,
                          std::vector<std::string>* fields) {
  shannon::ReadOptions read_options;
  const shannon::Snapshot* snapshot;

  std::string meta_value;
  int32_t version = 0;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  Status s;
  std::unordered_map<std::string, std::string*>::iterator meta_info =
      meta_infos_hashes_.find(key.data());
  if (meta_info != meta_infos_hashes_.end()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(meta_info->second);
    if (parsed_hashes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else {
      version = parsed_hashes_meta_value.version();
      HashesDataKey hashes_data_key(key, version, "");
      Slice prefix = hashes_data_key.Encode();
      auto iter = db_->NewIterator(read_options, handles_[1]);
      for (iter->Seek(prefix);
           iter->Valid() && iter->key().starts_with(prefix);
           iter->Next()) {
        ParsedHashesDataKey parsed_hashes_data_key(iter->key());
        fields->push_back(parsed_hashes_data_key.field().ToString());
      }
      delete iter;
    }
  } else {
      s = Status::NotFound();
  }
  return s;
}

Status RedisHashes::HLen(const Slice& key, int32_t* ret) {
  *ret = 0;
  std::string meta_value;
  std::unordered_map<std::string, std::string*>::iterator meta_info =
      meta_infos_hashes_.find(key.data());
  Status s;
  if (meta_info != meta_infos_hashes_.end()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(meta_info->second);
    if (parsed_hashes_meta_value.IsStale()) {
      *ret = 0;
      return Status::NotFound("Stale");
    } else if (parsed_hashes_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      *ret = parsed_hashes_meta_value.count();
    }
  } else {
    *ret = 0;
    s = Status::NotFound();
  }
  return s;
}

Status RedisHashes::HMGet(const Slice& key,
                          const std::vector<std::string>& fields,
                          std::vector<std::string>* values) {
  int32_t version = 0;
  std::string value;
  std::string meta_value;
  shannon::ReadOptions read_options;
  const shannon::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  Status s;
  std::unordered_map<std::string, std::string*>::iterator meta_info =
      meta_infos_hashes_.find(key.data());
  if (meta_info != meta_infos_hashes_.end()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(meta_info->second);
    if (parsed_hashes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else {
      version = parsed_hashes_meta_value.version();
      for (const auto& field : fields) {
        HashesDataKey hashes_data_key(key, version, field);
        s = db_->Get(read_options, handles_[1],
                hashes_data_key.Encode(), &value);
        if (s.ok()) {
          values->push_back(value);
        } else if (s.IsNotFound()) {
          values->push_back("");
        } else {
          return s;
        }
      }
    }
  } else {
    for (size_t idx = 0; idx < fields.size(); ++idx) {
      values->push_back("");
    }
    return Status::NotFound();
  }
  return Status::OK();
}

Status RedisHashes::HMSet(const Slice& key,
                          const std::vector<FieldValue>& fvs) {
  std::unordered_set<std::string> fields;
  std::vector<FieldValue> filtered_fvs;
  for (auto iter = fvs.rbegin(); iter != fvs.rend(); ++iter) {
    std::string field = iter->field;
    if (fields.find(field) == fields.end()) {
      fields.insert(field);
      filtered_fvs.push_back(*iter);
    }
  }

  shannon::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);

  int32_t version = 0;
  std::string *meta_value = NULL;
  Status s;
  std::unordered_map<std::string, std::string*>::iterator meta_info =
      meta_infos_hashes_.find(key.data());
  if (meta_info != meta_infos_hashes_.end()) {
    meta_value = new std::string();
    meta_value->resize(meta_info->second->size());
    memcpy(const_cast<char *>(meta_value->data()), meta_info->second->data(), meta_info->second->size());
    ParsedHashesMetaValue parsed_hashes_meta_value(meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      version = parsed_hashes_meta_value.UpdateVersion();
      parsed_hashes_meta_value.set_count(filtered_fvs.size());
      parsed_hashes_meta_value.set_timestamp(0);
      batch.Put(handles_[0], key, *meta_value);
      for (const auto& fv : filtered_fvs) {
        HashesDataKey hashes_data_key(key, version, fv.field);
        batch.Put(handles_[1], hashes_data_key.Encode(), fv.value);
      }
    } else {
      int32_t count = 0;
      std::string data_value;
      version = parsed_hashes_meta_value.version();
      for (const auto& fv : filtered_fvs) {
        HashesDataKey hashes_data_key(key, version, fv.field);
        s = db_->Get(default_read_options_, handles_[1],
                hashes_data_key.Encode(), &data_value);
        if (s.ok()) {
          batch.Put(handles_[1], hashes_data_key.Encode(), fv.value);
        } else if (s.IsNotFound()) {
          count++;
          batch.Put(handles_[1], hashes_data_key.Encode(), fv.value);
        } else {
          delete meta_value;
          return s;
        }
      }
      parsed_hashes_meta_value.ModifyCount(count);
      batch.Put(handles_[0], key, *meta_value);
    }
  } else {
    meta_value = new std::string();
    meta_value->resize(12);
    EncodeFixed32(const_cast<char*>(meta_value->data()), filtered_fvs.size());
    HashesMetaValue hashes_meta_value(*meta_value);
    version = hashes_meta_value.UpdateVersion();
    batch.Put(handles_[0], key, hashes_meta_value.Encode());
    for (const auto& fv : filtered_fvs) {
      HashesDataKey hashes_data_key(key, version, fv.field);
      batch.Put(handles_[1], hashes_data_key.Encode(), fv.value);
    }
  }
  s = db_->Write(default_write_options_, &batch);
  if (s.ok()) {
    if (meta_info != meta_infos_hashes_.end()) {
        delete meta_info->second;
        meta_info->second = meta_value;
    } else {
        meta_infos_hashes_.insert(make_pair(key.data(), meta_value));
    }
  } else {
      delete meta_value;
  }
  return s;
}

Status RedisHashes::HSet(const Slice& key, const Slice& field,
                         const Slice& value, int32_t* res) {
  shannon::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);

  int32_t version = 0;
  std::string *meta_value;
  std::unordered_map<std::string, std::string*>::iterator meta_info =
      meta_infos_hashes_.find(key.data());
  Status s;
  if (meta_info != meta_infos_hashes_.end()) {
    meta_value = new std::string();
    meta_value->resize(meta_info->second->size());
    memcpy(const_cast<char *>(meta_value->data()), meta_info->second->data(), meta_info->second->size());
    ParsedHashesMetaValue parsed_hashes_meta_value(meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      version = parsed_hashes_meta_value.InitialMetaValue();
      parsed_hashes_meta_value.ModifyCount(1);
      batch.Put(handles_[0], key, *meta_value);
      HashesDataKey data_key(key, version, field);
      batch.Put(handles_[1], data_key.Encode(), value);
      *res = 1;
      if (parsed_hashes_meta_value.timestamp() != 0 ) {
        char str[sizeof(int32_t)+key.size() +1];
        str[sizeof(int32_t)+key.size() ] = '\0';
        EncodeFixed32(str,parsed_hashes_meta_value.timestamp());
        memcpy(str + sizeof(int32_t) , key.data(),key.size());
        batch.Put(handles_[2], {str,sizeof(int32_t)+key.size()}, "1" );
      }
    } else {
      version = parsed_hashes_meta_value.version();
      std::string data_value;
      HashesDataKey hashes_data_key(key, version, field);
      s = db_->Get(default_read_options_, handles_[1], hashes_data_key.Encode(), &data_value);
      if (s.ok()) {
        *res = 0;
        if (data_value == value.ToString()) {
          delete meta_value;
          return Status::OK();
        } else {
          batch.Put(handles_[1], hashes_data_key.Encode(), value);
        }
      } else if (s.IsNotFound()) {
        parsed_hashes_meta_value.ModifyCount(1);
        if (parsed_hashes_meta_value.timestamp() != 0 ) {
          char str[sizeof(int32_t)+key.size() +1];
          str[sizeof(int32_t)+key.size() ] = '\0';
          EncodeFixed32(str,parsed_hashes_meta_value.timestamp());
          memcpy(str + sizeof(int32_t) , key.data(),key.size());
          batch.Put(handles_[2], {str,sizeof(int32_t)+key.size()}, "1" );
        }
        batch.Put(handles_[0], key, *meta_value);
        batch.Put(handles_[1], hashes_data_key.Encode(), value);
        *res = 1;
      } else {
        delete meta_value;
        return s;
      }
    }
  } else {
    meta_value = new std::string();
    meta_value->resize(12);
    EncodeFixed32(const_cast<char *>(meta_value->data()), 1);
    HashesMetaValue meta_value_destion(*meta_value);
    version = meta_value_destion.UpdateVersion();
    meta_value_destion.set_timestamp(0);
    batch.Put(handles_[0], key, meta_value_destion.Encode());
    HashesDataKey data_key(key, version, field);
    batch.Put(handles_[1], data_key.Encode(), value);
    *res = 1;
  }

  s = db_->Write(default_write_options_, &batch);
  if (s.ok()) {
      if (meta_info != meta_infos_hashes_.end()) {
          delete meta_info->second;
          meta_info->second = meta_value;
      } else {
          meta_infos_hashes_.insert(make_pair(key.data(), meta_value));
      }
  } else {
      delete meta_value;
  }
  return s;
}

Status RedisHashes::HSetnx(const Slice& key, const Slice& field,
                           const Slice& value, int32_t* ret) {
  shannon::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);

  int32_t version = 0;
  std::string *meta_value;
  Status s;
  std::unordered_map<std::string, std::string*>::iterator meta_info =
      meta_infos_hashes_.find(key.data());
  if (meta_info != meta_infos_hashes_.end()) {
    meta_value = new std::string();
    meta_value->resize(meta_info->second->size());
    memcpy(const_cast<char*>(meta_value->data()), meta_info->second->data(), meta_info->second->size());
    ParsedHashesMetaValue parsed_hashes_meta_value(meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      version = parsed_hashes_meta_value.UpdateVersion();
      parsed_hashes_meta_value.set_count(1);
      parsed_hashes_meta_value.set_timestamp(0);
      batch.Put(handles_[0], key, *meta_value);
      HashesDataKey hashes_data_key(key, version, field);
      batch.Put(handles_[1], hashes_data_key.Encode(), value);
      *ret = 1;
    } else {
      version = parsed_hashes_meta_value.version();
      HashesDataKey hashes_data_key(key, version, field);
      std::string data_value;
      s = db_->Get(default_read_options_, handles_[1],
              hashes_data_key.Encode(), &data_value);
      if (s.ok()) {
        *ret = 0;
      } else if (s.IsNotFound()) {
        parsed_hashes_meta_value.ModifyCount(1);
        batch.Put(handles_[0], key, *meta_value);
        batch.Put(handles_[1], hashes_data_key.Encode(), value);
        *ret = 1;
      } else {
        delete meta_value;
        return s;
      }
    }
  } else {
    meta_value = new std::string();
    meta_value->resize(12);
    EncodeFixed32(const_cast<char *>(meta_value->data()), 1);
    HashesMetaValue hashes_meta_value(*meta_value);
    version = hashes_meta_value.UpdateVersion();
    batch.Put(handles_[0], key, hashes_meta_value.Encode());
    HashesDataKey hashes_data_key(key, version, field);
    batch.Put(handles_[1], hashes_data_key.Encode(), value);
    *ret = 1;
  }
  s = db_->Write(default_write_options_, &batch);
  if (s.ok()) {
      if (meta_info != meta_infos_hashes_.end()) {
          delete meta_info->second;
          meta_info->second = meta_value;
      } else {
          meta_infos_hashes_.insert(make_pair(key.data(), meta_value));
          std::cout<<"key:"<<key.ToString()<<std::endl;
      }
  } else {
      delete meta_value;
  }
  return s;
}

Status RedisHashes::HVals(const Slice& key,
                          std::vector<std::string>* values) {
  shannon::ReadOptions read_options;
  const shannon::Snapshot* snapshot;

  std::string meta_value;
  int32_t version = 0;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  Status s;
  std::unordered_map<std::string, std::string*>::iterator meta_info =
      meta_infos_hashes_.find(key.data());
  if (meta_info != meta_infos_hashes_.end()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else {
      version = parsed_hashes_meta_value.version();
      HashesDataKey hashes_data_key(key, version, "");
      Slice prefix = hashes_data_key.Encode();
      auto iter = db_->NewIterator(read_options, handles_[1]);
      for (iter->Seek(prefix);
           iter->Valid() && iter->key().starts_with(prefix);
           iter->Next()) {
        values->push_back(iter->value().ToString());
      }
      delete iter;
    }
  }
  return s;
}

Status RedisHashes::HStrlen(const Slice& key,
                            const Slice& field, int32_t* len) {
  std::string value;
  Status s = HGet(key, field, &value);
  if (s.ok()) {
    *len = value.size();
  } else {
    *len = 0;
  }
  return s;
}

Status RedisHashes::HScan(const Slice& key, int64_t cursor, const std::string& pattern,
                          int64_t count, std::vector<FieldValue>* field_values, int64_t* next_cursor) {
  *next_cursor = 0;
  field_values->clear();
  if (cursor < 0) {
    *next_cursor = 0;
    return Status::OK();
  }

  int64_t rest = count;
  int64_t step_length = count;
  shannon::ReadOptions read_options;
  const shannon::Snapshot* snapshot;

  std::string meta_value;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  Status s;
  std::unordered_map<std::string, std::string*>::iterator meta_info =
      meta_infos_hashes_.find(key.data());
  if (meta_info != meta_infos_hashes_.end()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(meta_info->second);
    if (parsed_hashes_meta_value.IsStale()
      || parsed_hashes_meta_value.count() == 0) {
      *next_cursor = 0;
      return Status::NotFound();
    } else {
      std::string sub_field;
      std::string start_point;
      int32_t version = parsed_hashes_meta_value.version();
      s = GetScanStartPoint(key, pattern, cursor, &start_point);
      if (s.IsNotFound()) {
        cursor = 0;
        if (isTailWildcard(pattern)) {
          start_point = pattern.substr(0, pattern.size() - 1);
        }
      }
      if (isTailWildcard(pattern)) {
        sub_field = pattern.substr(0, pattern.size() - 1);
      }

      HashesDataKey hashes_data_prefix(key, version, sub_field);
      HashesDataKey hashes_start_data_key(key, version, start_point);
      std::string prefix = hashes_data_prefix.Encode().ToString();
      shannon::Iterator* iter = db_->NewIterator(read_options, handles_[1]);
      for (iter->Seek(hashes_start_data_key.Encode());
           iter->Valid() && rest > 0 && iter->key().starts_with(prefix);
           iter->Next()) {
        ParsedHashesDataKey parsed_hashes_data_key(iter->key());
        std::string field = parsed_hashes_data_key.field().ToString();
        if (StringMatch(pattern.data(), pattern.size(), field.data(), field.size(), 0)) {
          field_values->push_back({field, iter->value().ToString()});
        }
        rest--;
      }

      if (iter->Valid()
        && (iter->key().compare(prefix) <= 0 || iter->key().starts_with(prefix))) {
        *next_cursor = cursor + step_length;
        ParsedHashesDataKey parsed_hashes_data_key(iter->key());
        std::string next_field = parsed_hashes_data_key.field().ToString();
        StoreScanNextPoint(key, pattern, *next_cursor, next_field);
      } else {
        *next_cursor = 0;
      }
      delete iter;
    }
  } else {
    *next_cursor = 0;
    return s;
  }
  return Status::OK();
}

Status RedisHashes::HScanx(const Slice& key, const std::string start_field, const std::string& pattern,
                           int64_t count, std::vector<FieldValue>* field_values, std::string* next_field) {
  next_field->clear();
  field_values->clear();

  int64_t rest = count;
  std::string meta_value;
  shannon::ReadOptions read_options;
  const shannon::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  Status s = db_->Get(read_options, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()
      || parsed_hashes_meta_value.count() == 0) {
      *next_field = "";
      return Status::NotFound();
    } else {
      int32_t version = parsed_hashes_meta_value.version();
      HashesDataKey hashes_data_prefix(key, version, Slice());
      HashesDataKey hashes_start_data_key(key, version, start_field);
      std::string prefix = hashes_data_prefix.Encode().ToString();
      shannon::Iterator* iter = db_->NewIterator(read_options, handles_[1]);
      for (iter->Seek(hashes_start_data_key.Encode());
           iter->Valid() && rest > 0 && iter->key().starts_with(prefix);
           iter->Next()) {
        ParsedHashesDataKey parsed_hashes_data_key(iter->key());
        std::string field = parsed_hashes_data_key.field().ToString();
        if (StringMatch(pattern.data(), pattern.size(), field.data(), field.size(), 0)) {
          field_values->push_back({field, iter->value().ToString()});
        }
        rest--;
      }

      if (iter->Valid() && iter->key().starts_with(prefix)) {
        ParsedHashesDataKey parsed_hashes_data_key(iter->key());
        *next_field = parsed_hashes_data_key.field().ToString();
      } else {
        *next_field = "";
      }
      delete iter;
    }
  } else {
    *next_field = "";
    return s;
  }
  return Status::OK();
}

Status RedisHashes::Expire(const Slice& key, int32_t ttl) {
  std::string *meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s;
  std::unordered_map<std::string, std::string*>::iterator meta_info =
      meta_infos_hashes_.find(key.data());
  if (meta_info != meta_infos_hashes_.end()) {
    meta_value = new std::string();
    meta_value->resize(meta_info->second->size());
    memcpy(const_cast<char *>(meta_value->data()), meta_info->second->data(), meta_info->second->size());
    ParsedHashesMetaValue parsed_hashes_meta_value(meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    }
    if (ttl > 0) {
      parsed_hashes_meta_value.SetRelativeTimestamp(ttl);
      s = db_->Put(default_write_options_, handles_[0], key, *meta_value);
      if (parsed_hashes_meta_value.timestamp() != 0 ) {
        char str[sizeof(int32_t)+key.size() +1];
        str[sizeof(int32_t)+key.size() ] = '\0';
        EncodeFixed32(str,parsed_hashes_meta_value.timestamp());
        memcpy(str + sizeof(int32_t) , key.data(),key.size());
       db_->Put(default_write_options_,handles_[2], {str,sizeof(int32_t)+key.size()}, "1" );
      }
    } else {
      parsed_hashes_meta_value.set_count(0);
      parsed_hashes_meta_value.UpdateVersion();
      parsed_hashes_meta_value.set_timestamp(0);
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

Status RedisHashes::Del(const Slice& key) {
  std::string *meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s;
  std::unordered_map<std::string, std::string*>::iterator meta_info =
      meta_infos_hashes_.find(key.data());

  if (meta_info != meta_infos_hashes_.end()) {
    meta_value = new std::string();
    meta_value->resize(meta_info->second->size());
    memcpy(const_cast<char *>(meta_value->data()), meta_info->second->data(), meta_info->second->size());
    ParsedHashesMetaValue parsed_hashes_meta_value(meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      delete meta_value;
      return Status::NotFound("Stale");
    } else if (parsed_hashes_meta_value.count() == 0) {
      delete meta_value;
      return Status::NotFound();
    } else {
      parsed_hashes_meta_value.InitialMetaValue();
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

Status RedisHashes::AddDelKey(BlackWidow * bw,const string & str) {
  return bw->AddDelKey(db_,str,handles_[1]);
};

bool RedisHashes::Scan(const std::string& start_key,
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
    ParsedHashesMetaValue parsed_meta_value(it->value());
    if (parsed_meta_value.IsStale()
      || parsed_meta_value.count() == 0) {
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

Status RedisHashes::Expireat(const Slice& key, int32_t timestamp) {
  std::string *meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s;
  std::unordered_map<std::string, std::string*>::iterator meta_info = meta_infos_hashes_.find(key.data());
  if (meta_info != meta_infos_hashes_.end()) {
    meta_value = new std::string();
    meta_value->resize(meta_info->second->size());
    memcpy(const_cast<char *>(meta_value->data()), meta_info->second->data(), meta_info->second->size());
    ParsedHashesMetaValue parsed_hashes_meta_value(meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      delete meta_value;
      return Status::NotFound("Stale");
    } else {
      parsed_hashes_meta_value.set_timestamp(timestamp);
      if (parsed_hashes_meta_value.timestamp() != 0 ) {
        char str[sizeof(int32_t)+key.size() +1];
        str[sizeof(int32_t)+key.size() ] = '\0';
        EncodeFixed32(str,parsed_hashes_meta_value.timestamp());
        memcpy(str + sizeof(int32_t) , key.data(),key.size());
       db_->Put(default_write_options_,handles_[2], {str,sizeof(int32_t)+key.size()}, "1" );
      }
      s = db_->Put(default_write_options_, handles_[0], key, *meta_value);
    }
    if (s.ok()) {
        delete meta_info->second;
        meta_info->second = meta_value;
    } else {
        delete meta_value;
    }
  } else {
      return Status::NotFound();
  }
  return s;
}

Status RedisHashes::Persist(const Slice& key) {
  std::string *meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s;
  std::unordered_map<std::string, std::string*>::iterator meta_info = meta_infos_hashes_.find(key.data());
  if (meta_info != meta_infos_hashes_.end()) {
    meta_value = new std::string();
    meta_value->resize(meta_info->second->size());
    memcpy(const_cast<char *>(meta_value->data()), meta_info->second->data(), meta_info->second->size());
    ParsedHashesMetaValue parsed_hashes_meta_value(meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      delete meta_value;
      return Status::NotFound("Stale");
    } else {
      int32_t timestamp = parsed_hashes_meta_value.timestamp();
      if (timestamp == 0) {
        delete meta_value;
        return Status::NotFound("Not have an associated timeout");
      } else {
        parsed_hashes_meta_value.set_timestamp(0);
        s = db_->Put(default_write_options_, handles_[0], key, *meta_value);
        if (s.ok()) {
            delete meta_info->second;
            meta_info->second = meta_value;
        } else {
            delete meta_value;
        }
      }
    }
  } else {
      s = Status::NotFound();
  }
  return s;
}

Status RedisHashes::TTL(const Slice& key, int64_t* timestamp) {
  Status s;
  std::unordered_map<std::string, std::string*>::iterator meta_info =
      meta_infos_hashes_.find(key.data());
  if (meta_info != meta_infos_hashes_.end()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(meta_info->second);
    if (parsed_hashes_meta_value.IsStale()) {
      *timestamp = -2;
      return Status::NotFound("Stale");
    } else {
      *timestamp = parsed_hashes_meta_value.timestamp();
      if (*timestamp == 0) {
        *timestamp = -1;
      } else {
        int64_t curtime;
        shannon::Env::Default()->GetCurrentTime(&curtime);
        *timestamp = *timestamp - curtime > 0 ? *timestamp - curtime : -1;
      }
    }
  } else if (s.IsNotFound()) {
    *timestamp = -2;
    s = Status::NotFound();
  }
  return s;
}

void RedisHashes::ScanDatabase() {

  shannon::ReadOptions iterator_options;
  const shannon::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;
  int32_t current_time = time(NULL);

  printf("\n***************Hashes Meta Data***************\n");
  auto meta_iter = db_->NewIterator(iterator_options, handles_[0]);
  for (meta_iter->SeekToFirst();
       meta_iter->Valid();
       meta_iter->Next()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(meta_iter->value());
    int32_t survival_time = 0;
    if (parsed_hashes_meta_value.timestamp() != 0) {
      survival_time = parsed_hashes_meta_value.timestamp() - current_time > 0 ?
        parsed_hashes_meta_value.timestamp() - current_time : -1;
    }

    printf("[key : %-30s] [count : %-10d] [timestamp : %-10d] [version : %d] [survival_time : %d]\n",
           meta_iter->key().ToString().c_str(),
           parsed_hashes_meta_value.count(),
           parsed_hashes_meta_value.timestamp(),
           parsed_hashes_meta_value.version(),
           survival_time);
  }
  delete meta_iter;

  printf("\n***************Hashes Field Data***************\n");
  auto field_iter = db_->NewIterator(iterator_options, handles_[1]);
  for (field_iter->SeekToFirst();
       field_iter->Valid();
       field_iter->Next()) {
    ParsedHashesDataKey parsed_hashes_data_key(field_iter->key());
    printf("[key : %-30s] [field : %-20s] [value : %-20s] [version : %d]\n",
           parsed_hashes_data_key.key().ToString().c_str(),
           parsed_hashes_data_key.field().ToString().c_str(),
           field_iter->value().ToString().c_str(),
           parsed_hashes_data_key.version());
  }
  delete field_iter;
}

Status RedisHashes::DelTimeout(BlackWidow * bw,std::string * key) {
  Status s = Status::OK();
  shannon::Iterator *iter = db_->NewIterator(shannon::ReadOptions(), handles_[2]);
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
    if (s.ok()){
      s = db_->Delete(shannon::WriteOptions(), handles_[2], iter->key());
    }
  }
  else  *key = "";
  delete iter;
  return s;
}
Status RedisHashes::RealDelTimeout(BlackWidow * bw,std::string * key) {
    Status s = Status::OK();
    ScopeRecordLock l(lock_mgr_, *key);
    std::string meta_value;
    std::unordered_map<std::string, std::string *>::iterator meta_info =
        meta_infos_hashes_.find(*key);
    if (meta_info != meta_infos_hashes_.end()) {
      meta_value.resize(meta_info->second->size());
      memcpy(const_cast<char *>(meta_value.data()), meta_info->second->data(), meta_info->second->size());
      ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
      int64_t unix_time;
      shannon::Env::Default()->GetCurrentTime(&unix_time);
      if (parsed_hashes_meta_value.timestamp() < static_cast<int32_t>(unix_time))
      {
        AddDelKey(bw, *key);
        s = db_->Delete(shannon::WriteOptions(), handles_[0], *key);
        delete meta_info->second;
        meta_infos_hashes_.erase(*key);
      }
    }
    return s;
}

}  //  namespace blackwidow
