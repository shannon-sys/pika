//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "src/redis_sets.h"

#include <map>
#include <memory>
#include <random>
#include <algorithm>
#include <unordered_map>

#include "blackwidow/util.h"
#include "src/base_filter.h"
#include "src/scope_record_lock.h"
#include "src/scope_snapshot.h"

namespace blackwidow {
std::unordered_map<std::string, std::string*> meta_infos_set_;
RedisSets::RedisSets() {
  spop_counts_store_.max_size_ = 1000;
}

RedisSets::~RedisSets() {
  std::vector<shannon::ColumnFamilyHandle*> tmp_handles = handles_;
  handles_.clear();
  for (auto handle : tmp_handles) {
    delete handle;
  }
  for (std::unordered_map<std::string, std::string*>::iterator iter =
          meta_infos_set_.begin(); iter != meta_infos_set_.end();
          ++ iter) {
      delete iter->second;
  }
  meta_infos_set_.clear();
}

Status RedisSets::Open(const BlackwidowOptions& bw_options,
                       const std::string& db_path) {
  shannon::Options ops(bw_options.options);
  Status s = shannon::DB::Open(ops, db_path, default_device_name_, &db_);
  if (s.ok()) {
    // create column family
    shannon::ColumnFamilyHandle* cf ,*tcf;
    shannon::ColumnFamilyOptions cfo;
    s = db_->CreateColumnFamily(cfo, "member_cf", &cf);
    if (!s.ok()) {
      return s;
    }

    s = db_->CreateColumnFamily(shannon::ColumnFamilyOptions(), "timeout_cf", &tcf);
    if (!s.ok()) {
      return s;
    }
    // close DB
    delete cf;
    delete tcf;
    delete db_;
  }

  // Open
  shannon::DBOptions db_ops(bw_options.options);
  shannon::ColumnFamilyOptions meta_cf_ops(bw_options.options);
  shannon::ColumnFamilyOptions member_cf_ops(bw_options.options);
  shannon::ColumnFamilyOptions timeout_cf_ops(bw_options.options);
  meta_cf_ops.compaction_filter_factory =
      std::make_shared<SetsMetaFilterFactory>();
  member_cf_ops.compaction_filter_factory =
      std::make_shared<SetsMemberFilterFactory>(&db_, &handles_);

  //use the bloom filter policy to reduce disk reads
  shannon::BlockBasedTableOptions table_ops(bw_options.table_options);
  table_ops.filter_policy.reset(shannon::NewBloomFilterPolicy(10, true));
  shannon::BlockBasedTableOptions meta_cf_table_ops(table_ops);
  shannon::BlockBasedTableOptions member_cf_table_ops(table_ops);
  shannon::BlockBasedTableOptions timeout_cf_table_ops(table_ops);
  if (!bw_options.share_block_cache && bw_options.block_cache_size > 0) {
    meta_cf_table_ops.block_cache = shannon::NewLRUCache(bw_options.block_cache_size);
    member_cf_table_ops.block_cache = shannon::NewLRUCache(bw_options.block_cache_size);
    timeout_cf_table_ops.block_cache = shannon::NewLRUCache(bw_options.block_cache_size);
  }
  meta_cf_ops.table_factory.reset(shannon::NewBlockBasedTableFactory(meta_cf_table_ops));
  member_cf_ops.table_factory.reset(shannon::NewBlockBasedTableFactory(member_cf_table_ops));
  timeout_cf_ops.table_factory.reset(shannon::NewBlockBasedTableFactory(timeout_cf_table_ops));

  std::vector<shannon::ColumnFamilyDescriptor> column_families;
  // Meta CF
  column_families.push_back(shannon::ColumnFamilyDescriptor(
      shannon::kDefaultColumnFamilyName, meta_cf_ops));
  // Member CF
  column_families.push_back(shannon::ColumnFamilyDescriptor(
      "member_cf", member_cf_ops));
  column_families.push_back(shannon::ColumnFamilyDescriptor(
      "timeout_cf", shannon::ColumnFamilyOptions()));
  s = shannon::DB::Open(db_ops, db_path, default_device_name_, column_families, &handles_, &db_);
  if (s.ok()) {
      shannon::Iterator* iter = db_->NewIterator(shannon::ReadOptions(), handles_[0]);
      for (iter->SeekToFirst();
           iter->Valid();
           iter->Next()) {
          Slice slice_key = iter->key();
          Slice slice_value = iter->value();
          std::string* meta_value = new std::string();
          meta_value->resize(slice_value.size());
          memcpy(const_cast<char *>(meta_value->data()), slice_value.data(), slice_value.size());
          meta_infos_set_.insert(make_pair(slice_key.data(), meta_value));
      }
      delete iter;
  }
  return s;
}

Status RedisSets::CompactRange(const shannon::Slice* begin,
                               const shannon::Slice* end) {
  Status s = db_->CompactRange(default_compact_range_options_,
      handles_[0], begin, end);
  if (!s.ok()) {
    return s;
  }
  return db_->CompactRange(default_compact_range_options_,
      handles_[1], begin, end);
}
Status RedisSets::AddDelKey(BlackWidow * bw,const string  & str){
  return bw->AddDelKey(db_,str,handles_[1]);
};
Status RedisSets::GetProperty(const std::string& property, uint64_t* out) {
  std::string value;
  db_->GetProperty(handles_[0], property, &value);
  *out = std::strtoull(value.c_str(), NULL, 10);
  db_->GetProperty(handles_[1], property, &value);
  *out += std::strtoull(value.c_str(), NULL, 10);
  return Status::OK();
}

Status RedisSets::ScanKeyNum(uint64_t* num) {

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
    ParsedSetsMetaValue parsed_sets_meta_value(iter->value());
    if (!parsed_sets_meta_value.IsStale()
      && parsed_sets_meta_value.count() != 0) {
      count++;
    }
  }
  *num = count;
  delete iter;
  return Status::OK();
}

Status RedisSets::ScanKeys(const std::string& pattern,
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
    ParsedSetsMetaValue parsed_sets_meta_value(iter->value());
    if (!parsed_sets_meta_value.IsStale()
      && parsed_sets_meta_value.count() != 0) {
      key = iter->key().ToString();
      if (StringMatch(pattern.data(), pattern.size(), key.data(), key.size(), 0)) {
        keys->push_back(key);
      }
    }
  }
  delete iter;
  return Status::OK();
}

Status RedisSets::SAdd(const Slice& key,
                       const std::vector<std::string>& members, int32_t* ret) {
  std::unordered_set<std::string> unique;
  std::vector<std::string> filtered_members;
  for (const auto& member : members) {
    if (unique.find(member) == unique.end()) {
      unique.insert(member);
      filtered_members.push_back(member);
    }
  }

  shannon::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);
  int32_t version = 0;
  Status s;
  std::string* meta_value;
  std::unordered_map<std::string, std::string*>::iterator meta_info =
      meta_infos_set_.find(key.data());
  if (meta_info != meta_infos_set_.end()) {
    meta_value = new std::string();
    meta_value->resize(meta_info->second->size());
    memcpy(const_cast<char *>(meta_value->data()), meta_info->second->data(), meta_info->second->size());
    ParsedSetsMetaValue parsed_sets_meta_value(meta_value);
    if (parsed_sets_meta_value.IsStale()) {
      version = parsed_sets_meta_value.InitialMetaValue();
      parsed_sets_meta_value.set_count(filtered_members.size());
      batch.Put(handles_[0], key, *meta_value);
      for (const auto& member : filtered_members) {
        SetsMemberKey sets_member_key(key, version, member);
        batch.Put(handles_[1], sets_member_key.Encode(), Slice("u"));
      }
      *ret = filtered_members.size();
      if (parsed_sets_meta_value.timestamp() != 0 ) {
        char str[sizeof(int32_t)+key.size() +1];
        str[sizeof(int32_t)+key.size() ] = '\0';
        EncodeFixed32(str,parsed_sets_meta_value.timestamp());
        memcpy(str + sizeof(int32_t) , key.data(),key.size());
        batch.Put(handles_[2], {str,sizeof(int32_t)+key.size()}, "1" );
      }
    } else {
      int32_t cnt = 0;
      std::string member_value;
      version = parsed_sets_meta_value.version();
      if (parsed_sets_meta_value.timestamp() != 0 ) {
        char str[sizeof(int32_t)+key.size()];
        EncodeFixed32(str,parsed_sets_meta_value.timestamp());
        memcpy(str + sizeof(int32_t) , key.data(),key.size());
        batch.Put(handles_[2], {str,sizeof(int32_t)+key.size()}, "1" );
      }

      for (const auto& member : filtered_members) {
        SetsMemberKey sets_member_key(key, version, member);
        s = db_->Get(default_read_options_, handles_[1],
                     sets_member_key.Encode(), &member_value);
        if (s.ok()) {
        } else if (s.IsNotFound()) {
          cnt++;
          batch.Put(handles_[1], sets_member_key.Encode(), Slice("u"));
        } else {
          delete meta_value;
          return s;
        }
      }
      *ret = cnt;
      if (cnt == 0) {
        return Status::OK();
      } else {
        parsed_sets_meta_value.ModifyCount(cnt);
        batch.Put(handles_[0], key, *meta_value);
      }
    }
  } else {
    meta_value = new std::string();
    meta_value->resize(12);
    EncodeFixed32(const_cast<char *>(meta_value->data()), filtered_members.size());
    SetsMetaValue sets_meta_value(Slice(meta_value->data(), meta_value->size()));
    version = sets_meta_value.UpdateVersion();
    batch.Put(handles_[0], key, sets_meta_value.Encode());
    for (const auto& member : filtered_members) {
      SetsMemberKey sets_member_key(key, version, member);
      batch.Put(handles_[1], sets_member_key.Encode(), Slice("1"));
    }
    *ret = filtered_members.size();
    ParsedSetsMetaValue parsed_sets_meta_value(meta_value);
  }
  s = db_->Write(default_write_options_, &batch);
  if (s.ok()) {
    if (meta_info != meta_infos_set_.end()) {
        delete meta_info->second;
        meta_info->second = meta_value;
    } else {
        meta_infos_set_.insert(make_pair(key.data(), meta_value));
    }
  } else {
    delete meta_value;
  }
  return s;
}

Status RedisSets::SCard(const Slice& key, int32_t* ret) {
  *ret = 0;
  std::string* meta_value;
  Status s;
  std::unordered_map<std::string, std::string*>::iterator meta_info =
      meta_infos_set_.find(key.data());
  if (meta_info != meta_infos_set_.end()) {
    meta_value = new std::string();
    meta_value->resize(12);
    memcpy(const_cast<char *>(meta_value->data()), meta_info->second->data(), meta_info->second->size());
    ParsedSetsMetaValue parsed_sets_meta_value(meta_value);
    if (parsed_sets_meta_value.IsStale()) {
      delete meta_value;
      return Status::NotFound("Stale");
    } else {
      *ret = parsed_sets_meta_value.count();
      if (*ret == 0) {
        delete meta_value;
        return Status::NotFound("Deleted");
      }
    }
  } else {
      s = Status::NotFound();
  }
  return s;
}

Status RedisSets::SDiff(const std::vector<std::string>& keys,
                        std::vector<std::string>* members) {
  if (keys.size() <= 0) {
    return Status::Corruption("SDiff invalid parameter, no keys");
  }

  shannon::ReadOptions read_options;
  const shannon::Snapshot* snapshot;

  std::string meta_value;
  int32_t version = 0;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  std::vector<KeyVersion> vaild_sets;
  Status s;
  std::unordered_map<std::string, std::string*>::iterator meta_info;

  for (uint32_t idx = 1; idx < keys.size(); ++idx) {
    meta_info = meta_infos_set_.find(keys[idx].data());
    if (meta_info != meta_infos_set_.end()) {
      ParsedSetsMetaValue parsed_sets_meta_value(meta_info->second);
      if (!parsed_sets_meta_value.IsStale()) {
        vaild_sets.push_back({keys[idx], parsed_sets_meta_value.version()});
      }
    } else {
      return Status::NotFound();
    }
  }

  meta_info = meta_infos_set_.find(keys[0].data());
  if (meta_info != meta_infos_set_.end()) {
    ParsedSetsMetaValue parsed_sets_meta_value(meta_info->second);
    if (!parsed_sets_meta_value.IsStale()) {
      bool found;
      Slice prefix;
      std::string member_value;
      version = parsed_sets_meta_value.version();
      SetsMemberKey sets_member_key(keys[0], version, Slice());
      prefix = sets_member_key.Encode();
      auto iter = db_->NewIterator(read_options, handles_[1]);
      for (iter->Seek(prefix);
           iter->Valid() && iter->key().starts_with(prefix);
           iter->Next()) {
        ParsedSetsMemberKey parsed_sets_member_key(iter->key());
        Slice member = parsed_sets_member_key.member();

        found = false;
        for (const auto& key_version : vaild_sets) {
          SetsMemberKey sets_member_key(key_version.key,
                  key_version.version, member);
          s = db_->Get(read_options, handles_[1],
                  sets_member_key.Encode(), &member_value);
          if (s.ok()) {
            found = true;
            break;
          } else if (!s.IsNotFound()) {
            delete iter;
            return s;
          }
        }
        if (!found) {
          members->push_back(member.ToString());
        }
      }
      delete iter;
    }
  } else {
      return Status::NotFound();
  }
  return Status::OK();
}

Status RedisSets::SDiffstore(const Slice& destination,
                             const std::vector<std::string>& keys,
                             int32_t* ret) {
  if (keys.size() <= 0) {
    return Status::Corruption("SDiffsotre invalid parameter, no keys");
  }

  shannon::WriteBatch batch;
  shannon::ReadOptions read_options;
  const shannon::Snapshot* snapshot;

  std::string* meta_value;
  int32_t version = 0;
  ScopeRecordLock l(lock_mgr_, destination);
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  std::vector<KeyVersion> vaild_sets;
  Status s;
  std::unordered_map<std::string, std::string*>::iterator meta_info;

  for (uint32_t idx = 1; idx < keys.size(); ++idx) {
    meta_info = meta_infos_set_.find(keys[idx].data());
    if (meta_info != meta_infos_set_.end()) {
      ParsedSetsMetaValue parsed_sets_meta_value(meta_info->second);
      if (!parsed_sets_meta_value.IsStale()) {
        vaild_sets.push_back({keys[idx], parsed_sets_meta_value.version()});
      }
    } else {
      return Status::NotFound();
    }
  }

  std::vector<std::string> members;
  meta_info = meta_infos_set_.find(keys[0].data());
  if (meta_info != meta_infos_set_.end()) {
    ParsedSetsMetaValue parsed_sets_meta_value(meta_info->second);
    if (!parsed_sets_meta_value.IsStale()) {
      bool found;
      std::string member_value;
      version = parsed_sets_meta_value.version();
      SetsMemberKey sets_member_key(keys[0], version, Slice());
      Slice prefix = sets_member_key.Encode();
      auto iter = db_->NewIterator(read_options, handles_[1]);
      for (iter->Seek(prefix);
           iter->Valid() && iter->key().starts_with(prefix);
           iter->Next()) {
        ParsedSetsMemberKey parsed_sets_member_key(iter->key());
        Slice member = parsed_sets_member_key.member();

        found = false;
        for (const auto& key_version : vaild_sets) {
          SetsMemberKey sets_member_key(key_version.key,
                  key_version.version, member);
          s = db_->Get(read_options, handles_[1],
                  sets_member_key.Encode(), &member_value);
          if (s.ok()) {
            found = true;
            break;
          } else if (!s.IsNotFound()) {
            delete iter;
            return s;
          }
        }
        if (!found) {
          members.push_back(member.ToString());
        }
      }
      delete iter;
    }
  } else {
    return Status::NotFound();
  }

  meta_info = meta_infos_set_.find(destination.data());
  if (meta_info != meta_infos_set_.end()) {
    meta_value = new std::string();
    meta_value->resize(12);
    memcpy(const_cast<char *>(meta_value->data()), meta_info->second, meta_info->second->size());
    ParsedSetsMetaValue parsed_sets_meta_value(meta_value);
    version = parsed_sets_meta_value.InitialMetaValue();
    parsed_sets_meta_value.set_count(members.size());
    batch.Put(handles_[0], destination, *meta_value);
  } else {
    meta_value = new std::string();
    meta_value->resize(12);
    EncodeFixed32(const_cast<char *>(meta_value->data()), members.size());
    SetsMetaValue sets_meta_value(Slice(meta_value->data(), meta_value->size()));
    version = sets_meta_value.UpdateVersion();
    batch.Put(handles_[0], destination, sets_meta_value.Encode());
  }
  for (const auto& member : members) {
    SetsMemberKey sets_member_key(destination, version, member);
    batch.Put(handles_[1], sets_member_key.Encode(), Slice("0"));
  }
  *ret = members.size();
  s = db_->Write(default_write_options_, &batch);
  if (s.ok()) {
      if (meta_info != meta_infos_set_.end()) {
          delete meta_info->second;
          meta_info->second = meta_value;
      } else {
          meta_infos_set_.insert(make_pair(destination.data(), meta_value));
      }
  } else {
      delete meta_value;
  }
  return s;
}

Status RedisSets::SInter(const std::vector<std::string>& keys,
                         std::vector<std::string>* members) {
  if (keys.size() <= 0) {
    return Status::Corruption("SInter invalid parameter, no keys");
  }

  shannon::ReadOptions read_options;
  const shannon::Snapshot* snapshot;

  std::string meta_value;
  int32_t version = 0;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  std::vector<KeyVersion> vaild_sets;
  Status s;
  std::unordered_map<std::string, std::string*>::iterator meta_info;

  for (uint32_t idx = 1; idx < keys.size(); ++idx) {
    meta_info = meta_infos_set_.find(keys[idx].data());
    if (meta_info != meta_infos_set_.end()) {
      ParsedSetsMetaValue parsed_sets_meta_value(meta_info->second);
      if (parsed_sets_meta_value.IsStale() ||
        parsed_sets_meta_value.count() == 0) {
        return Status::OK();
      } else {
        vaild_sets.push_back({keys[idx], parsed_sets_meta_value.version()});
      }
    } else {
      return Status::OK();
    }
  }

  meta_info = meta_infos_set_.find(keys[0].data());
  if (meta_info != meta_infos_set_.end()) {
    ParsedSetsMetaValue parsed_sets_meta_value(meta_info->second);
    if (parsed_sets_meta_value.IsStale() ||
      parsed_sets_meta_value.count() == 0) {
      return Status::OK();
    } else {
      bool reliable;
      std::string member_value;
      version = parsed_sets_meta_value.version();
      SetsMemberKey sets_member_key(keys[0], version, Slice());
      Slice prefix = sets_member_key.Encode();
      auto iter = db_->NewIterator(read_options, handles_[1]);
      for (iter->Seek(prefix);
           iter->Valid() && iter->key().starts_with(prefix);
           iter->Next()) {
        ParsedSetsMemberKey parsed_sets_member_key(iter->key());
        Slice member = parsed_sets_member_key.member();

        reliable = true;
        for (const auto& key_version : vaild_sets) {
          SetsMemberKey sets_member_key(key_version.key,
                  key_version.version, member);
          s = db_->Get(read_options, handles_[1],
                  sets_member_key.Encode(), &member_value);
          if (s.ok()) {
            continue;
          } else if (s.IsNotFound()) {
            reliable = false;
            break;
          } else {
            delete iter;
            return s;
          }
        }
        if (reliable) {
          members->push_back(member.ToString());
        }
      }
      delete iter;
    }
  } else {
    return Status::OK();
  }
  return Status::OK();
}

Status RedisSets::SInterstore(const Slice& destination,
                              const std::vector<std::string>& keys,
                              int32_t* ret) {
  if (keys.size() <= 0) {
    return Status::Corruption("SInterstore invalid parameter, no keys");
  }

  shannon::WriteBatch batch;
  shannon::ReadOptions read_options;
  const shannon::Snapshot* snapshot;

  std::string* meta_value;
  int32_t version = 0;
  bool have_invalid_sets = false;
  ScopeRecordLock l(lock_mgr_, destination);
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  std::vector<KeyVersion> vaild_sets;
  Status s;
  std::unordered_map<std::string, std::string*>::iterator meta_info;

  for (uint32_t idx = 1; idx < keys.size(); ++idx) {
    meta_info = meta_infos_set_.find(keys[idx].data());
    if (meta_info != meta_infos_set_.end()) {
      ParsedSetsMetaValue parsed_sets_meta_value(meta_info->second);
      if (parsed_sets_meta_value.IsStale() ||
        parsed_sets_meta_value.count() == 0) {
        have_invalid_sets = true;
        break;
      } else {
        vaild_sets.push_back({keys[idx], parsed_sets_meta_value.version()});
      }
    } else {
      have_invalid_sets = true;
      break;
    }
  }

  std::vector<std::string> members;
  if (!have_invalid_sets) {
    meta_info = meta_infos_set_.find(keys[0].data());
    if (meta_info != meta_infos_set_.end()) {
      ParsedSetsMetaValue parsed_sets_meta_value(meta_info->second);
      if (parsed_sets_meta_value.IsStale() ||
        parsed_sets_meta_value.count() == 0) {
        have_invalid_sets = true;
      } else {
        bool reliable;
        std::string member_value;
        version = parsed_sets_meta_value.version();
        SetsMemberKey sets_member_key(keys[0], version, Slice());
        Slice prefix = sets_member_key.Encode();
        auto iter = db_->NewIterator(read_options, handles_[1]);
        for (iter->Seek(prefix);
             iter->Valid() && iter->key().starts_with(prefix);
             iter->Next()) {
          ParsedSetsMemberKey parsed_sets_member_key(iter->key());
          Slice member = parsed_sets_member_key.member();

          reliable = true;
          for (const auto& key_version : vaild_sets) {
            SetsMemberKey sets_member_key(key_version.key,
                    key_version.version, member);
            s = db_->Get(read_options, handles_[1],
                    sets_member_key.Encode(), &member_value);
            if (s.ok()) {
              continue;
            } else if (s.IsNotFound()) {
              reliable = false;
              break;
            } else {
              delete iter;
              return s;
            }
          }
          if (reliable) {
            members.push_back(member.ToString());
          }
        }
        delete iter;
      }
    } else {
        return Status::OK();
    }
  }

  meta_info = meta_infos_set_.find(destination.data());
  if (meta_info != meta_infos_set_.end()) {
    meta_value = new std::string();
    meta_value->resize(meta_info->second->size());
    memcpy(const_cast<char *>(meta_value->data()), meta_info->second->data(), meta_info->second->size());
    ParsedSetsMetaValue parsed_sets_meta_value(meta_value);
    version = parsed_sets_meta_value.InitialMetaValue();
    parsed_sets_meta_value.set_count(members.size());
    batch.Put(handles_[0], destination, *meta_value);
  } else {
    meta_value = new std::string();
    meta_value->resize(12);
    EncodeFixed32(const_cast<char *>(meta_value->data()), members.size());
    SetsMetaValue sets_meta_value(Slice(meta_value->data(), meta_value->size()));
    version = sets_meta_value.UpdateVersion();
    batch.Put(handles_[0], destination, sets_meta_value.Encode());
  }
  for (const auto& member : members) {
    SetsMemberKey sets_member_key(destination, version, member);
    batch.Put(handles_[1], sets_member_key.Encode(), Slice("0"));
  }
  *ret = members.size();
  s = db_->Write(default_write_options_, &batch);
  if (s.ok()) {
      if (meta_info != meta_infos_set_.end()) {
          delete meta_info->second;
          meta_info->second = meta_value;
      } else {
          meta_infos_set_.insert(make_pair(destination.data(), meta_value));
      }
  } else {
      delete meta_value;
  }
  return s;
}

Status RedisSets::SIsmember(const Slice& key, const Slice& member,
                            int32_t* ret) {
  shannon::ReadOptions read_options;
  const shannon::Snapshot* snapshot;

  int32_t version = 0;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  std::unordered_map<std::string, std::string*>::iterator meta_info;
  Status s;
  meta_info = meta_infos_set_.find(key.data());
  if (meta_info != meta_infos_set_.end()) {
    ParsedSetsMetaValue parsed_sets_meta_value(meta_info->second);
    if (parsed_sets_meta_value.IsStale()) {
      *ret = 0;
      return Status::NotFound("Stale");
    } else {
      std::string member_value;
      version = parsed_sets_meta_value.version();
      SetsMemberKey sets_member_key(key, version, member);
      s = db_->Get(read_options, handles_[1],
              sets_member_key.Encode(), &member_value);
      *ret = s.ok() ? 1 : 0;
    }
  } else {
    *ret = 0;
    s = Status::NotFound();
  }
  return s;
}

Status RedisSets::SMembers(const Slice& key,
                           std::vector<std::string>* members) {
  shannon::ReadOptions read_options;
  const shannon::Snapshot* snapshot;

  int32_t version = 0;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  std::unordered_map<std::string, std::string*>::iterator meta_info;
  Status s;
  meta_info = meta_infos_set_.find(key.data());
  if (meta_info != meta_infos_set_.end()) {
    ParsedSetsMetaValue parsed_sets_meta_value(meta_info->second);
    if (parsed_sets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else {
      version = parsed_sets_meta_value.version();
      SetsMemberKey sets_member_key(key, version, Slice());
      Slice prefix = sets_member_key.Encode();
      auto iter = db_->NewIterator(read_options, handles_[1]);
      for (iter->Seek(prefix);
           iter->Valid() && iter->key().starts_with(prefix);
           iter->Next()) {
        ParsedSetsMemberKey parsed_sets_member_key(iter->key());
        members->push_back(parsed_sets_member_key.member().ToString());
      }
      delete iter;
    }
  } else {
      s = Status::NotFound();
  }
  return s;
}

Status RedisSets::SMove(const Slice& source, const Slice& destination,
                        const Slice& member, int32_t* ret) {

  shannon::WriteBatch batch;
  shannon::ReadOptions read_options;

  std::string *meta_value1, *meta_value2;
  int32_t version = 0;
  std::vector<std::string> keys {source.ToString(), destination.ToString()};
  MultiScopeRecordLock ml(lock_mgr_, keys);
  std::unordered_map<std::string, std::string*>::iterator meta_info1, meta_info2;
  Status s;

  if (source == destination) {
    *ret = 1;
    return Status::OK();
  }

  meta_info1 = meta_infos_set_.find(source.data());
  if (meta_info1 != meta_infos_set_.end()) {
    meta_value1 = new std::string();
    meta_value1->resize(meta_info1->second->size());
    memcpy(const_cast<char *>(meta_value1->data()), meta_info1->second->data(), meta_info1->second->size());
    ParsedSetsMetaValue parsed_sets_meta_value(meta_value1);
    if (parsed_sets_meta_value.IsStale()) {
      *ret = 0;
      return Status::NotFound("Stale");
    } else {
      std::string member_value;
      version = parsed_sets_meta_value.version();
      SetsMemberKey sets_member_key(source, version, member);
      s = db_->Get(default_read_options_, handles_[1],
              sets_member_key.Encode(), &member_value);
      if (s.ok()) {
        *ret = 1;
        parsed_sets_meta_value.ModifyCount(-1);
        batch.Put(handles_[0], source, *meta_value1);
        batch.Delete(handles_[1], sets_member_key.Encode());
      } else if (s.IsNotFound()) {
        *ret = 0;
        return Status::NotFound();
      } else {
        return s;
      }
    }
  } else {
    *ret = 0;
    return Status::NotFound();
  }

  meta_info2 = meta_infos_set_.find(destination.data());
  if (meta_info2 != meta_infos_set_.end()) {
    meta_value2 = new std::string();
    meta_value2->resize(meta_info2->second->size());
    memcpy(const_cast<char *>(meta_value2->data()), meta_info2->second->data(), meta_info2->second->size());
    ParsedSetsMetaValue parsed_sets_meta_value(meta_value2);
    if (parsed_sets_meta_value.IsStale()) {
      version = parsed_sets_meta_value.InitialMetaValue();
      parsed_sets_meta_value.set_count(1);
      batch.Put(handles_[0], destination, *meta_value2);
      SetsMemberKey sets_member_key(destination, version, member);
      batch.Put(handles_[1], sets_member_key.Encode(), Slice("0"));
    } else {
      std::string member_value;
      version = parsed_sets_meta_value.version();
      SetsMemberKey sets_member_key(destination, version, member);
      s = db_->Get(default_read_options_, handles_[1],
              sets_member_key.Encode(), &member_value);
      if (s.IsNotFound()) {
        parsed_sets_meta_value.ModifyCount(1);
        batch.Put(handles_[0], destination, *meta_value2);
        batch.Put(handles_[1], sets_member_key.Encode(), Slice("0"));
      } else if (!s.ok()) {
        return s;
      }
    }
  } else {
    meta_value2 = new std::string();
    meta_value2->resize(12);
    EncodeFixed32(const_cast<char *>(meta_value2->data()), 1);
    SetsMetaValue sets_meta_value(Slice(meta_value2->data(), meta_value2->size()));
    version = sets_meta_value.UpdateVersion();
    batch.Put(handles_[0], destination, sets_meta_value.Encode());
    SetsMemberKey sets_member_key(destination, version, member);
    batch.Put(handles_[1], sets_member_key.Encode(), Slice("0"));
  }
  s = db_->Write(default_write_options_, &batch);
  if (s.ok()) {
      delete meta_info1->second;
      meta_info1->second = meta_value1;
      if (meta_info2 != meta_infos_set_.end()) {
          delete meta_info2->second;
          meta_info2->second = meta_value2;
      } else {
          meta_infos_set_.insert(make_pair(destination.data(), meta_value2));
      }
  } else {
      delete meta_value1;
      delete meta_value2;
  }
  return s;
}

Status RedisSets::SPop(const Slice& key, std::string* member, bool* need_compact) {

  std::default_random_engine engine;

  std::string *meta_value;
  shannon::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);
  std::unordered_map<std::string, std::string*>::iterator meta_info;
  Status s;

  uint64_t start_us = slash::NowMicros();
  meta_info = meta_infos_set_.find(key.data());
  if (meta_info != meta_infos_set_.end()) {
    meta_value = new std::string();
    meta_value->resize(meta_info->second->size());
    memcpy(const_cast<char *>(meta_value->data()), meta_info->second->data(), meta_info->second->size());
    ParsedSetsMetaValue parsed_sets_meta_value(meta_value);
    if (parsed_sets_meta_value.IsStale()) {
      delete meta_value;
      return Status::NotFound("Stale");
    } else if (parsed_sets_meta_value.count() == 0) {
      delete meta_value;
      return Status::NotFound();
    } else {
      engine.seed(time(NULL));
      int32_t cur_index = 0;
      int32_t size = parsed_sets_meta_value.count();
      int32_t target_index = engine() % (size < 50 ? size : 50);
      int32_t version = parsed_sets_meta_value.version();

      SetsMemberKey sets_member_key(key, version, Slice());
      auto iter = db_->NewIterator(default_read_options_, handles_[1]);
      for (iter->Seek(sets_member_key.Encode());
           iter->Valid() && cur_index < size;
           iter->Next(), cur_index++) {

        if (cur_index == target_index) {
          batch.Delete(handles_[1], iter->key());
          ParsedSetsMemberKey parsed_sets_member_key(iter->key());
          *member = parsed_sets_member_key.member().ToString();

          parsed_sets_meta_value.ModifyCount(-1);
          batch.Put(handles_[0], key, *meta_value);
          break;
        }
      }
      delete iter;
    }
  } else {
    return Status::NotFound();
  }

  uint64_t count = 0;
  uint64_t duration = slash::NowMicros() - start_us;
  AddAndGetSpopCount(key.ToString(), &count);
  if (duration >= SPOP_COMPACT_THRESHOLD_DURATION
    || count >= SPOP_COMPACT_THRESHOLD_COUNT) {
    *need_compact = true;
    ResetSpopCount(key.ToString());
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

Status RedisSets::ResetSpopCount(const std::string& key) {
  slash::MutexLock l(&spop_counts_mutex_);
  if (spop_counts_store_.map_.find(key) == spop_counts_store_.map_.end()) {
    return Status::NotFound();
  }
  spop_counts_store_.map_.erase(key);
  spop_counts_store_.list_.remove(key);
  return Status::OK();
}

Status RedisSets::AddAndGetSpopCount(const std::string& key, uint64_t* count) {
  slash::MutexLock l(&spop_counts_mutex_);
  if (spop_counts_store_.map_.find(key) == spop_counts_store_.map_.end()) {
    *count = ++spop_counts_store_.map_[key];
    spop_counts_store_.list_.push_front(key);
  } else {
    *count = ++spop_counts_store_.map_[key];
    spop_counts_store_.list_.remove(key);
    spop_counts_store_.list_.push_front(key);
  }

  if (spop_counts_store_.list_.size() > spop_counts_store_.max_size_) {
    std::string tail = spop_counts_store_.list_.back();
    spop_counts_store_.map_.erase(tail);
    spop_counts_store_.list_.pop_back();
  }
  return Status::OK();
}

Status RedisSets::SRandmember(const Slice& key, int32_t count,
                              std::vector<std::string>* members) {
  if (count == 0) {
    return Status::OK();
  }

  members->clear();
  int32_t last_seed = time(NULL);
  std::default_random_engine engine;

  shannon::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);
  std::vector<int32_t> targets;
  std::unordered_set<int32_t> unique;
  std::unordered_map<std::string, std::string*>::iterator meta_info;
  Status s;

  meta_info = meta_infos_set_.find(key.data());
  if (meta_info != meta_infos_set_.end()) {
    ParsedSetsMetaValue parsed_sets_meta_value(meta_info->second);
    if (parsed_sets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_sets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      int32_t size = parsed_sets_meta_value.count();
      int32_t version = parsed_sets_meta_value.version();
      if (count > 0) {
        count = count <= size ? count : size;
        while (targets.size() < static_cast<size_t>(count)) {
          engine.seed(last_seed);
          last_seed = engine();
          uint32_t pos = last_seed % size;
          if (unique.find(pos) == unique.end()) {
            unique.insert(pos);
            targets.push_back(pos);
          }
        }
      } else {
        count = -count;
        while (targets.size() < static_cast<size_t>(count)) {
          engine.seed(last_seed);
          last_seed = engine();
          targets.push_back(last_seed % size);
        }
      }
      std::sort(targets.begin(), targets.end());

      int32_t cur_index = 0, idx = 0;
      SetsMemberKey sets_member_key(key, version, Slice());
      auto iter = db_->NewIterator(default_read_options_, handles_[1]);
      for (iter->Seek(sets_member_key.Encode());
           iter->Valid() && cur_index < size;
           iter->Next(), cur_index++) {
        if (static_cast<size_t>(idx) >= targets.size()) {
          break;
        }
        ParsedSetsMemberKey parsed_sets_member_key(iter->key());
        while (static_cast<size_t>(idx) < targets.size()
          && cur_index == targets[idx]) {
          idx++;
          members->push_back(parsed_sets_member_key.member().ToString());
        }
      }
      random_shuffle(members->begin(), members->end());
      delete iter;
    }
  } else {
      return Status::NotFound();
  }
  return s;
}

Status RedisSets::SRem(const Slice& key,
                       const std::vector<std::string>& members,
                       int32_t* ret) {
  shannon::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);

  int32_t version = 0;
  std::string *meta_value;
  std::unordered_map<std::string, std::string*>::iterator meta_info;
  Status s;
  meta_info = meta_infos_set_.find(key.data());
  if (meta_info != meta_infos_set_.end()) {
    meta_value = new std::string();
    meta_value->resize(meta_info->second->size());
    memcpy(const_cast<char *>(meta_value->data()), meta_info->second->data(), meta_info->second->size());
    ParsedSetsMetaValue parsed_sets_meta_value(meta_value);
    if (parsed_sets_meta_value.IsStale()) {
      *ret = 0;
      return Status::NotFound("stale");
    } else {
      int32_t cnt = 0;
      std::string member_value;
      version = parsed_sets_meta_value.version();
      for (const auto& member : members) {
        SetsMemberKey sets_member_key(key, version, member);
        s = db_->Get(default_read_options_, handles_[1],
                sets_member_key.Encode(), &member_value);
        if (s.ok()) {
          cnt++;
          batch.Delete(handles_[1], sets_member_key.Encode());
        } else if (s.IsNotFound()) {
        } else {
          return s;
        }
      }
      *ret = cnt;
      parsed_sets_meta_value.ModifyCount(-cnt);
      batch.Put(handles_[0], key, *meta_value);
    }
  } else {
    *ret = 0;
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

Status RedisSets::SUnion(const std::vector<std::string>& keys,
                         std::vector<std::string>* members) {
  if (keys.size() <= 0) {
    return Status::Corruption("SUnion invalid parameter, no keys");
  }

  shannon::ReadOptions read_options;
  const shannon::Snapshot* snapshot;

  std::string meta_value;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  std::vector<KeyVersion> vaild_sets;
  Status s;
  std::unordered_map<std::string, std::string*>::iterator meta_info;

  for (uint32_t idx = 0; idx < keys.size(); ++idx) {
    meta_info = meta_infos_set_.find(keys[idx].data());
    if (meta_info != meta_infos_set_.end()) {
      ParsedSetsMetaValue parsed_sets_meta_value(meta_info->second);
      if (!parsed_sets_meta_value.IsStale() &&
        parsed_sets_meta_value.count() != 0) {
        vaild_sets.push_back({keys[idx], parsed_sets_meta_value.version()});
      }
    } else {
      return Status::NotFound();
    }
  }

  Slice prefix;
  std::map<std::string, bool> result_flag;
  for (const auto& key_version : vaild_sets) {
    SetsMemberKey sets_member_key(key_version.key, key_version.version, Slice());
    prefix = sets_member_key.Encode();
    auto iter = db_->NewIterator(read_options, handles_[1]);
    for (iter->Seek(prefix);
         iter->Valid() && iter->key().starts_with(prefix);
         iter->Next()) {
      ParsedSetsMemberKey parsed_sets_member_key(iter->key());
      std::string member = parsed_sets_member_key.member().ToString();
      if (result_flag.find(member) == result_flag.end()) {
        members->push_back(member);
        result_flag[member] = true;
      }
    }
    delete iter;
  }
  return Status::OK();
}

Status RedisSets::SUnionstore(const Slice& destination,
                              const std::vector<std::string>& keys,
                              int32_t* ret) {
  if (keys.size() <= 0) {
    return Status::Corruption("SUnionstore invalid parameter, no keys");
  }

  shannon::WriteBatch batch;
  shannon::ReadOptions read_options;
  const shannon::Snapshot* snapshot;

  std::string *meta_value;
  int32_t version = 0;
  ScopeRecordLock l(lock_mgr_, destination);
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  std::vector<KeyVersion> vaild_sets;
  Status s;
  std::unordered_map<std::string, std::string*>::iterator meta_info;

  for (uint32_t idx = 0; idx < keys.size(); ++idx) {
    meta_info = meta_infos_set_.find(keys[idx].data());
    if (meta_info != meta_infos_set_.end()) {
      ParsedSetsMetaValue parsed_sets_meta_value(meta_info->second);
      if (!parsed_sets_meta_value.IsStale() &&
        parsed_sets_meta_value.count() != 0) {
        vaild_sets.push_back({keys[idx], parsed_sets_meta_value.version()});
      }
    } else {
      return Status::NotFound();
    }
  }

  Slice prefix;
  std::vector<std::string> members;
  std::map<std::string, bool> result_flag;
  for (const auto& key_version : vaild_sets) {
    SetsMemberKey sets_member_key(key_version.key, key_version.version, Slice());
    prefix = sets_member_key.Encode();
    auto iter = db_->NewIterator(read_options, handles_[1]);
    for (iter->Seek(prefix);
         iter->Valid() && iter->key().starts_with(prefix);
         iter->Next()) {
      ParsedSetsMemberKey parsed_sets_member_key(iter->key());
      std::string member = parsed_sets_member_key.member().ToString();
      if (result_flag.find(member) == result_flag.end()) {
        members.push_back(member);
        result_flag[member] = true;
      }
    }
    delete iter;
  }

  meta_info = meta_infos_set_.find(destination.data());
  if (meta_info != meta_infos_set_.end()) {
    meta_value = new std::string();
    meta_value->resize(meta_info->second->size());
    memcpy(const_cast<char *>(meta_value->data()), meta_info->second->data(), meta_info->second->size());
    ParsedSetsMetaValue parsed_sets_meta_value(meta_value);
    version = parsed_sets_meta_value.InitialMetaValue();
    parsed_sets_meta_value.set_count(members.size());
    batch.Put(handles_[0], destination, *meta_value);
  } else {
    meta_value = new std::string();
    meta_value->resize(12);
    EncodeFixed32(const_cast<char *>(meta_value->data()), members.size());
    SetsMetaValue sets_meta_value(Slice(meta_value->data(), meta_value->size()));
    version = sets_meta_value.UpdateVersion();
    batch.Put(handles_[0], destination, sets_meta_value.Encode());
  }
  for (const auto& member : members) {
    SetsMemberKey sets_member_key(destination, version, member);
    batch.Put(handles_[1], sets_member_key.Encode(), Slice("0"));
  }
  *ret = members.size();
  s = db_->Write(default_write_options_, &batch);
  if (s.ok()) {
      if (meta_info != meta_infos_set_.end()) {
          delete meta_info->second;
          meta_info->second = meta_value;
      } else {
          meta_infos_set_.insert(make_pair(destination.data(), meta_value));
      }
  } else {
      delete meta_value;
  }
  return s;
}

Status RedisSets::SScan(const Slice& key, int64_t cursor, const std::string& pattern,
                        int64_t count, std::vector<std::string>* members, int64_t* next_cursor) {
  *next_cursor = 0;
  members->clear();
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
  Status s = db_->Get(read_options, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (parsed_sets_meta_value.IsStale()
      || parsed_sets_meta_value.count() == 0) {
      *next_cursor = 0;
      return Status::NotFound();
    } else {
      std::string sub_member;
      std::string start_point;
      int32_t version = parsed_sets_meta_value.version();
      s = GetScanStartPoint(key, pattern, cursor, &start_point);
      if (s.IsNotFound()) {
        cursor = 0;
        if (isTailWildcard(pattern)) {
          start_point = pattern.substr(0, pattern.size() - 1);
        }
      }
      if (isTailWildcard(pattern)) {
        sub_member = pattern.substr(0, pattern.size() - 1);
      }

      SetsMemberKey sets_member_prefix(key, version, sub_member);
      SetsMemberKey sets_member_key(key, version, start_point);
      std::string prefix = sets_member_prefix.Encode().ToString();
      shannon::Iterator* iter = db_->NewIterator(read_options, handles_[1]);
      for (iter->Seek(sets_member_key.Encode());
           iter->Valid() && rest > 0 && iter->key().starts_with(prefix);
           iter->Next()) {
        ParsedSetsMemberKey parsed_sets_member_key(iter->key());
        std::string member = parsed_sets_member_key.member().ToString();
        if (StringMatch(pattern.data(), pattern.size(), member.data(), member.size(), 0)) {
          members->push_back(member);
        }
        rest--;
      }

      if (iter->Valid()
        && (iter->key().compare(prefix) <= 0 || iter->key().starts_with(prefix))) {
        *next_cursor = cursor + step_length;
        ParsedSetsMemberKey parsed_sets_member_key(iter->key());
        std::string next_member = parsed_sets_member_key.member().ToString();
        StoreScanNextPoint(key, pattern, *next_cursor, next_member);
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

Status RedisSets::Expire(const Slice& key, int32_t ttl) {
  std::string *meta_value;
  Status s;
  ScopeRecordLock l(lock_mgr_, key);
  std::unordered_map<std::string, std::string*>::iterator meta_info = meta_infos_set_.find(key.data());
  if (meta_info != meta_infos_set_.end()) {
    meta_value = new std::string();
    meta_value->resize(meta_info->second->size());
    memcpy(const_cast<char *>(meta_value->data()), meta_info->second->data(), meta_info->second->size());
    ParsedSetsMetaValue parsed_sets_meta_value(meta_value);
    if (parsed_sets_meta_value.IsStale()) {
      delete meta_value;
      return Status::NotFound("Stale");
    }
    if (ttl > 0) {
      parsed_sets_meta_value.SetRelativeTimestamp(ttl);
      s = db_->Put(default_write_options_, handles_[0], key, *meta_value);
      if (parsed_sets_meta_value.timestamp() != 0 ) {
        char str[sizeof(int32_t)+key.size() +1];
        str[sizeof(int32_t)+key.size() ] = '\0';
        EncodeFixed32(str,parsed_sets_meta_value.timestamp());
        memcpy(str + sizeof(int32_t) , key.data(),key.size());
        db_->Put(default_write_options_,handles_[2], {str,sizeof(int32_t)+key.size()}, "1" );
      }
    } else {
      parsed_sets_meta_value.set_count(0);
      parsed_sets_meta_value.UpdateVersion();
      parsed_sets_meta_value.set_timestamp(0);
      s = db_->Put(default_write_options_, handles_[0], key, *meta_value);
    }
    if (meta_info != meta_infos_set_.end()) {
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

Status RedisSets::Del(const Slice& key) {
  std::string *meta_value;
  Status s;
  ScopeRecordLock l(lock_mgr_, key);
  std::unordered_map<std::string, std::string*>::iterator meta_info = meta_infos_set_.find(key.data());
  if (meta_info != meta_infos_set_.end()) {
    meta_value = new std::string();
    meta_value->resize(meta_info->second->size());
    memcpy(const_cast<char *>(meta_value->data()), meta_info->second->data(), meta_info->second->size());
    ParsedSetsMetaValue parsed_sets_meta_value(meta_value);
    if (parsed_sets_meta_value.IsStale()) {
      delete meta_value;
      return Status::NotFound("Stale");
    } else if (parsed_sets_meta_value.count() == 0) {
      delete meta_value;
      return Status::NotFound();
    } else {
      parsed_sets_meta_value.InitialMetaValue();
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

bool RedisSets::Scan(const std::string& start_key,
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
    ParsedSetsMetaValue parsed_meta_value(it->value());
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

Status RedisSets::Expireat(const Slice& key, int32_t timestamp) {
  std::string *meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s;
  std::unordered_map<std::string, std::string*>::iterator meta_info = meta_infos_set_.find(key.data());
  if (meta_info != meta_infos_set_.end()) {
    meta_value = new std::string();
    meta_value->resize(meta_info->second->size());
    memcpy(const_cast<char *>(meta_value->data()), meta_info->second->data(), meta_info->second->size());
    ParsedSetsMetaValue parsed_sets_meta_value(meta_value);
    if (parsed_sets_meta_value.IsStale()) {
      delete meta_value;
      return Status::NotFound("Stale");
    } else {
      parsed_sets_meta_value.set_timestamp(timestamp);
      if (parsed_sets_meta_value.timestamp() != 0 ) {
        char str[sizeof(int32_t)+key.size() +1];
        str[sizeof(int32_t)+key.size() ] = '\0';
        EncodeFixed32(str,parsed_sets_meta_value.timestamp());
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
      s = Status::NotFound();
  }
  return s;
}

Status RedisSets::Persist(const Slice& key) {
  std::string *meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s;
  std::unordered_map<std::string, std::string*>::iterator meta_info = meta_infos_set_.find(key.data());
  if (meta_info != meta_infos_set_.end()) {
    meta_value = new std::string();
    meta_value->resize(meta_info->second->size());
    memcpy(const_cast<char *>(meta_value->data()), meta_info->second->data(), meta_info->second->size());
    ParsedSetsMetaValue parsed_sets_meta_value(meta_value);
    if (parsed_sets_meta_value.IsStale()) {
      delete meta_value;
      return Status::NotFound("Stale");
    } else {
      int32_t timestamp = parsed_sets_meta_value.timestamp();
      if (timestamp == 0) {
        delete meta_value;
        return Status::NotFound("Not have an associated timeout");
      } else {
        parsed_sets_meta_value.set_timestamp(0);
        s = db_->Put(default_write_options_, handles_[0], key, *meta_value);
      }
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

Status RedisSets::TTL(const Slice& key, int64_t* timestamp) {
  Status s;
  std::unordered_map<std::string, std::string*>::iterator meta_info = meta_infos_set_.find(key.data());
  if (meta_info != meta_infos_set_.end()) {
    ParsedSetsMetaValue parsed_setes_meta_value(meta_info->second);
    if (parsed_setes_meta_value.IsStale()) {
      *timestamp = -2;
      return Status::NotFound("Stale");
    } else {
      *timestamp = parsed_setes_meta_value.timestamp();
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

void RedisSets::ScanDatabase() {

  shannon::ReadOptions iterator_options;
  const shannon::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;
  int32_t current_time = time(NULL);

  printf("\n***************Sets Meta Data***************\n");
  auto meta_iter = db_->NewIterator(iterator_options, handles_[0]);
  for (meta_iter->SeekToFirst();
       meta_iter->Valid();
       meta_iter->Next()) {
    ParsedSetsMetaValue parsed_sets_meta_value(meta_iter->value());
    int32_t survival_time = 0;
    if (parsed_sets_meta_value.timestamp() != 0) {
      survival_time = parsed_sets_meta_value.timestamp() - current_time > 0 ?
        parsed_sets_meta_value.timestamp() - current_time : -1;
    }

    printf("[key : %-30s] [count : %-10d] [timestamp : %-10d] [version : %d] [survival_time : %d]\n",
           meta_iter->key().ToString().c_str(),
           parsed_sets_meta_value.count(),
           parsed_sets_meta_value.timestamp(),
           parsed_sets_meta_value.version(),
           survival_time);
  }
  delete meta_iter;

  printf("\n***************Sets Member Data***************\n");
  auto member_iter = db_->NewIterator(iterator_options, handles_[1]);
  for (member_iter->SeekToFirst();
       member_iter->Valid();
       member_iter->Next()) {
    ParsedSetsMemberKey parsed_sets_member_key(member_iter->key());
    printf("[key : %-30s] [member : %-20s] [version : %d]\n",
           parsed_sets_member_key.key().ToString().c_str(),
           parsed_sets_member_key.member().ToString().c_str(),
           parsed_sets_member_key.version());
  }
  delete member_iter;
}

Status RedisSets::DelTimeout(BlackWidow * bw,std::string * key) {
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

Status RedisSets::RealDelTimeout(BlackWidow * bw,std::string * key) {
    Status s = Status::OK();
    ScopeRecordLock l(lock_mgr_, *key);
    std::string meta_value;
    std::unordered_map<std::string, std::string *>::iterator meta_info =
        meta_infos_set_.find(*key);
    if (meta_info != meta_infos_set_.end()) {
      meta_value.resize(meta_info->second->size());
      memcpy(const_cast<char *>(meta_value.data()), meta_info->second->data(), meta_info->second->size());
      ParsedSetsMetaValue parsed_set_meta_value(&meta_value);
      int64_t unix_time;
      shannon::Env::Default()->GetCurrentTime(&unix_time);
      if (parsed_set_meta_value.timestamp() < static_cast<int32_t>(unix_time))
      {
        AddDelKey(bw, *key);
        s = db_->Delete(shannon::WriteOptions(), handles_[0], *key);
        delete meta_info->second;
        meta_infos_set_.erase(*key);
      }
    } else {
        s = Status::NotFound();
    }
    return s;
}

}  //  namespace blackwidow
