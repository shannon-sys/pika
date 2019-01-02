#include "swift/shannon_db.h"
#include "swift/write_batch.h"
#include "write_batch_internal.h"
#include "util/coding.h"
#include "venice_kv.h"

#define OFFSET(Type, member) (size_t)&( ((Type*)0)->member)
#define PutFixedAlign(des, src)	if (sizeof(size_t) == 4) \
					PutFixed32(des, (size_t) (src));\
				else if (sizeof(size_t) == 8) \
					PutFixed64(des, (size_t) (src));

namespace shannon {

static const size_t kHeader = sizeof(struct write_batch_header);

enum ValueType {
  kTypeDeletion = 2,
  kTypeValue = 3
};

WriteBatch::WriteBatch() {
  Clear();
}

WriteBatch::~WriteBatch() { }

WriteBatch::Handler::~Handler() { }

void WriteBatch::Clear() {
  rep_.clear();
  rep_.resize(kHeader);
  value_.clear();
}

Status WriteBatch::Iterate(Handler* handler) const {
  Slice input(rep_);
  if (input.size() < kHeader) {
    return Status::Corruption("malformed WriteBatch (too small)");
  }

  input.remove_prefix(kHeader);
  Slice key, value;
  int found = 0;
  while (!input.empty()) {
    found++;
    struct batch_cmd *cmd = (struct batch_cmd *)input.data();
    if (cmd->watermark != CMD_START_MARK)
	return Status::Corruption("bad cmd start mark");
    switch (cmd->cmd_type) {
      case kTypeValue:
        key = Slice(cmd->key, cmd->key_len);
        value = Slice(cmd->value, cmd->value_len);
        handler->Put(key, value);
        break;
      case kTypeDeletion:
	key = Slice(cmd->key, cmd->key_len);
        handler->Delete(key);
        break;
      default:
        return Status::Corruption("unknown WriteBatch type");
    }
    input.remove_prefix(sizeof(*cmd));
  }
  if (found != WriteBatchInternal::Count(this)) {
    return Status::Corruption("WriteBatch has wrong count");
  } else {
    return Status::OK();
  }
}

int WriteBatchInternal::Count(const WriteBatch* b) {
  return DecodeFixed32(b->rep_.data() + OFFSET(write_batch_header, count));
}

void WriteBatchInternal::SetCount(WriteBatch* b, int n) {
  EncodeFixed32(&b->rep_[OFFSET(write_batch_header, count)], n);
}

void WriteBatchInternal::SetSize(WriteBatch* b, size_t n) {
  EncodeFixed64(&b->rep_[OFFSET(write_batch_header, size)], n);
}

void WriteBatchInternal::SetValueSize(WriteBatch* b, size_t n) {
	size_t value_size = DecodeFixed64(b->rep_.data() + OFFSET(write_batch_header, value_size));
  EncodeFixed64(&b->rep_[OFFSET(write_batch_header, value_size)], n + value_size);
}

void WriteBatchInternal::SetHandle(WriteBatch* b, int n) {
  EncodeFixed32(&b->rep_[OFFSET(write_batch_header, db_index)], n);
}

void WriteBatchInternal::SetFillCache(WriteBatch* b, int n) {
  EncodeFixed32(&b->rep_[OFFSET(write_batch_header, fill_cache)], n);
}

void WriteBatch::Put(ColumnFamilyHandle* column_family, const Slice& key,
        const Slice& value) {
    WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
    PutFixed64(&rep_, static_cast<uint64_t>(CMD_START_MARK));
    PutFixed64(&rep_, static_cast<uint64_t>(0));
    PutFixed32(&rep_, static_cast<int>(kTypeValue));
    PutFixed32(&rep_, static_cast<int>(
              (reinterpret_cast<const ColumnFamilyHandle *>(column_family))->GetID()));
    PutFixed32(&rep_, key.size());
    PutFixed32(&rep_, value.size());
    value_.push_back(value.ToString());
    PutFixedAlign(&rep_, value_.back().data());
    PutSliceData(&rep_, key);
    WriteBatchInternal::SetSize(this, WriteBatchInternal::ByteSize(this));
    WriteBatchInternal::SetValueSize(this, value.size());
}

void WriteBatch::Put(const Slice& key, const Slice& value) {
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  PutFixed64(&rep_, static_cast<uint64_t>(CMD_START_MARK));
  PutFixed64(&rep_, static_cast<uint64_t>(0));
  PutFixed32(&rep_, static_cast<int>(kTypeValue));
  PutFixed32(&rep_, static_cast<int>(0));
  PutFixed32(&rep_, key.size());
  PutFixed32(&rep_, value.size());
  value_.push_back(value.ToString());
  PutFixedAlign(&rep_, value_.back().data());
  PutSliceData(&rep_, key);
  WriteBatchInternal::SetSize(this, WriteBatchInternal::ByteSize(this));
  WriteBatchInternal::SetValueSize(this, value.size());
}

void WriteBatch::Delete(ColumnFamilyHandle* column_family, const Slice& key) {
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  PutFixed64(&rep_, static_cast<uint64_t>(CMD_START_MARK));
  PutFixed64(&rep_, static_cast<uint64_t>(0));
  PutFixed32(&rep_, static_cast<int>(kTypeDeletion));
  PutFixed32(&rep_, static_cast<int>(
            (reinterpret_cast<const ColumnFamilyHandle *>(column_family))->GetID()));
  PutFixed32(&rep_, key.size());
  PutFixed32(&rep_, static_cast<int>(0));
  PutFixedAlign(&rep_, 0);
  PutSliceData(&rep_, key);
  WriteBatchInternal::SetSize(this, WriteBatchInternal::ByteSize(this));
}

void WriteBatch::Delete(const Slice& key) {
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  PutFixed64(&rep_, static_cast<uint64_t>(CMD_START_MARK));
  PutFixed64(&rep_, static_cast<uint64_t>(0));
  PutFixed32(&rep_, static_cast<int>(kTypeDeletion));
  PutFixed32(&rep_, static_cast<int>(0));
  PutFixed32(&rep_, key.size());
  PutFixed32(&rep_, static_cast<int>(0));
  PutFixedAlign(&rep_, 0);
  PutSliceData(&rep_, key);
  WriteBatchInternal::SetSize(this, WriteBatchInternal::ByteSize(this));
}

}
