#ifndef LISTS_LOG_
#define LISTS_LOG_

#include <string>
#include <memory>
#include <vector>
#include "src/lists_meta_value_format.h"

namespace blackwidow {
using Status = shannon::Status;
using Slice = shannon::Slice;

class ListsMetaKeyLog {
    public:
        ListsMetaKeyLog(const Slice& key, int32_t version, uint32_t log_index) :
            key_(key), version_(version), log_index_(log_index) {
        }

        const Slice Encode() {
            size_t usize = key_.size();
            size_t needed = usize + sizeof(int32_t) * 2;
            char* dst;
            if (needed <= sizeof(space_)) {
                dst = space_;
            } else {
                dst = new char[needed];
                if (start_ != space_) {
                    delete[] start_;
                }
            }
            start_ = dst;
            memcpy(dst, key_.data(), key_.size());
            dst += key_.size();
            EncodeFixed32(dst, version_);
            dst += sizeof(int32_t);
            EncodeFixed32(dst, log_index_);
            return Slice(start_, needed);
        }
    private:
        char space_[200];
        char* start_;
        Slice key_;
        int32_t version_;
        uint32_t log_index_;
};

class ListsMetaValueLog {
    public:
        ListsMetaValueLog(char type, int32_t index) :
            type_(type), index_(index), count_(1) {
        }
        ListsMetaValueLog(char type, int32_t index, int32_t count) :
            type_(type), index_(index), count_(count) {
        }
        const Slice Encode() {
            size_t needed = 5;
            char *dst = space_;
            dst[0] = type_;
            // memcpy(&space_[1], &index_, sizeof(int32_t));
            dst = dst + 1;
            EncodeFixed32(dst, index_);
            if (type_ == ParsedListsMetaValue::LIST_META_VALUE_DEL) {
                needed += sizeof(int32_t);
                dst = dst + sizeof(int32_t);
                EncodeFixed32(dst, count_);
            }
            return Slice(space_, needed);
        }
    private:
        char type_;
        int32_t index_;
        int32_t count_;
        char space_[10];
};
};

#endif
