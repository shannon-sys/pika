// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/coding.h"
#include <stdlib.h>
#include <iostream>

namespace shannon {
void EncodeFloat32Sort(char *sorted_byte, float number) {
    void *addr = reinterpret_cast<void*>(&number);
    char *cs = reinterpret_cast<char*>(addr);
    int len = sizeof(number);
    if (number == (float)(0.0)) {
        sorted_byte[0] = 128;
        memset(sorted_byte+1, 0, sizeof(number)-1);
        return ;
    }
    if (kLittleEndian) {
        //change big endian
        int i = 0;
        while (i < len) {
            sorted_byte[len-i-1] = cs[i];
            i ++;
        }
    } else {
        memcpy(sorted_byte, cs, sizeof(float));
    }
    if (sorted_byte[0] & 128) {
        int i;
        for (i = 0; i < len; i ++) {
            sorted_byte[i] = sorted_byte[i] ^ 0xff;
        }
    } else {
       unsigned char exp = (sorted_byte[0]<<1) | ((sorted_byte[1]>>7)&1);
       exp += 1;
       sorted_byte[0] = exp>>1;
       sorted_byte[1] = (sorted_byte[1]&0x7f)|((exp&1)<<7);
       sorted_byte[0] |= (1 << 7);
    }
}
void DecodeFloat32Sort(const char *sorted_byte, float *number) {
	if (sorted_byte == NULL) {
        return;
    }
    // is zero
    if (sorted_byte[0] == char(128)) {
        bool is_zero = true;
        for (int i = 1; i < sizeof(float); i ++) {
            if (sorted_byte[i] != char(0)) {
                is_zero = false;
                break;
            }
        }
        if (is_zero) {
            *number = 0.0f;
            return;
        }
    }
    char *tmp = new char[sizeof(float)];
    memcpy(tmp, sorted_byte, sizeof(float));
    // no equal zero
    if (tmp[0]>>7 & 1 != 0) {
        // positive number
        unsigned char exp = (tmp[0]<<1) | ((tmp[1]>>7)&1);
        exp -= 1;
        tmp[0] = (exp>>1);
        tmp[1] = (tmp[1]&0x7f)|((exp&1)<<7);
        tmp[0] &= 0x7f;
    } else {
        // netavie number
        for (int i = 0; i < sizeof(float); ++ i) {
            tmp[i] = tmp[i] ^ 0xff;
        }
    }
    if (kLittleEndian) {
        int i = 0;
        while (i < sizeof(float)/2) {
            char t = tmp[i];
            tmp[i] = tmp[sizeof(float)-1-i];
            tmp[sizeof(float)-1-i] = t;
            ++ i;
        }
    }
    void *addr = reinterpret_cast<void*>(number);
    char *nr = reinterpret_cast<char*>(addr);
    memcpy(nr, tmp, sizeof(float));
    delete tmp;
}

void EncodeDouble64Sort(char *sorted_byte, double number) {
    unsigned char  *tmp = (unsigned char  *)sorted_byte;
    if (number == 0.0)
    {
        tmp[0] = (unsigned char )128;
        memset(tmp + 1, 0, sizeof(number) - 1);
    }
    else
    {
#ifdef WORDS_BIGENDIAN
        memcpy(tmp, &number, sizeof(number));
#else
        {
            unsigned char  *ptr = (unsigned char  *)&number;
#if defined(__FLOAT_WORD_ORDER) && (__FLOAT_WORD_ORDER == __BIG_ENDIAN)
            tmp[0] = ptr[3];
            tmp[1] = ptr[2];
            tmp[2] = ptr[1];
            tmp[3] = ptr[0];
            tmp[4] = ptr[7];
            tmp[5] = ptr[6];
            tmp[6] = ptr[5];
            tmp[7] = ptr[4];
#else
            tmp[0] = ptr[7];
            tmp[1] = ptr[6];
            tmp[2] = ptr[5];
            tmp[3] = ptr[4];
            tmp[4] = ptr[3];
            tmp[5] = ptr[2];
            tmp[6] = ptr[1];
            tmp[7] = ptr[0];
#endif
        }
#endif
        if (tmp[0] & 128)
        {
            uint i;
            for (i = 0; i < sizeof(number); i++)
                tmp[i] = tmp[i] ^ (unsigned char )255; //255=1<<8
        }
        else
        {
            ushort exp_part = (((ushort)tmp[0] << 8) | (ushort)tmp[1] |
                               (ushort)32768); //32768=2<<15
            exp_part += (ushort)1 << (16 - 1 - 11);
            tmp[0] = (unsigned char)(exp_part >> 8);
            tmp[1] = (unsigned char)exp_part;
        }
    }
}

void DecodeDouble64Sort(const char *sorted_byte, double *number) {
    char tmp[8];
    memcpy(tmp, sorted_byte, sizeof(double));
    bool flag = true;
    if (tmp[0] == (char)-128) {
        for (int i = 1;i < 8;i ++) {
            if (tmp[i] != char(0)) {
               flag = false ;
                break;
            }
        }
        if (flag) {
            *number = 0.0;
            return;
        }
    }
        if (((tmp[0]>>7)&1) != 1)
        {
            uint i;
            for (i = 0; i < sizeof(double); i++)
                tmp[i] = tmp[i] ^ 0xff;//255=1<<8-1
        }
        else
        {
            ushort exp_part = (((ushort)tmp[0] << 8) | ((ushort)tmp[1]&0x00ff)); //32768=2<<15
            exp_part -= (ushort)1 << (16 - 1 - 11);
            tmp[0] = (unsigned char)(exp_part >> 8);
            tmp[1] = (unsigned char)exp_part;
            tmp[0] &= 0x7f;
        }

        unsigned char ptr[8] ;
        memcpy(ptr, tmp, sizeof(double));
#ifdef WORDS_BIGENDIAN
             memcpy(number, tmp, sizeof(double));
#else
#if defined(__FLOAT_WORD_ORDER) && (__FLOAT_WORD_ORDER == __BIG_ENDIAN)
            tmp[0] = ptr[3];
            tmp[1] = ptr[2];
            tmp[2] = ptr[1];
            tmp[3] = ptr[0];
            tmp[4] = ptr[7];
            tmp[5] = ptr[6];
            tmp[6] = ptr[5];
            tmp[7] = ptr[4];
            memcpy(number, tmp, sizeof(double));
#else
            tmp[0] = ptr[7];
            tmp[1] = ptr[6];
            tmp[2] = ptr[5];
            tmp[3] = ptr[4];
            tmp[4] = ptr[3];
            tmp[5] = ptr[2];
            tmp[6] = ptr[1];
            tmp[7] = ptr[0];
            memcpy(number, tmp, sizeof(double));
#endif
#endif
}

void EncodeFixed32(char* buf, uint32_t value) {
  if (kLittleEndian) {
    memcpy(buf, &value, sizeof(value));
  } else {
    buf[0] = value & 0xff;
    buf[1] = (value >> 8) & 0xff;
    buf[2] = (value >> 16) & 0xff;
    buf[3] = (value >> 24) & 0xff;
  }
}

void EncodeFixed64(char* buf, uint64_t value) {
  if (kLittleEndian) {
    memcpy(buf, &value, sizeof(value));
  } else {
    buf[0] = value & 0xff;
    buf[1] = (value >> 8) & 0xff;
    buf[2] = (value >> 16) & 0xff;
    buf[3] = (value >> 24) & 0xff;
    buf[4] = (value >> 32) & 0xff;
    buf[5] = (value >> 40) & 0xff;
    buf[6] = (value >> 48) & 0xff;
    buf[7] = (value >> 56) & 0xff;
  }
}

void PutFixed32(std::string* dst, uint32_t value) {
  char buf[sizeof(value)];
  EncodeFixed32(buf, value);
  dst->append(buf, sizeof(buf));
}

void PutFixed64(std::string* dst, uint64_t value) {
  char buf[sizeof(value)];
  EncodeFixed64(buf, value);
  dst->append(buf, sizeof(buf));
}

char* EncodeVarint32(char* dst, uint32_t v) {
  // Operate on characters as unsigneds
  unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
  static const int B = 128;
  if (v < (1<<7)) {
    *(ptr++) = v;
  } else if (v < (1<<14)) {
    *(ptr++) = v | B;
    *(ptr++) = v>>7;
  } else if (v < (1<<21)) {
    *(ptr++) = v | B;
    *(ptr++) = (v>>7) | B;
    *(ptr++) = v>>14;
  } else if (v < (1<<28)) {
    *(ptr++) = v | B;
    *(ptr++) = (v>>7) | B;
    *(ptr++) = (v>>14) | B;
    *(ptr++) = v>>21;
  } else {
    *(ptr++) = v | B;
    *(ptr++) = (v>>7) | B;
    *(ptr++) = (v>>14) | B;
    *(ptr++) = (v>>21) | B;
    *(ptr++) = v>>28;
  }
  return reinterpret_cast<char*>(ptr);
}

void PutVarint32(std::string* dst, uint32_t v) {
  char buf[5];
  char* ptr = EncodeVarint32(buf, v);
  dst->append(buf, ptr - buf);
}

char* EncodeVarint64(char* dst, uint64_t v) {
  static const int B = 128;
  unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
  while (v >= B) {
    *(ptr++) = (v & (B-1)) | B;
    v >>= 7;
  }
  *(ptr++) = static_cast<unsigned char>(v);
  return reinterpret_cast<char*>(ptr);
}

void PutVarint64(std::string* dst, uint64_t v) {
  char buf[10];
  char* ptr = EncodeVarint64(buf, v);
  dst->append(buf, ptr - buf);
}

void PutLengthPrefixedSlice(std::string* dst, const Slice& value) {
  PutVarint32(dst, value.size());
  dst->append(value.data(), value.size());
}

void PutSliceData(std::string* dst, const Slice& value) {
  dst->append(value.data(), value.size());
}

int VarintLength(uint64_t v) {
  int len = 1;
  while (v >= 128) {
    v >>= 7;
    len++;
  }
  return len;
}

const char* GetVarint32PtrFallback(const char* p,
                                   const char* limit,
                                   uint32_t* value) {
  uint32_t result = 0;
  for (uint32_t shift = 0; shift <= 28 && p < limit; shift += 7) {
    uint32_t byte = *(reinterpret_cast<const unsigned char*>(p));
    p++;
    if (byte & 128) {
      // More bytes are present
      result |= ((byte & 127) << shift);
    } else {
      result |= (byte << shift);
      *value = result;
      return reinterpret_cast<const char*>(p);
    }
  }
  return NULL;
}

bool GetVarint32(Slice* input, uint32_t* value) {
  const char* p = input->data();
  const char* limit = p + input->size();
  const char* q = GetVarint32Ptr(p, limit, value);
  if (q == NULL) {
    return false;
  } else {
    *input = Slice(q, limit - q);
    return true;
  }
}

const char* GetVarint64Ptr(const char* p, const char* limit, uint64_t* value) {
  uint64_t result = 0;
  for (uint32_t shift = 0; shift <= 63 && p < limit; shift += 7) {
    uint64_t byte = *(reinterpret_cast<const unsigned char*>(p));
    p++;
    if (byte & 128) {
      // More bytes are present
      result |= ((byte & 127) << shift);
    } else {
      result |= (byte << shift);
      *value = result;
      return reinterpret_cast<const char*>(p);
    }
  }
  return NULL;
}

bool GetVarint64(Slice* input, uint64_t* value) {
  const char* p = input->data();
  const char* limit = p + input->size();
  const char* q = GetVarint64Ptr(p, limit, value);
  if (q == NULL) {
    return false;
  } else {
    *input = Slice(q, limit - q);
    return true;
  }
}

const char* GetLengthPrefixedSlice(const char* p, const char* limit,
                                   Slice* result) {
  uint32_t len;
  p = GetVarint32Ptr(p, limit, &len);
  if (p == NULL) return NULL;
  if (p + len > limit) return NULL;
  *result = Slice(p, len);
  return p + len;
}

bool GetLengthPrefixedSlice(Slice* input, Slice* result) {
  uint32_t len;
  if (GetVarint32(input, &len) &&
      input->size() >= len) {
    *result = Slice(input->data(), len);
    input->remove_prefix(len);
    return true;
  } else {
    return false;
  }
}

}  // namespace shannon
