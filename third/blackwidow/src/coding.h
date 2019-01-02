//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_CODING_H_
#define SRC_CODING_H_

#undef BLACKWIDOW_PLATFORM_IS_LITTLE_ENDIAN
#ifndef BLACKWIDOW_PLATFORM_IS_LITTLE_ENDIAN
#define BLACKWIDOW_PLATFORM_IS_LITTLE_ENDIAN (__BYTE_ORDER == __LITTLE_ENDIAN)
#endif
#include <string.h>
#include <iostream>

namespace blackwidow {
  static const bool kLittleEndian = BLACKWIDOW_PLATFORM_IS_LITTLE_ENDIAN;
#undef BLACKWIDOW_PLATFORM_IS_LITTLE_ENDIAN

inline void EncodeFloat32Sort(char *sorted_byte, float number) {
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
inline void DecodeFloat32Sort(const char *sorted_byte, float *number) {
	if (sorted_byte == NULL) {
        return;
    }
    // is zero
    if (sorted_byte[0] == char(128)) {
        bool is_zero = true;
        for (unsigned int i = 1; i < sizeof(float); i ++) {
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
    if ((tmp[0]>>7 & 1 )!= 0) {
        // positive number
        unsigned char exp = (tmp[0]<<1) | ((tmp[1]>>7)&1);
        exp -= 1;
        tmp[0] = (exp>>1);
        tmp[1] = (tmp[1]&0x7f)|((exp&1)<<7);
        tmp[0] &= 0x7f;
    } else {
        // netavie number
        for (unsigned int i = 0; i < sizeof(float); ++ i) {
            tmp[i] = tmp[i] ^ 0xff;
        }
    }
    if (kLittleEndian) {
        unsigned int i = 0;
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

inline void EncodeDouble64Sort(char *sorted_byte, double number) {
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

inline void DecodeDouble64Sort(const char *sorted_byte, double *number) {
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

inline void EncodeBigFixed32(char *buf, uint32_t value) {
    if (kLittleEndian) {
        buf[0] = (value >> 24) & 0xff;
        buf[1] = (value >> 16) & 0xff;
        buf[2] = (value >> 8) & 0xff;
        buf[3] = value & 0xff;
    } else {
        memcpy(buf, &value, sizeof(value));
    }
}

inline void EncodeBigFixed64(char *buf, uint64_t value) {
    if (kLittleEndian) {
        buf[0] = (value >> 56) & 0xff;
        buf[1] = (value >> 48) & 0xff;
        buf[2] = (value >> 40) & 0xff;
        buf[3] = (value >> 32) & 0xff;
        buf[4] = (value >> 24) & 0xff;
        buf[5] = (value >> 16) & 0xff;
        buf[6] = (value >> 8) & 0xff;
        buf[7] = value & 0xff;
    } else {
        memcpy(buf, &value, sizeof(value));
    }
}

inline uint32_t DecodeBigFixed32(const char* ptr) {
    if (kLittleEndian) {
        return ((static_cast<uint32_t>(static_cast<unsigned char>(ptr[0])) << 24)
            |  (static_cast<uint32_t>(static_cast<unsigned char>(ptr[1])) << 16)
            |  (static_cast<uint32_t>(static_cast<unsigned char>(ptr[2])) << 8)
            |  (static_cast<uint32_t>(static_cast<unsigned char>(ptr[3]))));
    } else {
        uint32_t result;
        memcpy(&result, ptr, sizeof(result));
        return result;
    }
}

inline uint64_t DecodeBigFixed64(const char* ptr) {
    if (kLittleEndian) {
        return ((static_cast<uint64_t>(static_cast<unsigned char>(ptr[0])) << 56)
            |  (static_cast<uint64_t>(static_cast<unsigned char>(ptr[1])) << 48)
            |  (static_cast<uint64_t>(static_cast<unsigned char>(ptr[2])) << 40)
            |  (static_cast<uint64_t>(static_cast<unsigned char>(ptr[3])) << 32)
            |  (static_cast<uint64_t>(static_cast<unsigned char>(ptr[4])) << 24)
            |  (static_cast<uint64_t>(static_cast<unsigned char>(ptr[5])) << 16)
            |  (static_cast<uint64_t>(static_cast<unsigned char>(ptr[6])) << 8)
            |  (static_cast<uint64_t>(static_cast<unsigned char>(ptr[7]))));
    } else {
        uint64_t result;
        memcpy(&result, ptr, sizeof(result));
        return result;
    }
}

inline void EncodeFixed32(char* buf, uint32_t value) {
  if (kLittleEndian) {
    memcpy(buf, &value, sizeof(value));
  } else {
    buf[0] = value & 0xff;
    buf[1] = (value >> 8) & 0xff;
    buf[2] = (value >> 16) & 0xff;
    buf[3] = (value >> 24) & 0xff;
  }
}

inline void EncodeFixed64(char* buf, uint64_t value) {
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

inline uint32_t DecodeFixed32(const char* ptr) {
  if (kLittleEndian) {
    // Load the raw bytes
    uint32_t result;
    memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
    return result;
  } else {
    return ((static_cast<uint32_t>(static_cast<unsigned char>(ptr[0])))
        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[1])) << 8)
        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[2])) << 16)
        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[3])) << 24));
  }
}

inline uint64_t DecodeFixed64(const char* ptr) {
  if (kLittleEndian) {
    // Load the raw bytes
    uint64_t result;
    memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
    return result;
  } else {
    uint64_t lo = DecodeFixed32(ptr);
    uint64_t hi = DecodeFixed32(ptr + 4);
    return (hi << 32) | lo;
  }
}

}  // namespace blackwidow
#endif  // SRC_CODING_H_
