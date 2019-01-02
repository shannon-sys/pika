//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/block_based_table_factory.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <stdint.h>

#include <memory>
#include <string>

//#include "options/options_helper.h"
//#include "port/port.h"
#include "swift/cache.h"
//#include "rocksdb/convenience.h"
//#include "rocksdb/flush_block_policy.h"
//#include "table/block_based_table_builder.h"
//#include "table/block_based_table_reader.h"
//#include "table/format.h"
//#include "util/string_util.h"

namespace shannon {
    TableFactory* NewBlockBasedTableFactory(
            const BlockBasedTableOptions& _table_options) {
        return NULL;
    }
#ifndef SHANNONDB_LITE
namespace {

}  // namespace

#endif  // !SHANNONDB_LITE

}  // namespace shannon
