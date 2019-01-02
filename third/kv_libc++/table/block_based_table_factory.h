//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <stdint.h>

#include <memory>
#include <string>

//#include "db/dbformat.h"
//#include "options/options_helper.h"
//#include "options/options_parser.h"
//#include "shannon/flush_block_policy.h"
#include "swift/table.h"

namespace shannon {

struct EnvOptions;

using std::unique_ptr;
class BlockBasedTableBuilder;

class BlockBasedTableFactory : public TableFactory {
};

#ifndef shannon_LITE

#endif  // !shannon_LITE
}  // namespace shannon
