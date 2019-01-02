#ifndef SHANNON_DB_INCLUDE_SNAPSHOT_H_
#define SHANNON_DB_INCLUDE_SNAPSHOT_H_

#include "swift/shannon_db.h"

namespace shannon {

class SnapshotImpl : public Snapshot {
 public:
  uint64_t timestamp_;  // const after creation

  void Delete(const SnapshotImpl* s) {
    delete s;
  }

  //virtual ~SnapshotImpl() {}
};

}

#endif  // SHANNON_DB_INCLUDE_SNAPSHOT_H_
