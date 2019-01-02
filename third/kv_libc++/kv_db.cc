#include <iostream>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include "venice_kv.h"
#include "venice_ioctl.h"
#include "swift/shannon_db.h"
#include "kv_impl.h"

using namespace std;
using shannon::Status;
using shannon::Options;

namespace shannon {
DB::~DB() {
  KVImpl *impl = reinterpret_cast<KVImpl *>(this);
  impl->Close();
}

Status DB::Open(const Options& options, const std::string& dbname,
		const std::string& device, DB** dbptr) {
  *dbptr = NULL;
  DBOptions db_options(options);
  KVImpl* impl = new KVImpl(db_options, dbname, device);
  impl->SetCfOptoions(options);
  Status s = impl->Open();
  if (s.ok()) {
    *dbptr = impl;
  } else {
    delete impl;
  }
  return s;
}

Status DB::Open(const DBOptions& db_options,
	const std::string& dbname, const std::string& device,
	const std::vector<ColumnFamilyDescriptor>& column_family_descriptor,
	std::vector<ColumnFamilyHandle*>* handles, DB** dbptr) {
    *dbptr = NULL;
    KVImpl* impl;
    impl = new KVImpl(Options(), dbname, device);
    Status s = impl->Open(db_options, column_family_descriptor, handles);
    if (s.ok()) {
      *dbptr = impl;
    } else {
      delete impl;
    }
	return s;
}

Status DB::ListColumnFamilies(const DBOptions& db_options,
                const std::string& name,
                const std::string& device,
                std::vector<std::string>* column_families) {
    Status s;
    struct cf_list list;
    struct kvdb_handle handle;
    int ret;
    int fd;

    if (name.length() >= DB_NAME_LEN) {
        return Status::InvalidArgument("database name is too longer.");
    }
    fd = open(device.data(), O_RDWR);
    if (fd < 0) {
        return Status::NotFound(strerror(errno));
    }
    /* open database */
    memset(&handle, 0, sizeof(handle));
    handle.flags = db_options.create_if_missing ? handle.flags | O_DB_CREATE : handle.flags;
    strncpy(handle.name, name.data(), name.length());
    ret = ioctl(fd, OPEN_DATABASE, &handle);
    if (ret < 0) {
        std::cout<<"ioctl open database failed!"<<std::endl;
        return Status::IOError(strerror(errno));
    }
    list.db_index = handle.db_index;
    ret = ioctl(fd, LIST_COLUMNFAMILY, &list);
    if (ret < 0) {
        std::cout<<"ioctl list columnfamily failed!"<<std::endl;
        return Status::IOError(strerror(errno));
    }
    for (int i = 0; i < list.cf_count; i ++) {
        column_families->push_back(std::string(list.cfs[i].name));
    }
    close(fd);
    return s;
}

}
