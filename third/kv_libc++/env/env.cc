#include "swift/shannon_db.h"
#include "swift/env.h"
#include <thread>

namespace shannon {
    Env::~Env() {

    }

    uint64_t Env::GetThreadID() const {
        std::hash<std::thread::id> hasher;
        return hasher(std::this_thread::get_id());
    }

    Status Env::ReuseWritableFile(const std::string& fname,
                                  const std::string& old_fname,
                                  unique_ptr<WritableFile>* result,
                                  const EnvOptions& options) {
        return  Status();
    }

    Status Env::GetChildrenFileAttributes(const std::string& dir,
            std::vector<FileAttributes>* result) {
        return Status();
    }

    EnvOptions Env::OptimizeForLogRead(const EnvOptions& env_options) const {
        return env_options;
    }

    EnvOptions Env::OptimizeForCompactionTableRead(
            const EnvOptions& env_options, const ImmutableDBOptions& db_options) const {
        return env_options;
    }

    EnvOptions Env::OptimizeForLogWrite(const EnvOptions& env_options,
            const DBOptions& db_options) const {
        return env_options;
    }

    EnvOptions Env::OptimizeForCompactionTableWrite(
            const EnvOptions& env_options, const ImmutableDBOptions& db_options
            ) const {
        return env_options;
    }

    EnvOptions Env::OptimizeForManifestRead(const EnvOptions& env_options) const {
        return env_options;
    }

    EnvOptions Env::OptimizeForManifestWrite(const EnvOptions& env_options) const {
        return env_options;
    }
}
