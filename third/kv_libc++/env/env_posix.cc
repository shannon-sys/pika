#include "swift/shannon_db.h"
#include "swift/env.h"
#include <stdlib.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <string.h>

namespace shannon {
    class PosixEnv : public Env {
        public:
            PosixEnv() {  }
            virtual ~PosixEnv() {
            }
            virtual Status NewSequentialFile(const std::string& fname,
                                            unique_ptr<SequentialFile>* r,
                                            const EnvOptions& options) override {
                return Status();
            }
            virtual Status NewRandomAccessFile(const std::string& f,
                                        unique_ptr<RandomAccessFile>* r,
                                        const EnvOptions& options) override {
                return Status();
            }
            virtual Status NewWritableFile(const std::string& f, unique_ptr<WritableFile>* r,
                    const EnvOptions& options) override {
                return Status();
            }
            virtual Status ReopenWritableFile(const std::string& fname,
                                              unique_ptr<WritableFile>* result,
                                              const EnvOptions& options) override {
                return Status();
            }
            virtual Status ReuseWritableFile(const std::string& fname,
                                             const std::string& old_fname,
                                             unique_ptr<WritableFile>* r,
                                             const EnvOptions& options) override {
                return Status();
            }
            virtual Status NewRandomRWFile(const std::string& fname,
                                           unique_ptr<RandomRWFile>* result,
                                           const EnvOptions& options) override {
                return Status();
            }
            virtual Status NewDirectory(const std::string& name,
                    unique_ptr<Directory>* result) override {
                return Status();
            }
            virtual Status FileExists(const std::string& f) override {
                return Status();
            }
            virtual Status GetChildren(const std::string& dir,
                    std::vector<std::string>* r) override {
                return Status();
            }
            virtual Status GetChildrenFileAttributes(
                    const std::string& dir, std::vector<FileAttributes>* result) override {
                return Status();
            }
            virtual Status DeleteFile(const std::string& f) override {
                return Status();
            }
            virtual Status CreateDir(const std::string& d) override {
                return Status();
            }
            virtual Status CreateDirIfMissing(const std::string& d) override {
                return Status();
            }
            virtual Status DeleteDir(const std::string& d) override {
                return Status();
            }
            virtual Status GetFileSize(const std::string& f, uint64_t* s) override {
                return Status();
            }
            virtual Status GetFileModificationTime(const std::string& fname,
                    uint64_t *file_mtime) override {
                return Status();
            }
	        virtual Status RenameFile(const std::string& s, const std::string& t) override {
                return Status();
	        }
	        virtual Status LinkFile(const std::string& s, const std::string& t) override {
	            return Status();
	        }

	        virtual Status NumFileLinks(const std::string& fname, uint64_t* count) override {
	            return Status();
	        }

	        virtual Status AreFilesSame(const std::string& first, const std::string& second,
		                      bool* res) override {
	            return Status();
	        }

	        virtual Status LockFile(const std::string& f, FileLock** l) override {
	            return Status();
	        }

	        virtual Status UnlockFile(FileLock* l) override { return Status(); }

	        virtual void Schedule(void (*f)(void* arg), void* a, Priority pri,
		                void* tag = nullptr, void (*u)(void* arg) = nullptr) override {
	        }

	        virtual int UnSchedule(void* tag, Priority pri) override {
	            return 0;
	        }

	        virtual void StartThread(void (*f)(void*), void* a) override {
	        }
	        virtual void WaitForJoin() override { }
	        unsigned int GetThreadPoolQueueLen(Priority pri = LOW) const override {
	            return 0;
	        }
	        virtual Status GetTestDirectory(std::string* path) override {
	            return Status();
	        }
	        virtual Status NewLogger(const std::string& fname,
		                    shared_ptr<Logger>* result) override {
	            return Status();
	        }
	        virtual uint64_t NowMicros() override { return 0; }
	        virtual uint64_t NowNanos() override { return 0; }

	        virtual void SleepForMicroseconds(int micros) override {
	        }
	        virtual Status GetHostName(char* name, uint64_t len) override {
	            return Status();
	        }
	        virtual Status GetCurrentTime(int64_t* unix_time) override {
                if (unix_time == NULL)
                    return Status::InvalidArgument("unix_time is not null!");
                struct timeval tv;
                gettimeofday(&tv, NULL);
                //*unix_time = tv.tv_sec * 1000000 + tv.tv_usec;
                *unix_time = tv.tv_sec;
	            return Status();
	        }
	        virtual Status GetAbsolutePath(const std::string& db_path,
		                         std::string* output_path) override {
	            return Status();
	        }
	        virtual void SetBackgroundThreads(int num, Priority pri) override {
	        }
	        virtual int GetBackgroundThreads(Priority pri) override {
	            return 0;
	        }

	        virtual Status SetAllowNonOwnerAccess(bool allow_non_owner_access) override {
	            return Status();
	        }

	        virtual void IncBackgroundThreadsIfNeeded(int num, Priority pri) override {
	        }

	        virtual void LowerThreadPoolIOPriority(Priority pool = LOW) override {
	        }

	        virtual void LowerThreadPoolCPUPriority(Priority pool = LOW) override {
	        }

	        virtual std::string TimeToString(uint64_t time) override {
                char buffer[32];
                sprintf(buffer, "%lu", time);
	            return string(buffer);
	        }

	        virtual Status GetThreadList(std::vector<ThreadStatus>* thread_list) override {
	            return Status();
	        }

	        virtual ThreadStatusUpdater* GetThreadStatusUpdater() const override {
	            return NULL;
	        }

	        virtual uint64_t GetThreadID() const override {
	            return 0;
	        }

	        virtual std::string GenerateUniqueId() override {
	            return std::string("0");
	        }

	        virtual EnvOptions OptimizeForLogRead(const EnvOptions& env_options) const override {
	            return env_options;
	        }

	        virtual EnvOptions OptimizeForManifestRead(
	            const EnvOptions& env_options) const override {
	            return env_options;
	        }

	        virtual EnvOptions OptimizeForLogWrite(const EnvOptions& env_options,
		                         const DBOptions& db_options) const override {
	            return env_options;
	        }

	        virtual EnvOptions OptimizeForManifestWrite(
	            const EnvOptions& env_options) const override {
	            return env_options;
	        }

	        virtual EnvOptions OptimizeForCompactionTableWrite(
	            const EnvOptions& env_options,
	            const ImmutableDBOptions& immutable_ops) const override {
	            return env_options;
	        }

	        virtual EnvOptions OptimizeForCompactionTableRead(
	            const EnvOptions& env_options,
	            const ImmutableDBOptions& db_options) const override {
	            return env_options;
	        }
    };

    std::string Env::GenerateUniqueId() {
        return string("0");
    }

    Env* Env::Default() {
        static PosixEnv default_env;
        return &default_env;
    }
}

