#ifndef KV_IMPL_INCLUDE_H
#define KV_IMPL_INCLUDE_H
#include <deque>
#include <set>
#include "swift/shannon_db.h"
#include "venice_kv.h"
#include "snapshot.h"
#include "column_family.h"

namespace shannon {
class KVImpl : public DB {
 public:
  KVImpl(const DBOptions& options, const std::string& dbname,const std::string& device);

  virtual ~KVImpl();
  virtual Status Put(const WriteOptions&, const Slice& key, const Slice& value);
  virtual Status Delete(const WriteOptions&, const Slice& key);
  virtual Status Write(const WriteOptions& options, WriteBatch* updates);
  virtual Status Get(const ReadOptions& options, const Slice& key, std::string* value);
  virtual const Snapshot* GetSnapshot();
  virtual Status ReleaseSnapshot(const Snapshot* snapshot);
  virtual Iterator* NewIterator(const ReadOptions&);
  // ColumnFamily Interface
  virtual Status CreateColumnFamily(const ColumnFamilyOptions& options,
				const std::string& column_family_name,
				ColumnFamilyHandle** handle) override;
  virtual Status CreateColumnFamilies(const ColumnFamilyOptions& options,
			const std::vector<std::string>& column_family_names,
			std::vector<ColumnFamilyHandle*>* handles) override;
  virtual Status CreateColumnFamilies(
		const std::vector<ColumnFamilyDescriptor>& column_families,
		std::vector<ColumnFamilyHandle*>* handles) override;
  virtual Status DropColumnFamily(ColumnFamilyHandle* column_family) override;
  virtual Status DropColumnFamilies(
		const std::vector<ColumnFamilyHandle*>& column_families) override;
  virtual Status DestroyColumnFamilyHandle(ColumnFamilyHandle* column_family) override;
  virtual Status Put(const WriteOptions& options,
		ColumnFamilyHandle* column_family, const Slice& key,
		const Slice& value) override;
  virtual Status Delete(const WriteOptions& options,
		ColumnFamilyHandle* column_family, const Slice& key) override;
  virtual Status Get(const ReadOptions& options,
		ColumnFamilyHandle* column_family, const Slice& key,
		std::string* value) override;
  virtual Iterator* NewIterator(const ReadOptions& options,
		ColumnFamilyHandle* column_family) override;
  virtual Status NewIterators(const ReadOptions& options,
		const std::vector<ColumnFamilyHandle*>& column_families,
		std::vector<Iterator*>* iterators) override;
  virtual ColumnFamilyHandle* DefaultColumnFamily() const override;

  virtual Status status() const {
      return status_;
  }
  virtual Env* GetEnv() const override;
  virtual Status CompactRange(const CompactRangeOptions& options,
                              ColumnFamilyHandle* column_family,
                              const Slice* begin, const Slice* end) override;
  virtual bool GetProperty(ColumnFamilyHandle* column_family,
                             const Slice& property, std::string* value) override;
  virtual SequenceNumber GetLatestSequenceNumber() const override;
  virtual Status DisableFileDeletions() override;
  virtual Status GetLiveFiles(std::vector<std::string>& vec,
                              uint64_t* manifest_file_size,
                              bool flush_memtable = true) override;
  virtual Status EnableFileDeletions(bool force = true) override;
  virtual Status GetSortedWalFiles(VectorLogPtr& files) override;
  virtual Options GetOptions(ColumnFamilyHandle* column_family) const override;
  virtual const std::string& GetName() const override;
   void SetCfOptoions(const Options & options){
            cf_options_ = options;
   }
 protected:
  Env* const env_;
 private:
  friend class DB;
  friend class KVIter;
  Status status_;
  int fd_;
  int db_;
  //Env* const env_;
  ColumnFamilyOptions cf_options_ ;
  const DBOptions options_;  // options_.comparator == &internal_comparator_
  const std::string dbname_;
  const std::string device_;
  ColumnFamilyHandleImpl* default_cf_handle_; // default column_family handle
  Status Open();
  Status Open(const DBOptions& db_options,
          const std::vector<ColumnFamilyDescriptor>& column_families,
          std::vector<ColumnFamilyHandle*>* handles);
  void Close();
  KVImpl(const KVImpl&);
  void operator=(const KVImpl&);
};

}
#endif
