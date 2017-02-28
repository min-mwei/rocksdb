#pragma once
#include <stdio.h>
#include <time.h>
#include <algorithm>
#include <iostream>
#include "port/sys_time.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"

#include "was/blob.h"
#include "was/common.h"
#include "was/queue.h"
#include "was/storage_account.h"
#include "was/table.h"

namespace rocksdb {

class XdbWritableFile;
class XdbSequentialFile;

class EnvXdb : public EnvWrapper {
  friend class XdbWritableFile;
  friend class XdbSequentialFile;

 public:
  explicit EnvXdb(Env* env,
                  const std::vector<std::pair<std::string, std::string>>& dbpathmap);

  virtual Status NewWritableFile(const std::string& fname,
                                 unique_ptr<WritableFile>* result,
                                 const EnvOptions& options) override;

  virtual Status NewSequentialFile(const std::string& fname,
                                   std::unique_ptr<SequentialFile>* result,
                                   const EnvOptions& options) override;

  virtual Status NewRandomAccessFile(const std::string& fname,
                                     std::unique_ptr<RandomAccessFile>* result,
                                     const EnvOptions& options) override;

  virtual Status NewDirectory(const std::string& name,
                              unique_ptr<Directory>* result) override;

  virtual Status GetAbsolutePath(const std::string& db_path,
                                 std::string* output_path) override;

  virtual Status GetFileSize(const std::string& f, uint64_t* s) override;

  virtual Status FileExists(const std::string& fname) override;

  virtual Status RenameFile(const std::string& src,
                            const std::string& target) override;
  virtual Status GetChildren(const std::string& dir,
                             std::vector<std::string>* r) override;

  virtual Status DeleteFile(const std::string& f) override;

  virtual Status CreateDir(const std::string& d) override;

  virtual Status CreateDirIfMissing(const std::string& d) override;

  virtual Status DeleteDir(const std::string& d) override;

  virtual Status LockFile(const std::string& fname, FileLock** lock) override;

  virtual Status UnlockFile(FileLock* lock) override;

  virtual uint64_t GetThreadID() const override;

  virtual size_t GetUniqueId(char* id, size_t max_size);

  virtual Status NewLogger(const std::string& fname,
                           std::shared_ptr<Logger>* result) override;

  virtual ~EnvXdb() {}

 private:
  int WASRename(const std::string& src, const std::string& target);
  Status DeleteBlob(const std::string& f);
  bool isWAS(const std::string& name);
  azure::storage::cloud_blob_container& GetContainer(const std::string& name);

 private:
  std::map<std::string, azure::storage::cloud_blob_container> _containermap;
};
}
