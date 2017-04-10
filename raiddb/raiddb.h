#include <cstdio>
#include <string>

#include <stdio.h>
#include <stdlib.h>
#include <algorithm>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <random>
#include <thread>
#include <vector>
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/persistent_cache.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"
#include "util/mutexlock.h"

using namespace rocksdb;

class RaidDB {
 public:
  RaidDB(std::vector<std::pair<std::string, std::string>> store1,
         std::vector<std::pair<std::string, std::string>> store2);

  /** returns a RocksDB object */
  Status OpenOrCreate(const std::string& name, Options& options);

  /** delete rocksdb */
  Status Delete();

  /** Write to one storage account or another  */
  Status Add(const std::vector<std::pair<Slice, Slice>>& data);

  /** Read key, read from db1 and db2, merge the results.*/
  std::vector<Status> Get(const std::vector<Slice>& keys,
                          std::vector<std::string>* values);

  void Flush();
#if 0
  /** opens two iterators if possible under the cover */
  Status GetScanToken(StartKey, **token);

  /** Scan from db1 and db2, merge the results.  */
  Status Scan(token, int batchsize, **vector<pair<Key, Value>>);

  /** release the two iterators */
  Status CloseScanToken(const std::string& token);
#endif

 private:
  void rotate() {
    _switch = 1 - _switch;
  }
 private:
  DB*_db[2];
  Env*_env[2];
  char _switch;
};
