#include <cstdio>
#include <string>

#include <stdio.h>
#include <stdlib.h>
#include <algorithm>
#include <cstdlib>
#include <functional>
#include <iostream>
//#include <atomic.h>
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
  RaidDB(const std::vector<std::pair<std::string, std::string>>& store1,
         const std::vector<std::pair<std::string, std::string>>& store2);

  RaidDB(const std::vector<std::pair<std::string, std::string>>& store1,
         const std::string& shadow1,
         const std::vector<std::pair<std::string, std::string>>& store2,
         const std::string& shadow2);

  Status OpenOrCreate(const std::string& name, Options& options);

  void Close();

  Status Add(const std::vector<std::pair<std::string, std::string>>& data);

  std::vector<Status> Get(const std::vector<std::string>& keys,
                          std::vector<std::string>* values);

  void Flush();

  Status Seek(std::string keyprefix, std::string* token);

  Status Scan(const std::string& token, int batchsize,
              std::vector<std::pair<std::string, std::string>>* data);

  void CloseScanToken(const std::string& token);

 private:
  void rotate() { _switch = 1 - _switch; }

 private:
  DB* _db[2];
  Env* _env[2];
  unsigned char _switch;
  std::atomic_uint_fast64_t _token;
  std::map<uint64_t, std::pair<Iterator*, Iterator*>> _itermap;
};
