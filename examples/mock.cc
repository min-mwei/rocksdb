// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <cstdio>
#include <string>

#include <stdio.h>
#include <stdlib.h>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <random>
#include <thread>
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/persistent_cache.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"
#include "util/mutexlock.h"

using namespace rocksdb;
using random_bytes_engine =
    std::independent_bits_engine<std::default_random_engine, CHAR_BIT,
                                 unsigned char>;

class ConsoleLogger : public Logger {
 public:
  using Logger::Logv;
  ConsoleLogger() : Logger(InfoLogLevel::ERROR_LEVEL) {}

  void Logv(const char* format, va_list ap) override {
    MutexLock _(&lock_);
    vprintf(format, ap);
    printf("\n");
  }

  port::Mutex lock_;
};

class Rand {
 public:
  Rand() : mt_(rd_()), dist_(1.0, 1e10) {}

  uint64_t randLong() { return static_cast<uint64_t>(dist_(mt_)); }

  void randBytes(char* bytes, int len) {
    std::generate(bytes, bytes + len, std::ref(rbe_));
  }

 private:
  std::random_device rd_;
  random_bytes_engine rbe_;
  std::mt19937_64 mt_;
  std::uniform_real_distribution<double> dist_;
};

std::string createKey(uint64_t key, int ts) {
  std::string result = std::to_string(key).append(std::to_string(ts));
  std::cout << "key: " << result << std::endl;
  return result;
}

std::string createValue(Rand& rnd) {
  char ret[300];
  rnd.randBytes(ret, 300);
  std::string result(ret, 300);
  std::cout << "value: " << result << std::endl;
  return result;
}

void batchInsert(DB* db, int size, Rand& rnd) {
  Status s;
  std::cout<<"batch insert" << std::endl;
  auto opt = WriteOptions();
  for (int k = 0; k < size; k++) {
    std::cout << "batch :" << k << std::endl;
    WriteBatch batch;
    for (int i = 0; i < 10; i++) {
      auto k = createKey(rnd.randLong(), i);
      auto v = createValue(rnd);
      batch.Put(k, v);
    }
    s = db->Write(WriteOptions(), &batch);
    if (!s.ok()) {
      std::cout << "batch insert:" << s.ToString() << std::endl;
    }
    assert(s.ok());
    db->Flush(FlushOptions());
  }
}

void read(DB* db) {
  std::string value;
  // get value
  auto opt = ReadOptions();
  opt.fill_cache = true;
  for (int i = 0; i < 5; i++) {
    //auto k = kprefix + PaddedNumber(i, 8);
    //Status s = db->Get(opt, k, &value);
    //assert(s.ok());
    //std::cout << "value " << value << std::endl;
  }
}

int main(int argc, char* argv[]) {
  if (argc < 4) {
    std::cout << " program conn container dbname cachepath" << std::endl;
    exit(-1);
  }
  DB* db;
  Options options;
  Env* env;
  std::vector<std::pair<std::string, std::string>> dbpathmap;
  std::string conn = argv[1];
  std::string container = argv[2];
  dbpathmap.push_back(std::make_pair(conn, container));
  NewXdbEnv(&env, dbpathmap);
  //options.env = env;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options.create_if_missing = true;

  Status status;
  uint64_t cache_size = 1024 * 1024 * 1024;
  std::cout<<"****000" << argv[4] << std::endl;
  std::string cache_path = argv[4];
  auto log = std::make_shared<ConsoleLogger>();
  std::shared_ptr<PersistentCache> cache;
  std::cout<<"****001xxx" << std::endl;
  status = NewPersistentCache(Env::Default(), cache_path,
                              /*size=*/cache_size, log, true, &cache);
  std::cout<<"****001" << std::endl;
  assert(status.ok());
  BlockBasedTableOptions table_options;
  table_options.persistent_cache = cache;
  table_options.cache_index_and_filter_blocks = true;
  // table_options.block_cache = NewLRUCache(100);
  // table_options.block_cache_compressed = nullptr;

  // bbt_opts.block_size = 32 * 1024;
  // bbt_opts.block_cache = cache;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  // open DB
  std::cout<<"****1" << std::endl;
  Status s = DB::Open(options, argv[3], &db);
  assert(s.ok());

  std::cout<<"****3" << std::endl;
  Rand rnd;
  batchInsert(db, 5, rnd);
  db->Flush(FlushOptions());
  read(db);
  delete db;

  return 0;
}
