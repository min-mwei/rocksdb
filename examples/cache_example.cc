// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <cstdio>
#include <string>

#include <functional>
#include <iostream>
#include <memory>
#include <thread>
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/persistent_cache.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"
#include "util/mutexlock.h"

using namespace rocksdb;

std::string kDBPath = "./acme_data";

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

std::string PaddedNumber(const size_t data, const size_t pad_size) {
  assert(pad_size);
  char* ret = new char[pad_size];
  int pos = static_cast<int>(pad_size) - 1;
  size_t count = 0;
  size_t t = data;
  // copy numbers
  while (t) {
    count++;
    ret[pos--] = '0' + t % 10;
    t = t / 10;
  }
  // copy 0s
  while (pos >= 0) {
    ret[pos--] = '0';
  }
  // post condition
  assert(count <= pad_size);
  assert(pos == -1);
  std::string result(ret, pad_size);
  delete[] ret;
  return result;
}

const std::string kprefix = "key_prefix_";
const std::string vprefix = "value_prefix_";

void insert(DB* db, int size) {
  Status s;
  auto opt = WriteOptions();
  for (int i = 0; i < size; i++) {
    auto k = kprefix + PaddedNumber(i, 8);
    auto v = vprefix + PaddedNumber(i, 8);
    s = db->Put(opt, k, v);
  }
}

void read(DB* db) {
  std::string value;
  // get value
  auto opt = ReadOptions();
  opt.fill_cache = true;
  for (int i = 0; i < 5; i++) {
    auto k = kprefix + PaddedNumber(i, 8);
    Status s = db->Get(opt, k, &value);
    assert(s.ok());
    std::cout << "value " << value << std::endl;
  }
}

void update(DB* db) {
  Status s;
  auto opt = WriteOptions();

  WriteBatch batch;
  auto k = kprefix + PaddedNumber(1, 8);
  //batch.Delete(k);
  k = kprefix + PaddedNumber(2, 8);
  auto v = vprefix + PaddedNumber(200, 8);
  batch.Put(k, v);
  s = db->Write(WriteOptions(), &batch);
  assert(s.ok());
}

int main() {
  DB* db;
  Options options;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options.create_if_missing = true;

  Status status;
  uint64_t cache_size = 1024 * 1024 * 1024;
  std::string cache_path = "./acme_cache";
  auto log = std::make_shared<ConsoleLogger>();
  std::shared_ptr<PersistentCache> cache;
  status = NewPersistentCache(Env::Default(), cache_path,
                              /*size=*/cache_size, log, true, &cache);
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
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  insert(db, 10000000);
  read(db);
  //std::cout << "pcache opts: " << cache->GetPrintableOptions() << std::endl;
  read(db);
  //std::cout << "pcache opts: " << cache->GetPrintableOptions() << std::endl;
  update(db);
  read(db);
  /*
  // atomically apply a set of updates
  {
    WriteBatch batch;
    batch.Delete("key1");
    batch.Put("key2", value);
    s = db->Write(WriteOptions(), &batch);
  }

  s = db->Get(ReadOptions(), "key1", &value);
  assert(s.IsNotFound());

  db->Get(ReadOptions(), "key2", &value);
  assert(value == "valuex");
  */
  delete db;

  return 0;
}
