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
using random_bytes_engine =
    std::independent_bits_engine<std::default_random_engine, CHAR_BIT,
                                 unsigned int>;

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
  return result;
}

std::string createValue(Rand& rnd) {
  char ret[500];
  rnd.randBytes(ret, 500);
  std::string result(ret, 500);
  return result;
}

void batchInsert(DB* db, int size, Rand& rnd) {
  Status s;
  std::cout << "batch insert" << std::endl;
  auto opt = WriteOptions();
  for (int k = 0; k < size; k++) {
    //std::cout << "batch :" << k << std::endl;
    WriteBatch batch;
    for (int i = 0; i < 5000; i++) {
      auto k = createKey(rnd.randLong(), i);
      auto v = createValue(rnd);
      batch.Put(k, v);
    }
    int i = 3;
    while (i-- > 0) {
      s = db->Write(WriteOptions(), &batch);
      if (s.ok())
        break;
      else {
        std::cout << "batch insert:" << s.ToString() << std::endl;
      }
      int k = 1000000;
      while (k-- > 0)
        ;
    }
    assert(s.ok());
  }
}

int main(int argc, char* argv[]) {
  DB* db;
  Options options;
  options.IncreaseParallelism();
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = 4;
  options.write_buffer_size = (uint64_t)(4.0 * 1024 * 1024 * 1024);
  options.max_bytes_for_level_base = (uint64_t)(4.0 * 1024 * 1024 * 1024);
  options.level0_file_num_compaction_trigger = 4;
  options.level0_slowdown_writes_trigger = 50;
  options.min_write_buffer_number_to_merge = 8;
  options.max_write_buffer_number = 16;
  options.target_file_size_base = (int)(1.5 * 1024 * 1024 * 1024);
  options.max_subcompactions = 16;
  options.max_background_compactions = 32;
  options.max_background_flushes = 32;
  options.writable_file_max_buffer_size = (int)(512 * 1024 * 1024);
  options.base_background_compactions = 8;
  options.compaction_readahead_size = 1024 * 1024;
  options.OptimizeUniversalStyleCompaction(
      (uint64_t)(4.0 * 1024 * 1024 * 1024));
  // create the DB if it's not already present
  options.create_if_missing = true;

  Status status;
  // open DB
  options.wal_dir = "wal"; //argv[6];
  Status s = DB::Open(options, "db", &db);
  std::cout << "open: " << s.ToString() << std::endl;
  assert(s.ok());
  Rand rnd;
  batchInsert(db, 1000, rnd);
  std::cout << "flushing.." << std::endl;
  db->Flush(FlushOptions());
  delete db;
  return 0;
}
