#include "raiddb.h"

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

RaidDB::RaidDB(std::vector<std::pair<std::string, std::string>> store1,
               std::vector<std::pair<std::string, std::string>> store2)
    : _switch(0) {
  NewXdbEnv(&_env[0], "shadow1", store1);
  NewXdbEnv(&_env[1], "shadow2", store2);
}

Status RaidDB::OpenOrCreate(const std::string& name, Options& options) {
  options.env = _env[0];
  Status s = DB::Open(options, "was:" + name, &_db[0]);
  if (s.ok()) {
    options.env = _env[1];
    s = DB::Open(options, "was:" + name, &_db[1]);
  }
  return s;
}

Status RaidDB::Add(const std::vector<std::pair<Slice&, Slice&>> data) {
  WriteBatch batch;
  for (auto it = data.begin(); it < data.end(); it++) {
    batch.Put(it->first, it->second);
  }
  return _db[_switch]->Write(WriteOptions(), &batch);
}

std::vector<Status> RaidDB::Get(const std::vector<Slice>& keys,
                                std::vector<std::string>* values) {
  auto s = _db[0]->MultiGet(ReadOptions(), keys, values);
  return _db[1]->MultiGet(ReadOptions(), keys, values);
}

Status RaidDB::Delete() { return Status::OK(); }

int main() {}
