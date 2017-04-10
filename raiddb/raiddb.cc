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
  // NewXdbEnv(&_env[0], "shadow1", store1);
  // NewXdbEnv(&_env[1], "shadow2", store2);
  NewXdbEnv(&_env[0], store1);
  NewXdbEnv(&_env[1], store2);
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

Status RaidDB::Add(
    const std::vector<std::pair<std::string, std::string>>& data) {
  WriteBatch batch;
  for (auto it = data.begin(); it < data.end(); it++) {
    const std::string& k = it->first;
    const std::string& v = it->second;
    batch.Put(Slice(k), Slice(v));
  }
  WriteOptions opts;
  Status s = _db[_switch]->Write(opts, &batch);
  rotate();
  if (!s.ok()) {
    std::cout << "write to backup: " << (int)_switch << std::endl;
    s = _db[_switch]->Write(opts, &batch);
  }
  return s;
}

std::vector<Status> RaidDB::Get(const std::vector<std::string>& keys,
                                std::vector<std::string>* values) {
  ReadOptions opts;
  std::vector<Slice> skeys;
  for (const std::string& s : keys) {
    skeys.push_back(Slice(s));
  }
  std::vector<std::string> values1;
  auto s = _db[0]->MultiGet(opts, skeys, &values1);
  std::vector<std::string> values2;
  s = _db[1]->MultiGet(opts, skeys, &values2);
  for (auto it1 = values1.begin(), it2 = values2.begin(); it1 < values1.end();
       it1++, it2++) {
    const std::string& s = *it1;
    values->push_back(s.size() > 0 ? s : (std::string&)*it2);
  }
  return s;
}

Status RaidDB::Delete() { return Status::OK(); }

void RaidDB::Flush() {
  FlushOptions opts;
  _db[0]->Flush(opts);
  _db[1]->Flush(opts);
}
