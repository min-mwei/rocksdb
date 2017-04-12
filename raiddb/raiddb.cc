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

RaidDB::RaidDB(const std::vector<std::pair<std::string, std::string>>& store1,
               const std::vector<std::pair<std::string, std::string>>& store2)
    : _switch(0), _token(0) {
  NewXdbEnv(&_env[0], store1);
  NewXdbEnv(&_env[1], store2);
}

RaidDB::RaidDB(const std::vector<std::pair<std::string, std::string>>& store1,
               const std::string& shadow1,
               const std::vector<std::pair<std::string, std::string>>& store2,
               const std::string& shadow2)
    : _switch(0), _token(0) {
  NewXdbEnv(&_env[0], shadow1, store1);
  NewXdbEnv(&_env[1], shadow2, store2);
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
    const std::string& k = std::move(it->first);
    const std::string& v = std::move(it->second);
    batch.Put(Slice(k), Slice(v));
  }
  WriteOptions opts;
  Status s = _db[_switch]->Write(opts, &batch);
  rotate();
  if (!s.ok()) {
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

Status RaidDB::Seek(std::string keyprefix, std::string* token) {
  ReadOptions opts;
  Iterator* iter1 = _db[0]->NewIterator(opts);
  Iterator* iter2 = _db[1]->NewIterator(opts);
  if (iter1 == NULL && iter2 == NULL) {
    return Status::IOError();
  }
  Slice slice(keyprefix);
  if (iter1 != NULL) {
    iter1->Seek(slice);
  }
  if (iter1 != NULL) {
    iter2->Seek(slice);
  }
  if (!iter1->status().ok() && !iter2->status().ok()) {
    Status st = iter1->status();
    delete iter1;
    delete iter2;
    return st;
  }
  uint64_t num = _token++;
  _itermap[num] = std::make_pair(iter1, iter2);
  *token = std::to_string(num);
  return Status::OK();
}

int walk(Iterator* iter, int batchSize,
         std::vector<std::pair<std::string, std::string>>& kvs) {
  int count = 0;
  while (iter->Valid()) {
    if (count < batchSize) {
      kvs.push_back(
          std::make_pair(iter->key().ToString(), iter->value().ToString()));
    }
    count++;
    iter->Next();
  }
  return batchSize - count;
}

Status RaidDB::ScanPartialOrder(
    const std::string& token, int batchSize,
    std::vector<std::pair<std::string, std::string>>* data) {
  uint64_t num = std::stoll(token);
  auto iters = _itermap.find(num);
  if (iters != _itermap.end()) {
    std::vector<std::pair<std::string, std::string>> kvs;
    Iterator* iter1 = iters->second.first;
    int count = walk(iter1, batchSize, kvs);
    if (count > 0) {
      Iterator* iter2 = iters->second.second;
      count = walk(iter2, count, kvs);
    }
    *data = kvs;
  }
  return Status::OK();
}

Status RaidDB::Scan(const std::string& token, int batchSize,
                    std::vector<std::pair<std::string, std::string>>* data) {
  uint64_t num = std::stoll(token);
  auto iters = _itermap.find(num);
  if (iters != _itermap.end()) {
    std::vector<std::pair<std::string, std::string>> kvs;
    Iterator* iter1 = iters->second.first;
    Iterator* iter2 = iters->second.second;
    int count = 0;
    while (true) {
      if (count > batchSize) break;
      std::string k1;
      std::string k2;
      if (iter1->Valid()) {
        k1 = std::move(iter1->key().ToString());
      }
      if (iter2->Valid()) {
        k2 = std::move(iter2->key().ToString());
      }
      if (k1.empty() && k2.empty()) break;
      if (k1.empty()) {
        kvs.push_back(std::make_pair(k2, iter2->value().ToString()));
        iter2->Next();
      } else if (k2.empty()) {
        kvs.push_back(std::make_pair(k1, iter1->value().ToString()));
        iter1->Next();
      } else if (k1 <= k2) {
        kvs.push_back(std::make_pair(k1, iter1->value().ToString()));
        iter1->Next();
      } else {
        kvs.push_back(std::make_pair(k2, iter2->value().ToString()));
        iter2->Next();
      }
      count++;
    }
    *data = kvs;
  }
  return Status::OK();
}

void RaidDB::CloseScanToken(const std::string& token) {
  uint64_t num = std::stoll(token);
  auto iters = _itermap.find(num);
  if (iters != _itermap.end()) {
    delete iters->second.first;
    delete iters->second.second;
  }
}

void RaidDB::Close() {
  delete _db[0];
  delete _db[1];
  delete _env[0];
  delete _env[1];
}

void RaidDB::Flush() {
  FlushOptions opts;
  _db[0]->Flush(opts);
  _db[1]->Flush(opts);
}
