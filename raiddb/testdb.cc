#include "raiddb.h"

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

void setOptions(Options& options) {
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
  options.writable_file_max_buffer_size = (int)(1.5 * 1024 * 1024 * 1024);
  options.base_background_compactions = 8;
  options.OptimizeUniversalStyleCompaction(
      (uint64_t)(4.0 * 1024 * 1024 * 1024));
  options.create_if_missing = true;
}

const std::string kprefix = "key_prefix_";
const std::string vprefix = "value_prefix_";

void batchInsert(RaidDB& db, int start, int size) {
  std::vector<std::pair<std::string, std::string>> data;
  for (int i = start; i < start + size; i++) {
    auto k = kprefix + PaddedNumber(i, 8);
    auto v = vprefix + PaddedNumber(i, 8);
    data.push_back(std::make_pair(k, v));
  }
  db.Add(data);
}

void dump(std::vector<std::string> values) {
  for (const std::string& s : values) {
    std::cout << "v: " << s << std::endl;
  }
}

void dump(std::vector<Slice> values) {
  for (const Slice& s : values) {
    std::cout << "d: " << s.ToString() << std::endl;
  }
}

void dump(std::vector<std::pair<std::string, std::string>> data) {
  for (auto p : data) {
    std::cout << "k: " << p.first << " v: " << p.second << std::endl;
  }
}

void read(RaidDB& db) {
  std::vector<std::string> keys;
  keys.push_back(kprefix + PaddedNumber(0, 8));
  keys.push_back(kprefix + PaddedNumber(5, 8));
  keys.push_back(kprefix + PaddedNumber(10, 8));
  keys.push_back(kprefix + PaddedNumber(15, 8));
  keys.push_back(kprefix + PaddedNumber(20, 8));
  std::vector<std::string> values;
  db.Get(keys, &values);
  dump(values);
}

int main(int argc, char* argv[]) {
  if (argc < 5) {
    std::cout << " program dbconn1 dbcontainer1 dbconn2 dbcontainer2 dbname"
              << std::endl;
    exit(-1);
  }
  std::string dbconn1 = argv[1];
  std::string dbcontainer1 = argv[2];
  std::string dbconn2 = argv[3];
  std::string dbcontainer2 = argv[4];

  std::vector<std::pair<std::string, std::string>> store1;
  std::vector<std::pair<std::string, std::string>> store2;
  store1.push_back(std::make_pair(dbconn1, dbcontainer1));
  store2.push_back(std::make_pair(dbconn2, dbcontainer2));

  RaidDB raiddb(store1, store2);
  Options options;
  setOptions(options);
  Status s = raiddb.OpenOrCreate(argv[5], options);
  assert(s.ok());

  for (int i = 0; i < 6; i++) {
    batchInsert(raiddb, i * 5, 5);
  }
  read(raiddb);
  std::string token;
  raiddb.Seek(kprefix, &token);
  std::cout << "seek token: " << token << std::endl;
  std::vector<std::pair<std::string, std::string>> data;
  std::cout << "partial order scan" << std::endl;
  raiddb.ScanPartialOrder(token, 100, &data);
  raiddb.CloseScanToken(token);
  dump(data);
  data.clear();
  std::cout << "total order scan" << std::endl;
  raiddb.Seek(kprefix, &token);
  raiddb.Scan(token, 100, &data);
  dump(data);
  raiddb.CloseScanToken(token);
  raiddb.Close();
  return 0;
}
