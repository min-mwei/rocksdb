#include "raiddb.h"
#define SERVERDLL_API __declspec(dllexport)

std::vector<std::string> v;

extern "C" SERVERDLL_API RaidDB* CreateRaidDB(const char* conn1,
                                              const char* container1,
                                              const char* conn2,
                                              const char* container2) {
  std::string dbconn1 = conn1;
  std::string dbcontainer1 = container1;
  std::string dbconn2 = conn2;
  std::string dbcontainer2 = container2;

  std::vector<std::pair<std::string, std::string>> store1;
  std::vector<std::pair<std::string, std::string>> store2;
  store1.push_back(std::make_pair(dbconn1, dbcontainer1));
  store2.push_back(std::make_pair(dbconn2, dbcontainer2));

  return new RaidDB(store1, store2);
}

extern "C" SERVERDLL_API RaidDB* CreateRaidDBWithLocalShadow(const char* conn1,
	const char* container1,
	const char* shadow1,
	const char* conn2,
	const char* container2,
	const char* shadow2) {
	std::string dbconn1 = conn1;
	std::string dbcontainer1 = container1;
	std::string dbconn2 = conn2;
	std::string dbcontainer2 = container2;

	std::vector<std::pair<std::string, std::string>> store1;
	std::vector<std::pair<std::string, std::string>> store2;
	store1.push_back(std::make_pair(dbconn1, dbcontainer1));
	store2.push_back(std::make_pair(dbconn2, dbcontainer2));

	return new RaidDB(store1, std::string(shadow1), store2, std::string(shadow2));
}
extern "C" SERVERDLL_API void DeleteRaidDB(RaidDB* raiddb) { delete raiddb; }

extern "C" SERVERDLL_API int Open(RaidDB* raiddb, const char* name) {
  Options options;
  options.IncreaseParallelism();
  options.compaction_style = kCompactionStyleUniversal;
#if 0
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
#endif
  options.create_if_missing = true;
  Status s = raiddb->OpenOrCreate(name, options);
  return s.code();
}

extern "C" SERVERDLL_API void Close(RaidDB* raiddb) { raiddb->Close(); }

extern "C" SERVERDLL_API void Flush(RaidDB* raiddb) { raiddb->Flush(); }

extern "C" SERVERDLL_API int Add(RaidDB* raiddb, int size, const char** keyptrs,
                                 const int* keylens, const char** valueptrs,
                                 const int* valuelens) {
  std::vector<std::pair<std::string, std::string>> data;

  for (int i = 0; i < size; i++) {
    const char* kptr = keyptrs[i];
    int klen = keylens[i];
    const char* vptr = valueptrs[i];
    int vlen = valuelens[i];
    data.push_back(
        std::make_pair(std::string(kptr, klen), std::string(vptr, vlen)));
  }
  return raiddb->Add(data).code();
}

extern "C" SERVERDLL_API int Get(RaidDB* raiddb, int size, const char** keyptrs,
                                 const int* keylens, char** valuesptr,
                                 int** valuelensptr) {
  std::vector<std::string> keys;
  for (int i = 0; i < size; i++) {
    const char* kptr = keyptrs[i];
    int klen = keylens[i];
    keys.push_back(std::string(kptr, klen));
  }
  std::vector<std::string> values;
  std::vector<Status>& status = raiddb->Get(keys, &values);
  int valuessize = 0;
  for (int i = 0; i < values.size(); i++) {
    valuessize += (int)values[i].size();
  }
  char* valuesbuf = new char[valuessize];
  int* valuelens = new int[values.size()];
  char* values_src = valuesbuf;
  for (int i = 0; i < values.size(); i++) {
    memcpy(values_src, values[i].c_str(), values[i].size());
    values_src += values[i].size();
    valuelens[i] = (int)values[i].size();
  }
  *valuesptr = valuesbuf;
  *valuelensptr = valuelens;
  return 0;
}

extern "C" SERVERDLL_API void FreeGet(char* valuesbuf, int* valuelensptr) {
  delete[] valuesbuf;
  delete[] valuelensptr;
}

extern "C" SERVERDLL_API int Seek(RaidDB* raiddb, const char* keyprefix,
                                  uint64_t* token) {
  uint64_t tok;
  int code = raiddb->Seek(keyprefix, &tok).code();
  *token = tok;
  return code;
}

extern "C" SERVERDLL_API void CloseScanToken(RaidDB* raiddb, uint64_t token) {
  raiddb->CloseScanToken(token);
}

void BuildScanResult(std::vector<std::pair<std::string, std::string>>& data,
                     int* size, char** keysptr, int** keylensptr,
                     char** valuesptr, int** valuelensptr) {
  int len = (int)data.size();
  size_t keyssize = 0;
  size_t valuessize = 0;
  for (int i = 0; i < len; i++) {
    keyssize += data[i].first.size();
    valuessize += data[i].second.size();
  }
  char* keys = new char[keyssize];
  int* keylens = new int[len];
  char* values = new char[valuessize];
  int* valuelens = new int[len];
  char* keys_src = keys;
  char* values_src = values;

  for (int i = 0; i < len; i++) {
    memcpy(keys_src, data[i].first.c_str(), data[i].first.size());
    keylens[i] = (int)data[i].first.size();
    keys_src += data[i].first.size();
    memcpy(values_src, data[i].second.c_str(), data[i].second.size());
    valuelens[i] = (int)data[i].second.size();
    values_src += data[i].second.size();
  }
  *size = len;
  *keysptr = keys;
  *keylensptr = keylens;
  *valuesptr = values;
  *valuelensptr = valuelens;
}

extern "C" SERVERDLL_API int Scan(RaidDB* raiddb, const uint64_t token,
                                  const char* endkey, int batchsize, int* size,
                                  char** keysptr, int** keylensptr,
                                  char** valuesptr, int** valuelensptr) {
  std::vector<std::pair<std::string, std::string>> data;
  int code = raiddb->Scan(token, endkey, batchsize, &data).code();
  BuildScanResult(data, size, keysptr, keylensptr, valuesptr, valuelensptr);
  return code;
}

extern "C" SERVERDLL_API void FreeScan(char* keysptr, int* keylensptr,
                                       char* valuesptr, int* valuelensptr) {
  delete[] keysptr;
  delete[] keylensptr;
  delete[] valuesptr;
  delete[] valuelensptr;
}
