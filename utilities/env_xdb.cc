#include "rocksdb/utilities/env_xdb.h"
#include <stdio.h>
#include <chrono>
#include <ctime>
#include <iostream>
#include <thread>
#include "cpprest/filestream.h"
#include "was/blob.h"
#include "was/common.h"
#include "was/queue.h"
#include "was/storage_account.h"
#include "was/table.h"

using namespace azure::storage;

namespace rocksdb {

const char* default_conn = "XDB_WAS_CONN";
const char* default_container = "XDB_WAS_CONTAINER";
const char* was_store = "was";
const char* xdb_size = "__xdb__size";
const std::string xdb_magic = "__xdb__";
static Logger* mylog = nullptr;

Status err_to_status(int r) {
  switch (r) {
    case 0:
      return Status::OK();
    case -ENOENT:
      return Status::IOError();
    case -ENODATA:
    case -ENOTDIR:
      return Status::NotFound(Status::kNone);
    case -EINVAL:
      return Status::InvalidArgument(Status::kNone);
    case -EIO:
      return Status::IOError(Status::kNone);
    default:
      // FIXME :(
      IOError("fixme", -1);
      assert(0 == "unrecognized error code");
      return Status::NotSupported(Status::kNone);
  }
}

static size_t XdbGetUniqueId(const cloud_page_blob& page_blob, char* id,
                             size_t max_size) {
  const std::string path = page_blob.uri().path();
  size_t len = path.size() > max_size ? max_size : path.size();
  memcpy(id, path.c_str(), len);
  return len;
}

class XdbReadableFile : virtual public SequentialFile,
                        virtual public RandomAccessFile {
 public:
  XdbReadableFile(cloud_page_blob& page_blob)
      : _page_blob(page_blob), _offset(0) {
    try {
      Log(InfoLogLevel::DEBUG_LEVEL, mylog,
          "[xdb] XdbReadableFile opening file %s\n", page_blob.name().c_str());
      _page_blob.download_attributes();
      std::string size = _page_blob.metadata()[xdb_size];
      _size = size.empty() ? -1 : std::stoi(size);
    } catch (const azure::storage::storage_exception& e) {
      Log(InfoLogLevel::DEBUG_LEVEL, mylog,
          "[xdb] XdbReadableFile opening file %s with exception %s\n",
          page_blob.name().c_str(), e.what());
      _size = -1;
    }
  }

  ~XdbReadableFile() {
    Log(InfoLogLevel::DEBUG_LEVEL, mylog,
        "[xdb] XdbReadableFile closing file %s\n", _page_blob.name().c_str());
  }

  virtual Status Read(size_t n, Slice* result, char* scratch) {
    return ReadContents(&_offset, n, result, scratch);
  }

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
    return ReadContents(&offset, n, result, scratch);
  }

  Status Skip(uint64_t n) {
    std::cout << "Skip:" << n << std::endl;
    _offset += n;
    return Status::OK();
  }

  virtual size_t GetUniqueId(char* id, size_t max_size) const {
    return XdbGetUniqueId(_page_blob, id, max_size);
  }

  const char* Name() { return _page_blob.name().c_str(); }

 private:
  Status ReadContents(uint64_t* origin, size_t n, Slice* result,
                      char* scratch) const {
    try {
      uint64_t offset = *origin;
      uint64_t page_offset = (offset >> 9) << 9;
      uint64_t sz = _size - offset;
      if (sz > n) sz = n;
      if (sz <= 0) {
        *result = Slice(scratch, 0);
        return Status::OK();
      }
      size_t cursor = offset - page_offset;
      assert(cursor <= 512);
      size_t nz = ((sz >> 9) + 1 + ((cursor > 0) ? 1 : 0)) << 9;
      std::vector<page_range> pages =
          _page_blob.download_page_ranges(page_offset, nz);
      char* target = scratch;
      size_t remain = sz;
      size_t r = 0;
      for (std::vector<page_range>::iterator it = pages.begin();
           it < pages.end(); it++) {
        concurrency::streams::istream blobstream =
            (const_cast<cloud_page_blob&>(_page_blob)).open_read();
        blobstream.seek(it->start_offset(), std::ios_base::beg);
        concurrency::streams::stringstreambuf buffer;
        blobstream.read(buffer, it->end_offset() - it->start_offset()).wait();
        const char* src = buffer.collection().c_str();
        size_t bsize = buffer.size();
        size_t len = remain < bsize ? remain : bsize - cursor;
        assert(cursor + len <= bsize);
        memcpy(target, src + cursor, len);
        cursor = 0;
        remain -= len;
        target += len;
        r += len;
        if (remain <= 0) break;
      }
      *result = Slice(scratch, r);
      *origin = offset + r;
      return err_to_status(0);
    } catch (const azure::storage::storage_exception& e) {
      Log(InfoLogLevel::DEBUG_LEVEL, mylog,
          "[xdb] XdbReadableFile Read file %s with exception %s\n",
          _page_blob.name().c_str(), e.what());
    }
    return Status::IOError(Status::kNone);
  }

 private:
  cloud_page_blob _page_blob;
  uint64_t _offset;
  uint64_t _size;
};

class XdbWritableFile : public WritableFile {
 public:
  XdbWritableFile(cloud_page_blob& page_blob)
      : _page_blob(page_blob), _pageindex(0), _pageoffset(0) {
    Log(InfoLogLevel::DEBUG_LEVEL, mylog,
        "[xdb] XdbWritableFile opening file %s\n", page_blob.name().c_str());
    _page_blob.create(1 * 1024 * 1024);
  }

  ~XdbWritableFile() {
    Log(InfoLogLevel::DEBUG_LEVEL, mylog,
        "[xdb] XdbWritableFile closing file %s\n", _page_blob.name().c_str());
  }

  virtual Status Append(const char* src, size_t size) {
    try {
      if (size + CurrSize() >= Capacity()) {
        Expand();
      }
      while (size > 0) {
        size_t left = _page_size - _pageoffset;
        if (size > left) {
          memcpy(&_buffer[_pageoffset], src, left);
          _pageoffset = 0;
          size -= left;
          src += left;
        } else {
          memcpy(&_buffer[_pageoffset], src, size);
          _pageoffset += size;
          size = 0;
          src += size;
        }
        std::vector<char> buffer;
        buffer.assign(&_buffer[0], &_buffer[_page_size]);
        concurrency::streams::istream page_stream =
            concurrency::streams::bytestream::open_istream(buffer);
        _page_blob.upload_pages(page_stream, _pageindex * _page_size,
                                utility::string_t(U("")));
        if (size != 0 && _pageoffset == 0) _pageindex++;
      }
      return err_to_status(0);

    } catch (const azure::storage::storage_exception& e) {
      Log(InfoLogLevel::DEBUG_LEVEL, mylog,
          "[xdb] XdbWritableFile Append file %s with exception %s\n",
          _page_blob.name().c_str(), e.what());
    }
    return Status::IOError();
  }

  Status Append(const Slice& data) { return Append(data.data(), data.size()); }

  Status PositionedAppend(const Slice& /* data */, uint64_t /* offset */) {
    Log(InfoLogLevel::DEBUG_LEVEL, mylog,
        "[xdb] XdbWritableFile PositionedAppend %s not supported\n",
        _page_blob.name().c_str());
    return Status::NotSupported();
  }

  Status InvalidateCache(size_t offset, size_t length) {
    Log(InfoLogLevel::DEBUG_LEVEL, mylog,
        "[xdb] XdbWritableFile InvalidateCache %s not supported\n",
        _page_blob.name().c_str());
    return Status::OK();
  }

  Status Truncate(uint64_t size) {
    try {
      if (_page_blob.exists()) {
        _page_blob.resize(((size >> 9) + 1) << 9);
      }
    } catch (const azure::storage::storage_exception& e) {
      Log(InfoLogLevel::DEBUG_LEVEL, mylog,
          "[xdb] XdbWritableFile Append file %s with exception %s\n",
          _page_blob.name().c_str(), e.what());
    }
    return Status::OK();
  }

  Status Close() {
    Log(InfoLogLevel::DEBUG_LEVEL, mylog,
        "[xdb] XdbWritableFile closing file %s\n", _page_blob.name().c_str());
    Sync();
    return err_to_status(0);
  }

  Status Flush() { return err_to_status(0); }

  Status Sync() {
    try {
      if (_page_blob.exists()) {
        _page_blob.metadata().reserve(1);
        _page_blob.metadata()[xdb_size] = std::to_string(CurrSize());
        _page_blob.upload_metadata();
      }
    } catch (const azure::storage::storage_exception& e) {
      Log(InfoLogLevel::DEBUG_LEVEL, mylog,
          "[xdb] XdbWritableFile Sync file %s with exception %s\n",
          _page_blob.name().c_str(), e.what());
    }
    return err_to_status(0);
  }

  size_t GetUniqueId(char* id, size_t max_size) const {
    return XdbGetUniqueId(_page_blob, id, max_size);
  }

  const char* Name() { return _page_blob.name().c_str(); }

 private:
  inline uint64_t CurrSize() const {
    return _pageindex * _page_size + _pageoffset;
  }

  inline uint64_t Capacity() const { return _page_blob.properties().size(); }

  inline void Expand() { _page_blob.resize(Capacity() * 2); }

 private:
  const static int _page_size = 1024 * 4;
  cloud_page_blob _page_blob;
  int _pageindex;
  int _pageoffset;
  char _buffer[_page_size];
};

EnvXdb::EnvXdb(Env* env) : EnvWrapper(env) {
  // static EnvXdb default_env(env, std::getenv(default_conn));
  // char* connect_string =
  char* connect_string = std::getenv(default_conn);
  char* container_name = std::getenv(default_container);
  if (connect_string == NULL || container_name == NULL) {
    Log(InfoLogLevel::DEBUG_LEVEL, mylog,
        "[xdb] connect_string or container_name not provided\n");
    assert(0);
    exit(-1);
  }
  try {
    cloud_storage_account storage_account =
        cloud_storage_account::parse(connect_string);
    _blob_client = storage_account.create_cloud_blob_client();
    _container = _blob_client.get_container_reference(container_name);
    _container.create_if_not_exists();
  } catch (const azure::storage::storage_exception& e) {
    std::cout << e.what() << std::endl;
    exit(-1);
  } catch (...) {
    Log(InfoLogLevel::DEBUG_LEVEL, mylog, "[xdb] connect_string %s invalid\n",
        connect_string);
    assert(0);
    exit(-1);
  }
}

Status EnvXdb::NewWritableFile(const std::string& fname,
                               unique_ptr<WritableFile>* result,
                               const EnvOptions& options) {
  if (fname.find(was_store) == 0) {
    cloud_page_blob page_blob =
        _container.get_page_blob_reference(fname.substr(4));
    result->reset(new XdbWritableFile(page_blob));
    return Status::OK();
  }
  return EnvWrapper::NewWritableFile(fname, result, options);
}

Status EnvXdb::NewRandomAccessFile(const std::string& fname,
                                   std::unique_ptr<RandomAccessFile>* result,
                                   const EnvOptions& options) {
  if (fname.find(was_store) == 0) {
    cloud_page_blob page_blob =
        _container.get_page_blob_reference(fname.substr(4));
    if (page_blob.exists()) {
      result->reset(new XdbReadableFile(page_blob));
      return Status::OK();
    }
    return Status::NotFound();
  }
  return EnvWrapper::NewRandomAccessFile(fname, result, options);
}

Status EnvXdb::NewSequentialFile(const std::string& fname,
                                 std::unique_ptr<SequentialFile>* result,
                                 const EnvOptions& options) {
  if (fname.find(was_store) == 0) {
    cloud_page_blob page_blob =
        _container.get_page_blob_reference(fname.substr(4));
    if (page_blob.exists()) {
      result->reset(new XdbReadableFile(page_blob));
      return Status::OK();
    }
    return Status::NotFound();
  }
  return EnvWrapper::NewSequentialFile(fname, result, options);
}

class XdbDirectory : public Directory {
 public:
  explicit XdbDirectory(int fd) : fd_(fd) {}
  ~XdbDirectory() {}

  virtual Status Fsync() {
    fd_ = 0;
    return Status::OK();
  }

 private:
  int fd_;
};

Status EnvXdb::NewDirectory(const std::string& name,
                            unique_ptr<Directory>* result) {
  if (name.find(was_store) == 0) {
    try {
      cloud_page_blob page_blob =
          _container.get_page_blob_reference(name.substr(4));
      result->reset(new XdbDirectory(0));
      return Status::OK();
    } catch (const azure::storage::storage_exception& e) {
      Log(InfoLogLevel::DEBUG_LEVEL, mylog,
          "[xdb] EnvXdb NewDirectory %s with exception \n", name.c_str(),
          e.what());
      return Status::IOError();
    }
  }
  return EnvWrapper::NewDirectory(name, result);
}

Status EnvXdb::GetAbsolutePath(const std::string& db_path,
                               std::string* output_path) {
  return EnvWrapper::GetAbsolutePath(db_path, output_path);
}

std::string lastname(const std::string& name) {
  std::size_t pos = name.find_last_of("/");
  return name.substr(pos + 1);
}

std::string firstname(const std::string& name) {
  std::string dirent(name);
  dirent.resize(dirent.size() - 1);
  std::size_t pos = dirent.find_last_of("/");
  return dirent.substr(pos + 1);
}

Status EnvXdb::GetChildren(const std::string& dir,
                           std::vector<std::string>* result) {
  if (dir.find(was_store) == 0) {
    try {
      result->clear();
      list_blob_item_iterator end;
      for (list_blob_item_iterator it = _container.list_blobs(
               dir.substr(4), false, blob_listing_details::none, 0,
               blob_request_options(), operation_context());
           it != end; it++) {
        if (!it->is_blob()) {
          list_blob_item_iterator bend;
          Log(InfoLogLevel::DEBUG_LEVEL, mylog,
              "[xdb] EnvXdb GetChildren for %s \n",
              it->as_directory().prefix().c_str());
          for (list_blob_item_iterator bit = it->as_directory().list_blobs();
               bit != bend; bit++) {
            if (bit->is_blob()) {
              result->push_back(lastname(bit->as_blob().name()));
            } else {
              result->push_back(firstname(bit->as_directory().prefix()));
            }
          }
        }
      }
    } catch (const azure::storage::storage_exception& e) {
      Log(InfoLogLevel::DEBUG_LEVEL, mylog,
          "[xdb] EnvXdb GetChildren %s with exception %s\n", dir.c_str(),
          e.what());
    }
    return Status::OK();
  }
  return EnvWrapper::GetChildren(dir, result);
}

void fixname(std::string& name) {
  std::size_t pos = name.find("//");
  if (pos != std::string::npos) {
    name.erase(pos, 1);
  }
}

int EnvXdb::WASRename(const std::string& source, const std::string& target) {
  try {
    std::string src(source);
    fixname(src);
    cloud_page_blob src_blob = _container.get_page_blob_reference(src);
    if (!src_blob.exists()) return 0;
    cloud_page_blob target_blob = _container.get_page_blob_reference(target);
    target_blob.create(src_blob.properties().size());
    try {
      utility::string_t copy_id = target_blob.start_copy(src_blob);
    } catch (const azure::storage::storage_exception& e) {
      Log(InfoLogLevel::DEBUG_LEVEL, mylog,
          "[xdb] EnvXdb WASRename src %s target %s with exception %s\n",
          source.c_str(), target.c_str(), e.what());
      target_blob.delete_blob();
      return -EIO;
    }
    target_blob.download_attributes();
    copy_state state = target_blob.copy_state();
    if (state.status() == copy_status::success) {
      src_blob.delete_blob();
      return 0;
    } else {
      utility::string_t state_description;
      switch (state.status()) {
        case copy_status::aborted:
          state_description = "aborted";
          break;
        case copy_status::failed:
          state_description = "failed";
          break;
        case copy_status::invalid:
          state_description = "invalid";
          break;
        case copy_status::pending:
          state_description = "pending";
          break;
        case copy_status::success:
          state_description = "success";
          break;
      }
      Log(InfoLogLevel::DEBUG_LEVEL, mylog,
          "[xdb] EnvXdb WASRename src %s target %s with exception %s\n",
          source.c_str(), target.c_str(), state_description.c_str());
    }
  } catch (const azure::storage::storage_exception& e) {
    Log(InfoLogLevel::DEBUG_LEVEL, mylog,
        "[xdb] EnvXdb WASRename src %s target %s with exception %s\n",
        source.c_str(), target.c_str(), e.what());
  }
  return -EIO;
}

Status EnvXdb::RenameFile(const std::string& src, const std::string& target) {
  if (src.find(was_store) == 0 && target.find(was_store) == 0) {
    return err_to_status(WASRename(src.substr(4), target.substr(4)));
  }
  return EnvWrapper::RenameFile(src, target);
}

Status EnvXdb::FileExists(const std::string& fname) {
  if (fname.find(was_store) == 0) {
    try {
      std::string name = fname.substr(4);
      cloud_page_blob page_blob = _container.get_page_blob_reference(name);
      if (page_blob.exists()) {
        return Status::OK();
      }
      cloud_blob_directory dir_blob = _container.get_directory_reference(name);
      if (dir_blob.is_valid()) {
        cloud_page_blob mblob = dir_blob.get_page_blob_reference(xdb_magic);
        if (mblob.exists()) return Status::OK();
      }
    } catch (const azure::storage::storage_exception& e) {
      Log(InfoLogLevel::DEBUG_LEVEL, mylog,
          "[xdb] EnvXdb FileExists %s target with exception %s\n",
          fname.c_str(), e.what());
    }
    return Status::NotFound();
  }
  return EnvWrapper::FileExists(fname);
}

Status EnvXdb::GetFileSize(const std::string& f, uint64_t* s) {
  if (f.find(was_store) == 0) {
    try {
      cloud_page_blob page_blob =
          _container.get_page_blob_reference(f.substr(4));
      page_blob.download_attributes();
      std::string size = page_blob.metadata()[xdb_size];
      *s = size.empty() ? -1 : std::stoi(size);
    } catch (const azure::storage::storage_exception& e) {
      Log(InfoLogLevel::DEBUG_LEVEL, mylog,
          "[xdb] EnvXdb GetFileSize %s target with exception %s\n", f.c_str(),
          e.what());
    }
    return Status::OK();
  }
  return EnvWrapper::GetFileSize(f, s);
}

Status EnvXdb::DeleteBlob(const std::string& f) {
  try {
    cloud_page_blob page_blob = _container.get_page_blob_reference(f);
    page_blob.delete_blob();
    return Status::OK();
  } catch (const azure::storage::storage_exception& e) {
    Log(InfoLogLevel::DEBUG_LEVEL, mylog,
        "[xdb] EnvXdb DeleteBlob %s target with exception %s\n", f.c_str(),
        e.what());
    return Status::IOError();
  }
}

Status EnvXdb::DeleteFile(const std::string& f) {
  if (f.find(was_store) == 0) {
    return DeleteBlob(f.substr(4));
  }
  return EnvWrapper::DeleteFile(f);
}

Status EnvXdb::LockFile(const std::string& fname, FileLock** lock) {
  *lock = nullptr;
  return Status::OK();
}

Status EnvXdb::UnlockFile(FileLock* lock) { return Status::OK(); }

Status EnvXdb::CreateDir(const std::string& d) {
  if (d.find(was_store) == 0) {
    try {
      std::string name = d.substr(4);
      cloud_blob_directory dir_blob = _container.get_directory_reference(name);
      cloud_page_blob page_blob = dir_blob.get_page_blob_reference(xdb_magic);
      if (page_blob.exists()) return Status::IOError();
      page_blob.create(512);
      return Status::OK();
    } catch (const azure::storage::storage_exception& e) {
      Log(InfoLogLevel::DEBUG_LEVEL, mylog,
          "[xdb] EnvXdb CreateDir %s target with exception %s\n", d.c_str(),
          e.what());
      return Status::IOError();
    }
  }
  return EnvWrapper::CreateDir(d);
}

Status EnvXdb::CreateDirIfMissing(const std::string& d) {
  if (d.find(was_store) == 0) {
    try {
      std::string name = d.substr(4);
      cloud_blob_directory dir_blob = _container.get_directory_reference(name);
      cloud_page_blob page_blob = dir_blob.get_page_blob_reference(xdb_magic);
      if (page_blob.exists()) return Status::OK();
      page_blob.create(512);
      return Status::OK();
    } catch (const azure::storage::storage_exception& e) {
      Log(InfoLogLevel::DEBUG_LEVEL, mylog,
          "[xdb] EnvXdb CreateDir %s target with exception %s\n", d.c_str(),
          e.what());
      return Status::IOError();
    }
  }
  return EnvWrapper::CreateDirIfMissing(d);
}

Status EnvXdb::DeleteDir(const std::string& d) {
  if (d.find(was_store) == 0) {
    std::string name = d.substr(4);
    cloud_blob_directory dir_blob = _container.get_directory_reference(name);
    try {
      cloud_page_blob mblob = dir_blob.get_page_blob_reference(xdb_magic);
      mblob.delete_blob();
    } catch (const azure::storage::storage_exception& e) {
      Log(InfoLogLevel::DEBUG_LEVEL, mylog,
          "[xdb] EnvXdb DeleteDir magic %s with exception %s\n", xdb_magic.c_str(),
          e.what());
      return Status::IOError();
    }
    return DeleteBlob(name);
  }
  return EnvWrapper::DeleteDir(d);
}

class XdbLogger : public Logger {
 private:
  XdbWritableFile* file_;
  uint64_t (*gettid_)();  // Return the thread id for the current thread

 public:
  XdbLogger(XdbWritableFile* f, uint64_t (*gettid)())
      : file_(f), gettid_(gettid) {
    Log(InfoLogLevel::DEBUG_LEVEL, mylog, "[xdb] XdbLogger opened %s\n",
        file_->Name());
  }

  virtual ~XdbLogger() {
    Log(InfoLogLevel::DEBUG_LEVEL, mylog, "[xdb] XdbLogger closed %s\n",
        file_->Name());
    delete file_;
    if (mylog != nullptr && mylog == this) {
      mylog = nullptr;
    }
  }

  virtual void Logv(const InfoLogLevel log_level, const char* format,
                    va_list ap) override {
    Logv(format, ap);
  }

  virtual void Logv(const char* format, va_list ap) override {
    const uint64_t thread_id = (*gettid_)();
    // We try twice: the first time with a fixed-size stack allocated buffer,
    // and the second time with a much larger dynamically allocated buffer.
    char buffer[500];
    for (int iter = 0; iter < 2; iter++) {
      char* base;
      int bufsize;
      if (iter == 0) {
        bufsize = sizeof(buffer);
        base = buffer;
      } else {
        bufsize = 30000;
        base = new char[bufsize];
      }
      char* p = base;
      char* limit = base + bufsize;

      struct timeval now_tv;
      gettimeofday(&now_tv, nullptr);
      const time_t seconds = now_tv.tv_sec;
      struct tm t;
      localtime_r(&seconds, &t);
      p += snprintf(p, limit - p, "%04d/%02d/%02d-%02d:%02d:%02d.%06d %llx ",
                    t.tm_year + 1900, t.tm_mon + 1, t.tm_mday, t.tm_hour,
                    t.tm_min, t.tm_sec, static_cast<int>(now_tv.tv_usec),
                    static_cast<long long unsigned int>(thread_id));

      // Print the message
      if (p < limit) {
        va_list backup_ap;
        va_copy(backup_ap, ap);
        p += vsnprintf(p, limit - p, format, backup_ap);
        va_end(backup_ap);
      }

      // Truncate to available space if necessary
      if (p >= limit) {
        if (iter == 0) {
          continue;  // Try again with larger buffer
        } else {
          p = limit - 1;
        }
      }

      // Add newline if necessary
      if (p == base || p[-1] != '\n') {
        *p++ = '\n';
      }

      assert(p <= limit);
      file_->Append(base, p - base);
      file_->Flush();
      if (base != buffer) {
        delete[] base;
      }
      break;
    }
  }
};

size_t EnvXdb::GetUniqueId(char* id, size_t max_size) { return 0; }

static uint64_t gettid() {
  assert(sizeof(pthread_t) <= sizeof(uint64_t));
  return (uint64_t)pthread_self();
}

uint64_t EnvXdb::GetThreadID() const { return gettid(); }

Status EnvXdb::NewLogger(const std::string& fname,
                         std::shared_ptr<Logger>* result) {
  if (fname.find(was_store) == 0) {
    cloud_page_blob page_blob =
        _container.get_page_blob_reference(fname.substr(4));
    XdbWritableFile* f = new XdbWritableFile(page_blob);
    if (f == nullptr) {
      *result = nullptr;
      return Status::IOError();
    }
    XdbLogger* h = new XdbLogger(f, &gettid);
    result->reset(h);
    if (mylog == nullptr) {
      // mylog = h; // uncomment this for detailed logging
    }
    return Status::OK();
  }
  return EnvWrapper::NewLogger(fname, result);
}

EnvXdb* EnvXdb::Default(Env* env) {
  static EnvXdb default_env(env);
  return &default_env;
}
}
