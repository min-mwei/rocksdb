#include "rocksdb/utilities/env_xdb.h"
#include <stdio.h>
#include <chrono>
#include <ctime>
#include <iostream>
#include <thread>
#include "cpprest/asyncrt_utils.h"
#include "cpprest/filestream.h"
#include "was/blob.h"
#include "was/common.h"
#include "was/queue.h"
#include "was/storage_account.h"
#include "was/table.h"

using namespace azure::storage;

namespace rocksdb {

static Logger* mylog = nullptr;
const char* was_store = "was";

#if defined(OS_WIN)

const wchar_t* xdb_size = L"__xdb__size";
const std::wstring xdb_magic = L"__xdb__";
const char* filesep = "/";

static inline std::string&& xdb_to_utf8string(std::string&& value) {
  return std::move(value);
}

static inline const std::string& xdb_to_utf8string(const std::string& value) {
  return value;
}

static inline std::string xdb_to_utf8string(const utf16string& value) {
  return utility::conversions::to_utf8string(value);
}

static inline utf16string xdb_to_utf16string(const std::string& value) {
  return utility::conversions::to_utf16string(value);
}

static inline const utf16string& xdb_to_utf16string(const utf16string& value) {
  return value;
}

static inline utf16string&& xdb_to_utf16string(utf16string&& value) {
  return std::move(value);
}

static inline utf16string xdb_utf8_to_utf16(const std::string& s) {
  return utility::conversions::utf8_to_utf16(s);
}

#else

const char* xdb_size = "__xdb__size";
const char* filesep = "/";
const std::string xdb_magic = "__xdb__";
#define xdb_to_utf8string(x) (x)

#define xdb_to_utf16string(x) (x)

#define xdb_utf8_to_utf16(x) (x)

#endif
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
      assert(0 == "unrecognized error code");
      return Status::NotSupported(Status::kNone);
  }
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

std::string prefix(const std::string& name) {
  std::size_t pos = name.find("/");
  return name.substr(0, pos);
}

bool isSST(const std::string& name) {
  return name.find(".sst", name.size() - 4) != std::string::npos;
}

static size_t XdbGetUniqueId(const cloud_page_blob& page_blob, char* id,
                             size_t max_size) {
  const std::string path = xdb_to_utf8string(page_blob.uri().path());
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
      std::string size = xdb_to_utf8string(_page_blob.metadata()[xdb_size]);
      _size = size.empty() ? -1 : std::stoll(size);
      _shadow = nullptr;
    } catch (const azure::storage::storage_exception& e) {
      Info(mylog, "[xdb] XdbReadableFile opening file %s with exception %s\n",
           Name(), e.what());
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
    if (_shadow != nullptr) {
      Status s = _shadow->Read(offset, n, result, scratch);
      if (s.ok()) {
        return s;
      } else {
        Info(mylog, "[xdb] XdbReadableFile Shadow read file %s error: %s\n",
             xdb_to_utf8string(_page_blob.name()).c_str(),
             s.ToString().c_str());
        const_cast<XdbReadableFile*>(this)->_shadow = nullptr;
      }
    }
    return ReadContents(&offset, n, result, scratch);
  }

  Status Skip(uint64_t n) {
    _offset += n;
    return Status::OK();
  }

  virtual size_t GetUniqueId(char* id, size_t max_size) const {
    return XdbGetUniqueId(_page_blob, id, max_size);
  }

  const char* Name() { return xdb_to_utf8string(_page_blob.name()).c_str(); }

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
      Info(mylog, "[xdb] XdbReadableFile Read file %s with exception %s\n",
           xdb_to_utf8string(_page_blob.name()).c_str(), e.what());
    }
    return Status::IOError(Status::kNone);
  }

 public:
  unique_ptr<RandomAccessFile> _shadow;

 private:
  cloud_page_blob _page_blob;
  uint64_t _offset;
  uint64_t _size;
};

class XdbWritableFile : public WritableFile {
 public:
  XdbWritableFile(cloud_page_blob& page_blob)
      : _page_blob(page_blob),
        _bufoffset(0),
        _pageindex(0),
        _size(0),
        _iofail(false) {
    Log(InfoLogLevel::DEBUG_LEVEL, mylog,
        "[xdb] XdbWritableFile opening file %s\n", page_blob.name().c_str());
    _page_blob.create(4 * 1024);
    std::string name = xdb_to_utf8string(_page_blob.name());
    _shadow = nullptr;
  }

  ~XdbWritableFile() {
    Log(InfoLogLevel::DEBUG_LEVEL, mylog,
        "[xdb] XdbWritableFile closing file %s\n", _page_blob.name().c_str());
  }

  virtual Status Append(const char* src, size_t size) {
    size_t remain = size;
    while (remain > 0) {
      size_t cap = _buf_size - _bufoffset;
      char* target = _buffer + _bufoffset;
      int len = (int)(remain > cap ? cap : remain);
      memcpy(target, src, len);
      target += len;
      _bufoffset += len;
      src += len;
      _size += len;
      if (cap < _page_size) {
        Status s = Flush();
        if (!s.ok()) {
          Info(mylog, "[xdb] XdbWritableFile Append file %s with error %s\n",
               Name(), s.ToString().c_str());
          return s;
        }
      }
      remain -= len;
    }
    return Status::OK();
  }

  virtual Status Flush() {
    if (!_page_blob.exists()) return Status::NotFound();
    int numpages = _bufoffset / _page_size;
    int remain = _bufoffset % _page_size;
    int len = (numpages + (remain > 0 ? 1 : 0)) * _page_size;
    if (len == 0) return Status::OK();
    try {
      if ((CurrSize() + _buf_size) >= Capacity()) {
        Expand();
      }
      std::vector<char> buffer;
      buffer.assign(&_buffer[0], &_buffer[len]);
      concurrency::streams::istream page_stream =
          concurrency::streams::bytestream::open_istream(buffer);
      _page_blob.upload_pages(page_stream, _pageindex * _page_size,
                              utility::string_t(U("")));
      if (remain > 0) {
        memcpy(_buffer, _buffer + numpages * _page_size, remain);
      }
      _bufoffset = remain;
      _pageindex += numpages;
      return Status::OK();
    } catch (const azure::storage::storage_exception& e) {
      if (!_iofail) {
        _iofail = true;
        Info(mylog,
             "[xdb] XdbWritableFile Flush file %s with exception %s file size "
             "%d page index %d data len "
             "%d capacity %d \n",
             Name(), e.what(), (int)_size, (int)_pageindex, len,
             (int)Capacity());
      } else {
        return Status::Aborted();
      }
    }
    return Status::NoSpace();
  }

  Status Append(const Slice& data) {
    if (_shadow != nullptr) {
      Status s = _shadow->Append(data);
      if (!s.ok()) {
        Info(mylog,
             "[xdb] XdbWritableFile Shadow Append file %s with error %s\n",
             _shadowname.c_str(), s.ToString().c_str());
        _shadow->Close();
        Env::Default()->DeleteFile(_shadowname);
        _shadow = nullptr;
      }
    }
    return Append(data.data(), data.size());
  }

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
    if (_shadow != nullptr) {
      _shadow->Truncate(size);
    }
    try {
      if (_page_blob.exists()) {
        Sync();
        _size = size;
        _page_blob.resize(((size >> 9) + 1) << 9);
      }
    } catch (const azure::storage::storage_exception& e) {
      Info(mylog, "[xdb] XdbWritableFile Append file %s with exception %s\n",
           Name(), e.what());
    }
    return Status::OK();
  }

  Status Close() {
    Log(InfoLogLevel::DEBUG_LEVEL, mylog,
        "[xdb] XdbWritableFile closing file %s\n", _page_blob.name().c_str());
    if (_shadow != nullptr) {
      _shadow->Close();
    }
    return err_to_status(0);
  }

  Status Sync() {
    if (_shadow != nullptr) {
      _shadow->Sync();
    }
    try {
      if (_page_blob.exists()) {
        Flush();
        _page_blob.metadata().reserve(1);
        _page_blob.metadata()[xdb_size] =
            xdb_to_utf16string(std::to_string(CurrSize()));
        _page_blob.upload_metadata();
      }
    } catch (const azure::storage::storage_exception& e) {
      Info(mylog, "[xdb] XdbWritableFile Sync file %s with exception %s\n",
           Name(), e.what());
    }
    return err_to_status(0);
  }

  size_t GetUniqueId(char* id, size_t max_size) const {
    return XdbGetUniqueId(_page_blob, id, max_size);
  }

  const char* Name() { return xdb_to_utf8string(_page_blob.name()).c_str(); }

 private:
  inline uint64_t CurrSize() const { return _size; }

  inline uint64_t Capacity() const { return _page_blob.properties().size(); }

  inline void Expand() {
    uint64_t size = (((_size + _buf_size) >> 9) + 1) << 9;
    _page_blob.resize(size * 2);
  }

 public:
  unique_ptr<WritableFile> _shadow;
  std::string _shadowname;

 private:
  const static int _page_size = 512;
  const static int _buf_size = 1024 * 2 * _page_size;
  cloud_page_blob _page_blob;
  int _bufoffset;
  uint64_t _pageindex;
  uint64_t _size;
  bool _iofail;
  char _buffer[_buf_size + _page_size];
};

EnvXdb::EnvXdb(
    Env* env, std::string shadowpath,
    const std::vector<std::pair<std::string, std::string>>& dbpathmap = {})
    : EnvWrapper(env) {
  try {
    for (auto it = dbpathmap.begin(); it != dbpathmap.end(); ++it) {
      cloud_storage_account storage_account =
          cloud_storage_account::parse(xdb_to_utf16string(it->first));
      auto blob_client = storage_account.create_cloud_blob_client();
      auto container =
          blob_client.get_container_reference(xdb_to_utf16string(it->second));
      container.create_if_not_exists();
      Info(mylog, "[xdb] create/open container %s\n", it->second.c_str());
      _containermap[it->second] = container;
      _shadowpath = shadowpath;
      if (_shadowpath.size() > 0 && !EnvWrapper::FileExists(_shadowpath).ok()) {
        throw shadowpath;
      }
    }
  } catch (const azure::storage::storage_exception& e) {
    Info(mylog, "[xdb] %s \n", e.what());
    throw e;
  }
}

bool EnvXdb::isWAS(const std::string& name) {
  return name.find(was_store) == 0;
}

cloud_blob_container& EnvXdb::GetContainer(const std::string& name) {
  try {
    return _containermap[prefix(name)];
  } catch (const std::out_of_range& e) {
    throw storage_exception("container does not exist",
                            std::make_exception_ptr(e));
  }
}

Status EnvXdb::NewWritableFile(const std::string& fname,
                               unique_ptr<WritableFile>* result,
                               const EnvOptions& options) {
  if (isWAS(fname)) {
    try {
      std::string n = fname.substr(4);
      auto container = GetContainer(n);
      cloud_page_blob page_blob =
          container.get_page_blob_reference(xdb_to_utf16string(n));
      XdbWritableFile* xf = new XdbWritableFile(page_blob);
      result->reset(xf);
      if (!_shadowpath.empty() && isSST(n)) {
        Status s =
            EnvWrapper::CreateDirIfMissing(_shadowpath + filesep + prefix(n));
        if (s.ok()) {
          xf->_shadowname =
              _shadowpath + filesep + prefix(n) + filesep + lastname(n);
          s = EnvWrapper::NewWritableFile(xf->_shadowname, &xf->_shadow,
                                          options);
          if (!s.ok()) {
            Info(mylog,
                 "[xdb] XdbWritableFile Shadow Creation %s with error %s\n",
                 xf->_shadowname.c_str(), s.ToString().c_str());
            xf->_shadow = nullptr;
          }
        }
      }
    } catch (const azure::storage::storage_exception& e) {
      Info(mylog, "[xdb] EnvXdb NewWritableFile %s with exception %s \n",
           xdb_to_utf8string(fname).c_str(), e.what());
      return Status::IOError();
    }
    return Status::OK();
  }
  return EnvWrapper::NewWritableFile(fname, result, options);
}

Status EnvXdb::NewRandomAccessFile(const std::string& fname,
                                   std::unique_ptr<RandomAccessFile>* result,
                                   const EnvOptions& options) {
  if (isWAS(fname)) {
    try {
      std::string n = fname.substr(4);
      auto container = GetContainer(n);
      cloud_page_blob page_blob =
          container.get_page_blob_reference(xdb_to_utf16string(n));
      if (page_blob.exists()) {
        XdbReadableFile* xf = new XdbReadableFile(page_blob);
        result->reset(xf);
        if (!_shadowpath.empty() && isSST(n)) {
          Status s = EnvWrapper::NewRandomAccessFile(
              _shadowpath + filesep + prefix(n) + filesep + lastname(n),
              &xf->_shadow, options);
          if (!s.ok()) {
            xf->_shadow = nullptr;
          }
        }
        return Status::OK();
      }
      return Status::NotFound();
    } catch (const azure::storage::storage_exception& e) {
      Info(mylog,
           "[xdb] EnvXdb NewRandomAccessFile %s target with exception %s\n",
           xdb_to_utf8string(fname).c_str(), e.what());
      return Status::IOError();
    }
  }
  return EnvWrapper::NewRandomAccessFile(fname, result, options);
}

Status EnvXdb::NewSequentialFile(const std::string& fname,
                                 std::unique_ptr<SequentialFile>* result,
                                 const EnvOptions& options) {
  if (isWAS(fname)) {
    try {
      std::string n = fname.substr(4);
      auto container = GetContainer(n);
      cloud_page_blob page_blob =
          container.get_page_blob_reference(xdb_to_utf16string(n));
      if (page_blob.exists()) {
        result->reset(new XdbReadableFile(page_blob));
        return Status::OK();
      }
      return Status::NotFound();
    } catch (const azure::storage::storage_exception& e) {
      Info(mylog,
           "[xdb] EnvXdb NewRandomAccessFile %s target with exception %s\n",
           xdb_to_utf8string(fname).c_str(), e.what());
      return Status::IOError();
    }
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
  if (isWAS(name)) {
    try {
      std::string n = name.substr(4);
      auto container = GetContainer(n);
      cloud_page_blob page_blob =
          container.get_page_blob_reference(xdb_to_utf16string(n));
      result->reset(new XdbDirectory(0));
      return Status::OK();
    } catch (const azure::storage::storage_exception& e) {
      Info(mylog, "[xdb] EnvXdb NewDirectory %s with exception \n",
           xdb_to_utf8string(name).c_str(), e.what());
      return Status::IOError();
    }
  }
  return EnvWrapper::NewDirectory(name, result);
}

Status EnvXdb::GetAbsolutePath(const std::string& db_path,
                               std::string* output_path) {
  return EnvWrapper::GetAbsolutePath(db_path, output_path);
}

Status EnvXdb::GetChildren(const std::string& dir,
                           std::vector<std::string>* result) {
  if (isWAS(dir)) {
    try {
      result->clear();
      std::string n = dir.substr(4);
      auto container = GetContainer(n);
      list_blob_item_iterator end;
      for (list_blob_item_iterator it = container.list_blobs(
               xdb_to_utf16string(n), false, blob_listing_details::none, 0,
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
              result->push_back(
                  lastname(xdb_to_utf8string(bit->as_blob().name())));
            } else {
              result->push_back(
                  firstname(xdb_to_utf8string(bit->as_directory().prefix())));
            }
          }
        }
      }
    } catch (const azure::storage::storage_exception& e) {
      Info(mylog, "[xdb] EnvXdb GetChildren %s with exception %s\n",
           xdb_to_utf8string(dir).c_str(), e.what());
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
    auto container = GetContainer(src);
    cloud_page_blob src_blob =
        container.get_page_blob_reference(xdb_to_utf16string(src));
    if (!src_blob.exists()) return 0;
    cloud_page_blob target_blob =
        container.get_page_blob_reference(xdb_to_utf16string(target));
    target_blob.create(src_blob.properties().size());
    try {
      utility::string_t copy_id = target_blob.start_copy(src_blob);
    } catch (const azure::storage::storage_exception& e) {
      Info(mylog, "[xdb] EnvXdb WASRename src %s target %s with exception %s\n",
           xdb_to_utf8string(source).c_str(), xdb_to_utf8string(target).c_str(),
           e.what());
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
          state_description = xdb_to_utf16string("aborted");
          break;
        case copy_status::failed:
          state_description = xdb_to_utf16string("failed");
          break;
        case copy_status::invalid:
          state_description = xdb_to_utf16string("invalid");
          break;
        case copy_status::pending:
          state_description = xdb_to_utf16string("pending");
          break;
        case copy_status::success:
          state_description = xdb_to_utf16string("success");
          break;
      }
      Info(mylog, "[xdb] EnvXdb WASRename src %s target %s with exception %s\n",
           xdb_to_utf8string(source).c_str(), xdb_to_utf8string(target).c_str(),
           state_description.c_str());
    }
  } catch (const azure::storage::storage_exception& e) {
    Info(mylog, "[xdb] EnvXdb WASRename src %s target %s with exception %s\n",
         xdb_to_utf8string(source).c_str(), xdb_to_utf8string(target).c_str(),
         e.what());
  }
  return -EIO;
}

Status EnvXdb::RenameFile(const std::string& src, const std::string& target) {
  if (isWAS(src) && isWAS(target)) {
    return err_to_status(WASRename(src.substr(4), target.substr(4)));
  }
  return EnvWrapper::RenameFile(src, target);
}

Status EnvXdb::FileExists(const std::string& fname) {
  if (isWAS(fname)) {
    try {
      std::string name = fname.substr(4);
      auto container = GetContainer(name);
      cloud_page_blob page_blob =
          container.get_page_blob_reference(xdb_to_utf16string(name));
      if (page_blob.exists()) {
        return Status::OK();
      }
      cloud_blob_directory dir_blob =
          container.get_directory_reference(xdb_to_utf16string(name));
      if (dir_blob.is_valid()) {
        cloud_page_blob mblob = dir_blob.get_page_blob_reference(xdb_magic);
        if (mblob.exists()) return Status::OK();
      }
    } catch (const azure::storage::storage_exception& e) {
      Info(mylog, "[xdb] EnvXdb FileExists %s target with exception %s\n",
           xdb_to_utf8string(fname).c_str(), e.what());
    }
    return Status::NotFound();
  }
  return EnvWrapper::FileExists(fname);
}

Status EnvXdb::GetFileSize(const std::string& f, uint64_t* s) {
  if (isWAS(f)) {
    try {
      std::string n = f.substr(4);
      auto container = GetContainer(n);
      cloud_page_blob page_blob =
          container.get_page_blob_reference(xdb_to_utf16string(n));
      page_blob.download_attributes();
      std::string size = xdb_to_utf8string(page_blob.metadata()[xdb_size]);
      *s = size.empty() ? -1 : std::stoll(size);
    } catch (const azure::storage::storage_exception& e) {
      Info(mylog, "[xdb] EnvXdb GetFileSize %s target with exception %s\n",
           xdb_to_utf8string(f).c_str(), e.what());
    }
    return Status::OK();
  }
  return EnvWrapper::GetFileSize(f, s);
}

Status EnvXdb::DeleteBlob(const std::string& f) {
  try {
    auto container = GetContainer(f);
    cloud_page_blob page_blob =
        container.get_page_blob_reference(xdb_to_utf16string(f));
    page_blob.delete_blob();
    return Status::OK();
  } catch (const azure::storage::storage_exception& e) {
    Info(mylog, "[xdb] EnvXdb DeleteBlob %s target with exception %s\n",
         xdb_to_utf8string(f).c_str(), e.what());
    return Status::IOError();
  }
}

Status EnvXdb::DeleteFile(const std::string& f) {
  if (isWAS(f)) {
    std::string name = f.substr(4);
    if (!_shadowpath.empty() && isSST(name)) {
      std::string sn =
          _shadowpath + filesep + prefix(name) + filesep + lastname(name);
      if (EnvWrapper::FileExists(sn).ok()) {
        EnvWrapper::DeleteFile(sn);
      }
    }
    fixname(name);
    return DeleteBlob(name);
  }
  return EnvWrapper::DeleteFile(f);
}

Status EnvXdb::LockFile(const std::string& fname, FileLock** lock) {
  *lock = nullptr;
  return Status::OK();
}

Status EnvXdb::UnlockFile(FileLock* lock) { return Status::OK(); }

Status EnvXdb::CreateDir(const std::string& d) {
  if (isWAS(d)) {
    try {
      std::string name = d.substr(4);
      auto container = GetContainer(name);
      cloud_blob_directory dir_blob =
          container.get_directory_reference(xdb_to_utf16string(name));
      cloud_page_blob page_blob = dir_blob.get_page_blob_reference(xdb_magic);
      if (page_blob.exists()) return Status::IOError();
      page_blob.create(512);
      return Status::OK();
    } catch (const azure::storage::storage_exception& e) {
      Info(mylog, "[xdb] EnvXdb CreateDir %s target with exception %s\n",
           xdb_to_utf8string(d).c_str(), e.what());
      return Status::IOError();
    }
  }
  return EnvWrapper::CreateDir(d);
}

Status EnvXdb::CreateDirIfMissing(const std::string& d) {
  if (isWAS(d)) {
    try {
      std::string name = d.substr(4);
      auto container = GetContainer(name);
      cloud_blob_directory dir_blob =
          container.get_directory_reference(xdb_to_utf16string(name));
      cloud_page_blob page_blob = dir_blob.get_page_blob_reference(xdb_magic);
      if (page_blob.exists()) return Status::OK();
      page_blob.create(512);
      return Status::OK();
    } catch (const azure::storage::storage_exception& e) {
      Info(mylog, "[xdb] EnvXdb CreateDir %s target with exception %s\n",
           xdb_to_utf8string(d).c_str(), e.what());
      return Status::IOError();
    }
  }
  return EnvWrapper::CreateDirIfMissing(d);
}

Status EnvXdb::DeleteDir(const std::string& d) {
  if (isWAS(d)) {
    std::string name = d.substr(4);
    auto container = GetContainer(name);
    cloud_blob_directory dir_blob =
        container.get_directory_reference(xdb_to_utf16string(name));
    try {
      cloud_page_blob mblob = dir_blob.get_page_blob_reference(xdb_magic);
      mblob.delete_blob();
    } catch (const azure::storage::storage_exception& e) {
      Info(mylog, "[xdb] EnvXdb DeleteDir magic %s with exception %s\n",
           xdb_magic.c_str(), e.what());
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
#if defined(OS_WIN)
  return GetCurrentThreadId();
#else
  assert(sizeof(pthread_t) <= sizeof(uint64_t));
  return (uint64_t)pthread_self();
#endif
}

uint64_t EnvXdb::GetThreadID() const { return gettid(); }

Status EnvXdb::NewLogger(const std::string& fname,
                         std::shared_ptr<Logger>* result) {
  if (isWAS(fname)) {
    std::string n = fname.substr(4);
    auto container = GetContainer(n);
    cloud_page_blob page_blob =
        container.get_page_blob_reference(xdb_to_utf16string(n));
    XdbWritableFile* f = new XdbWritableFile(page_blob);
    if (f == nullptr) {
      *result = nullptr;
      return Status::IOError();
    }
    XdbLogger* h = new XdbLogger(f, &gettid);
    result->reset(h);
    if (mylog == nullptr) {
      mylog = h;
    }
    return Status::OK();
  }
  return EnvWrapper::NewLogger(fname, result);
}

Status NewXdbEnv(
    Env** xdb_env,
    const std::vector<std::pair<std::string, std::string>>& dbpathmap) {
  *xdb_env = new EnvXdb(Env::Default(), (""), dbpathmap);
  return Status::OK();
}

Status NewXdbEnv(
    Env** xdb_env, std::string shadowpath,
    const std::vector<std::pair<std::string, std::string>>& dbpathmap) {
  *xdb_env = new EnvXdb(Env::Default(), shadowpath, dbpathmap);
  return Status::OK();
}
}
