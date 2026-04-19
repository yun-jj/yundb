#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <dirent.h>
#include <fcntl.h>
#include <string.h>
#include <set>
#include <queue>
#include <atomic>

#include "util/error_print.h"
#include "util/sync.h"
#include "yundb/en.h"
#include "yundb/options.h"

namespace yundb
{

namespace
{

// Up to 1000 mmap regions for 64-bit binaries; none for 32-bit.
constexpr const int DefaultMmapLimit = (sizeof(void*) >= 8) ? 1000 : 0;

int MaxOpenFiles = -1;

int MaxMmapLimit = DefaultMmapLimit;
// Common flags defined for all posix open operations
#if defined(HAVE_O_CLOEXEC)
constexpr int OpenBaseFlags = O_CLOEXEC;
#else
constexpr int OpenBaseFlags = 0;
#endif  // defined(HAVE_O_CLOEXEC)

constexpr size_t PosixWritableBufferSize =  65536;

// Limit the number of open file descriptors and the mmap file usage
// so that we do not run out of file descriptors or virtual memory
class ResourceLimiter
{
 public:
  ResourceLimiter(int maxResource) : 
#ifndef NDEBUG
      _maxResource(maxResource),
#endif
      _resource(maxResource)
  {assert(maxResource >= 0);}

  bool acquire()
  {
    int oldValue = _resource.fetch_sub(1);

    if (oldValue > 0) return true;
    int preValue = _resource.fetch_add(1);
    // Silence compiler warning
    (void)preValue;
    // If the check below fails, Release() was called more times than acquire.
    assert(preValue < _maxResource);
    return false;
  }

  void release()
  {
    int oldValue = _resource.fetch_add(1);
    // Silence compiler warning
    (void)oldValue;
    assert(oldValue < _maxResource);
  }
 private:
#ifndef NDEBUG
  const int _maxResource;
#endif
  std::atomic<int> _resource;
};

class SequentialPosixFile final : public SequentialFile
{
 public:
  SequentialPosixFile(std::string fileName, int fd, 
                      std::shared_ptr<ResourceLimiter> limiter)
      : _limiter(std::move(limiter)),
        _permanentFd(fd > 0), 
        _filename(std::move(fileName)),
        _fd(_permanentFd ? fd : -1) {}

  ~SequentialPosixFile() override
  {
    if (_permanentFd)
    {
      if (_limiter != nullptr) _limiter->release();
      if (::close(_fd) < 0)
        std::cerr << "SequentialPosixFile: close file: " << _filename << "fail\n";
    }
  }

  bool skip(uint64_t n) override
  {
    _offset += static_cast<off_t>(n);
    return true;
  }

  bool read(Slice* str, char* scratch, uint64_t bytes) override
  {
    int fd = _fd;

    if (!_permanentFd)
    {
      fd = ::open(_filename.c_str(), O_RDONLY | OpenBaseFlags);
      if (fd < 0)
      {
        std::cerr << "SequentialPosixFile: open file: "
                  << _filename << " fail\n";
        return false;
      }
    }

    while (true)
    {
      ssize_t readSize = ::pread(fd, scratch, bytes, _offset);

      if (readSize < 0)
      {
        if (errno == EINTR) continue;
        std::cerr << "SequentialPosixFile: read file: "
                  << _filename << " fail\n";
        return false;
      }
      _offset += static_cast<off_t>(readSize);
      *str = Slice(scratch, static_cast<size_t>(bytes));
      break;
    }

    if (!_permanentFd)
    {
      if (::close(fd) < 0)
        std::cerr << "SequentialPosixFile: close file: "
                  << _filename << " fail\n";
    }
    return true;
  }
 private:
  std::shared_ptr<ResourceLimiter> _limiter;
  bool _permanentFd;
  off_t _offset;
  const int _fd;
  const std::string _filename;
};

// Implements random read access in a file using pread().
//
// Instances of this class are thread-safe, as required by the RandomAccessFile
// API. Instances are immutable and Read() only calls thread-safe library
// functions.
class RandomAccessPosixFile final : public RandomAccessFile
{
 public:
  RandomAccessPosixFile(std::string fileName, int fd,
                        std::shared_ptr<ResourceLimiter> limiter)
    :   _limiter(std::move(limiter)),
        _permanentFd(fd > 0),
        _fileName(std::move(fileName)),
        _fd(_permanentFd ? fd : -1) {}

  ~RandomAccessPosixFile() override
  {
    if (_permanentFd)
    {
      if (_limiter != nullptr) _limiter->release();
      if (_fd > 0)
      {
        if (::close(_fd) < 0)
          std::cerr << "RandomAccessPosixFile: close file: " << _fileName << " fail\n";
      }

    }
  }

  bool read(uint64_t offset, Slice* str,
            char* scratch, uint64_t bytes) const override
  {
    bool result = false;

    if (_permanentFd) {
      result = doRead(_fd, static_cast<off_t>(offset), str, scratch, bytes);
    } else {
      int fd = ::open(_fileName.c_str(), O_RDONLY | OpenBaseFlags);
      if (fd < 0)
      {
        std::cerr << "RandomAccessPosixFile: open file: " << _fileName << " fail\n";
        return false;
      }
      result = doRead(fd, static_cast<off_t>(offset), str, scratch, bytes);
      if (::close(fd) < 0)
        std::cerr << "RandomAccessPosixFile: close file: " << _fileName << " fail\n";
    }

    return result;
  }
 private:
  bool doRead(int fd, off_t offset, Slice* str,
              char* scratch, uint64_t bytes) const
  {
    while (true)
    {
      ssize_t readSize = ::pread(fd, scratch, bytes, offset);
      if (readSize < 0)
      {
        if (errno == EINTR) continue;
        std::cerr << "RandomAccessPosixFile: read file: " << _fileName << "fail\n";
        if (::close(fd) < 0)
          std::cerr << "RandomAccessPosixFile: close file: " << _fileName << "fail\n";
        return false;
      }
      *str = Slice(scratch, static_cast<size_t>(bytes));
      break;
    }
  }
  // If permanentFd is false, then _fd = -1
  // If true then the file descriptor is stored in the class,
  // otherwise the file descriptor open on every read
  std::shared_ptr<ResourceLimiter> _limiter;
  bool _permanentFd;
  const std::string _fileName;
  int _fd;
};

class WritablePosixFile final : public WritableFile
{
 public:
  WritablePosixFile(std::string filename, int fd,
                    std::shared_ptr<ResourceLimiter> limiter)
      : _limiter(std::move(limiter)),
        _permanentFd(fd > 0),
        _closed(false),
        _fd(_permanentFd ? fd : -1), 
        _filename(std::move(filename)) {}

  ~WritablePosixFile() override {close();}

  // Append data to buf
  // when buf is full then flush buf data to os
  void append(const Slice& data)
  {
    const char* data_pointer = data.data();
    size_t write_size = data.size();

    size_t copy_size = std::min(write_size, PosixWritableBufferSize - _pos);
    ::memcpy((_buf + _pos), data_pointer, copy_size);
    _pos += copy_size;
    data_pointer += copy_size;
    write_size -= copy_size;

    if (write_size == 0) return;

    flush();
    _pos = 0;
    if (write_size < PosixWritableBufferSize)
    {
      ::memcpy(_buf, data_pointer, write_size);
      _pos = write_size;
      return;
    }

    writeUnbuffer(data_pointer, write_size);
  }

  // flush buf data to os
  void flush() override {writeUnbuffer(_buf, _pos);}

  // close file
  void close() override
  {
    if (!_closed)
    {
      if (_permanentFd)
      {
        if (::close(_fd) == 0)
        {
          _fd = -1;
          _limiter->release();
        }
        else
        {
          std::cerr << "close file: " << _filename << " fail\n";
          return;
        }
      }
      _closed = true;
    }
  }

  // Flush data to os and sysnc these data
  void sync() override
  {
    writeUnbuffer(_buf, _pos);
    if (!_permanentFd) return;
#if HAVE_PDATASYNC
    bool success = ::fdatasync(_fd) == 0;
#else
    bool success = ::fsync(_fd) == 0;
#endif
    if (!success)
      std::cerr << "sync file: " << _filename << "fail\n";
  }
 private:
  void writeUnbuffer(const char* data, size_t size)
  {
    if (_closed)
    {
      std::cerr << "WritablePosixFile: append file: "
                << _filename << " fail, file already closed\n";
      return;
    }

    int fd = _fd;
    if (!_permanentFd)
    {
      fd = ::open(_filename.c_str(),
                  O_APPEND | O_WRONLY | O_CREAT | OpenBaseFlags, 0644);
      if (fd < 0)      {
        std::cerr << "WritablePosixFile: open file: "
                  << _filename << " fail\n";
        return;
      }
    }

    while (size > 0)
    {
      ssize_t write_result = ::write(fd, data, size);
      if (write_result < 0)
      {
          if (errno == EINTR) {
            continue;
          } else {
            std::cerr << "write file: " << _filename << "fail\n";
          }
      }

      size -= write_result;
      data += write_result;
    }

      if (!_permanentFd)
      {
        if (::close(fd) != 0)
          std::cerr << "close file: " << _filename << " fail\n";
      }
  }

  std::shared_ptr<ResourceLimiter> _limiter;
  bool _permanentFd;
  bool _closed;
  int _fd;
  size_t _pos;
  const std::string _filename;
  char _buf[PosixWritableBufferSize];
};
// Limite virtual memory usage by mmap()

// Implements random read access in a file using mmap().
//
// Instances of this class are thread-safe, as required by the RandomAccessFile
// API. Instances are immutable and Read() only calls thread-safe library
// functions.
class MmapReadablePosixFile final : public RandomAccessFile
{
 public:
  // mmap_base[0, length-1] points to the memory-mapped contents of the file. It
  // must be the result of a successful call to mmap(). This instances takes
  // over the ownership of the region.
  //
  // |mmap_limiter| must outlive this instance. The caller must have already
  // acquired the right to use one mmap region, which will be released when this
  // instance is destroyed.
  MmapReadablePosixFile(std::string fileName, char* mmapBase, size_t length,
                        std::shared_ptr<ResourceLimiter> mmapLimiter)
      : _mmapBase(mmapBase),
        _length(length),
        _mmapLimiter(std::move(mmapLimiter)),
        _fileName(std::move(fileName)) {}

  ~MmapReadablePosixFile() override
  {
    ::munmap(static_cast<void*>(_mmapBase), _length);
    if (_mmapLimiter != nullptr) _mmapLimiter->release();
  }

  bool read(uint64_t offset, Slice* str, 
            char* scratch, uint64_t bytes) const override
  {
    if (offset + bytes > _length)
    {
      *str = Slice();
      std::cerr << "MmapReadablePosixFile: read file: " <<
      _fileName << "fail, offset + n > file length\n";
      return false;
    }

    *str = Slice(_mmapBase + offset, bytes);
    return true;
  }

 private:
  char* const _mmapBase;
  const size_t _length;
  std::shared_ptr<ResourceLimiter> _mmapLimiter;
  const std::string _fileName;
};

class PosixFileLock : public FileLock
{
 public:
  PosixFileLock(std::string fileName, int fd)
      : _fd(fd),
        _fileName(std::move(fileName)) {}

  ~PosixFileLock() override
  {
    if (::close(_fd) < 0)
      std::cerr << "PosixFileLock: close file: " << _fileName << " fail\n";
  }

  std::string fileName() const { return _fileName; }

  int getFd() const { return _fd; }
 private:
  const std::string _fileName;
  const int _fd;
};

// Tracks the files locked by PosixEnv::LockFile().
class LockFileTable
{
 public:
  LockFileTable() = default;
  ~LockFileTable() = default;
  bool insert(const std::string& fileName)
  {
    sync::LockGuard<sync::Mutex> guard(_mu);
    bool success = _lockedFiles.insert(fileName).second;
    return success;
  }

  bool remove(const std::string& fileName)
  {
    sync::LockGuard<sync::Mutex> guard(_mu);
    size_t erased = _lockedFiles.erase(fileName);
    return erased > 0;
  }
 private:
  sync::Mutex _mu;
  std::set<std::string> _lockedFiles;
};

class PosixEnv : public Env
{
 public:
  PosixEnv();
  ~PosixEnv() override
  {
    std::cerr << "PosixEnv: PosixEnv destructor\n";
    std::abort();
  }

  void newWritableFile(std::string& fileName, WritableFile** result) override
  {
    int fd = ::open(fileName.c_str(),
                    O_WRONLY | O_CREAT | OpenBaseFlags, 0644);
    if (fd < 0)
    {
      std::cerr << "PosixEnv: open file: " << fileName << " fail\n";
      *result = nullptr;
      return;
    }

    if (_fdNumberLimiter->acquire()) {
      *result = new WritablePosixFile(fileName, fd, _fdNumberLimiter);
    } else {
      *result = new WritablePosixFile(fileName, -1, _fdNumberLimiter);
      if (::close(fd) < 0)
        std::cerr << "PosixEnv: close file: " << fileName << " fail\n";
    }
  }

  void newAppendableFile(std::string& fileName, WritableFile** result) override
  {
    int fd = ::open(fileName.c_str(),
                    O_WRONLY | O_CREAT | OpenBaseFlags, 0644);
    if (fd < 0)
    {
      std::cerr << "PosixEnv: open file: " << fileName << " fail\n";
      *result = nullptr;
      return;
    }

    if (_fdNumberLimiter->acquire()) {
      *result = new WritablePosixFile(fileName, fd, _fdNumberLimiter);
    } else {
      *result = new WritablePosixFile(fileName, -1, _fdNumberLimiter);
      if (::close(fd) < 0)
        std::cerr << "PosixEnv: close file: " << fileName << " fail\n";
    }
  }

  void newSequentialFile(std::string& fileName, SequentialFile** result) override
  {
    int fd = ::open(fileName.c_str(), O_RDONLY | OpenBaseFlags);
    if(fd < 0)
    {
      std::cerr << "PosixEnv: open file: " << fileName << " fail\n";
      *result = nullptr;
      return;
    }

    if (_fdNumberLimiter->acquire()) {
      *result = new SequentialPosixFile(fileName, fd, _fdNumberLimiter);
    } else {
      *result = new SequentialPosixFile(fileName, -1, _fdNumberLimiter);
      if (::close(fd) < 0)
        std::cerr << "PosixEnv: close file: " << fileName << " fail\n";
    }
  }

  void newRandomAccessFile(std::string& fileName, RandomAccessFile** result) override
  {
    int fd = ::open(fileName.c_str(), O_RDONLY | OpenBaseFlags);
    if (fd < 0)
    {
      std::cerr << "PosixEnv: open file: " << fileName << " fail\n";
      *result = nullptr;
      return;
    }

    if (_mmapLimiter->acquire())
    {
      uint64_t fileSize;
      if (getFileSize(fileName, &fileSize)) 
      {
        void* mmapBase = ::mmap(nullptr, fileSize, PROT_READ, MAP_SHARED, fd, 0);
        *result = new MmapReadablePosixFile(fileName, static_cast<char*>(mmapBase), fileSize, _mmapLimiter);
      }
      else
      {
        std::cerr << "PosixEnv: get file size: " << fileName << " fail\n";
        if (::close(fd) < 0)
          std::cerr << "PosixEnv: close file: " << fileName << " fail\n";
      }
      return;
    }

    if(_fdNumberLimiter->acquire()) {
      *result = new RandomAccessPosixFile(fileName, fd, _fdNumberLimiter);
    } else {
      *result = new RandomAccessPosixFile(fileName, -1, _fdNumberLimiter);
      if (::close(fd) < 0)
        std::cerr << "PosixEnv: close file: " << fileName << " fail\n";
    }
  }

  bool fileExists(const std::string& fileName) override {
    return ::access(fileName.c_str(), F_OK) == 0;
  }

  bool getFileSize(const std::string& fileName, uint64_t* file_size) override
  {
    struct stat sb;
    if (::stat(fileName.c_str(), &sb) != 0)
    {
      std::cerr << "PosixEnv: stat file: " << fileName << " fail\n";
      return false;
    }
    *file_size = sb.st_size;
    return true;
  }

  bool getChildren(const std::string& dir, std::vector<std::string>* result) override
  {
    result->clear();
    ::DIR* d = ::opendir(dir.c_str());
    if (d == nullptr)
    {
      std::cerr << "PosixEnv: open dir: " << dir << " fail\n";
      return false;
    }

    struct ::dirent* entry;
    while ((entry = ::readdir(d)) != nullptr)
      result->emplace_back(entry->d_name);

    if (::closedir(d) != 0)
      std::cerr << "PosixEnv: close dir: " << dir << " fail\n";

    return true;
  }

  bool removeFile(const std::string& fileName) override
  {
    if (::unlink(fileName.c_str()) != 0)
    {
      std::cerr << "PosixEnv: remove file: " << fileName << " fail\n";
      return false;
    }
    return true;
  }

  bool renameFile(const std::string& src, const std::string& target) override
  {
    if(::rename(src.c_str(), target.c_str()) != 0)
    {
      std::cerr << "PosixEnv: rename file: " << src << " to " << target << " fail\n";
      return false;
    }
    return true;
  }

  bool createDir(const std::string& fileName) override
  {
    if (::mkdir(fileName.c_str(), 0755) != 0)
    {
      std::cerr << "PosixEnv: create dir: " << fileName << " fail\n";
      return false;
    }
    return true;
  }

  bool removeDir(const std::string& dirName) override
  {
    if (::rmdir(dirName.c_str()) != 0)
    {
      std::cerr << "PosixEnv: remove dir: " << dirName << " fail\n";
      return false;
    }
    return true;
  }

  bool lockFile(const std::string& fileName, FileLock** lock) override
  {
    *lock = nullptr;
    if (!_lockFileTable.insert(fileName))
    {
      std::cerr << "PosixEnv: lock file: " << fileName << " fail, already locked\n";
      return false;
    }

    int fd = ::open(fileName.c_str(), O_RDWR | O_CREAT | OpenBaseFlags, 0644);
    if (fd < 0)
    {
      std::cerr << "PosixEnv: open file: " << fileName << " fail\n";
      return false;
    }
    
    *lock = new PosixFileLock(fileName, fd);
    if (*lock == nullptr)
    {
      std::cerr << "PosixEnv: new PosixFileLock for file: " << fileName << " fail\n";
      if (::close(fd) < 0)
        std::cerr << "PosixEnv: close file: " << fileName << " fail\n";
      return false;
    }

    if (LockOrUnlock(fd, true) == -1)
    {
      std::cerr << "PosixEnv: lock file: " << fileName << " fail\n";
      delete *lock;
      *lock = nullptr;
      return false;
    }

    return true;
  }

  bool unlockFile(FileLock* lock) override
  {
    if (lock == nullptr)
    {
      std::cerr << "PosixEnv: unlock file fail, lock is nullptr\n";
      return false;
    }

    PosixFileLock* posixLock = static_cast<PosixFileLock*>(lock);
    std::string fileName = posixLock->fileName();
    if (!_lockFileTable.remove(fileName))
    {
      std::cerr << "PosixEnv: unlock file: " << fileName << " fail, not locked\n";
      return false;
    }

    if (LockOrUnlock(posixLock->getFd(), false) == -1)
    {
      std::cerr << "PosixEnv: unlock file: " << fileName << " fail\n";
      return false;
    }

    delete posixLock;
    return true;
  }

  static Env* Default()
  {
    static SinglePosixEnv envContainer;
    return envContainer.env();
  }
 private:
  sync::Mutex _backGroundWorkMutex;
  sync::CondVar _backGroundWorkCondVar; // Protected by _backgroundWorkMutex.
  bool _startedBackgroundWork; // protected by _backgroundWorkMutex.
  std::shared_ptr<ResourceLimiter> _fdNumberLimiter; // Thread-safe.
  std::shared_ptr<ResourceLimiter> _mmapLimiter;     // Thread-safe.
  LockFileTable _lockFileTable;                      // Thread-safe.
};


int getMaxMmapUsage() { return MaxMmapLimit; }

int getMaxOpenFile()
{
  if (MaxOpenFiles > 0) return MaxOpenFiles;

  struct ::rlimit rlim;
  if (::getrlimit(RLIMIT_NOFILE, &rlim) != 0) {
    MaxOpenFiles = 50;
  } else if (rlim.rlim_cur == RLIM_INFINITY) {
    MaxOpenFiles = std::numeric_limits<int>::max();
  } else {
    MaxOpenFiles = static_cast<int>(rlim.rlim_cur);
  }
  return MaxOpenFiles;
}


PosixEnv::PosixEnv()
    : _backGroundWorkCondVar(&_backGroundWorkMutex),
      _startedBackgroundWork(false),
      _fdNumberLimiter(std::make_shared<ResourceLimiter>(getMaxOpenFile())),
      _mmapLimiter(std::make_shared<ResourceLimiter>(getMaxMmapUsage())) {}

int LockOrUnlock(int fd, bool lock)
{
  errno = 0;
  struct ::flock file_lock_info;
  std::memset(&file_lock_info, 0, sizeof(file_lock_info));
  file_lock_info.l_type = (lock ? F_WRLCK : F_UNLCK);
  file_lock_info.l_whence = SEEK_SET;
  file_lock_info.l_start = 0;
  file_lock_info.l_len = 0;  // Lock/unlock entire file.
  return ::fcntl(fd, F_SETLK, &file_lock_info);
}

template <typename EnvType>
class SingleEnv
{
 public:
  SingleEnv()
  {
#ifndef NDEBUG
    _initialized.store(true, std::memory_order_relaxed);
#endif
    new (_envStorage) EnvType();
  }
  ~SingleEnv() = default;

  SingleEnv(const SingleEnv& other) = delete;
  SingleEnv& operator=(const SingleEnv& other) = delete;

  Env* env() {return reinterpret_cast<Env*>(_envStorage);}
 private:
  char _envStorage[sizeof(EnvType)];
#ifndef NDEBUG
  static std::atomic<bool> _initialized;
#endif
};

#ifndef NDEBUG
template <typename EnvType>
std::atomic<bool> SingleEnv<EnvType>::_initialized(false);
#endif

using SinglePosixEnv = SingleEnv<PosixEnv>;

}

}