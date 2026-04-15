#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <string.h>
#include <memory>
#include <atomic>

#include "util/error_print.h"
#include "yundb/en.h"
#include "yundb/options.h"

namespace yundb
{
// Common flags defined for all posix open operations
#if defined(HAVE_O_CLOEXEC)
constexpr int OpenBaseFlags = O_CLOEXEC;
#else
constexpr int OpenBaseFlags = 0;
#endif  // defined(HAVE_O_CLOEXEC)

constexpr size_t PosixWritableBufferSize =  65536;

Env* Env::Default()
{
  return nullptr;
}

bool Env::removeFile(const std::string& fname)
{return doRemoveFile(fname);}

bool Env::renameFile(const std::string& src, const std::string& target)
{return doRenameFile(src, target);}

namespace
{

// Limte the number of open file descriptors and the mmap file usage
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
        _permanentFd(limiter == nullptr ? true : limiter->acquire()), 
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
        std::cerr << "SequentialPosixFile: open file: " << _filename << "fail\n";
        return false;
      }
    }

    while (true)
    {
      ssize_t readSize = ::pread(fd, scratch, bytes, _offset);

      if (readSize < 0)
      {
        if (errno == EINTR) continue;
        std::cerr << "SequentialPosixFile: read file: " << _filename << "fail\n";
        return false;
      }
      _offset += static_cast<off_t>(readSize);
      *str = Slice(scratch, static_cast<size_t>(bytes));
      break;
    }
    if (::close(fd) < 0)
      std::cerr << "SequentialPosixFile: close file: " << _filename << "fail\n";
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
        _permanentFd(limiter == nullptr ? true : limiter->acquire()),
        _fileName(std::move(fileName)),
        _fd(_permanentFd ? fd : -1) {}

  ~RandomAccessPosixFile() override
  {
    if (_permanentFd)
    {
      if (_limiter != nullptr) _limiter->release();
      if (::close(_fd) < 0)
        std::cerr << "RandomAccessPosixFile: close file: " << _fileName << "fail\n";
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
      assert(fd >= 0);
      result = doRead(fd, static_cast<off_t>(offset), str, scratch, bytes);
      if (::close(fd) < 0)
        std::cerr << "RandomAccessPosixFile: close file: " << _fileName << "fail\n";
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
        _permanentFd(limiter == nullptr ? true : limiter->acquire()),
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
          std::cerr << "close file: " << _filename << "fail\n";
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
      std::cerr << "WritablePosixFile: append file: " << _filename << "fail, file already closed\n";
      return;
    }

    int fd = _fd;
    if (!_permanentFd)
    {
      fd = ::open(_filename.c_str(),
                  O_APPEND | O_WRONLY | O_CREAT | OpenBaseFlags, 0644);
      if (fd < 0)
      {
        std::cerr << "open file: " << _filename << "fail\n";
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
          std::cerr << "close file: " << _filename << "fail\n";
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

}

class PosixEnv : public Env
{
 public:
  PosixEnv() {}
  ~PosixEnv() override
  {
    std::cerr << "PosixEnv: PosixEnv destructor\n";
    std::abort();
  }

  bool removeFile(const std::string& fname) override;

  bool renameFile(const std::string& src, const std::string& target) override;

  void newWritableFile(std::string& file_name, WritableFile** result) override;

  void newRandomAccessFile(std::string& file_name, RandomAccessFile** result) override;

  static Env* Default()
  {
    static SinglePosixEnv envContainer;
    return envContainer.env();
  }
};

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

static bool doRemoveFile(const std::string& fname)
{
  if (::unlink(fname.c_str()) != 0)
  {
    std::cerr << "removeFile: remove fail\n"; 
    return false;
  }
  return true;
}

bool writeStringToFile(const Slice& data, std::string fname)
{return doWriteStringToFile(data, fname, false);}

bool writeStringToFileSync(const Slice& data, std::string fname)
{return doWriteStringToFile(data, fname, true);}

bool readFileToString(const std::string& fname, std::string* data)
{
  data->clear();
  SequentialFile* file = nullptr;
  if (file == nullptr)
  {
    std::cerr << "readFileToString: open file fail\n";
    return false;
  }

  char buf[8192] = {0};

  while (true)
  {

  }
}

static bool doWriteStringToFile(const Slice& data, std::string& fname,
                                bool shouldSync)
{
  WritableFile* file = nullptr;
  newWritableFile(fname, &file);

  if (file == nullptr) return false;

  std::shared_ptr<WritableFile> filePtr(file);

  filePtr->append(data);
  if (shouldSync) filePtr->sync();

  return true;
}

static bool doRenameFile(const std::string& src, const std::string& target)
{
  if (std::rename(target.c_str(), target.c_str()) != 0)
  {
    std::cerr << "renameFile: rename fail\n";
    return false;
  }

  return true;
}

}