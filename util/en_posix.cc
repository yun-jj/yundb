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
  SequentialPosixFile(std::string fileName, int fd)
      : _filename(std::move(fileName)),
        _fd(fd) {}
  ~SequentialPosixFile() override {close(_fd);}

  bool skip(uint64_t n) override
  {
    if (::lseek(_fd, n, SEEK_CUR) == static_cast<off_t>(-1))
    {
      std::cerr << "SequentialPosixFile: skip file: " << _filename << "fail\n";
      return false;
    }
    return true;
  }

  bool read(Slice* str, char* scratch, uint64_t bytes) override
  {
    while (true)
    {
      ssize_t readSize = ::read(_fd, scratch, bytes);
      if (readSize < 0)
      {
        if (errno == EINTR) continue;
        std::cerr << "SequentialPosixFile: read file: " << _filename << "fail\n";
        return false;
      }
      *str = Slice(scratch, static_cast<size_t>(bytes));
      break;
    }
    return true;
  }
 private:
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
  RandomAccessPosixFile(int fd, std::string fileName)
      : _fd(fd), _fileName(std::move(fileName)) {}
  ~RandomAccessPosixFile() override {close(_fd);}

  bool read(uint64_t offset, Slice* str, char* scratch, uint64_t bytes) override
  {
  
  }
 private:
  int _fd;
  ResourceLimiter* _limiter;
  const std::string _fileName;
};

class WritablePosixFile final : public WritableFile
{
 public:
  WritablePosixFile(int fd, std::string filename)
    : _fd(fd), _filename(filename) {}

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
    if (_fd > 0)
    {
      if (::close(_fd) == 0)
        _fd = -1;
      else
        std::clog << "close file: " << _filename << "fail\n";
    }
  }

  // Flush data to os and sysnc these data
  void sync() override
  {
    writeUnbuffer(_buf, _pos);
#if HAVE_PDATASYNC
    bool success = ::fdatasync(_fd) == 0;
#else
    bool success = ::fsync(_fd) == 0;
#endif
    if (!success)
      std::clog << "sync file: " << _filename << "fail\n";
  }
 private:
  void writeUnbuffer(const char* data, size_t size)
  {
    while (size > 0)
    {
     ssize_t write_result = ::write(_fd, data, size);
     if (write_result < 0)
     {
      if (errno == EINTR)
        continue;
      else
        std::clog << "write file: " << _filename << "fail\n";
     }

     size -= write_result;
     data += write_result;
    }
  }

  int _fd;
  size_t _pos;
  const std::string _filename;
  char _buf[PosixWritableBufferSize];
};

class MmapReadablePosixFile final : public RandomAccessFile 
{
 public:
  MmapReadablePosixFile(int fd, std::string fileName) 
        : _fd(fd),
          _file_name(fileName)
  {
    struct stat fileStat;

    if (::fstat(_fd, &fileStat) == -1)
    {
      CERR_PRINT("RandomAccessPosixFile: fstat error");
      ::close(_fd);
    }

    _file_size = fileStat.st_size;

    _data = static_cast<char*>(
      ::mmap(
        nullptr,
        _file_size,
        PROT_READ,
        MAP_PRIVATE,
        fd, 0
      ));
      
    if (_data == MAP_FAILED)
    {
      CERR_PRINT("RandomAccessPosixFile: mmap error");
      ::close(_fd);
    }
  }

  ~MmapReadablePosixFile() {close();}

  bool read(uint64_t offset, Slice* str, char* scratch, uint64_t bytes) override
  {

    CERR_PRINT_WITH_CONDITIONAL(
      "RandomAccessPosixFile: None str",
      str == nullptr
    );
    CERR_PRINT_WITH_CONDITIONAL(
      "RandomAccessPosixFile: None scratch",
      scratch == nullptr
    );
    CERR_PRINT_WITH_CONDITIONAL(
      "RandomAccessPosixFile: dst space too short to fill",
      str->size() < bytes
    );

    if (std::strncpy(scratch, _data + offset, static_cast<size_t>(bytes)) != 0)
    {
      CERR_PRINT("RandomAccessPosixFile: copy error");
      return false;
    }

    *str = Slice(scratch, static_cast<size_t>(bytes));
    return true;
  }

  size_t fileSize() const
  {return _file_size;}

  void close()
  {
    char* tmp = const_cast<char*>(_data);
    ::munmap(reinterpret_cast<void*>(tmp), _file_size);
    ::close(_fd);
  }

 private:
  std::string _file_name;
  size_t _file_size;
  int _fd;
  const char* _data;
};

}

void newWritableFile(const std::string& file_name, WritableFile** result)
{
  int fd = ::open(file_name.c_str(), O_APPEND | O_WRONLY | O_CREAT, 0644);
  if (fd < 0)
  {
    *result = nullptr;
    return; 
  }

  *result = new WritablePosixFile(fd, file_name);
  return; 
}

void newRandomAccessFile(const std::string& file_name, RandomAccessFile** result)
{
  int fd = ::open(file_name.c_str(), O_RDONLY);

  if (fd < 0)
  {
    *result = nullptr;
    return;
  }

  *result = new RandomAccessPosixFile(fd, file_name);
  return;
}

static bool doRemoveFile(const std::string& fname)
{
  if (::unlink(fname.c_str()) != 0)
  {
    CERR_PRINT("removeFile: remove fail");
    return false;
  }
  return true;
}

bool writeStringToFile(const Slice& data, const std::string& fname)
{return doWriteStringToFile(data, fname, false);}

bool writeStringToFileSync(const Slice& data, const std::string& fname)
{return doWriteStringToFile(data, fname, true);}

bool readFileToString(const std::string& fname, std::string* data)
{
  data->clear();
  SequentialFile* file = nullptr;
  if (file == nullptr)
  {
    CERR_PRINT("readFileToString: open file fail");
    return false;
  }

  char buf[8192] = {0};

  while (true)
  {

  }
}

static bool doWriteStringToFile(const Slice& data, const std::string& fname,
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
    CERR_PRINT("renameFile: rename fail");
    return false;
  }

  return true;
}

}