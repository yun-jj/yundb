#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <string.h>
#include <memory>

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

class RandomAccessPosixFile final : public RandomAccessFile 
{
 public:
  RandomAccessPosixFile(int fd, std::string fileName) 
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

  ~RandomAccessPosixFile(){close();}

  bool read(uint64_t offset, Slice* str, uint64_t bytes) override
  {
    if (str == nullptr)
    {
      CERR_PRINT("RandomAccessPosixFile: None str");
      return false;
    }

    if (str->size() < bytes)
    {
      CERR_PRINT("RandomAccessPosixFile: dst space too short to fill");
      return false;
    }

    char* dst = const_cast<char*>(str->data());
    if (std::strncpy(dst, _data + offset, static_cast<size_t>(bytes)) != 0)
    {
      CERR_PRINT("RandomAccessPosixFile: copy error");
      return false;
    }

    return true;
  }

  size_t fileSize() const override
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