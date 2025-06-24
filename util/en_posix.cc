#include <unistd.h>
#include <iostream>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "yundb/en.h"

namespace yundb
{

constexpr size_t PosixWritableBufferSize =  65536;

class WritablePosixFile final : public env::WritableFile
{
 public:
  WritablePosixFile(int fd, std::string filename)
    : _fd(fd), _filename(std::move(filename)) {}

  ~WritablePosixFile() override {close();}

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
      ::memcpy((_buf + _pos), data_pointer, write_size);
      _pos = write_size;
      return;
    }

    writeUnbuffer(data_pointer, write_size);
  }

  void flush() override {writeUnbuffer(_buf, _pos);}

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

  void flushBuffer()
  {
    writeUnbuffer(_buf, _pos);    
    _pos = 0;
  }

  int _fd;
  size_t _pos;
  const std::string _filename;
  char _buf[PosixWritableBufferSize];
};


void newWritableFile(std::string& file_name, env::WritableFile** result)
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

}