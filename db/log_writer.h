#ifndef LOG_WRITER_H
#define LOG_WRITER_H

#include <memory>

#include "yundb/en.h"

class WritableFile;

namespace yundb
{
namespace log
{

class Writer
{
 public:
  Writer() = default;
  Writer(Writer& other) = delete;
  Writer& operator=(Writer& other) = delete;
  explicit Writer(env::WritableFile* file, size_t block_offset) 
    : _dest(file), _block_offset(block_offset) {}
  ~Writer() = default;
  void appendRecord(const Slice& record);
 private:
  std::unique_ptr<env::WritableFile> _dest;
  size_t _block_offset;
};

}
}

#endif