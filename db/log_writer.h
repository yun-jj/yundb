#ifndef YUNDB_DB_LOG_WRITER_H
#define YUNDB_DB_LOG_WRITER_H
// Header guard standardized to YUNDB_DB_LOG_WRITER_H

#include <memory>

#include "yundb/en.h"
#include "db/log_format.h"

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
  explicit Writer(WritableFile* file, size_t block_offset) 
      : _dest(file), _block_offset(block_offset) {}
  explicit Writer(WritableFile* file)
      : _dest(file), _block_offset(0) {}
  ~Writer() = default;
  void appendRecord(const Slice& record);
 private:
  void emitPhysicalRecord(const char* data, RecordType type, size_t length);
  void initTypeCrc();
  std::unique_ptr<WritableFile> _dest;
  size_t _block_offset;
  uint32_t _type_crc[maxRecordType + 1];
};

}

}

#endif // YUNDB_DB_LOG_WRITER_H