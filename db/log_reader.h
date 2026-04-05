#ifndef YUNDB_DB_LOG_READER_H
#define YUNDB_DB_LOG_READER_H

#include <memory>

#include "yundb/en.h"

namespace yundb
{

namespace log
{

class Reader
{
 public:
  Reader() = default;
  Reader(Reader& other) = delete;
  Reader& operator=(Reader& other) = delete;
  explicit Reader(SequentialFile* file, size_t initial_offset, bool checksum);
  ~Reader() = default;
 private:
  std::unique_ptr<SequentialFile> _file;
  size_t _offset;
  bool _checksum;
};

}

}

#endif // YUNDB_DB_LOG_READER_H