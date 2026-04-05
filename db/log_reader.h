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
  explicit Reader(SequentialFile* file, size_t initialOffset, bool checksum);
  ~Reader() = default;

  size_t lastRecordOffset() const;

  bool readRecord(Slice* record, std::string* scratch);
 private:
  void skipToInitialBlock();
  // Return type of record read.
  // stored record with out head in *result
  unsigned int readPhysicalRecord(Slice* result);
  std::unique_ptr<SequentialFile> _file;
  size_t _initialOffset;
  size_t _lastRecordOffset;
  size_t _endOfBufferOffset;
  // Actually data read from file
  char* _data;
  // Need parse data buffer
  Slice _readBuf;
  // Used to check data integrity
  bool _checksum;
  bool _eof;
  bool _resyncing;
};

}

}

#endif // YUNDB_DB_LOG_READER_H