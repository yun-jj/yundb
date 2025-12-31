#ifndef SSTABLE_READER_H
#define SSTABLE_READER_H

#include "yundb/en.h"
#include "yundb/options.h"
#include "yundb/slice.h"
#include "block_reader.h"

#include <string>

namespace yundb
{

class SstableReader
{
 public:
  SstableReader(const Options& options, std::string fileName);
  SstableReader() = delete;
  SstableReader(const SstableReader& other) = delete;
  ~SstableReader();

 private:
  Options _options;
  std::string _fileName;
  uint64_t _fileNumber;
  std::shared_ptr<RandomAccessFile> _randomFile;
  std::string _indexBlock;
  std::string _metaIndexBlock;
};

}

#endif