#include "sstable_reader.h"

#include "util/file_name.h"
#include "util/error_print.h"
#include "table_format.h"

namespace yundb
{

SstableReader::SstableReader(const Options& options, std::string fileName)
      : _options(options),
        _fileName(fileName)
{
  FileType type;
  if (!parseFileName(fileName, &_fileNumber, &type) || type != FileType::TableFile)
    CERR_PRINT("SstableReader: file name error");
  
  RandomAccessFile* randomFile; 
  newRandomAccessFile(fileName, &randomFile);
  _randomFile = std::make_shared<RandomAccessFile>(randomFile);

  char buf[Footer::MaxFooterSize];

  size_t fileSize = randomFile->fileSize();
  Slice footerSlice(buf);
  randomFile->read(fileSize - Footer::MaxFooterSize, &footerSlice, Footer::MaxFooterSize);
  Footer footer(footerSlice);

  uint64_t metaIndexPos, metaIndexSize, indexBlockPos, indexBlockSize;
  footer.getIndexBlockPosAndSize(&indexBlockPos, &indexBlockSize);
  footer.getMetaIndexPosAndSize(&metaIndexPos, &metaIndexSize);

  // Coding
  
}

}