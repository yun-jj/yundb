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
 
}

}