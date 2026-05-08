#include "yundb/table.h"
#include "yundb/en.h"
#include "yundb/options.h"
#include "db/table_format.h"
#include "db/filter_block_reader.h"


namespace yundb
{

struct Table::Rep
{
 public:
   Rep(const Options& options, RandomAccessFile* file, uint64_t file_size)
       : options(options), file(file), file_size(file_size), filter(nullptr, Slice()), metaindex_handle(), index_handle() {}
 private:
  Options options;
  RandomAccessFile* file;
  uint64_t file_size;
  FilterBlockReader filter;
  BlockHandle metaindex_handle;  // Handle to metaindex block (offset,size)
  BlockHandle index_handle;      // Handle to index block (offset,size)
};

}