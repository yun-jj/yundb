#ifndef SSTABLE_BUILDER_H
#define SSTABLE_BUILDER_H

#include "yundb/en.h"
#include "yundb/options.h"
#include "filter_block_builder.h"
#include "memtable.h"
#include "block_builder.h"
#include "table_format.h"


namespace yundb
{

class SstableBuilder
{
 public:
  SstableBuilder(Options& options, WritableFile* file);
  SstableBuilder(SstableBuilder& other) = delete;
  ~SstableBuilder();
  // Builde sstable
  void build(const MemTable* memtable);
 private:
  std::string writeIndexBlock();
  size_t writeBlock(const DataBlockBuilder& block);
  void writeRawBlock(const Slice& block, CompressionType type);
  uint64_t _cur_block_position;
  Options _options;
  WritableFile* _file;
  BlockHandle _handle_builder;
  FilterBlockBuilder _filter_block_builder;
  DataBlockBuilder _data_block_builder;
  DataBlockBuilder _index_block_builder;
};

}
#endif