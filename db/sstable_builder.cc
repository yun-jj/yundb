#include "sstable_builder.h"
#include "yundb/en.h"
#include "util/snappy_wrapper.h"
#include "util/error_print.h"
#include "dbformat.h"
#include "util/crc32c.h"

namespace yundb
{

SstableBuilder::SstableBuilder(Options& options, WritableFile* file)
    : _cur_block_position(0),
      _options(options),
      _file(file),
      _filter_block_builder(options.filter_policy),
      _data_block_builder(options),
      _index_block_builder(options)
{
  CERR_PRINT_WITH_CONDITIONAL(
    "SstableBuilder: policy is null",
    options.filter_policy == nullptr
  );
  CERR_PRINT_WITH_CONDITIONAL(
    "SstableBuilder: file is null",
    _file == nullptr
  );
  Options tmpOption = options;
  tmpOption.block_restart_interval = 1;
  _index_block_builder.changeOptions(tmpOption);
}

SstableBuilder::~SstableBuilder() {}

size_t SstableBuilder::writeBlock(std::string& block)
{
  auto type = _options.compression;
  std::string compressionBlock;
  Slice writeData;

  switch (type)
  {
  case NoCompression:
  {
    writeData = block;
    break;
  }
  case SnappyCompression:
  {
    if (Snappy_Compress(block.data(), block.size(), &compressionBlock) && 
        compressionBlock.size() < block.size() - (block.size() / 8u))
      writeData = compressionBlock; 
    else
    {
      writeData = block;
      type = NoCompression;
    }
    break;
  }
  } 

  writeRawBlock(writeData, type);
  return writeData.size();
}

void SstableBuilder::writeRawBlock(const Slice& block, CompressionType type)
{
  // Write blcok
  _file->append(block);
  // Init trailer
  char trailer[BlockTrailerSize];
  trailer[0] = type;
  uint32_t crc = crc32c::Value(block.data(), block.size());
  crc = crc32c::Extend(crc, trailer, 1);
  EncodeFixed32(trailer + 1, crc32c::Mask(crc));
  // Write trailer
  _file->append(Slice(trailer, BlockTrailerSize));
  // Update position
  _cur_block_position += block.size() + BlockTrailerSize;
}

void SstableBuilder::flushBlock()
{
  // Put restart_ptrs
  std::string block = _data_block_builder.finish();
  auto oldBlockPos = _cur_block_position;
  auto writeSize = writeBlock(block);
  // Put index entry = | data block max key | position && data block size | 
  _index_block_builder.put(_data_block_builder.getLastKey(),
                           _handle_builder.encode(oldBlockPos, writeSize)); 
  // Generate filter
  _filter_block_builder.generateFilter();
}

void SstableBuilder::build(const MemTable* memtable)
{
  auto iter = memtable->iter();

  while(!iter.empty())
  {
    // Trying put key and value
    if (_data_block_builder.assumeBlockSize(iter.getKey(), iter.getValue()) >=
        _options.block_size) {
      flushBlock();
    }
    // Put key value pair
    _data_block_builder.put(iter.getKey(), iter.getValue());
    _filter_block_builder.addKey(iter.getKey());
    iter++;
  }
  
  if (_data_block_builder.getSize() != 0)
    flushBlock();

  // Write filter block
  size_t oldBlockPos = _cur_block_position;
  Slice filterBlock = _filter_block_builder.finish();
  writeRawBlock(filterBlock, NoCompression);
  std::string filterHandle = _handle_builder.encode(oldBlockPos, filterBlock.size());

  // Write meta index block
  oldBlockPos = _cur_block_position;
  writeRawBlock(filterHandle, NoCompression);
  std::string metaIndexHandle = _handle_builder.encode(oldBlockPos, filterHandle.size());

  // Write index block
  oldBlockPos = _cur_block_position;
  std::string indexBlock = _index_block_builder.finish();
  writeRawBlock(indexBlock, NoCompression);
  std::string indexBlockHandle = _handle_builder.encode(oldBlockPos, indexBlock.size());

  // Write footer
  Footer footer;
  std::string footerBlock;
  footer.encodeTo(&footerBlock, metaIndexHandle, indexBlockHandle);
  writeRawBlock(footerBlock, NoCompression);
}

}