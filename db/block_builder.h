#ifndef BLOCK_H
#define BLOCK_H

#include <string>
#include <vector>

#include "yundb/slice.h"
#include "yundb/options.h"

namespace yundb
{


class DataBlockBuilder
{
 public:
  DataBlockBuilder(DataBlockBuilder& other) = delete;
  DataBlockBuilder(DataBlockBuilder&& other) = delete;
  DataBlockBuilder& operator=(DataBlockBuilder& other) = delete;
  DataBlockBuilder(const Options& options);
  // Entry format =
  // | shared key len | no shared key len | value len | key | value |
  // Put restart ptr to data block tail and return the data Block
  void put(const Slice& key, const Slice& value);
  // Assume put the key and value after current blcok size 
  size_t assumeBlockSize(const Slice& key, const Slice& value) const;
  // Append restart ptr and return the block
  // this function will clear the space for new blcok
  std::string finish();
  // Get all data in DataBlockBuilder
  Slice getData() const
  {return Slice(_data);}
  // Get all data size in DataBlockBuilder
  uint64_t getSize() const
  {return _data.size();}
  // Return current max head Key
  Slice getLastKey() const
  {
    CERR_PRINT_WITH_CONDITIONAL(
      "DataBlockBuilder: None LastKey",
      _last_Key.empty()
    );
    return _last_Key;
  }
  void changeOptions(const Options& options)
  {_options = options;}
  private:
  Options _options;
  int _count;
  std::string _head_Key;
  std::string _last_Key;
  std::string _data;
  std::vector<uint32_t> _restart_Ptrs;
};

}

#endif