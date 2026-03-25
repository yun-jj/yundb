#include "block_builder.h"

#include <cstddef>

#include "yundb/options.h"
#include "util/coding.h"
#include "util/error_print.h"

namespace yundb
{
  DataBlockBuilder::DataBlockBuilder(const Options& options)
      : _options(options),
        _count(0)
  {
    // Pre allocate space
    _data.reserve(options.block_size);
  }
  // Find string first mismatch pos
  static size_t find_first_mismatch(const char* s1, const char* s2)
  {
    if (!s1 || !s2) return 0;
    
    const char* p1 = s1;
    const char* p2 = s2;
    
    while (*p1 && *p2 && *p1 == *p2)
    {++p1;++p2;}
    
    return (size_t)(p1 - s1);
  }

  void DataBlockBuilder::put(const Slice& key, const Slice& value)
  {
    CERR_PRINT_WITH_CONDITIONAL(
      "DataBlockBuilder: empty key or value",
      key.empty() || value.empty()
    );

    size_t pos = 0;
    if (_count != 0)
      pos = find_first_mismatch(key.data(), _head_Key.data());
    else
    {
      _head_Key.clear();
      _head_Key.append(key.data(), key.size());
      _restart_Ptrs.push_back(_data.size());
    }
    // Put shared key len
    PutVarint64(&_data, pos);
    // Put no shared key len
    PutVarint64(&_data, key.size() - pos);
    // Put value len
    PutVarint64(&_data, value.size());
    // Put no shared key
    _data.append(key.data() + pos, key.size() - pos);
    // Put value
    _data.append(value.data(), value.size());
    ++_count;
    // Get restart interval
    if (_count == _options.block_restart_interval)
      _count = 0;
    // Update Last Key
    _last_Key.assign(key.data(), key.size());
  }

  std::string DataBlockBuilder::finish()
  {
    for (size_t i = 0; _restart_Ptrs.size() > i; i++)
      PutFixed32(&_data, _restart_Ptrs[i]);
    PutFixed32(&_data, static_cast<uint32_t>(_restart_Ptrs.size()));
    // Clear and prepare new data block
    _count = 0;
    _restart_Ptrs.clear();
    std::string block; 
    block.swap(_data);
    _data.reserve(_options.block_size);
    return block;
  }

  size_t DataBlockBuilder::assumeBlockSize(const Slice& key,
                                           const Slice& value) const
  {
    size_t newBlcokSize = _data.size();
    // 30 is max 3 time variant size
    newBlcokSize += 30 + key.size() + value.size();
    newBlcokSize += (_restart_Ptrs.size() + 1) * 4;
    return newBlcokSize;
  }
}