#ifndef YUNDB_DB_FILTER_BLOCK_READER_H
#define YUNDB_DB_FILTER_BLOCK_READER_H

#include "yundb/filter_policy.h"

#include <cstdint>
#include <memory>
#include <string>

namespace yundb
{

class FilterBlockReader 
{
 public:
  FilterBlockReader(const FilterPolicy* policy, const Slice& contents);

  FilterBlockReader(const FilterBlockReader& other) = delete;
  FilterBlockReader& operator=(const FilterBlockReader& other) = delete;

  ~FilterBlockReader() = default;

  bool keyMayMatch(uint32_t filterIndex, const Slice& key) const;

 private:
  uint32_t getFilterOffset(uint32_t index) const;

  const FilterPolicy* _policy;
  const Slice _contents;
  const char* _filterData;
  const char* _filterOffsets;
  uint32_t _filterDataSize;
  uint32_t _filterNum;
};

}

#endif // YUNDB_DB_FILTER_BLOCK_READER_H