#ifndef FILTER_BLOCK_BUILDER_H
#define FILTER_BLOCK_BUILDER_H

#include "yundb/filter_policy.h"

#include <string>
#include <vector>

namespace yundb
{
/*
FilterBlockBuilder format
|filter data|filter data|....|filter data|filter data offsets|filter data size|filter data number|
Filter data offsets refer all filter data.
Filter data size refer end of all filter data.
*/

class FilterBlockBuilder
{
 public:
  FilterBlockBuilder(const FilterPolicy* policy);
  FilterBlockBuilder(const FilterBlockBuilder& other) = delete;
  FilterBlockBuilder& operator=(const FilterBlockBuilder& ohter) = delete;
  ~FilterBlockBuilder();

  // Generater a filter
  void generateFilter();
  // Add key to tmp keys
  void addKey(const Slice& key);

  const Slice finish();
 private:
  const FilterPolicy* _policy;
  // Computed filter data
  std::string _result;
  // For filter->createFilter() argument
  std::vector<Slice> _tmp_keys;
  std::vector<uint32_t> _filter_offsets;
};

}

#endif