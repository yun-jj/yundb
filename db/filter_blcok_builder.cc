#include "filter_block_builder.h"
#include "util/coding.h"

namespace yundb
{

FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy)
      : _policy(policy) {}

FilterBlockBuilder::~FilterBlockBuilder() {}

void FilterBlockBuilder::addKey(const Slice& key)
{
  CERR_PRINT_WITH_CONDITIONAL("FilterBlockBuilder: None key", key.empty());
  _tmp_keys.push_back(key);
}

void FilterBlockBuilder::generateFilter()
{
  CERR_PRINT_WITH_CONDITIONAL("FilterBlockBuilder: None keys", _tmp_keys.empty());
  _policy->createFilter(&_tmp_keys[0], _tmp_keys.size(), &_result);
  _filter_offsets.push_back(_result.size());
  _tmp_keys.clear();
}

const Slice FilterBlockBuilder::finish()
{
  CERR_PRINT_WITH_CONDITIONAL("FilterBlockBuilder: None filter block",
                               _result.empty());

  uint32_t filterDataSize = _result.size();

  // Put filter data offset
  for (int i = 0; _filter_offsets.size() > i; i++) 
    PutFixed32(&_result, _filter_offsets[i]);  

  // Put filter data size
  PutFixed32(&_result, filterDataSize);

  // Put filter data number
  PutFixed32(&_result, _filter_offsets.size());

  return Slice(_result.data(), _result.size());
}

}