#include "filter_block_builder.h"
#include "util/coding.h"

namespace yundb
{

FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy)
      : _policy(policy) {}

FilterBlockBuilder::~FilterBlockBuilder() {}

void FilterBlockBuilder::addKey(const Slice& key)
{
  if (key.empty()) printError("FilterBlockBuilder: None key");
  _tmpKeys.push_back(key);
}

void FilterBlockBuilder::generateFilter()
{
  if (_tmpKeys.empty()) printError("FilterBlockBuilder: None keys");
  auto offset = _result.size();
  auto writeSize = _policy->createFilter(&_tmpKeys[0], _tmpKeys.size(), &_result);
  _filterOffsets.push_back(offset);
  _filterDataSizes.push_back(writeSize);
  _tmpKeys.clear();
}


const Slice FilterBlockBuilder::finish()
{
  if (_result.empty()) printError("FilterBlockBuilder: None filter block");

  // Put filter data offset
  for (int i = 0; _filterOffsets.size() > i; i++) {
    PutFixed32(&_result, _filterOffsets[i]);
  }

  // Put filter data size
  for (int i = 0; _filterDataSizes.size() > i; i++) {
    PutFixed32(&_result, _filterDataSizes[i]);
  }

  // Put filter data number
  PutFixed32(&_result, _filterOffsets.size());

  return Slice(_result.data(), _result.size());
}

}