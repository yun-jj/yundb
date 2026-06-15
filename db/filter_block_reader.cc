#include "filter_block_reader.h"
#include "util/error_print.h"

#include "util/coding.h"

namespace yundb
{

FilterBlockReader::FilterBlockReader(const FilterPolicy* policy,
																		 const Slice& contents)
			: _policy(policy),
				_contents(contents.data(), contents.size()),
				_filterData(nullptr),
				_filterOffsets(nullptr),
				_filterSizes(nullptr),
				_filterDataSize(0),
				_filterNum(0)
{
	if (_contents.size() < 12) {
    printError("FilterBlockReader: contents size less than 12");
    return;
	}

	const size_t size = _contents.size();
	_filterData = _contents.data();
	_filterNum = DecodeFixed32(_filterData + size - 4);
	_filterOffsets = _filterData + size - _filterNum * 8 - 4;
	_filterSizes = _filterData + size - _filterNum * 4 - 4;
	_filterDataSize = static_cast<uint32_t>(_filterOffsets - _filterData);
}

bool FilterBlockReader::keyMayMatch(uint32_t filterIndex, const Slice& key) const
{
	if (_policy == nullptr)	{
		printError("FilterBlockReader: None filter policy");
		return false;
	}

	if (_filterNum == 0 || filterIndex >= _filterNum)	{
		printError("FilterBlockReader: filter index out of range");
		return false;
	}

	auto start = getFilterOffset(filterIndex);
	auto size = getFilterSize(filterIndex);

	return _policy->keyMayMatch(key, Slice(_filterData + start, size));
}

uint32_t FilterBlockReader::getFilterOffset(uint32_t filterIndex) const
{ return DecodeFixed32(_filterOffsets + filterIndex * 4); }

uint32_t FilterBlockReader::getFilterSize(uint32_t filterIndex) const
{ return DecodeFixed32(_filterSizes + filterIndex * 4); }

}