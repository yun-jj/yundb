#include "filter_block_reader.h"
#include "util/error_print.h"

#include "util/coding.h"

namespace yundb
{

FilterBlockReader::FilterBlockReader(std::shared_ptr<const FilterPolicy> policy,
																		 const Slice& contents)
			: _policy(std::move(policy)),
				_contents(contents.data(), contents.size()),
				_filterData(nullptr),
				_filterOffsets(nullptr),
				_filterDataSize(0),
				_filterNum(0)
{
	if (_contents.size() < 8) {
    printError("FilterBlockReader: contents size less than 8");
    return;
	}

	const size_t size = _contents.size();
	const char* base = _contents.data();
	const uint32_t dataSize = DecodeFixed32(base + size - 8);
	const uint32_t filterNum = DecodeFixed32(base + size - 4);

	if (dataSize > size - 8) {
    printError("FilterBlockReader: data size more than contents size");
    return;
	}

	const size_t offsetBytes = static_cast<size_t>(filterNum) * sizeof(uint32_t);
	if (offsetBytes > size - 8 - dataSize) {
    printError("FilterBlockReader: offset bytes more than contents size");
    return;
	}

	_filterData = base;
	_filterOffsets = base + dataSize;
	_filterDataSize = dataSize;
	_filterNum = filterNum;
}

uint32_t FilterBlockReader::getFilterOffset(uint32_t index) const
{
	return DecodeFixed32(_filterOffsets + static_cast<size_t>(index) * sizeof(uint32_t));
}

bool FilterBlockReader::keyMayMatch(uint32_t filterIndex, const Slice& key) const
{
	if (_policy == nullptr) {
    printError("FilterBlockReader: None filter policy");
    return false;
	}

	if (_filterNum == 0 || filterIndex >= _filterNum) {
    printError("FilterBlockReader: filter index out of range");
		return false;
	}

	const uint32_t start = (filterIndex == 0) ? 0 : getFilterOffset(filterIndex - 1);
	const uint32_t end = getFilterOffset(filterIndex);

	if (start > end || end > _filterDataSize) {
    printError("FilterBlockReader: invalid filter offset");
		return false;
	}

	if (start == end) {
    printError("FilterBlockReader: empty filter");
		return false;
	}

	return _policy->keyMayMatch(key, Slice(_filterData + start, end - start));
}

}