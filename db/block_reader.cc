#include "block_reader.h"
#include "util/coding.h"
#include "yundb/comparator.h"
#include "dbformat.h"
#include <utility>

namespace yundb
{

DataBlockReader::DataBlockReader(const Options& options)
    : _options(options) {}

 DataBlockReader::Iter::Iter() 
      : _blockStart(nullptr),
        _restartEntry(nullptr),
        _headRestartEntry(nullptr),
        _tailRestartEntry(nullptr),
        _sharedKeyLen(0){}

DataBlockReader::Iter::Iter(const char* blockStart, const char* restartEntry,
                            const char* headEntry, const char* tailEntry)
      : _blockStart(blockStart),
        _restartEntry(restartEntry),
        _headRestartEntry(headEntry),
        _tailRestartEntry(tailEntry),
        _sharedKeyLen(0)
{
  if (_restartEntry >= _headRestartEntry && _restartEntry <= _tailRestartEntry) {
    uint32_t offset = DecodeFixed32(_restartEntry);
    decodeEntry(blockStart + offset);
  }
  _headKey = _keyDelta;
}

std::string DataBlockReader::Iter::getValue()
{ return _value; }

std::string DataBlockReader::Iter::getKey()
{
  if (_sharedKeyLen == 0) return _keyDelta;
  std::string key;
  key.reserve(_sharedKeyLen + _keyDelta.size());
  key.append(_headKey, 0, _sharedKeyLen);
  key.append(_keyDelta);
  return key;
}

bool DataBlockReader::Iter::operator<(const Iter& other)
{return _restartEntry < other._restartEntry;}

bool DataBlockReader::Iter::operator>(const Iter& other)
{return _restartEntry > other._restartEntry;}


bool DataBlockReader::Iter::operator<=(const Iter& other)
{return !this->operator>(other);}

bool DataBlockReader::Iter::operator>=(const Iter& other)
{return !this->operator<(other);}

DataBlockReader::Iter DataBlockReader::Iter::operator+(ptrdiff_t number)
{
  const char* newRestartPtr = _restartEntry + number * 4;
  if (newRestartPtr > _tailRestartEntry || newRestartPtr < _headRestartEntry) {
    return Iter();
  }
  return Iter(_blockStart, newRestartPtr, _headRestartEntry, _tailRestartEntry);
}

DataBlockReader::Iter DataBlockReader::Iter::operator-(ptrdiff_t number)
{return this->operator+(-number);}

inline bool DataBlockReader::Iter::empty() const
{return _restartEntry == nullptr;}

bool DataBlockReader::Iter::next()
{
  Iter tmp = *this;
  if (_nextDataEntry == _headRestartEntry) {
    return false;
  }

  if (!decodeEntry(_nextDataEntry)) {
    *this = tmp;
    return false;
  }

  return true;
}

bool DataBlockReader::Iter::seek(const Slice& key, const Comparator* comparator,
                                 int restartInterval, std::string* result)
{
  bool found = false;
  std::string curResult;
  const char* keyPtr = key.data();
  const Slice userKey(keyPtr, key.size() - KeyTagSize);
  SequenceNumber keySeq;
  decodeSeqAndType(keyPtr + key.size() - KeyTagSize, &keySeq, nullptr);

  for (int i = 0; restartInterval > i; i++)
  {
    const std::string cur = getKey();
    const char* curPtr = cur.data();
    const Slice curUserKey(curPtr, cur.size() - KeyTagSize);

    if (comparator->cmp(userKey, curUserKey) == 0) {
      SequenceNumber curKeySeq;
      ValueType type;
      decodeSeqAndType(curPtr + cur.size() - KeyTagSize, &curKeySeq, &type);
      if (keySeq > curKeySeq) {
        found = true;
        curResult = getValue();
      }
    }
    
    if (!next()) break;
  }

  if (!curResult.empty()) {
    result->append(std::move(curResult));
  }
  
  return found;
}

DataBlockReader::Iter mid(const DataBlockReader::Iter& left,
                          const DataBlockReader::Iter& right)
{
  if (left._restartEntry > right._restartEntry) {
    printError("DataBlockReader: error left and right Iter");
    return DataBlockReader::Iter();
  }

  size_t n = (right._restartEntry - left._restartEntry) / 4;
  const char* newRestartPtr = left._restartEntry + (n / 2) * 4;
  if (newRestartPtr > right._tailRestartEntry ||
      newRestartPtr < left._headRestartEntry) {
    printError("DataBlockReader: error restart ptr");
    return DataBlockReader::Iter();
  }

  return DataBlockReader::Iter(
    left._blockStart,
    newRestartPtr,
    left._headRestartEntry,
    left._tailRestartEntry
  );
}

bool DataBlockReader::Iter::decodeEntry(const char* start)
{
  if (start == nullptr) {
    printError("DataBlockReader: None start ptr");
    return false;
  }

  uint64_t unsharedKeyLen = 0, valueLen = 0;
  start = GetVarint64Ptr(start, start + 10, &_sharedKeyLen);
  start = GetVarint64Ptr(start, start + 10, &unsharedKeyLen);

  if (unsharedKeyLen == 0) return false;

  start = GetVarint64Ptr(start, start + 10, &valueLen);

  _keyDelta.assign(start, unsharedKeyLen);
  _value.assign(start + unsharedKeyLen, valueLen);
  _nextDataEntry = start + unsharedKeyLen + valueLen;
  return true;
}

// Key format is | key | seq, type | 
bool DataBlockReader::queryValue(const Slice& block, const Slice& key, std::string* result)
{
  if (block.empty() || key.empty() || result == nullptr) {
    printError("DatablockReader: None block, key or result");
  }

  const char* data = block.data();
  size_t blockSize = block.size();
  uint32_t restartPtrLen = DecodeFixed32(data + blockSize - 4);
  const char* headEntry = data + blockSize - 4 * (restartPtrLen + 1);
  const char* tailEntry = data + blockSize - 8;

  Iter left(data, headEntry, headEntry, tailEntry);
  Iter right(data, tailEntry, headEntry, tailEntry);

  const Comparator* comparator = _options.comparator;
  Iter midIter;

  auto cmp = [&](const Slice& key1, const Slice& key2) {
    const char* key1Ptr = key1.data();
    const char* key2Ptr = key2.data();

    // Cmp user key
    Slice userKey1 = key1;
    userKey1.removeTailfix(KeyTagSize);
    Slice userKey2 = key2;
    userKey2.removeTailfix(KeyTagSize);
    int rs = comparator->cmp(userKey1, userKey2);

    // Cmp seq
    if (rs == 0)
    {
      SequenceNumber key1Seq, key2Seq;
      decodeSeqAndType(key1Ptr + key1.size() - KeyTagSize, &key1Seq, nullptr);
      decodeSeqAndType(key2Ptr + key2.size() - KeyTagSize, &key2Seq, nullptr);

      if (key1Seq > key2Seq) {
        rs = +1;
      } else if (key1Seq < key2Seq) {
        rs = -1;
      }
    }

    return rs;
  };

  Iter seekIter = mid(left, right);

  while (left <= right)
  {
    midIter = mid(left, right);
    if (midIter.empty()) {
      printError("DataBlockReader: error mid Iter");
      return false;
    }
    int rs = cmp(midIter.getKey(), key);

    if (rs > 0) {
      right = midIter - 1;
    } else {
      seekIter = midIter;
      left = midIter + 1;
    }

    if (left.empty() || right.empty()) {
      break;
    }
  }

  return seekIter.seek(key, comparator, _options.block_restart_interval, result);
}

}