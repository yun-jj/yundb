#include "block_reader.h"
#include "util/coding.h"
#include "yundb/comparator.h"
#include "dbformat.h"
#include <utility>

namespace yundb
{  
DataBlockReader::Iter::Iter(const char* blockStart, const char* restartPtr,
                            const char* restartPtrHead, const char* restartPtrTail)
      : _block_start(blockStart),
        _restart_ptr(restartPtr),
        _restart_ptr_head(restartPtrHead),
        _restart_ptr_tail(restartPtrTail),
        _shared_Key_Len(0)
{
  if (_restart_ptr >= _restart_ptr_head && _restart_ptr <= _restart_ptr_tail)
  {
    uint32_t offset = DecodeFixed32(_restart_ptr);
    decodeEntry(blockStart + offset);
  }
  _head_Key = _key_Delta;
}

bool DataBlockReader::Iter::operator<(const Iter& other)
{return _restart_ptr < other._restart_ptr;}

bool DataBlockReader::Iter::operator>(const Iter& other)
{return _restart_ptr > other._restart_ptr;}


bool DataBlockReader::Iter::operator<=(const Iter& other)
{return !this->operator>(other);}

bool DataBlockReader::Iter::operator>=(const Iter& other)
{return !this->operator<(other);}

DataBlockReader::Iter DataBlockReader::Iter::operator+(ptrdiff_t number)
{
  const char* newRestartPtr = _restart_ptr + number * 4;
  if (newRestartPtr > _restart_ptr_tail || newRestartPtr < _restart_ptr_head)
    printError("DataBlockReader: error restart ptr");
  return Iter(_block_start, newRestartPtr, _restart_ptr_head, _restart_ptr_tail);
}

DataBlockReader::Iter DataBlockReader::Iter::operator-(ptrdiff_t number)
{return this->operator+(-number);}

bool DataBlockReader::Iter::next()
{
  Iter tmp = *this;
  if (_end == _restart_ptr_head) 
    printError("DataBlockReader: end ptr overflow");
  decodeEntry(_end);

  if (_shared_Key_Len == 0)
  {
    *this = tmp;
    return false;
  }

  return true;
}

bool DataBlockReader::Iter::seek(const Slice& key, const Comparator* comparator,
                                 int restartInterval, std::string* result)
{
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

    if (comparator->cmp(userKey, curUserKey) == 0)
    {
      SequenceNumber curKeySeq;
      decodeSeqAndType(curPtr + cur.size() - KeyTagSize, &curKeySeq, nullptr);
      
      if (keySeq > curKeySeq) curResult = getValue();
    }
    
    if (!next()) return false;
  }

  if (!curResult.empty())
  {
    result->append(std::move(curResult));
    return true;
  }
  
  return false;
}

DataBlockReader::Iter mid(const DataBlockReader::Iter& left,
                          const DataBlockReader::Iter& right)
{
  if (left._restart_ptr <= right._restart_ptr)
    printError("DataBlockReader: error left and right Iter");
  size_t n = (right._restart_ptr - left._restart_ptr) / 4;
  const char* newRestartPtr = left._restart_ptr + (n / 2) * 4;
  if (newRestartPtr > left._restart_ptr_tail ||
      newRestartPtr < left._restart_ptr_head) {
    printError("DataBlockReader: error restart ptr");
  }

  return DataBlockReader::Iter(
    left._block_start,
    newRestartPtr,
    left._restart_ptr_head,
    left._restart_ptr_tail
  );
}

void DataBlockReader::Iter::decodeEntry(const char* start)
{
  if (start == nullptr) {
    printError("DataBlockReader: None start ptr");
  }

  uint64_t unsharedKeyLen = 0, valueLen = 0;
  start = GetVarint64Ptr(start, start + 10, &_shared_Key_Len);
  start = GetVarint64Ptr(start, start + 10, &unsharedKeyLen);

  if (unsharedKeyLen == 0) {
    printError("DataBlockReader: unSharedKeyLen is zero");
  }

  start = GetVarint64Ptr(start, start + 10, &valueLen);

  if (valueLen == 0) {
    printError("DataBlockReader: valueLen is zero");
  }

  _key_Delta.append(start, unsharedKeyLen);

  if (_key_Delta.empty()) {
    printError("DataBlockReader: keyDelta append zero");
  }

  _value.append(start + unsharedKeyLen, valueLen);

  if (_value.empty()) {
    printError("DataBlockReader: valueLen append zero");
  }

  _end = start + unsharedKeyLen + valueLen;
}

// Key format is | key | seq, type | 
bool DataBlockReader::queryValue(const Slice& block, const Slice& key, std::string* result)
{
  if (block.empty() || key.empty() || result->empty())
    printError("DatablockReader: None block, key or result");

  const char* data = block.data();
  size_t blockSize = block.size();
  uint32_t restartPtrLen = DecodeFixed32(data + blockSize - 4);
  const char* restartPtrHead = data + blockSize - 4 * (restartPtrLen + 1);
  const char* restartPtrTail = data + blockSize - 8;

  Iter left(data, restartPtrHead, restartPtrHead, restartPtrTail);
  Iter right(data, restartPtrTail, restartPtrHead, restartPtrTail);

  const Comparator* comparator = _options.comparator;
  Iter midIter;

  auto cmp = [&](const Slice& key1, const Slice& key2){
    const char* key1Ptr = key1.data();
    const char* key2Ptr = key2.data();

    // Cmp user key
    Slice userKey1(key1Ptr, key1.size() - KeyTagSize); 
    Slice userKey2(key2Ptr, key2.size() - KeyTagSize);
    int rs = comparator->cmp(userKey1, userKey2);

    // Cmp seq
    if (rs == 0)
    {
      SequenceNumber key1Seq, key2Seq;
      decodeSeqAndType(key1Ptr + key1.size() - KeyTagSize, &key1Seq, nullptr);
      decodeSeqAndType(key2Ptr + key2.size() - KeyTagSize, &key2Seq, nullptr);

      if (key1Seq > key2Seq) rs = +1;
      else if (key1Seq < key2Seq) rs = -1;
    }

    return rs;
  };

  Iter seekIter;

  while (left <= right)
  {
    midIter = mid(left, right);
    int rs = cmp(midIter.getKey(), key);

    if (rs < 0)
    {
      seekIter = midIter;
      left =  midIter + 1;
    }
    else right = midIter - 1;
  }

  return seekIter.seek(key, comparator,
                       _options.block_restart_interval, result);
}

}