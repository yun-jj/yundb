#include "dbformat.h"
#include "util/coding.h"

namespace yundb
{

uint64_t packSeqAndType(SequenceNumber seq, ValueType type)
{
  if (seq > MaxSequenceNumber)
    std::cerr << "PackseqAndType: seq more than Max\n";
  else if (type > TypeForSeek)
    std::cerr << "PackseqAndType: type more than Max\n";
  return (seq << 8) | type; 
}

LookUpKey::LookUpKey(const Slice& key, SequenceNumber seq)
{
  _size = key.size();
  int needSize = _size + 13;

  if (needSize > sizeof(_key))
    _start = new char[needSize];
  else
    _start = _key;
  
  char* start = _start;
  /* Put userkey */
  memcpy(start, key.data(), _size);
  start += _size;
  /* Put seq and type */
  EncodeFixed64(start, packSeqAndType(seq, TypeForSeek));
}

LookUpKey::~LookUpKey()
{
  if (_start != _key)
    delete[] _start;
}

Slice LookUpKey::getUserKey()
{return Slice(_start, _size);}

}