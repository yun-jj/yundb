#include "dbformat.h"
#include "util/coding.h"

namespace yundb
{

uint64_t packSeqAndType(SequenceNumber seq, ValueType type)
{
  CERR_PRINT_WITH_CONDITIONAL(
    "PackseqAndType: seq more than Max",
    seq > MaxSequenceNumber
  );
  CERR_PRINT_WITH_CONDITIONAL(
    "packseqAndType: type more than Max",
    type > TypeForSeek
  );
  return (seq << 8) | type; 
}

void decodeSeqAndType(const char* data, SequenceNumber* seq, ValueType* type)
{
  uint64_t seqAndType = DecodeFixed64(data);
  if (seq != nullptr) *seq = static_cast<SequenceNumber>(seqAndType >> 8);
  if (type != nullptr) *type = static_cast<ValueType>(seqAndType & 0xff);
}

LookUpKey::LookUpKey(const Slice& key, SequenceNumber seq)
{
  size_t keySize = key.size();
  int keyVarintLen = VarintLength(keySize); 
  int needSize = keyVarintLen + keySize + 8;

  if (needSize > sizeof(_key))
    _start = new char[needSize];
  else
    _start = _key;
  
  char* start = _start;
  /* Put varint key len */
  start = EncodeVarint64(start, keySize);
  _user_key_start = start;
  /* Put userkey */
  memcpy(start, key.data(), keySize);
  start += keySize;
  _seq_and_type_start = start;
  /* Put seq and type */
  EncodeFixed64(_seq_and_type_start, packSeqAndType(seq, TypeForSeek));
  _end = _seq_and_type_start + 8;
}

LookUpKey::~LookUpKey()
{
  if (_start != _key)
    delete[] _start;
}

Slice LookUpKey::getUserKey()
{return Slice(_user_key_start, _seq_and_type_start - _user_key_start);}

Slice LookUpKey::getKey()
{return Slice(_start, _end - _start);}

Slice LookUpKey::getUserKeyWithSeqAndType()
{return Slice(_user_key_start, _end - _user_key_start);}

}