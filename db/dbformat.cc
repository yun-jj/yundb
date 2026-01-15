#include "dbformat.h"
#include "yundb/options.h"
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

InternalComparator::InternalComparator(const Options& options)
      : _options(options){}

Slice decodeKey(const Slice& entry)
{
  CERR_PRINT_WITH_CONDITIONAL(
    "DecodeKey: entry is null",
    entry.data() == nullptr
  );

  const char* keyEntry = entry.data();
  uint64_t keyLen;
  keyEntry = GetVarint64Ptr(keyEntry, keyEntry + 10, &keyLen);
  return Slice(keyEntry, keyLen + KeyTagSize);
}

Slice decodeValue(const Slice& entry)
{
  CERR_PRINT_WITH_CONDITIONAL(
    "DecodeValue: entry is null",
    entry.data() == nullptr
  );

  const char* valueEntry = entry.data() + entry.size();
  uint64_t valueLen;
  GetVarint64Ptr(valueEntry, valueEntry + 10, &valueLen);
  return Slice(valueEntry + VarintLength(valueLen), valueLen);
}


const char* InternalComparator::name() const
{
  return "InternalComparator";
}

int InternalComparator::cmp(const Slice& key1, const Slice& key2) const 
{

  Slice decodedKey1 = decodeKey(key1);
  Slice decodedKey2 = decodeKey(key2);

  size_t decodedKey1Len = decodedKey1.size(), decodedKey2Len = decodedKey2.size();

  // Cmp user key
  int rs = _options.comparator->cmp(
    Slice(decodedKey1.data(), decodedKey1Len - KeyTagSize),
    Slice(decodedKey2.data(), decodedKey2Len - KeyTagSize)
  );

  // Cmp seq
  if (rs == 0)
  {
    SequenceNumber key1Seq, key2Seq;
    decodeSeqAndType(decodedKey1.data() + decodedKey1Len - KeyTagSize, 
                     &key1Seq, nullptr);
    decodeSeqAndType(decodedKey2.data() + decodedKey2Len - KeyTagSize,
                     &key2Seq, nullptr);

    if (key1Seq > key2Seq) rs = +1;
    else if (key1Seq < key2Seq) rs = -1;
  }

  return rs;
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