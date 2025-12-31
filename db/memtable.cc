#include "memtable.h"

namespace yundb
{

InternalComparator::InternalComparator(const Options& options)
      : _options(options){}

// Decode format | key | seq, type |
static Slice decodeKey(const Slice& entry)
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

// Decode format | value |
static Slice decodeValue(const Slice& entry)
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

int InternalComparator::operator()(const Slice& key1, const Slice& key2) const
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

// Format is | key | seq, type |
Slice MemTable::Iter::getKey()
{
  if (_cur != nullptr)
  {
    Slice entry = _cur->getKey();
    return decodeKey(entry);
  }

  return Slice("");
}

// Format is | value |
Slice MemTable::Iter::getValue()
{
  if (_cur != nullptr)
  {
    Slice entry = _cur->getKey();
    return decodeValue(entry);
  }

  return Slice("");
}

// The Node data format is | VarintKeySize | key | seq, type | VarintValueSize | Value |
// but Node just refer | VarintKeySize |key| seq, type |

void MemTable::add(SequenceNumber seq, ValueType type,
                   const Slice& key, const Slice& value)
{
  countData(key, value);
  /* Compute this need size and allocate */
  size_t keySize = key.size();
  size_t valueSize = value.size();
  size_t valueVarintSize = VarintLength(valueSize);
  size_t keyVarintSize = VarintLength(keySize);
  size_t needSize =  keyVarintSize + keySize + KeyTagSize +
      valueVarintSize + valueSize;
  char* buf = _arena->allocateAligned(needSize);
  char* keyStart = buf;
  /* Put key size */
  buf = EncodeVarint64(buf, keySize);
  /* Put userkey and tag */
  memcpy(buf, key.data(), keySize);
  buf += keySize;
  EncodeFixed64(buf, packSeqAndType(seq, type));
  buf += KeyTagSize;
  /* Put value size */
  buf = EncodeVarint64(buf, valueSize);
  /* Put value */
  memcpy(buf, value.data(), valueSize);
  _skiplist.insert(Slice(keyStart, keyVarintSize + keySize + KeyTagSize));
}

bool MemTable::get(LookUpKey& key, std::string* value, bool& found)
{
  if (!found) 
  {
    CERR_PRINT("found not true");
    return false;
  }
  /* Find key */
  Slice findKey = key.getKey();
  Slice result = _skiplist.contains(findKey);

  if (result.empty()) return false;

  Slice decodedKey = decodeKey(result);
  size_t decodedKeyLen = decodedKey.size();

  if (_options.comparator->cmp(
    Slice(decodedKey.data(), decodedKeyLen - KeyTagSize),
    key.getUserKey())){
    return false;
  }
  
  ValueType t;
  decodeSeqAndType(decodedKey.data() + decodedKeyLen - KeyTagSize, nullptr, &t);

  switch (t)
  {
    case TypeForSeek:
    {
      Slice v = decodeValue(result);
      value->assign(v.data(),v.size());
      break;
    }
   case TypeDeletion:
   {
      found = false;
      break;
   }
  }

  return true;
}

}