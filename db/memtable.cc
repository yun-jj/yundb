#include "memtable.h"

namespace yundb
{

InternalComparator::InternalComparator(const Options& options)
      : _options(options){}

int InternalComparator::operator()(const Slice& key1, const Slice& key2) const
{
  uint64_t key1Len, key2Len;
  const char* key1Ptr = key1.data();
  const char* key2Ptr = key2.data();

  key1Ptr = GetVarint64Ptr(key1Ptr, key1Ptr + 10, &key1Len);
  key2Ptr = GetVarint64Ptr(key2Ptr, key2Ptr + 10, &key2Len);

  // Cmp user key
  Slice userKey1(key1Ptr, key1Len), userKey2(key2Ptr, key2Len);
  int rs = _options.comparator->cmp(userKey1, userKey2);

  // Cmp seq
  if (rs == 0)
  {
    SequenceNumber key1Seq, key2Seq;
    decodeSeqAndType(key1Ptr + key1Len, &key1Seq, nullptr);
    decodeSeqAndType(key2Ptr + key2Len, &key2Seq, nullptr);

    if (key1Seq > key2Seq) rs = +1;
    else if (key1Seq < key2Seq) rs = -1;
  }

  return rs;
}

static Slice getValue(Slice entry)
{
  CERR_PRINT_WITH_CONDITIONAL(
    "getValue: entry is null",
    entry.data() == nullptr
  );

  const char* valueEntry = entry.data() + entry.size();
  uint32_t valueLen;
  GetVarint32Ptr(valueEntry, valueEntry + 5, &valueLen);
  return Slice(valueEntry + VarintLength(valueLen), valueLen);
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

  const char* KeyStart = result.data();
  uint64_t keyLen;
  const char* userKeyStart = GetVarint64Ptr(KeyStart, KeyStart + 10, &keyLen);
  if (_options.comparator->cmp(Slice(userKeyStart, keyLen), key.getUserKey()))
    return false;
  
  uint64_t tag = DecodeFixed64(userKeyStart + keyLen);
  ValueType t = static_cast<ValueType>(tag & 0xff);

  switch (t)
  {
    case TypeForSeek:
    {
      Slice v = getValue(result);
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