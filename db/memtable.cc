#include "memtable.h"

namespace yundb
{

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