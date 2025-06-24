#include "memtable.h"
#include "util/coding.h"

namespace yundb
{
namespace memtable
{


static Slice getValue(const char* valueEntry)
{
  if (valueEntry == nullptr) std::cerr << "getValue: ptr is null\n";
  uint32_t valueLen;
  GetVarint32Ptr(valueEntry, valueEntry + 5, &valueLen);
  return Slice(valueEntry + VarintLength(valueLen), valueLen);
}

template<class Comparator>
void MemTable<Comparator>::add(SequenceNumber seq, ValueType type,
                                        const Slice& key, Slice& value)
{
  /* Compute this need size and allocate */
  size_t keySize = key.size();
  size_t valueSize = value.size();
  size_t valueVarintSize = VarintLength(valueSize);
  size_t needSize = keySize + KeyTagSize +
      valueVarintSize + valueSize;
  char* buf = _arena->allocateAligned(needSize);
  /* Put userkey and tag */
  memcpy(buf, key.data(), keySize);
  buf += keySize;
  EncodeFixed64(buf, packSeqAndType(seq, type));
  buf += KeyTagSize;
  /* Put value size */
  buf = EncodeVarint32(buf, valueSize);
  /* Put value */
  memcpy(buf, value.data(), valueSize);
  _skiplist.insert(Slice(buf, keySize));
}

template <class Comparator>
bool MemTable<Comparator>::get(const LookUpKey& key, std::string* value, bool& isNotFound)
{
  if (isNotFound) 
  {
    std::cerr << "isNotFound not false\n";
    return false;
  }
  /* Find key */
  Slice findKey = key.getUserKey();
  Slice result = _skiplist->contains(findKey); 

  if (result.empty()) return false;

  char* KeyStart = result.data();
  uint64_t tag = DecodeFixed64(KeyStart + result.size());
  ValueType v = static_cast<ValueType>(tag & 0xff);

  switch (v)
  {
    case TypeForSeek:
      Slice v = getValue(result.data() + result.size() + KeyTagSize);
      value->assign(v.data(),v.size());
      return true;
    case TypeDeletion:
      isNotFound = true;
      return true;
  }
}

}
}