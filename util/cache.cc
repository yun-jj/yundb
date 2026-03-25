#include "cache.h"
#include "yundb/slice.h"
#include "hash.h"

#include <stddef.h>
#include <vector>

namespace yundb
{

struct LRUHandle;

LRUHandle* newLRUHandle(const Slice& key, size_t hash, void* value,
                        void (*deleter)(const Slice& key, void* value));

void freeLRUHandle(LRUHandle* handle);


struct LRUHandle
{
  const Slice getKey() const
  {return Slice(_keyData, _keyLen);}

  const size_t getHashValue() const
  {return _hashValue;}

  void (*_deleter)(const Slice& key, void* value);
  void* _value;
  LRUHandle* nextHash;
  LRUHandle* pre;
  LRUHandle* next;

  // _inUse = true when _incache = true and refs > 1
  bool _inUse;
  bool _inCache;
  int refs;
  size_t _hashValue;
  size_t _keyLen;
  char _keyData[1];
};

class LRUTable
{
 public:
  LRUTable();

  ~LRUTable();

  LRUHandle* lookup(const Slice& key, const size_t hash);

  LRUHandle* remove(size_t hashValue, const Slice& key);

  void insert(LRUHandle* handle);

  void resize();

 private:
  LRUHandle** findPointer(const Slice& key, size_t hash);
  int elems;
  std::vector<LRUHandle*> _buckets;
};

Cache::Cache(const Options& options)
    : _options(options),
      _usage(0),
      _capacity(options.max_cache_size)
{
  _lru.pre = &_lru;
  _lru.next = &_lru;
  _inUse.pre = &_inUse;
  _inUse.next = &_inUse;
}

Cache::~Cache()
{
  std::lock_guard<std::mutex> lock(_mutex);
  LRUHandle* handle = _lru.next;
  while (handle != &_lru)
  {
    LRUHandle* next = handle->next;
    freeLRUHandle(handle);
    handle = next;
  }
}

void* Cache::lookup(const Slice& key)
{

}

void Cache::insert(const Slice& key, void* value, size_t charge,
                   void (*deleter)(const Slice& key, void* value))
{

}

size_t Cache::getUsage() const
{
  std::lock_guard<std::mutex> lock(_mutex);
  return _usage;
}

void Cache::changeOptions(const Options& options)
{
  _options = options;
  _capacity = options.max_cache_size;
}



LRUTable::LRUTable() : elems(0), _buckets(4, nullptr) {}

LRUTable::~LRUTable()
{
  for (size_t i = 0; i < _buckets.size(); ++i)
  {
    LRUHandle* handle = _buckets[i];
    while (handle != nullptr)
    {
      freeLRUHandle(handle);
      handle = handle->nextHash;
    }
  }
}

LRUHandle** LRUTable::findPointer(const Slice& key, size_t hash)
{
  LRUHandle** ptr = &_buckets[hash & (_buckets.size() - 1)];
  while (*ptr != nullptr)
  {
      if ((*ptr)->_hashValue == hash && (*ptr)->getKey() == key)
        return ptr;
      ptr = &((*ptr)->nextHash);
  }
  return ptr;
}

LRUHandle* LRUTable::lookup(const Slice& key, const size_t hash)
{
  LRUHandle** ptr = findPointer(key, hash);
  if (*ptr != nullptr)
    return *ptr;
  return nullptr;
}

LRUHandle* LRUTable::remove(size_t hashValue, const Slice& key)
{
  LRUHandle** ptr = findPointer(key, hashValue);
  if (*ptr != nullptr)
  {
    LRUHandle* result = *ptr;
    *ptr = (*ptr)->nextHash;
    --elems;
    return result;
  }
  return nullptr;
}
void LRUTable::insert(LRUHandle* handle)
{
  LRUHandle** ptr = findPointer(handle->getKey(), handle->getHashValue());
  if (*ptr != nullptr)
  {
    handle->nextHash = (*ptr)->nextHash;
    *ptr = handle;
    ++elems;
  }
  else
  {
    handle->nextHash = *ptr;
    *ptr = handle;
    ++elems;
  }

  if (elems >= _buckets.size() * 4) resize();
}

void LRUTable::insert(LRUHandle* handle)
{
  LRUHandle** ptr = findPointer(handle->getKey(), handle->getHashValue());
  if (*ptr != nullptr)
  {
    handle->nextHash = (*ptr)->nextHash;
    *ptr = handle;
    ++elems;
  }
  else
  {
    handle->nextHash = *ptr;
    *ptr = handle;
    ++elems;
  }

  if (elems >= _buckets.size() * 4) resize();
}

void LRUTable::resize()
  {
    std::vector<LRUHandle*> newBuckets(_buckets.size() * 2, nullptr);
    for (size_t i = 0; i < _buckets.size(); ++i)
    {
      LRUHandle* handle = _buckets[i];
      while (handle != nullptr)
      {
        LRUHandle* next = handle->nextHash;
        size_t idx = handle->getHashValue() & (newBuckets.size() - 1);
        handle->nextHash = newBuckets[idx];
        newBuckets[idx] = handle;
        handle = next;
      }
    }
    _buckets.swap(newBuckets);
  }

LRUHandle* newLRUHandle(const Slice& key, size_t hash, void* value,
                        void (*deleter)(const Slice& key, void* value))
{
  size_t keyLen = key.size();
  char* mem = new char[sizeof(LRUHandle) + keyLen - 1];
  LRUHandle* handle = reinterpret_cast<LRUHandle*>(mem);
  handle->_value = value;
  handle->_deleter = deleter;
  handle->_hashValue = hash;
  handle->_keyLen = keyLen;
  handle->refs = 0;
  handle->_inUse = false;
  handle->_inCache = false;
  memcpy(handle->_keyData, key.data(), keyLen);
  return handle;
}

void freeLRUHandle(LRUHandle* handle)
{
  if (handle->_deleter != nullptr)
  {
    handle->_deleter(handle->getKey(), handle->_value);
  }
  delete[] reinterpret_cast<char*>(handle);

}

}