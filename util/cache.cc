#include "cache.h"
#include "yundb/slice.h"
#include "hash.h"

#include <stddef.h>
#include <vector>

namespace yundb
{

constexpr uint32_t HASHSEED = 0xdeadbeef;

struct LRUHandle;

static LRUHandle* newLRUHandle(const Slice& key, size_t hash, void* value, size_t charge,
                               void (*deleter)(const Slice& key, void* value));

static void freeLRUHandle(LRUHandle* handle);


struct LRUHandle
{
  const Slice getKey() const
  {return Slice(keyData, keyLen);}

  const size_t getHashValue() const
  {return hashValue;}

  void (*deleter)(const Slice& key, void* value);
  void* value;
  LRUHandle* nextHash;
  LRUHandle* pre;
  LRUHandle* next;

  // _inUse = true when _incache = true and refs > 1
  bool inUse;
  bool inCache;
  int refs;
  size_t hashValue;
  size_t keyLen;
  size_t charge;
  char keyData[1];
};

class LRUTable
{
 public:
  LRUTable();

  ~LRUTable();

  LRUHandle** lookup(const Slice& key, const size_t hash);

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
  _mutex.Lock();
  LRUHandle* handle = _lru.next;
  while (handle != &_lru)
  {
    LRUHandle* next = handle->next;
    freeLRUHandle(handle);
    handle = next;
  }
  _mutex.Unlock();
}

void* Cache::lookup(const Slice& key)
{
  _mutex.Lock();
  LRUHandle** handle = _hashTable.lookup(key, hash(key.data(), key.size(), HASHSEED));

  if (handle == nullptr) return nullptr;

  ref(handle);
  _mutex.Unlock();
  return (*handle)->value;
}

void Cache::insert(const Slice& key, void* value, size_t charge,
                   void (*deleter)(const Slice& key, void* value))
{

  LRUHandle* handle = newLRUHandle(key, hash(key.data(), key.size(), HASHSEED),
                                   value, charge, deleter);
  _mutex.Lock();
  _hashTable.insert(handle);
  _mutex.Unlock();
  handle->inCache = true;
  handle->refs = 1;
  _usage += charge;
  _mutex.Lock();
  LRUInsert(&handle);

   while (_usage > _capacity && _lru.next != &_lru)
  {
    LRUHandle* old = _lru.next;
    LRURemove(&old);
    _hashTable.remove(old->getHashValue(), old->getKey());
    _usage -= old->charge;
    freeLRUHandle(old);
  }
  _mutex.Unlock();
}

void Cache::unRef(const Slice& key)
{
  _mutex.Lock();
  LRUHandle** handle = _hashTable.lookup(key, hash(key.data(), key.size(), HASHSEED));
  if (handle != nullptr) unRef(handle);
  _mutex.Unlock();
}

void Cache::prune()
{
  _mutex.Lock();
  LRUHandle* handle = _lru.next;
  while (handle != &_lru)
  {
    LRUHandle* next = handle->next;
    LRURemove(&handle);
    _hashTable.remove(handle->getHashValue(), handle->getKey());
    freeLRUHandle(handle);
    handle = next;
  }
  _mutex.Unlock();
}

size_t Cache::getUsage() const
{
  _mutex.Lock();
  size_t usage = _usage;
  _mutex.Unlock();
  return usage;
}

void Cache::changeOptions(const Options& options)
{
  _options = options;
  _capacity = options.max_cache_size;
}

void Cache::LRUInsert(LRUHandle** handle)
{
  _lru.pre->next = *handle;
  (*handle)->pre = _lru.pre;
  _lru.pre = *handle;
  (*handle)->next = &_lru;
}

void Cache::LRURemove(LRUHandle** handle)
{
  (*handle)->pre->next = (*handle)->next;
  (*handle)->next->pre = (*handle)->pre;
}

void Cache::inUseInsert(LRUHandle** handle)
{
  _inUse.pre->next = *handle;
  (*handle)->pre = _inUse.pre;
  _inUse.pre = *handle;
  (*handle)->next = &_inUse;
}

void Cache::inUseRemove(LRUHandle** handle)
{
  (*handle)->pre->next = (*handle)->next;
  (*handle)->next->pre = (*handle)->pre;
}

void Cache::ref(LRUHandle** handle)
{
  ++(*handle)->refs;
  if (!(*handle)->inUse && (*handle)->refs >= 2 && (*handle)->inCache)
  {
    (*handle)->inUse = true;
    inUseInsert(handle);
  }
}

void Cache::unRef(LRUHandle** handle)
{
  --(*handle)->refs;
  if ((*handle)->refs == 1 && (*handle)->inCache && (*handle)->inUse)
  {
    (*handle)->inUse = false;
    inUseRemove(handle);
    LRUInsert(handle);
  }
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
      if ((*ptr)->hashValue == hash && (*ptr)->getKey() == key)
        return ptr;
      ptr = &((*ptr)->nextHash);
  }
  return ptr;
}

LRUHandle** LRUTable::lookup(const Slice& key, const size_t hash)
{
  LRUHandle** ptr = findPointer(key, hash);
  if (*ptr != nullptr)
    return ptr;
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

static LRUHandle* newLRUHandle(const Slice& key, size_t hash, void* value, size_t charge,
                               void (*deleter)(const Slice& key, void* value))
{
  size_t keyLen = key.size();
  char* mem = new char[sizeof(LRUHandle) + keyLen - 1];
  LRUHandle* handle = reinterpret_cast<LRUHandle*>(mem);
  handle->value = value;
  handle->deleter = deleter;
  handle->hashValue = hash;
  handle->keyLen = keyLen;
  handle->charge = charge;
  handle->refs = 0;
  handle->inUse = false;
  handle->inCache = false;
  memcpy(handle->keyData, key.data(), keyLen);
  return handle;
}

static void freeLRUHandle(LRUHandle* handle)
{
  if (handle->deleter != nullptr)
    handle->deleter(handle->getKey(), handle->value);
  delete[] reinterpret_cast<char*>(handle);
}

}