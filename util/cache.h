#ifndef YUNDB_UTIL_CACHE_H
#define YUNDB_UTIL_CACHE_H
// Header guard standardized to YUNDB_UTIL_CACHE_H

#include "yundb/options.h"
#include "sync.h"

#include <mutex>

namespace yundb
{

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

class Cache
{
 public:
  Cache(const Options& options);

  ~Cache();

  // Find the value of key. If found, it will increase the reference count and
  // return the value. If not found, it will return nullptr.
  void* lookup(const Slice& key);

  // Insert key-value pair into cache, charge is the memory usage of this key-value pair
  // deleter is the function to delete this key-value pair when evicted from cache
  void insert(const Slice& key, void* value, size_t charge,
              void (*deleter)(const Slice& key, void* value));

  // Decrease the reference count of key.
  void unRef(const Slice& key);

  // Remove all cache entries that in lru list
  void prune(); 

  size_t getUsage() const;

  void changeOptions(const Options& options);

 private:
  void LRUInsert(LRUHandle** handle);
  
  void LRURemove(LRUHandle** handle);

  void inUseInsert(LRUHandle** handle);

  void inUseRemove(LRUHandle** handle);

  void ref(LRUHandle** handle);

  void unRef(LRUHandle** handle);

  Options _options;

  mutable sync::Mutex _mutex;;

  // Current memory usage of the cache
  size_t _usage;

  // Maximum memory usage allowed for the cache
  size_t _capacity;

  LRUTable _hashTable;
  // _lru.pre is newlest, _lru.next is oldest
  // _lru is a dummy head of LRU list
  // this list cantain in_cache = true and refs = 1 handle
  LRUHandle _lru;
  // _inUse is a dummy head of in_use list
  // this list contain in_cache = true and refs > 1 handle
  LRUHandle _inUse;
};

}

#endif // YUNDB_UTIL_CACHE_H