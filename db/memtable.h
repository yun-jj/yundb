#ifndef MEMTABLE_H
#define MEMTABLE_H

#include "yundb/slice.h"
#include "yundb/comparator.h"
#include "yundb/options.h"
#include "dbformat.h"
#include "skiplist.h"
#include "util/arena.h"
#include "util/coding.h"

#include <atomic>
#include <memory>
#include <string>

namespace yundb
{

class MemTable
{
 private:
 // Iter use traversaling skiplist
class Iter
{
 public:
  Iter(const SkipList<Slice, InternalComparator>* skiplist)
      : _cur(skiplist->getFirstNode()),
        _skiplist(skiplist){}
  Iter() = default;
  Iter(const Iter& other) = default;
  ~Iter() = default;

  Iter& operator++()
  {
    goToNext();
    return *this;
  }

  Iter operator++(int)
  {
    auto tmp = *this;
    goToNext();
    return tmp;
  }

  bool empty()
  {return (_cur == nullptr);}

  Slice getKey();

  Slice getValue();

 private:
  const typename SkipList<Slice, InternalComparator>::Node* _cur;
  const SkipList<Slice, InternalComparator>* _skiplist;
  // Make _cur refer a different key node
  void goToNext()
  {
    if (_cur == nullptr) return;
    _cur = _cur->getNext(0);
  }
};

 public:
  MemTable(MemTable& other) = delete;
  MemTable& operator=(MemTable& other) = delete;
  MemTable(std::shared_ptr<Arena> arena, const Options& options)
      : _options(options), _ref(0), _kv_count(0), _kv_size(0), _arena(arena),
        _skiplist(arena, options){}
  // Add an entry into memtable that maps key to value at the
  // specified sequence number and with the specified type.
  // Typically value will be empty if type==kTypeDeletion.
  void add(SequenceNumber seq, ValueType type, 
           const Slice& key, const Slice& value);
  // If memtable contains a value for key, store it in *value and return true.
  // If memtable contains a deletion for key, set flase for found
  // and return true. Else, return false.
  bool get(LookUpKey& key, std::string* value, bool& found);
  // Add reference count
  void addRef()
  {_ref.fetch_add(1, std::memory_order_relaxed);}
  // Sub reference count
  int subRef()
  {
    _ref.fetch_sub(1, std::memory_order_relaxed);
    return _ref.load(std::memory_order_relaxed);
  }
  // Get the level 0 iter
  Iter iter() const
  {return Iter(&_skiplist);}
  int getKvCount() const
  {return _kv_count.load(std::memory_order_relaxed);}
  size_t getKvSize() const
  {return _kv_size.load(std::memory_order_relaxed);}
  size_t getMemoryUsage()
  {return _arena->getMemoryUsage();}
 private:
  // Update kv_count and kv_size
  void countData(const Slice& key, const Slice& value)
  {
    _kv_count.fetch_add(1, std::memory_order_relaxed);
    _kv_size.fetch_add(key.size() + value.size(), std::memory_order_relaxed);
  }
  Options _options;
  std::atomic<int> _ref;
  std::atomic<int> _kv_count;
  std::atomic<size_t> _kv_size;
  std::shared_ptr<Arena> _arena;
  SkipList<Slice, InternalComparator> _skiplist;
};

}

#endif