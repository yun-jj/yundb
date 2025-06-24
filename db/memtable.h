#ifndef MEMTABLE_H
#define MEMTABLE_H

#include "yundb/slice.h"
#include "dbformat.h"
#include "skiplist.h"

#include <atomic>
#include <memory>
#include <string>

namespace yundb
{

namespace memtable
{

template <class Comparator>
class MemTable
{
 private:
  class Iter;
 public:
  MemTable(MemTable& other) = delete;
  MemTable& operator=(MemTable& other) = delete;
  // Add an entry into memtable that maps key to value at the
  // specified sequence number and with the specified type.
  // Typically value will be empty if type==kTypeDeletion.
  void add(SequenceNumber seq, ValueType type, 
      const Slice& key, Slice& value);
  // If memtable contains a value for key, store it in *value and return true.
  // If memtable contains a deletion for key, set true for isNotFound
  // and return true. Else, return false.
  bool get(const LookUpKey& key, std::string* value, bool& isNotFound);
  void addRef()
  {_ref.fetch_add(1, std::memory_order_relaxed);}
  int subRef()
  {
    _ref.fetch_sub(1, std::memory_order_relaxed);
    return _ref.load(std::memory_order_relaxed);
  }
 private:
  std::atomic<int> _ref;
  std::shared_ptr<Arena> _arena;
  skiplist::SkipList<Slice, Comparator> _skiplist;
};

template <class Comparator>
class MemTable<Comparator>::Iter
{
 public:
  Iter(skiplist::SkipList<Slice, Comparator>* skiplist) : _skiplist(skiplist) {}
  ~Iter() = default;

 private:
  skiplist::SkipList<Slice, Comparator>* _skiplist;
};

}
}

#endif