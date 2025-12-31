#ifndef SKIP_LIST_H
#define SKIP_LIST_H

#include <memory>
#include <atomic>
#include <string>

#include "util/random.h"
#include "util/arena.h"
#include "util/error_print.h"
#include "yundb/options.h"
#include "yundb/comparator.h"

namespace yundb
{

class MemTable;
/* Skiplist max height */
static constexpr int MaxHeight = 12;
/* InternalComparator must define int operator() */
template <typename KeyType, typename InternalComparator>
class SkipList
{
 private:
  class Node;
  friend class MemTable;
 public:
  SkipList() = delete;
  SkipList(std::shared_ptr<yundb::Arena> arena, const Options& options);
  SkipList(SkipList& other) = delete;
  ~SkipList() = default;
  SkipList& operator=(SkipList& other) = delete;
  /* Insert node */
  void insert(const KeyType& key);
  /* Find key if in the list */
  KeyType contains(const KeyType& key);
 private:
  /* memory pool */
  std::shared_ptr<yundb::Arena> _arena;
  InternalComparator _comparator;
  yundb::Random _rand;
  std::atomic<int> _max_height;
  /* Node list head, head value is a infinitesimal number */
  Node* const _head;
  /* Create a new node */
  Node* newNode(int height, const KeyType& key)
  {
      char* const node = _arena->allocateAligned(
        sizeof(Node) + sizeof(std::atomic<Node*>) * (height - 1)
      );
      return new (node) Node(key);
  }
  Node* getFirstNode() const
  {return _head->getNext(0);}
  void findNoLessThanNodePre(Node* pre[], const KeyType& key) const;
  int randomHeight();
  void doInsert(Node* pre[], const KeyType& key);
  int getMaxHeight() const
  {return _max_height.load(std::memory_order_relaxed);}
  void increaseMaxHeight(int h)
  {_max_height.fetch_add(h, std::memory_order_relaxed);}
};

/* skiplist node in memory */
template <typename KeyType, typename InternalComparator>
class SkipList<KeyType, InternalComparator>::Node
{
 public:
  Node() = default;
  Node(Node& other) = delete;
  Node& operator=(Node& other) = delete;
  Node(const KeyType& key) : _key(key){}
  Node* getNext(int level) const
  {
    CERR_PRINT_WITH_CONDITIONAL("level is negative", level < 0);
    return _next[level].load(std::memory_order_acquire);
  }
  Node* noBarrierGetNext(int level) const
  {
    CERR_PRINT_WITH_CONDITIONAL("level is negative", level < 0);
    return _next[level].load(std::memory_order_relaxed);
  }
  void setNext(int level, Node* next)
  {
    CERR_PRINT_WITH_CONDITIONAL("level is negative", level < 0);
    _next[level].store(next, std::memory_order_release);
  }
  void noBarrierSetNext(int level, Node* next)
  {
    CERR_PRINT_WITH_CONDITIONAL("level is negative", level < 0);
    _next[level].store(next, std::memory_order_relaxed);
  }
  KeyType getKey() const
  {return _key;}
 private:
  const KeyType _key;
  /* Cur node next node _next[0] is level 0 */
  std::atomic<Node*> _next[1];
};

template <typename KeyType, typename InternalComparator>
SkipList<KeyType, InternalComparator>::SkipList(
      std::shared_ptr<yundb::Arena> arena,
      const Options& options
)
    : _arena(arena),
      _comparator(options),
      _rand(0xdeadbeef),
      _max_height(1),
      _head(newNode(MaxHeight, 0)) 
{
  for (int i = 0; i < MaxHeight; i++)
    _head->setNext(i, nullptr);
}

/* Before insert choice the node insert height */
template <typename KeyType, typename InternalComparator>
int SkipList<KeyType, InternalComparator>::randomHeight()
{
  constexpr int Brancing = 4;
  int height = 1;
  /* 1/Brancing chance insert */
  while (_rand.OneIn(Brancing) && height < MaxHeight)
    height++;
  CERR_PRINT_WITH_CONDITIONAL("height over than MaxHeight",
                               height > MaxHeight);
  return height;
}

/* Find the node pre that no less than key  */
template <typename KeyType, typename InternalComparator>
void SkipList<KeyType, InternalComparator>::findNoLessThanNodePre(
    Node* pre[], const KeyType& key) const
{
  Node* cur = _head;
  int level = getMaxHeight() - 1;

  while (level >= 0)
  {
    Node* next = cur->getNext(level);
    if (next == nullptr)
    {
      pre[level] = cur;
      level -= 1;
      continue;
    }
    int rs = _comparator(key, next->getKey());

    if (rs > 0) cur = next;
    else if (rs < 0)
    {
      pre[level] = cur;
      level -= 1;
    }
  }
}

/* Do the actually insert */
template <typename KeyType, typename InternalComparator>
void SkipList<KeyType, InternalComparator>::doInsert(Node* pre[], const KeyType& key)
{
  Node* node = newNode(MaxHeight, key);

  int height = randomHeight();

  if (height > getMaxHeight())
  {
    for (int i = getMaxHeight(); height > i; i++)
      pre[i] = _head;
    increaseMaxHeight(height - getMaxHeight()); 
  }

  for (int i = 0; height > i; i++)
  {
    node->noBarrierSetNext(i, pre[i]->noBarrierGetNext(i));
    pre[i]->setNext(i, node);
  }
}

template <typename KeyType, typename InternalComparator>
void SkipList<KeyType, InternalComparator>::insert(const KeyType& key)
{
  Node* pre[MaxHeight] = {nullptr};
  findNoLessThanNodePre(pre, key);
  doInsert(pre, key);
}

template <typename KeyType, typename InternalComparator>
KeyType SkipList<KeyType, InternalComparator>::contains(const KeyType& key)
{
  Node* pre[MaxHeight] = {nullptr};
  findNoLessThanNodePre(pre, key);
  return pre[0]->getKey();
}

}

#endif