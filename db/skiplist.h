#ifndef SKIP_LIST_H
#define SKIP_LIST_H

#include <memory>
#include <iostream>
#include <atomic>
#include <string>

#include "util/random.h"
#include "util/arena.h"

namespace yundb
{
namespace skiplist
{

/* Skiplist max height */
static constexpr int MaxHeight = 12;

/* skiplist node in memory */
template <typename KeyType, class Comparator>
class SkipList<KeyType, Comparator>::Node
{
 public:
  Node() = default;
  Node(Node& other) = delete;
  Node& operator=(Node& other) = delete;
  Node(const KeyType& key) : _key(key){}
  Node* getNext(int level)
  {
    if (level < 0) std::cerr << "level is negative\n";
    return _next[level].load(std::memory_order_acquire);
  }
  Node* noBarrierGetNext(int level)
  {
    if (level < 0) std::cerr << "level is negative\n";
    return _next[level].load(std::memory_order_relaxed);
  }
  void setNext(int level, Node* next)
  {
    if (level < 0) std::cerr << "level is negative\n";
    _next[level].store(next, std::memory_order_release);
  }
  void noBarrierSetNext(int level, Node* next)
  {
    if (level < 0) std::cerr << "level is negative\n";
    _next[level].store(next, std::memory_order_relaxed);
  }
  KeyType getKey() const
  {return _key;}
 private:
  const KeyType _key;
  /* Cur node next node _next[0] is level 0 */
  std::atomic<Node*> _next[1];
};


template <typename KeyType, class Comparator>
class SkipList
{
 private:
  class Node;
 public:
  SkipList() = default;
  SkipList(std::shared_ptr<yundb::Arena> arena, Comparator comparator);
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
  const Comparator _comparator;
  yundb::Random _rand;
  std::atomic<int> _max_height;
  /* Node list head, head value is infinitesimal number */
  Node* const _head;
  /* Create a new node */
  Node* newNode(int height, const KeyType& key)
  {
      char* const node = _arena->allocateAligned(
      sizeof(Node) + sizeof(std::atomic<Node*>) * (height - 1));
      return new (node) Node<KeyType>(key);
  }
  void findNoLessThanNodePre(Node* pre[], const KeyType& key);
  bool checkDuplicates(Node* pre[], const KeyType& key) const;
  int randomHeight();
  void doInsert(Node* pre[], const KeyType& key);
  int getMaxHeight() const
  {return _max_height.load(std::memory_order_relaxed);}
  void increaseMaxHeight(int h)
  {_max_height.fetch_add(h, std::memory_order_relaxed);}
};

template <typename KeyType, class Comparator>
SkipList<KeyType, Comparator>::SkipList(std::shared_ptr<yundb::Arena> arena, Comparator comparator)
    : _arena(arena),
     _comparator(comparator),
     _rand(0xdeadbeef),
     _max_height(1),
     _head(newNode(MaxHeight, 0)) 
{
  for (int i = 0; i < MaxHeight; i++)
    _head->setNext(i, nullptr);
}

/* Before insert choice the node insert height */
template <typename KeyType, class Comparator>
int SkipList<KeyType, Comparator>::randomHeight()
{
  constexpr int Brancing = 4;
  int height = 1;
  /* 1/Brancing chance insert */
  while (_rand.OneIn(Brancing) && height < MaxHeight)
    height++;
  if (height > MaxHeight) std::cerr << "height over than MaxHeight\n";
  return height;
}

/* Find the node pre that no less than key  */
template <typename KeyType, class Comparator>
void SkipList<KeyType, Comparator>::findNoLessThanNodePre(
    Node* pre[], const KeyType& key)
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
    if (rs == 0)
    {
      pre[level] = cur;
      return;
    }
    else if (rs > 0) cur = next;
    else if (rs < 0) level -= 1;
  }
}

/* Do the actually insert */
template <typename KeyType, class Comparator>
void SkipList<KeyType, Comparator>::doInsert(Node* pre[], const KeyType& key)
{
  constexpr int Branching = 4;
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

template <typename KeyType, class Comparator>
void SkipList<KeyType, Comparator>::insert(const KeyType& key)
{
  Node* pre[MaxHeight] = {nullptr};
  findNoLessThanNodePre(pre, key);

  /* Ensure key not duplicate */
  if (checkDuplicates(pre, key))
  {
    std::cerr << "Skiplist: duplicate insert\n";
    return;
  } 
  doInsert(pre, key);
}

template <typename KeyType, class Comparator>
KeyType SkipList<KeyType, Comparator>::contains(const KeyType& key)
{
  Node* cur = _head;
  int level = getMaxHeight() - 1;
  
  while (level >= 0)
  {
    Node* next = cur->getNext(level);
    if (next == nullptr)
    {
      level -= 1;
      continue; 
    }
    int rs = _comparator(key, next->getKey());
    if (rs == 0) return next->getKey();
    else if (rs > 0) cur = next; 
    else if (rs < 0) level -= 1;
  }
  return KeyType();
}

template <typename KeyType, class Comparator>
bool SkipList<KeyType, Comparator>::checkDuplicates(
    Node* pre[], const KeyType& key) const
{
  for (int i = 0; MaxHeight > i; i++)
  {
    if (pre[i] != nullptr)
      if (_comparator(key, pre[i]->getKey()) == 0)
        return true;
  }

  return false;
}

}

}

#endif