#ifndef ARENA_H
#define ARENA_H

#include <vector>
#include <atomic>

/* Memory allocate class */
namespace yundb
{

class Arena
{
 public:
  Arena();
  ~Arena();
  Arena(Arena& other) = delete;
  Arena& operator=(Arena& other) = delete;
  char* allocate(size_t bytes);
  char* allocateAligned(size_t bytes);
 private:
  char* allocateNewBlock(size_t bytes);
  std::vector<char*> _block;
  std::atomic<size_t> _memory_usage;
  char* _available_block;
  int _pos;
};

}

#endif