#include "arena.h"
#include "util/error_print.h"

#include <cassert>

namespace yundb
{

static constexpr size_t KBlockSize = 4096;

Arena::Arena()
    : _available_block(nullptr), _pos(KBlockSize), _memory_usage(0) {} 

Arena::~Arena()
{
  size_t len = _block.size();
  for (size_t i = 0; len > i; i++)
    delete[] _block[i];
}

size_t Arena::getMemoryUsage()
{return _memory_usage.load(std::memory_order_relaxed);}

char* Arena::allocateNewBlock(size_t bytes)
{
  char* result = new char[bytes];
  CERR_PRINT_WITH_CONDITIONAL("Arena allocate fail", result == nullptr);
  _block.push_back(result);
  _memory_usage.fetch_add(sizeof(char*) + bytes, std::memory_order_relaxed);
  return result;
}

char* Arena::allocate(size_t bytes)
{
  CERR_PRINT_WITH_CONDITIONAL("allocate bytes param error", bytes <= 0);
  if (bytes > (KBlockSize / 4))
  {
    char* block = allocateNewBlock(bytes);
    return block;
  }

  if (KBlockSize - _pos < bytes)
  {
    char* block = allocateNewBlock(KBlockSize);
    _available_block = block;
    _pos = bytes;
    return block;
  }
  else
  {
    _pos += bytes;
    return _available_block + _pos;
  }
}

char* Arena::allocateAligned(size_t bytes)
{
  const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8;
  static_assert((align & (align - 1)) == 0,
                "Pointer size should be a power of 2");
  size_t current_mod = reinterpret_cast<uintptr_t>(_available_block + _pos) & (align - 1);
  size_t slop = (current_mod == 0 ? 0 : align - current_mod);
  size_t needed = bytes + slop;
  char* result;

  if (needed <= KBlockSize - _pos)
  {
    result = _available_block + _pos + slop;
    _pos += needed;
  }
  else
  {
    result = allocateNewBlock(KBlockSize);
    _available_block = result;
    _pos = static_cast<int>(needed);
  }
  assert((reinterpret_cast<uintptr_t>(result) & (align - 1)) == 0);
  return result;
}

}
