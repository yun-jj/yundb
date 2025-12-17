#ifndef TEST_UTIL_H
#define TEST_UTIL_H

#include "util/random.h"

#include <algorithm>
#include <string>
#include <memory>

struct StringGenerater
{
  StringGenerater(uint64_t seed = 0xdeadbeef, size_t maxStringSize = 200)
        : _rand(seed),
          _maxStringSize(maxStringSize){}
  
  std::string getRandString()
  {
    static char c[10] =
        {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'y', 'j'};
    
    std::string result;

    size_t size = _rand.Uniform(_maxStringSize) + 1;
    for (int i = 0; size > i; i++)
      result.append(std::string{c[_rand.Uniform(10)]});
    return result;
  }

  yundb::Random _rand;
  size_t _maxStringSize;
};

#endif