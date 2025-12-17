#ifndef COMPARATOR_H
#define COMPARATOR_H

#include "yundb/slice.h"

namespace yundb
{

class Comparator
{
 public:
  Comparator() = default;
  virtual ~Comparator() = default;
  // Three-way comparison.  Returns value:
  //   < 0 iff "a" < "b",
  //   == 0 iff "a" == "b",
  //   > 0 iff "a" > "b"
  virtual int cmp(const Slice& key1, const Slice& key2) const = 0;
};

Comparator* BytewiseCmp();

}

#endif