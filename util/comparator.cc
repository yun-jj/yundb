#include "yundb/comparator.h"
#include "yundb/slice.h"

namespace yundb
{

class BytewiseComparator : public Comparator
{
 public:
  BytewiseComparator() = default;
  ~BytewiseComparator() = default;
  inline int cmp(const Slice& key1, const Slice& key2) const override
  {return key1.cmp(key2);}
};

Comparator* BytewiseCmp()
{
  static BytewiseComparator cmp;
  return &cmp;
}

}