#include "yundb/comparator.h"
#include "util/no_destructor.h"

namespace yundb
{

class BytewiseComparator : public Comparator
{
 public:
  BytewiseComparator() = default;
  ~BytewiseComparator() = default;
  inline int operator()(const Slice& key1, const Slice& key2) override
  {return key1.cmp(key2);}
};

Comparator* getBytewiseComparator()
{
  static NoDestructor<BytewiseComparator> singleton;
  return singleton.get();
}

}