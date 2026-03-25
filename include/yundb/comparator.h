#ifndef YUNDB_INCLUDE_YUNDB_COMPARATOR_H
#define YUNDB_INCLUDE_YUNDB_COMPARATOR_H
// Header guard standardized to YUNDB_INCLUDE_YUNDB_COMPARATOR_H

#include "yundb/slice.h"

namespace yundb
{

class Comparator
{
 public:
   Comparator() = default;
   virtual ~Comparator() = default;

   // Comparator name
   virtual const char* name() const = 0;
   virtual int cmp(const Slice& key1, const Slice& key2) const = 0;
};



Comparator* BytewiseCmp();

}

#endif // YUNDB_INCLUDE_YUNDB_COMPARATOR_H