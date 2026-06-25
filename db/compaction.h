#ifndef YUNDB_DB_COMPACTION_H
#define YUNDB_DB_COMPACTION_H

#include "version_set.h"

namespace yundb
{

class Compaction
{
 public:
  Compaction(Version* v);
  ~Compaction();
  Compaction(const Compaction& other) = delete;
  Compaction& operator=(const Compaction& other) = delete;
 private:
  int _compactionLevel;
};

}

#endif