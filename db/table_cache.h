#ifndef YUNDB_DB_TABLE_CACHE_H
#define YUNDB_DB_TABLE_CACHE_H

#include "yundb/en.h"
#include "util/cache.h"

namespace yundb
{

class TableCache
{
 public:
  TableCache(const std::string& dbname, const Options& options, int entries);

  ~TableCache();

 private:
  Cache* cache_;
  Options options_;
  std::string dbname_;
  int entries_;
};

}

#endif // YUNDB_DB_TABLE_CACHE_H