#ifndef YUNDB_DB_TABLE_CACHE_H
#define YUNDB_DB_TABLE_CACHE_H

#include "yundb/en.h"
#include "util/cache.h"

#include <memory>

namespace yundb
{

class Footer;

class TableCache
{
 public:
  TableCache(const std::string& dbname, const Options& options,
             std::shared_ptr<Cache> cache);

  ~TableCache();

  // Deleter is the function to delete this key-value pair when evicted from cache
  void insert(uint64_t fileNumber, void* value, size_t valueSize,
              void (*deleter)(const Slice& key, void* value));
    
  // Find the value of key in specified fileNumber 
  bool lookup(uint64_t fileNumber, size_t fileSize, const Slice key, std::string* value);

  // Remove fileNumber entry from cache
  void evict(uint64_t fileNumber);

  void changeOptions(const Options& options);

 private:
  Slice getFilterBlock(const Footer& footer, const char* data);
  Slice getIndexBlock(const Footer& footer, const char* data);
  std::shared_ptr<Cache> _cache;
  Options _options;
  std::string _dbname;
};

}

#endif // YUNDB_DB_TABLE_CACHE_H