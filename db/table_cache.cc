#include "table_cache.h"

#include "db/block_reader.h"
#include "db/filter_block_reader.h"
#include "db/table_format.h"
#include "util/coding.h"

#include <cstring>

namespace yundb
{

constexpr size_t FileNumberSize = sizeof(uint64_t);

Slice TableCache::getFilterBlock(const Footer& footer, const char* data)
{
  PosAndSize p = footer.getMetaIndexPosAndSize();
  Slice metaIndexBlock(data + p.first, p.second);
  const char* filterPolicyName = _options.filter_policy->Name();
  size_t filterNameSize = std::strlen(filterPolicyName);

  if (metaIndexBlock.size() < filterNameSize) {
    printError("TableCache: meta index block size error");
    return Slice();
  }

  if (std::memcmp(metaIndexBlock.data(),filterPolicyName, filterNameSize) != 0)
  {
    printError("TableCache: filter policy name not match");
    return Slice();
  }

  const char* ptr = metaIndexBlock.data() + filterNameSize;
  const char* limit = metaIndexBlock.data() + metaIndexBlock.size();
  uint64_t filterBlockPos = 0;
  uint64_t filterBlockSize = 0;

  ptr = GetVarint64Ptr(ptr, limit, &filterBlockPos);
  if (ptr == nullptr) {
    printError("TableCache: filter block pos error");
    return Slice();
  }

  ptr = GetVarint64Ptr(ptr, limit, &filterBlockSize);
  if (ptr == nullptr || ptr != limit) {
    return Slice();
  }

  return Slice(data + filterBlockPos, filterBlockSize);
}

Slice getIndexBlock(const Footer& footer, const char* data)
{
  PosAndSize p = footer.getIndexBlockPosAndSize();
  return Slice(data + p.first, p.second);
}

TableCache::TableCache(const std::string& dbname, const Options& options,
                         std::shared_ptr<Cache> cache)
    : _cache(std::move(cache)), _options(options), _dbname(dbname) {}

TableCache::~TableCache() = default;

void TableCache::insert(uint64_t fileNumber, void* value, size_t valueSize,
                        void (*deleter)(const Slice& key, void* value))
{
  char* key = reinterpret_cast<char*>(&fileNumber);
  _cache->insert(Slice(key, FileNumberSize), value, FileNumberSize + valueSize, deleter);
}

bool TableCache::lookup(uint64_t fileNumber, size_t fileSize, const Slice key, void* value)
{
  char* fileNumberKey = reinterpret_cast<char*>(&fileNumber);
  char* result = static_cast<char*>(_cache->lookup(Slice(fileNumberKey, FileNumberSize)));

  if (result == nullptr) return false;

  Footer footer(Slice(result + fileSize - Footer::MaxFooterSize,
                      Footer::MaxFooterSize));

  Slice FilterBlock = getFilterBlock(footer, result);
  Slice indexBlockHandle = getIndexBlock(footer, result);
  // coding
}

void TableCache::evict(uint64_t fileNumber)
{
  char* fileNumberKey = reinterpret_cast<char*>(&fileNumber);
  _cache->unRef(Slice(fileNumberKey, FileNumberSize));
}

void TableCache::changeOptions(const Options& options)
{
  _options = options;
  _cache->changeCpacity(options.max_cache_size);
}

}