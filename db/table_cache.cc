#include "table_cache.h"

#include "db/block_reader.h"
#include "db/filter_block_reader.h"
#include "db/table_format.h"
#include "util/coding.h"

#include <cstring>
#include <memory>

namespace yundb
{

constexpr size_t FileNumberSize = sizeof(uint64_t);

static const char* entryLimit(const char* start, const char* end)
{
  if (start == nullptr || end == nullptr || end - start < 4) {
    return nullptr;
  }

  const uint32_t restartNum = DecodeFixed32(end - 4);
  const size_t restartBytes = static_cast<size_t>(restartNum + 1) * sizeof(uint32_t);
  if (static_cast<size_t>(end - start) < restartBytes) {
    return nullptr;
  }

  return end - restartBytes;
}

class IndexBlockIterator
{
 public:
  // Start and end of whole index block
  IndexBlockIterator(const char* start, const char* end)
      : _start(start),
        _end(end),
        _cur(start),
        _limit(entryLimit(start, end)),
        _index(0),
        _valid(start != nullptr && end != nullptr && start < end && _limit != nullptr)
  { seekToFirst(); }

  IndexBlockIterator(const IndexBlockIterator&) = delete;

  IndexBlockIterator& operator=(const IndexBlockIterator&) = delete;

  ~IndexBlockIterator() = default;

  inline bool valid() const;

  void seekToFirst();

  void seek(const Slice& target);

  // Get data block handle
  Slice value() const;

  size_t index() const { return _index; }

  const char* blockStart() const { return _start; }

  const char* blockEnd() const { return _end; }
 private:

  const char* _start;
  const char* _end;
  const char* _cur;
  const char* _limit;
  int _index;
  bool _valid;
};

inline bool IndexBlockIterator::valid() const
{ return _valid; }

void IndexBlockIterator::seekToFirst()
{
  if (!_valid) return;

  const char* key = nullptr;
  const char* value = nullptr;
  const char* next = nullptr;
  uint64_t keyLen = 0;
  uint64_t valueLen = 0;
  if (!decodeIndexEntry(_start, _limit, &key, &keyLen, &value, &valueLen, &next)) {
    _index = -1;
    _cur = _start;
    _valid = false;
    return;
  }

  _index = 0;
  _cur = _start;
  _valid = true;
}

void IndexBlockIterator::seek(const Slice& target)
{
  if (!_valid) return;

  _index = 0;
  const char* entry = _start;
  while (entry < _limit)
  {
    const char* key = nullptr;
    const char* value = nullptr;
    const char* next = nullptr;
    uint64_t keyLen = 0;
    uint64_t valueLen = 0;

    if (!decodeIndexEntry(entry, _limit, &key, &keyLen, &value, &valueLen, &next)) {
      _index = -1;
      _cur = _start;
      _valid = false;
      return;
    }

    if (Slice(key, static_cast<size_t>(keyLen)).cmp(target) >= 0) {
      _cur = entry;
      _valid = true;
      return;
    }

    entry = next;
    _index++;
  }

  _index = -1;
  _cur = _start;
  _valid = false;
}

Slice IndexBlockIterator::value() const
{
  if (!_valid) return Slice();

  const char* const limit = entryLimit(_start, _end);
  if (limit == nullptr) return Slice();

  const char* key = nullptr;
  const char* value = nullptr;
  const char* next = nullptr;
  uint64_t keyLen = 0;
  uint64_t valueLen = 0;
  if (!decodeIndexEntry(_cur, limit, &key, &keyLen, &value, &valueLen, &next)) {
    return Slice();
  }

  return Slice(value, static_cast<size_t>(valueLen));
}


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

  BlockHandle filterBlockHandle;
  filterBlockHandle.decodeFrom(ptr);
  uint64_t filterBlockPos = filterBlockHandle.getPosition();
  uint64_t filterBlockSize = filterBlockHandle.getSize();

  return Slice(data + filterBlockPos, filterBlockSize);
}

Slice TableCache::getIndexBlock(const Footer& footer, const char* data)
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

bool TableCache::lookup(uint64_t fileNumber, size_t fileSize, const Slice key, std::string* value)
{
  if (value == nullptr) {
    printError("TableCache: None value ptr");
    return false;
  }

  if (fileSize < Footer::MaxFooterSize) {
    printError("TableCache: file size less than footer size");
    return false;
  }

  value->clear();
  char* fileNumberKey = reinterpret_cast<char*>(&fileNumber);
  char* result = static_cast<char*>(_cache->lookup(Slice(fileNumberKey, FileNumberSize)));

  if (result == nullptr) return false;

  Footer footer(Slice(result + fileSize - Footer::MaxFooterSize,
                      Footer::MaxFooterSize));

  Slice FilterBlock = getFilterBlock(footer, result);
  Slice indexBlock = getIndexBlock(footer, result);
  
  FilterBlockReader filterBlockReader(_options.filter_policy, FilterBlock);
  IndexBlockIterator indexBlockIter(indexBlock.data(), indexBlock.data() + indexBlock.size());
  indexBlockIter.seek(key);

  if (indexBlockIter.valid())
  {
    Slice dataBlockHandle = indexBlockIter.value();
    BlockHandle handle;
    handle.decodeFrom(dataBlockHandle.data());

    if (filterBlockReader.keyMayMatch(indexBlockIter.index(), key)) {
      DataBlockReader dataBlockReader(_options);
      if (dataBlockReader.queryValue(Slice(result + handle.getPosition(), handle.getSize()),
                                     key, value)) {
        return true;
      } else {
        return false;
      }
    } else {
      return false;
    }
  }

  return false;
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