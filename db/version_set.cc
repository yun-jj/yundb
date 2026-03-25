#include "version_set.h"
#include "util/file_name.h"
#include "log_writer.h"

#include <algorithm>

namespace yundb
{

int findFile(const std::shared_ptr<Comparator> cmp,
             const std::vector<std::shared_ptr<FileMeta>>& files,
             const Slice& key)
{
  uint32_t left = 0;
  uint32_t right = files.size();
  while (left < right) {
    uint32_t mid = (left + right) / 2;
    const auto& f = files[mid];
    if (cmp->cmp(f->largest, key) < 0) {
      // Key at "mid.largest" is < "target".  Therefore all
      // files at or before "mid" are uninteresting.
      left = mid + 1;
    } else {
      // Key at "mid.largest" is >= "target".  Therefore all files
      // after "mid" are uninteresting.
      right = mid;
    }
  }
  return right;
}

bool someFileOverlapsRange(const std::shared_ptr<Comparator> cmp,
                           bool disjointSortedFiles,
                           const std::vector<std::shared_ptr<FileMeta>>& files,
                           const Slice* smallestUserKey,
                           const Slice* largestUserKey)
{
  if (!disjointSortedFiles) {
    // Need to check against all files
    for (size_t i = 0; i < files.size(); i++) {
      const auto& f = files[i];
      if (afterFile(cmp, smallestUserKey, f) ||
          beforeFile(cmp, largestUserKey, f)) {
        // No overlap
      } else {
        return true;  // Overlap
      }
    }
    return false;
  }

  // Binary search over file list
  uint32_t index = 0;
  if (smallestUserKey != nullptr) {
    // Find the earliest possible internal key for smallest_user_key
    index = findFile(cmp, files, *smallestUserKey);
  }

  if (index >= files.size()) {
    // beginning of range is after all files, so no overlap.
    return false;
  }

  return !beforeFile(cmp, largestUserKey, files[index]);
}

static size_t targetFileSize(const Options* options)
{return options->max_file_size;}

// stop building a single file in a level->level+1 compaction.
static int64_t maxGrandParentOverlapBytes(const Options* options)
{return 10 * targetFileSize(options);}


static size_t totalFileSize(const std::vector<std::shared_ptr<FileMeta>>& files)
{
  size_t sums = 0;
  for (auto& f : files)
    sums += f->fileSize;
  return sums;
}

static bool afterFile(const std::shared_ptr<Comparator> cmp, const Slice* key,
                      const std::shared_ptr<FileMeta> f)
{
  // null user_key occurs before all keys and is therefore never after *f
  return (key != nullptr && cmp->cmp(*key, f->largest) > 0); 
}

static bool beforeFile(const std::shared_ptr<Comparator> cmp, const Slice* key,
                       const std::shared_ptr<FileMeta> f)
{
  // null user_key occurs after all keys and is therefore never before *f
  return (key != nullptr && cmp->cmp(*key, f->smallest) < 0);
}



Version::~Version()
{
  _pre->_next = _next;
  _next->_pre = _pre;
}

void Version::ref()
{++_ref;}

void Version::unRef()
{
  CERR_PRINT_WITH_CONDITIONAL(
    "Version: ref <= 0",
    _ref <= 0
  );

  CERR_PRINT_WITH_CONDITIONAL(
    "Version: unRef a dummyVersion",
    this == &_versionSet->_dummyVersion
  );
  _ref--;

  if (_ref == 0) delete this;
}

bool Version::overlapInLevel(int level, const Slice* smallestUserKey,
                             const Slice* largestUserKey)
{
  return someFileOverlapsRange(_versionSet->_comparator, (level > 0), _files[level],
                               smallestUserKey, largestUserKey);
}

// Store in "*inputs" all files in "level" that overlap [begin,end]
void Version::getOverlappingInputs(int level, const Slice* begin,
                                   const Slice* end,
                                   std::vector<std::shared_ptr<FileMeta>>& inputs)
{
  assert(level >= 0);
  assert(level < MaxFileLevel);

  inputs.clear();
  std::shared_ptr<Comparator> cmp = _versionSet->_comparator;

  for (size_t i = 0; i < _files[level].size();)
  {
    auto& f = _files[level][i++];
    const Slice fileStart(f->smallest);
    const Slice fileLimit(f->largest);
    if (begin != nullptr && cmp->cmp(fileLimit, *begin) < 0) {
      // "f" is completely before specified range; skip it
    } else if (end != nullptr && cmp->cmp(fileStart, *end) > 0) {
      // "f" is completely after specified range; skip it
    }
    else
    {
      inputs.push_back(f);
      if (level == 0)
      {
        // Level-0 files may overlap each other.  So check if the newly
        // added file has expanded the range.  If so, restart search.
        if (begin != nullptr && cmp->cmp(fileStart, *begin) < 0)
        {
          begin = &fileStart;
          inputs.clear();
          i = 0;
        }
        else if (end != nullptr && cmp->cmp(fileLimit, *end) > 0)
        {
          end = &fileLimit;
          inputs.clear();
          i = 0;
        }
      }
    }
  }
}

int Version::pickLevelForMemTableOutput(const Slice& smallestUserKey,
                                        const Slice& largestUserKey)
{
  int level = 0;
  if (!overlapInLevel(0, &smallestUserKey, &largestUserKey))
  {
    std::vector<std::shared_ptr<FileMeta>> overlaps;
    // Push to next level if there is no overlap in next level,
    // and the #bytes overlapping in the level after that are limited.
    while (level < MaxMemCompactLevel)
    {
      if (overlapInLevel(level + 1, &smallestUserKey, &largestUserKey)) {
        break;
      }

      if (level + 2 < MaxFileLevel)
      {
        // Check that file does not overlap too many grandparent bytes.
        getOverlappingInputs(level + 2, &smallestUserKey, &largestUserKey,
                             overlaps);
        const int64_t sum = totalFileSize(overlaps);
        if (sum > maxGrandParentOverlapBytes(&(_versionSet->_options))) {
          break;
        }
      }
      level++;
    }
  }
  return level;
}

class VersionSet::Builder
{
 private:
  struct FileComparator
  {
    std::shared_ptr<Comparator> internalComparator;

    bool operator()(const std::shared_ptr<FileMeta> f1,
                    const std::shared_ptr<FileMeta> f2) const
    {
      int r = internalComparator->cmp(f1->smallest, f2->smallest);
      if (r != 0) return (r < 0);
      return (f1->number < f2->number);
    }
  };

  void maybeAddFile(Version* v, int level, std::shared_ptr<FileMeta> f);
  
 public:
  Builder(VersionSet* set, Version* version);
  ~Builder();

  Builder(const Builder& other) = delete; 
  Builder& operator=(const Builder& other) = delete;
  void apply(VersionEdit* edit);
  void saveTo(Version* v);
 private:
  VersionSet* _set;
  Version* _curVersion;
  std::shared_ptr<Comparator> _comparator;
  std::set<std::shared_ptr<FileMeta>, FileComparator> _addedFiles[MaxFileLevel];
  std::set<uint64_t> _deleteFiles[MaxFileLevel];
};

VersionSet::Builder::Builder(VersionSet* set, Version* version)
      : _set(set),
        _curVersion(version),
        _comparator(set->_comparator)
{version->ref();}

VersionSet::Builder::~Builder()
{_curVersion->unRef();}

void VersionSet::Builder::maybeAddFile(Version* v, int level,
                                       std::shared_ptr<FileMeta> f)
{
  if (_deleteFiles[level].count(f->number) > 0) {
    // Deleted file do nothing
  } else {
    std::vector<std::shared_ptr<FileMeta>>& files = v->_files[level];
    if (level > 0 && !files.empty())
    {
      assert(_set->_comparator->cmp(
        files.back()->largest, f->smallest) < 0);
    }
    f->ref++;
    files.push_back(f);
  }
}

void VersionSet::Builder::apply(VersionEdit* edit)
{
  // Update compaction pointers
  for (int i = 0; edit->_compactPoints.size() > i; i++) 
  {
    int level = edit->_compactPoints[i].first;
    _set->_compactPoints[level] = edit->_compactPoints[i].second;
  }

  // Update deleted files
  for (const auto& pair : edit->_deleteFiles)
  {
    int level = pair.first;
    _deleteFiles[level].insert(pair.second);
  }

  // Update new file
  for (const auto& pair : edit->_newFiles)
  {
    int level = pair.first;
    pair.second->ref = 1;
    int allowedSeek = static_cast<int>(pair.second->fileSize / 16384U);
    if (allowedSeek < 100) allowedSeek = 100;
    // We arrange to automatically compact this file after
    // a certain number of seeks.  Let's assume:
    //   (1) One seek costs 10ms
    //   (2) Writing or reading 1MB costs 10ms (100MB/s)
    //   (3) A compaction of 1MB does 25MB of IO:
    //         1MB read from this level
    //         10-12MB read from next level (boundaries may be misaligned)
    //         10-12MB written to next level
    // This implies that 25 seeks cost the same as the compaction
    // of 1MB of data.  I.e., one seek costs approximately the
    // same as the compaction of 40KB of data.  We are a little
    // conservative and allow approximately one seek for every 16KB
    // of data before triggering a compaction.

    // That is, we consider compacting this file to the next level
    // when the wasted seek cost on the current file exceeds the cost of compaction.
    _deleteFiles[level].erase(pair.second->number);
    _addedFiles[level].insert(pair.second);
  }

}

void VersionSet::Builder::saveTo(Version* v)
{
  FileComparator cmp;
  cmp.internalComparator = _set->_comparator;

  for (int level = 0; level < MaxFileLevel; level++)
  {
    const auto& baseFiles = _curVersion->_files[level];
    std::vector<std::shared_ptr<FileMeta>>::const_iterator baseIter = 
      baseFiles.begin();
    std::vector<std::shared_ptr<FileMeta>>::const_iterator baseEnd =
      baseFiles.end(); 
    const auto& addedFiles = _addedFiles[level];

    v->_files[level].reserve(baseFiles.size() + addedFiles.size());
    // Merge sort
    for (const auto& file : addedFiles)
    {
      for (auto bpos = std::upper_bound(baseIter, baseEnd, file, cmp);
           baseIter != bpos; ++baseIter) {
        maybeAddFile(v, level, *baseIter);
      }

      maybeAddFile(v, level, file);
    }

    // Add remaining base files
    for (; baseIter != baseEnd; ++baseIter)
      maybeAddFile(v, level, *baseIter);
    
    // Checking there is no overlap
    if (level > 0)
    {
      for (uint32_t i = 1; i < v->_files[level].size(); i++)
      {
        const auto& prevEnd = v->_files[level][i - 1]->largest;
        const auto& thisBegin= v->_files[level][i]->smallest;
        CERR_PRINT_WITH_CONDITIONAL(
          "VersionSet: overlapping ranges in level " << level,
          _set->_comparator->cmp(prevEnd, thisBegin) >= 0
        );
      }
    }
  }
}

VersionSet::VersionSet(const std::string dbName, const Options options,
                       std::shared_ptr<Comparator> InternalComparator,
                       std::shared_ptr<TableCache> tableCache)
      : _dbName(dbName),
        _options(options), 
        _comparator(InternalComparator),
        _tableCache(tableCache),
        _cur(nullptr),
        _dummyVersion(this) {}

VersionSet::~VersionSet() {}

void VersionSet::finalize(Version* version)
{
  int baseLevel = -1;
  double baseScore = -1.0;
  double curScore;

  for (int level = 0; MaxFileLevel > level; level++)
  {
    if (level == 0)
    {
      // We treat level-0 specially by bounding the number of files
      // instead of number of bytes for two reasons:
      //
      // (1) With larger write-buffer sizes, it is nice not to do too
      // many level-0 compactions.
      //
      // (2) The files in level-0 are merged on every read and
      // therefore we wish to avoid too many files when the individual
      // file size is small (perhaps because of a small write-buffer
      // setting, or very high compression ratios, or lots of
      // overwrites/deletions).

      curScore = static_cast<double>(version->_files[level].size())
        / static_cast<double>(L0CompactionTrigger);
    }
    else
    {
      curScore = static_cast<double>(levelTablesBytes(level))
        / static_cast<double>(maxBytesForLevel(level));
    }

    if (curScore > baseScore)
    {
      baseScore = curScore;
      baseLevel = level;
    }
  }

  version->_compactFileLevel = baseLevel;
  version->_compactionScore = baseScore;
}

void VersionSet::appendVersion(Version* version)
{
  assert(version->_ref == 0);  
  assert(version != _cur);

  if (_cur != nullptr) _cur->unRef();

  _cur = version;
  version->ref();

  version->_pre = _dummyVersion._pre;
  version->_next = &_dummyVersion;
  version->_pre->_next = version;
  version->_next->_pre = version;
}

void VersionSet::saveSnapshot(Writer* log)
{
  VersionEdit edit;
  edit.setComparatorName(_comparator->name());

  // Save compaction pointers
  for (int level = 0; MaxFileLevel > level; level++)
  {
    if (!_compactPoints[level].empty())
      edit.setCompactPointer(level, _compactPoints[level]);
  }

  // Save files
  for (int level = 0; MaxFileLevel > level; level++)
  {
    auto& fileVector = _cur->_files[level];
    for (auto& f : fileVector)
      edit.addFile(level, f->number, f->fileSize, f->smallest, f->largest);
  }

  std::string record;
  edit.encode(&record);
  log->appendRecord(record);
}

bool VersionSet::logAndApply(VersionEdit& edit, sync::Mutex* mu) noexcept
{
  if (edit._hasLogNumber)
  {
    CERR_PRINT_WITH_CONDITIONAL(
      "VersionSet: log number error",
      edit._logNumber < _logNumber || 
      edit._logNumber >= _nextFileNumber
    );
  }
  else edit.setLogNumber(_logNumber);

  if (!edit._hasPreLogNumber)
    edit.setPreLogNumber(_preLogNumber);

  edit.setNextFileNumber(_nextFileNumber);
  edit.setLastSequence(_lastSequence);

  Version* v = new Version(this);
  if (v == nullptr) return false;

  {
    Builder builder(this, _cur);
    builder.apply(&edit);
    builder.saveTo(v);
  }
  
  finalize(v);

  std::string newManifestFile;
  if (_descriptorLog == nullptr)
  {
    // No reason to unlock *mu here since we only hit this path in the
    // first call to LogAndApply (when opening the database).
    assert(_descriptorFile == nullptr);
    newManifestFile = generateDescriptorFileName(_manifestFileNumber, _dbName);
    newWritableFile(newManifestFile, &_descriptorFile);
    // Writer need crc code
    _descriptorLog = new Writer(_descriptorFile);
    saveSnapshot(_descriptorLog);
  }

  // Unlock during expensive MANIFEST log write
  {
    mu->Unlock();

    // Write new record to MANIFEST log
    std::string record;
    edit.encode(&record);
    _descriptorLog->appendRecord(record);
    _descriptorFile->sync();

    if (!newManifestFile.empty())
      setCurrentFile(_options.env, _dbName, _manifestFileNumber);

    mu->Lock();
  }

  // Update new version
  appendVersion(v);
  _logNumber = edit._logNumber;
  _preLogNumber = edit._preLogNumber;
  return true;
}

static uint64_t maxBytesForLevel(int level)
{
  // Init for 10 MB
  uint64_t result = 10 * 1048576;
  
  while (level > 0)
  {
    result *= 10;
    level--;
  }

  return result;
}

}