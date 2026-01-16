#include "version_set.h"

namespace yundb
{

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

class VersionSet::Builder
{
 private:
  struct FileComparator
  {
    std::shared_ptr<Comparator> _internalComparator;

    bool operator()(const std::shared_ptr<FileMeta> f1,
                    const std::shared_ptr<FileMeta> f2) const
    {
      int r = _internalComparator->cmp(f1->smallest, f2->smallest);
      if (r != 0) return (r < 0);
      return (f1->number < f2->number);
    }
  };
  
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
  std::set<std::shared_ptr<FileMeta>, FileComparator> _newFiles[MaxFileLevel];
  std::set<uint64_t> _deleteFiles[MaxFileLevel];
};

VersionSet::Builder::Builder(VersionSet* set, Version* version)
      : _set(set),
        _curVersion(version),
        _comparator(set->_comparator)
{version->ref();}

VersionSet::Builder::~Builder()
{_curVersion->unRef();}

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
    _newFiles[level].insert(pair.second);
  }

}

VersionSet::VersionSet(const std::string dbName, const Options options,
                       std::shared_ptr<Comparator> InternalComparator)
      : _dbName(dbName),
        _options(options), 
        _comparator(InternalComparator),
        _cur(nullptr),
        _dummyVersion(this) {}

VersionSet::~VersionSet() {}

}