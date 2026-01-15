#ifndef VERSION_SET_H
#define VERSION_SET_H

#include "yundb/en.h"
#include "yundb/options.h"
#include "log_writer.h"
#include "dbformat.h"
#include "version_edit.h"
#include "util/sync.h"

#include <memory>
#include <vector>

namespace yundb
{

class Version
{
 public:
  explicit Version(VersionSet* versonSet)
      : _ref(0),
        _compactFileLevel(-1),
        _compactFile(nullptr),
        _compactionScore(-1),
        _compactionLevel(-1),
        _versionSet(versonSet),
        _pre(this),
        _next(this) {}
  Version(const Version& other) = delete;
  Version& operator=(const Version& other) = delete;

  ~Version();

  void ref();
  void unRef();
 private:
  int _ref;
  // Next compact file and level
  int _compactFileLevel;
  FileMeta * _compactFile;
  // Level that should be compacted next and its compaction score.
  // Score < 1 means compaction is not strictly needed.  These fields
  // are initialized by VersionSet::Finalize().
  double _compactionScore;
  int _compactionLevel;
  VersionSet* _versionSet;
  Version* _pre;
  Version* _next;

  std::vector<std::shared_ptr<FileMeta>> _files[MaxFileLevel];
  // Next file to compact based on seek stats.
  std::shared_ptr<FileMeta> _nextCompactFile;
};

class VersionSet
{
 public:
  VersionSet(const std::string dbName, const Options options,
             std::shared_ptr<Comparator> internalComparator);
  ~VersionSet();
  // Apply *edit to the current version to form a new descriptor that
  // is both saved to persistent state and installed as the new
  // current version.  Will release *mu while actually writing to the file.
  // REQUIRES: *mu is held on entry.
  // REQUIRES: no other thread concurrently calls LogAndApply()
  void logAndApply(const VersionEdit& edit, sync::Mutex* mu);
 private:
  class Builder;
  friend class Version;
  friend class VersionEdit;
  
  const std::string _dbName;
  const Options _options;
  std::shared_ptr<Comparator> _comparator;
  uint64_t _nextFileNumber;
  uint64_t _manifestFileNumber;
  uint64_t _lastSequence;
  uint64_t _logNumber;
  uint64_t _preLogNumber;

  // Opened lazily
  std::shared_ptr<WritableFile> _descriptorFile;
  std::shared_ptr<Writer> _descriptorLog;
  // Cur version
  Version* _cur;
  Version _dummyVersion;
  // 
  std::vector<std::pair<int, std::string>> _compactPoints;
};

}

#endif