#ifndef VERSION_SET_H
#define VERSION_SET_H

#include "dbformat.h"
#include "version_edit.h"

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
        _compactionScore(-1),
        _compactionLevel(-1),
        _versionSet(versonSet) {}
  Version(const Version& other) = delete;
  Version& operator=(const Version& other) = delete;

  ~Version();

  void ref();
  void unRef();
 private:
  int _ref;
  int _compactFileLevel;
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
 private:
  friend class Version;
  // Cur version
  Version* _cur;
  Version _dummyVersion;
};

}

#endif