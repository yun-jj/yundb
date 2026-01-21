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
#include <array>

namespace yundb
{

// Return the smallest index i such that files[i]->largest >= key.
// Return files.size() if there is no such file.
// REQUIRES: "files" contains a sorted list of non-overlapping files.
int FindFile(const std::shared_ptr<Comparator> cmp,
             const std::vector<std::shared_ptr<FileMeta>>& files,
             const Slice& key);

// Returns true iff some file in "files" overlaps the user key range
// [*smallest,*largest].
// smallest==nullptr represents a key smaller than all keys in the DB.
// largest==nullptr represents a key largest than all keys in the DB.
// REQUIRES: If disjoint_sorted_files, files[] contains disjoint ranges
//           in sorted order.
bool SomeFileOverlapsRange(const std::shared_ptr<Comparator> cmp,
                           bool disjointSortedFiles,
                           const std::vector<std::shared_ptr<FileMeta>>& files,
                           const Slice* smallestUserKey,
                           const Slice* largestUserKey);

class Version
{
 public:
  // Returns true iff some file in the specified level overlaps
  // some part of [*smallest_user_key,*largest_user_key].
  // smallest_user_key==nullptr represents a key smaller than all the DB's keys.
  // largest_user_key==nullptr represents a key largest than all the DB's keys.
  bool overlapInLevel(int level, const Slice* smallest_user_key,
                      const Slice* largest_user_key);
  // Return a level for compact memtable
  int pickLevelForMemTableOutput(const Slice& smallestUserKey,
                                 const Slice& largestUserKey);
  void ref();
  void unRef();
 private:

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

  friend class VersionSet;

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
  bool logAndApply(VersionEdit& edit, sync::Mutex* mu) noexcept;

  // Return current version
  Version* current() {return _cur;}

  uint64_t getManifestFileNumber() {return _manifestFileNumber;}

  uint64_t getNewFileNumber() {return _nextFileNumber++;}

  int levelTablesNumber(int level) const
  {
    CERR_PRINT_WITH_CONDITIONAL(
      "VersionSet: level number error",
      level < 0 || level > MaxFileLevel
    );
    return _cur->_files[level].size();
  }

  uint64_t levelTablesBytes(int level) const
  {
    CERR_PRINT_WITH_CONDITIONAL(
      "VersionSet: level number error",
      level < 0 || level > MaxFileLevel
    );
    uint64_t result = 0;
    for (auto f : _cur->_files[level])
      result += f->fileSize;

    return result;
  }

 private:
  
  // Choice level for compaction
  void finalize(Version* version);

  void appendVersion(Version* version);

  void saveSnapshot(Writer* log);

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
  WritableFile* _descriptorFile;
  Writer* _descriptorLog;
  // Cur version
  Version* _cur;
  // node<->node<-.......node<->dummy
  Version _dummyVersion;
  // Per-level key at which the next compaction at that level should start.
  // Either an empty string, or a valid InternalKey.
  std::array<std::string, MaxFileLevel> _compactPoints;
};

}

#endif