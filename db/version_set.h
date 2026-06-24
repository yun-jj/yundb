#ifndef YUNDB_DB_VERSION_SET_H
#define YUNDB_DB_VERSION_SET_H
// Header guard standardized to YUNDB_DB_VERSION_SET_H

#include "dbformat.h"
#include "version_edit.h"
#include "util/sync.h"
#include "log_writer.h"

#include <memory>
#include <vector>
#include <array>

class TableCache;

namespace yundb
{

// Return the smallest index i such that files[i]->largest >= key.
// Return files.size() if there is no such file.
// REQUIRES: "files" contains a sorted list of non-overlapping files.
int findFile(const std::shared_ptr<Comparator> cmp,
             const std::vector<std::shared_ptr<FileMeta>>& files,
             const Slice& key);

// Returns true iff some file in "files" overlaps the user key range
// [*smallest,*largest].
// smallest==nullptr represents a key smaller than all keys in the DB.
// largest==nullptr represents a key largest than all keys in the DB.
// REQUIRES: If disjoint_sorted_files, files[] contains disjoint ranges
//           in sorted order.
bool someFileOverlapsRange(const std::shared_ptr<Comparator> cmp,
                           bool disjointSortedFiles,
                           const std::vector<std::shared_ptr<FileMeta>>& files,
                           const InternalKey* smallestKey,
                           const InternalKey* largestKey);
class VersionSet;

class Version
{
 public:
  // Call func(arg, level, f) for every file that overlaps user_key in
  // order from newest to oldest.  If an invocation of func returns
  // false, makes no more calls.
  // REQUIRES: user portion of internal_key == user_key.
  void forEachOverlapping(const Slice& userKey, const Slice& internalKey,
                          bool (*func)(void* arg, int level, FileMeta* f), void* arg);
  // Returns true iff some file in the specified level overlaps
  // some part of [*smallest_internalkey,*largest_internalkey].
  // smallest_internalkey==nullptr represents a key smaller than all the DB's keys.
  // largest_internalkey==nullptr represents a key largest than all the DB's keys.
  bool overlapInLevel(int level, const InternalKey* smallestKey, const InternalKey* largestKey);
  // Begin is nullptr means before all keys
  // end is nullptr means after all keys
  void getOverlappingInputs(int level, const Slice& beginUserKey, const Slice& endUserKey,
                            std::vector<std::shared_ptr<FileMeta>>& inputs);
  // Return a level for compact memtable
  int pickLevelForMemTableOutput(const InternalKey& smallestKey, const InternalKey& largestKey);

  void ref();

  // Return false if the reference count has already dropped to zero, and deletes this.
  // otherwise return true.
  bool unRef();
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
  // Next compact file and level based on seek stats.
  int _compactFileLevel;
  FileMeta* _compactFile;
  // Level that should be size compacted next and its compaction score.
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
             std::shared_ptr<Comparator> internalComparator,
             std::shared_ptr<TableCache> tableCache);

  ~VersionSet();
  // Apply *edit to the current version to form a new descriptor that
  // is both saved to persistent state and installed as the new
  // current version.  Will release *mu while actually writing to the file.
  // REQUIRES: *mu is held on entry.
  // REQUIRES: no other thread concurrently calls LogAndApply()
  bool logAndApply(VersionEdit& edit, sync::Mutex* mu) noexcept;

  void addLiveFiles(std::set<uint64_t>& liveFiles) const;

  // Resume the version from CURRENT file pointed MANIFEST.
  // Merge all the version edits to the one version and set it as current version.
  bool resume();

  // Return current version
  Version* current() {return _cur;}

  uint64_t getManifestFileNumber() {return _manifestFileNumber;}

  uint64_t getNewFileNumber() {return _nextFileNumber++;}

  inline int levelTablesNumber(int level) const;

  inline uint64_t levelTablesBytes(int level) const;

 private:
  // Choice level for compaction
  void finalize(Version* version);

  void appendVersion(Version* version);

  void saveSnapshot(log::Writer* log);

  bool parseManifestFile(log:: Reader* reader, Version *version);

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

  std::shared_ptr<TableCache> _tableCache;
  // Opened lazily
  WritableFile* _descriptorFile;
  log::Writer* _descriptorLog;
  // Cur version
  Version* _cur;
  // node<->node<-.......node<->dummy
  Version _dummyVersion;
  // Per-level key at which the next compaction at that level should start.
  // Either an empty string, or a valid InternalKey.
  std::array<std::string, MaxFileLevel> _compactPoints;
};

}

#endif // YUNDB_DB_VERSION_SET_H