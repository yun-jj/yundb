#ifndef VERSION_EDIT_H
#define VERSION_EDIT_H

#include "dbformat.h"

#include <memory>
#include <cstdint>
#include <string>
#include <vector>
#include <set>
#include <utility>

namespace yundb
{
// FileMeta requires std::shared_ptr for automatic cleanup
struct FileMeta
{
  FileMeta() : ref(0), allowedSeek(AllowedSeekTime), fileSize(0) {}
  FileMeta(uint64_t fileNumber, size_t fileSize,
           const std::string& largest, const std::string& smallest)
      : ref(0),
        allowedSeek(AllowedSeekTime),
        number(fileNumber),
        largest(largest),
        smallest(smallest) {}
  
  int ref;
  // Allowed seek time
  int allowedSeek;
  // The file number
  uint64_t number;
  // The largest key
  std::string largest;
  // The smallest key;
  std::string smallest;
  size_t fileSize;
};

class VersionEdit
{
 public:
  void clear();
  VersionEdit(){clear();};
  ~VersionEdit() = default;

  // Encode edit to dst
  void encode(std::string* dst);

  // Decode edit informations
  void decode(const Slice& data);

  void addFile(int level, uint64_t fileNumber, size_t fileSize,
               const std::string& largest, const std::string& smallest)
  {
    _newFiles.push_back(
      std::make_pair(level, std::make_shared<FileMeta>(
        fileNumber, fileSize, largest, smallest))
      );
  }

  void deleteFile(int level, uint64_t fileNumber){
    _deleteFiles.insert(std::make_pair(level, fileNumber));
  }


  void setComparatorName(std::string name)
  {
    CERR_PRINT_WITH_CONDITIONAL(
      "VersionEdit: comparator name is empty",
      name.empty()
    )
    _comparatorName = name;
  }

  void setLogNumber(uint64_t number)
  {
    _logNumber = number;
    _hasLogNumber = true;
  }

  void setPreLogNumber(uint64_t number)
  {
    _preLogNumber = number;
    _hasPreLogNumber = true;
  }

  void setNextLogNumber(uint64_t number)
  {
    _nextLogNumber = number;
    _hasNextLogNumber = true;
  }

  void setLastSequence(SequenceNumber number)
  {
    _lastSequenceNumber = number;
    _hasLastSequenceNumber = true;
  }

 private:
  friend class VersionSet;

  bool _hasComparatorName;
  bool _hasLogNumber;
  bool _hasPreLogNumber;
  bool _hasNextLogNumber;
  bool _hasLastSequenceNumber;

  std::string _comparatorName;
  uint64_t _logNumber;
  uint64_t _preLogNumber;
  uint64_t _nextLogNumber;
  SequenceNumber _lastSequenceNumber;
  // pair first is level
  std::vector<std::pair<int, std::string>> _compactPoints;
  std::vector<std::pair<int, std::shared_ptr<FileMeta>>> _newFiles;
  // int for level uint64_t for delete file number
  std::set<std::pair<int, uint64_t>> _deleteFiles;
};

}

#endif