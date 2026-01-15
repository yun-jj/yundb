#ifndef VERSION_EDIT_H
#define VERSION_EDIT_H

#include "dbformat.h"

#include <cstdint>
#include <string>
#include <vector>
#include <set>
#include <utility>

namespace yundb
{

// When time == 0 this file will be compation
constexpr uint32_t AllowedSeekTime = (1 << 30);

constexpr int MaxFileLevel = 7;

// FileMeta requires std::shared_ptr for automatic cleanup
struct FileMeta
{
   FileMeta() : allowedSeek(AllowedSeekTime), fileSize(0) {}
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
    FileMeta file;
    file.fileSize = fileSize;
    file.largest = largest;
    file.smallest = smallest;
    file.number = fileNumber;
    _newFiles.push_back(std::make_pair(level, file));
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
  std::vector<std::pair<int, FileMeta>> _newFiles;
  // int for level uint64_t for delete file number
  std::set<std::pair<int, uint64_t>> _deleteFiles;
};

}

#endif