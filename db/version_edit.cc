#include "version_edit.h"
#include "util/coding.h"

namespace yundb
{

// Log tag used for manifest file
enum LogTag
{
  ComparatorNameTag = 1,
  LogNumber = 2,
  PreLogNumber = 3,
  NextFileNumber = 4,
  LastSequnce = 5,
  CompactPointer = 6,
  DeleteFile = 7,
  // 8 was used for large value refs
  NewFile = 9,
};

void VersionEdit::clear()
{
  _comparatorName.clear();
  _compactPoints.clear();
  _newFiles.clear();
  _deleteFiles.clear();
  _logNumber = 0;
  _preLogNumber = 0;
  _nextFileNumber = 0;
  _lastSequenceNumber = 0;
  _hasComparatorName = false;
  _hasLogNumber = false;
  _hasPreLogNumber = false;
  _hasNextFileNumber = false;
  _hasLastSequenceNumber = false;
}

void VersionEdit::encode(std::string* dst)
{
  if (_hasComparatorName)
  {
    PutVarint32(dst, ComparatorNameTag);
    PutLengthPrefixedSlice(dst, _comparatorName);
  }

  if (_hasLogNumber)
  {
    PutVarint32(dst, LogNumber);
    PutVarint64(dst, _logNumber);
  }

  if (_hasPreLogNumber)
  {
    PutVarint32(dst, PreLogNumber);
    PutVarint64(dst, _preLogNumber);
  }
  
  if (_hasNextFileNumber)
  {
    PutVarint32(dst, NextFileNumber);
    PutVarint64(dst, _nextFileNumber);
  }

  if (_hasLastSequenceNumber)
  {
    PutVarint32(dst, LastSequnce);
    PutVarint64(dst, _lastSequenceNumber);
  }

  for (size_t i = 0; _compactPoints.size(); i++)
  {
    PutVarint32(dst, CompactPointer);
    PutVarint64(dst, _compactPoints[i].first);
    PutLengthPrefixedSlice(dst, _compactPoints[i].second);
  }

  for(const auto& file : _deleteFiles)
  {
    PutVarint32(dst, DeleteFile);
    PutVarint32(dst, file.first);
    PutVarint64(dst, file.second);
  }

  for (size_t i = 0; _newFiles.size() > i; i++)
  {
    auto file = _newFiles[i].second;
    PutVarint32(dst, NewFile);
    PutVarint32(dst, _newFiles[i].first);
    PutVarint32(dst, file->number);
    PutVarint32(dst, file->fileSize);
    PutLengthPrefixedSlice(dst, file->largest);
    PutLengthPrefixedSlice(dst, file->smallest);
  }
}

static bool getLevel(Slice* input, int* level) {
  uint32_t v;
  if (GetVarint32(input, &v) && v < MaxFileLevel) {
    *level = v;
    return true;
  } else {
    return false;
  }
}

static bool getKey(Slice* input, Slice* dst)
{
  if (GetLengthPrefixedSlice(input, dst))
    return true;
  return false;
}

void VersionEdit::decode(const Slice& data)
{
  clear();
  Slice input = data;
  const char* msg = nullptr;
  uint32_t tag;

  // Temporary storage for parsing
  int level;
  uint64_t number;
  std::shared_ptr<FileMeta> f = std::make_shared<FileMeta>();
  Slice str;
  Slice key;

  while (msg == nullptr && GetVarint32(&input, &tag)) {
    switch (tag) {
      case ComparatorNameTag:
        if (GetLengthPrefixedSlice(&input, &str)) {
          _comparatorName = str.toString();
          _hasComparatorName = true;
        } else {
          msg = "comparator name";
        }
        break;

      case LogNumber:
        if (GetVarint64(&input, &_logNumber)) {
          _hasLogNumber = true;
        } else {
          msg = "log number";
        }
        break;

      case PreLogNumber:
        if (GetVarint64(&input, &_preLogNumber)) {
          _hasPreLogNumber = true;
        } else {
          msg = "previous log number";
        }
        break;

      case NextFileNumber:
        if (GetVarint64(&input, &_nextFileNumber)) {
          _hasNextFileNumber = true;
        } else {
          msg = "next file number";
        }
        break;

      case LastSequnce:
        if (GetVarint64(&input, &_lastSequenceNumber)) {
          _hasLastSequenceNumber = true;
        } else {
          msg = "last sequence number";
        }
        break;

      case CompactPointer:
        if (getLevel(&input, &level) && getKey(&input, &key)) {
          _compactPoints.push_back(std::make_pair(level, key.toString()));
        } else {
          msg = "compaction pointer";
        }
        break;

      case DeleteFile:
        if (getLevel(&input, &level) && GetVarint64(&input, &number)) {
          _deleteFiles.insert(std::make_pair(level, number));
        } else {
          msg = "deleted file";
        }
        break;

      case NewFile:
        Slice smallest, largest;
        if (getLevel(&input, &level) && GetVarint64(&input, &f->number) &&
            GetVarint64(&input, &f->fileSize) &&
            getKey(&input, &smallest) &&
            getKey(&input, &largest))
        {
          f->largest = largest.toString();
          f->smallest = smallest.toString();
          _newFiles.push_back(std::make_pair(level, f));
        } else {
          msg = "new-file entry";
        }
        break;

      default:
        msg = "unknown tag";
        break;
    }
  }

  if (msg == nullptr && !input.empty()) {
    msg = "invalid tag";
  }

  CERR_PRINT_WITH_CONDITIONAL(
    msg,
    msg != nullptr
  );
}

};