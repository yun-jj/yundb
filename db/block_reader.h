#ifndef BLOCK_READER_H
#define BLOCK_READER_H

#include "yundb/options.h"
#include "yundb/slice.h"
#include "util/coding.h"

namespace yundb
{

class DataBlockReader
{
 private:

class Iter
{
 public:
  Iter(const char* blockStart, const char* restartPtr, const char* restartPtrHead,
       const char* restartPtrTail);

  Iter(const Iter& other) = default;

  Iter() 
      : _block_start(nullptr),
        _restart_ptr(nullptr),
        _restart_ptr_head(nullptr),
        _restart_ptr_tail(nullptr),
        _start(nullptr),
        _end(nullptr),
        _shared_Key_Len(0){}

  ~Iter(){}

  Iter& operator=(const Iter& other) = default;

  bool operator<(const Iter& other);

  bool operator>(const Iter& other);

  bool operator<=(const Iter& other);

  bool operator>=(const Iter& other);

  // Move to another restart ptr entry
  Iter operator+(ptrdiff_t number);

  Iter operator-(ptrdiff_t number);

  bool seek(const Slice& key, const Comparator* comparator,
            int restartInterval, std::string* result);

  std::string getValue(){return _value;}

  std::string getKey()
  {
    if (_shared_Key_Len == 0) return _key_Delta;
    std::string key;
    key.reserve(_shared_Key_Len + _key_Delta.size());
    key.append(_head_Key, 0, _shared_Key_Len);
    key.append(_key_Delta);
    return key;
  }

 private:
  // Get mid Iter for binary search
  friend Iter mid(const Iter& left, const Iter& right);
  // Decode the entry and store it inside the object
  void decodeEntry(const char* start);
  // Move to next entry, success return true otherwise return false
  bool next();
  const char* _block_start;
  const char* _restart_ptr;
  const char* _restart_ptr_head;
  const char* _restart_ptr_tail;
  const char* _start;
  const char* _end;
  size_t _shared_Key_Len;
  std::string _head_Key;
  std::string _key_Delta;
  std::string _value;
};

 public:
  DataBlockReader() = default;
  DataBlockReader(const Options& options)
        : _options(options)
  {}
  ~DataBlockReader() = default;

  bool queryValue(const Slice& block, const Slice& key, std::string* result);
 private:
  friend Iter mid(const Iter& left, const Iter& right);
  Options _options;
};

}

#endif