#ifndef YUNDB_DB_BLOCK_READER_H
#define YUNDB_DB_BLOCK_READER_H
// Header guard standardized to YUNDB_DB_BLOCK_READER_H

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
    Iter(const char* blockStart, const char* restartEntry,
         const char* headEntry, const char* tailEntry);

    Iter(const Iter& other) = default;

    Iter();

    ~Iter() = default;

    Iter& operator=(const Iter& other) = default;

    bool operator<(const Iter& other);

    bool operator>(const Iter& other);

    bool operator<=(const Iter& other);

    bool operator>=(const Iter& other);

    // Move to another restart ptr entry
    Iter operator+(ptrdiff_t number);

    Iter operator-(ptrdiff_t number);

    inline bool empty() const;

    bool seek(const Slice& key, const Comparator* comparator,
              int restartInterval, std::string* result);

    inline std::string getValue();

    inline std::string getKey();

    inline std::string getUserKey();

  private:
    // Get mid Iter for binary search
    friend Iter mid(const Iter& left, const Iter& right);
    // Decode the entry and store it inside the object
    // Return false when decode false
    bool decodeEntry(const char* start);
    // Move to next entry, success return true otherwise return false
    bool next();
    const char* _blockStart;
    const char* _restartEntry;
    const char* _headRestartEntry;
    const char* _tailRestartEntry;
    const char* _nextDataEntry;
    size_t _sharedKeyLen;
    std::string _headKey;
    std::string _keyDelta;
    std::string _value;
  };

 public:
  DataBlockReader(const Options& options);
  ~DataBlockReader() = default;

  bool queryValue(const Slice& block, const Slice& key, std::string* result);
 private:
  friend Iter mid(const Iter& left, const Iter& right);
  Options _options;
};

}

#endif // YUNDB_DB_BLOCK_READER_H