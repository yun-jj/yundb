#ifndef DB_FORMAT_H
#define DB_FORMAT_H

#include <cstdint>

#include "yundb/slice.h"

namespace yundb
{

enum ValueType
{
  TypeDeletion = 0x0,
  TypeForSeek = 0x1
};

using SequenceNumber = uint64_t;

constexpr SequenceNumber MaxSequenceNumber = ((0x1ull << 56) - 1);

constexpr ValueType MaxValueType = TypeForSeek; 

/* Format: sequence 7B type 1B */
constexpr int KeyTagSize = 8;

/* Useful for get() in memtable */
class LookUpKey
{
 public:
  LookUpKey() = default;
  LookUpKey(const Slice& key, SequenceNumber seq);
  ~LookUpKey();
  Slice getUserKey();
 private:
  char* _start;
  uint64_t _size;
  char _key[200];
};

uint64_t packSeqAndType(SequenceNumber seq, ValueType type);

}

#endif