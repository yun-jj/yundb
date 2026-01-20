#ifndef DB_FORMAT_H
#define DB_FORMAT_H

#include <cstdint>

#include "yundb/options.h"
#include "yundb/slice.h"
#include "yundb/comparator.h"

namespace yundb
{

enum ValueType
{
  TypeDeletion = 0x0,
  TypeValue= 0x1
};

using SequenceNumber = uint64_t;

constexpr SequenceNumber MaxSequenceNumber = ((0x1ull << 56) - 1);

constexpr ValueType MaxValueType = TypeValue;

constexpr ValueType TypeForSeek = TypeValue;

// Format: sequence 7B type 1B 
constexpr const int KeyTagSize = 8;

// 1-byte type + 32-bit crc
constexpr const int BlockTrailerSize = 5;

// When time == 0 this file will be compation
constexpr const uint32_t AllowedSeekTime = (1 << 30);

constexpr const int MaxFileLevel = 7;

// Level-0 compaction is started when we hit this many files.
constexpr const int L0CompactionTrigger = 4;

// Soft limit on number of level-0 files. We slow down writes at this point. 
constexpr const int L0SlowdownWritesTrigger = 8;

// Maximum number of level-0 files.  We stop writes at this point.
constexpr const int L0StopWritesTrigger = 12;

// Maximum level to which a new compacted memtable is pushed if it
// does not create overlap.  We try to push to level 2 to avoid the
// relatively expensive level 0=>1 compactions and to avoid some
// expensive manifest file operations.  We do not push all the way to
// the largest level since that can generate a lot of wasted disk
// space if the same key space is being repeatedly overwritten.
constexpr const int MaxMemCompactLevel = 2;

uint64_t packSeqAndType(SequenceNumber seq, ValueType type);

// Data pointer start adress 
// Set nullptr for seq or type meaning you dont want get
void decodeSeqAndType(const char* data, SequenceNumber* seq, ValueType* type);


// Decode format | key | seq, type |
Slice decodeKey(const Slice& entry);

// Decode format | value |
Slice decodeValue(const Slice& entry);

class InternalComparator : public Comparator
{
 public:
  InternalComparator(const Options& options);
  ~InternalComparator() = default;

  const char* name() const override;
  int cmp(const Slice& key1, const Slice& key2) const override;
 private:
  Options _options;
};

// Useful for get() in memtable 
// Lookup entry format is | VarintKeySize | user key | seq and type |
class LookUpKey
{
 public:
  LookUpKey() = default;
  LookUpKey(const Slice& key, SequenceNumber seq);
  ~LookUpKey();
  Slice getUserKey();
  // Get all entry
  Slice getKey();
  // Get user key with seq and type
  Slice getUserKeyWithSeqAndType();
  // Get seq and type
  void getSeqAndType(SequenceNumber& seq, ValueType& type)
  {decodeSeqAndType(_seq_and_type_start, &seq, &type);}
 private:
  char* _start;
  char* _user_key_start;
  char* _seq_and_type_start;
  char* _end;
  char _key[200];
};


}

#endif