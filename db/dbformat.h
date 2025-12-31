#ifndef DB_FORMAT_H
#define DB_FORMAT_H

#include <cstdint>

#include "yundb/slice.h"

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
constexpr int KeyTagSize = 8;

// 1-byte type + 32-bit crc
constexpr int BlockTrailerSize = 5;


uint64_t packSeqAndType(SequenceNumber seq, ValueType type);
/* Data pointer start adress 
   Set nullptr for seq or type meaning I dont want get
*/
void decodeSeqAndType(const char* data, SequenceNumber* seq, ValueType* type);
/* Useful for get() in memtable 
   Lookup entry format is | VarintKeySize | user key | seq and type |
*/
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