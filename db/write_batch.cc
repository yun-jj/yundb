#include "yundb/write_batch.h"

#include "util/coding.h"
#include "db/dbformat.h"

// WriteBatch::rep_ :=
//    sequence: fixed64
//    count: fixed32
//    data: record[count]
// record :=
//    kTypeValue varstring varstring         |
//    kTypeDeletion varstring
// varstring :=
//    len: varint32
//    data: uint8[len]

constexpr size_t HeaderSize = 8;

namespace yundb
{

WriteBatch::WriteBatch() : rep_() {}

WriteBatch::~WriteBatch() { clear(); }

void WriteBatch::insert(const Slice& key, const Slice& value)
{
  setCount(count() + 1);
  rep_.push_back(static_cast<char>(TypeValue));
  PutLengthPrefixedSlice(&rep_, key);
  PutLengthPrefixedSlice(&rep_, value);
}

void WriteBatch::remove(const Slice& key)
{
  setCount(count() + 1);
  rep_.push_back(static_cast<char>(TypeDeletion));
  PutLengthPrefixedSlice(&rep_, key);
}

void WriteBatch::clear()
{
  rep_.clear();
  rep_.resize(HeaderSize);
}

SequenceNumber WriteBatch::insert(MemTable* memtable, SequenceNumber seq) const
{
  Slice input(rep_);
  if (input.size() < HeaderSize) {
    printError("WriteBatch::insert: malformed WriteBatch");
    return seq;
  }

  input.removePrefix(HeaderSize);
  size_t found = 0;
  SequenceNumber curSeq = seq;

  while (found < count())
  {
    Slice key, value;
    char type = input[0];
    input.removePrefix(1);
    switch (type)
    {
    case TypeValue:
      GetLengthPrefixedSlice(&input, &key);
      GetLengthPrefixedSlice(&input, &value);
      memtable->add(curSeq, TypeValue, key, value);
      break;
    case TypeDeletion:
      GetLengthPrefixedSlice(&input, &key);
      memtable->add(curSeq, TypeDeletion, key, Slice());
      break;
    default:
      printError("WriteBatch::insert: unknown WriteBatch tag type %d", type);
      break;
    } 
    found++;
    curSeq++;
  }

  return curSeq;
}

size_t WriteBatch::approximateSize() const
{ return rep_.size(); }

void WriteBatch::append(const WriteBatch& source)
{
  setCount(count() + source.count());
  rep_.append(source.rep_.data() + HeaderSize + 4,
              source.rep_.size() - HeaderSize - 4);
}

uint32_t WriteBatch::count() const
{ return DecodeFixed32(&rep_[HeaderSize]); }

void WriteBatch::setCount(uint32_t n)
{ EncodeFixed32(&rep_[HeaderSize], n); }

}