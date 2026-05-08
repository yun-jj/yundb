#include "yundb/write_batch.h"

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

namespace yundb
{
  WriteBatch::WriteBatch() : rep_() {}

  WriteBatch::~WriteBatch() { clear(); }

  void WriteBatch::insert(const Slice& key, const Slice& value)
  {}

  void WriteBatch::remove(const Slice& key)
  {}

  void WriteBatch::clear()
  {}

  size_t WriteBatch::approximateSize() const
  { return rep_.size(); }

  void WriteBatch::append(const WriteBatch& source)
  {}
}