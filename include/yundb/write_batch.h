#ifndef YUNDB_INCLUDE_YUNDB_DB_H
#define YUNDB_INCLUDE_YUNDB_DB_H

#include "slice.h"
#include "memtable.h"

namespace yundb
{

class WriteBatch
{
 public:

  WriteBatch();

  // Intentionally copyable.
  WriteBatch(const WriteBatch&) = default;
  WriteBatch& operator=(const WriteBatch&) = default;

  ~WriteBatch();

  // Store the mapping "key->value" in the database.
  void insert(const Slice& key, const Slice& value);

  // If the database contains a mapping for "key", erase it.  Else do nothing.
  void remove(const Slice& key);

  // Clear all updates buffered in this batch.
  void clear();

  // Insert all of the updates from "rep_" into this batch.
  // seq is first sequence number for the first update in this batch.
  // Returns SequenceNumber of next new Sequence.
  SequenceNumber insert(MemTable* memtable, SequenceNumber seq) const;

  // The size of the database changes caused by this batch.
  //
  // This number is tied to implementation details, and may change across
  // releases. It is intended for LevelDB usage metrics.
  size_t approximateSize() const;

  // Copies the operations in "source" to this batch.
  //
  // This runs in O(source size) time. However, the constant factor is better
  // than calling Iterate() over the source batch with a Handler that replicates
  // the operations into this batch.
  void append(const WriteBatch& source);

 private:
  // Get the number of entries in this batch.
  uint32_t count() const;
  // Store the number of entries in this batch.
  void setCount(uint32_t n);
  // See comment in write_batch.cc for the format of rep_
  // Sequence number for this batch need user to put in the header of rep_
  std::string rep_;  
};

}

#endif // YUNDB_INCLUDE_YUNDB_DB_H