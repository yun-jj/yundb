#ifndef YUNDB_DB_SNAPSHOT_H
#define YUNDB_DB_SNAPSHOT_H

#include "yundb/db.h"
#include "db/dbformat.h"

namespace yundb
{

class SnapshotImpl : public Snapshot
{
 public:
  SnapshotImpl(SequenceNumber seq, SnapshotImpl* pre, SnapshotImpl* next);
  ~SnapshotImpl() override;
 private:
  friend class SnapshotList;
  SnapshotImpl* _pre;
  SnapshotImpl* _next;
  SequenceNumber _seq;
};

class SnapshotList
{
 public:
  SnapshotList();
  ~SnapshotList();
  // Insert new snapshot
  void insert(SnapshotImpl* snapshot);
  // Remove snapshot, if snapshot not in the list return false otherwise return true
  bool remove(SnapshotImpl* snapshot);

  bool empty();
 private:
  // node<->node<->...<->_dummy
  SnapshotImpl _dummy;
};

}

#endif