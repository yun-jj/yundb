#include "snapshot.h"

namespace yundb
{

SnapshotImpl::SnapshotImpl(SequenceNumber seq, SnapshotImpl* pre, SnapshotImpl* next)
      : _seq(seq),
        _pre(pre),
        _next(pre){}

SnapshotImpl::~SnapshotImpl(){}

SnapshotList::SnapshotList()
      : _dummy(0, &_dummy, &_dummy) {}

SnapshotList::~SnapshotList()
{
  while (!empty())
  {
    SnapshotImpl* snapshot = _dummy._pre;
    _dummy._pre = snapshot->_pre;
    snapshot->_pre->_next = &_dummy;

    delete snapshot;
  }
}

void SnapshotList::insert(SnapshotImpl* snapshot)
{
  snapshot->_next = &_dummy;
  snapshot->_pre = _dummy._pre;
  _dummy._pre = snapshot;
  snapshot->_pre->_next = snapshot;
}

bool SnapshotList::remove(SnapshotImpl* snapshot)
{
  SnapshotImpl* cur = _dummy._pre;
  while (cur != &_dummy)
  {
    if (cur == snapshot)
    {
      snapshot->_pre->_next = snapshot->_next;
      snapshot->_next->_pre = snapshot->_pre;
      return true;
    }
  }

  return false;
}

bool SnapshotList::empty()
{ return _dummy._pre == &_dummy; }

}