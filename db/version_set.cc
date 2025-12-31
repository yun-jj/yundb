#include "version_set.h"

namespace yundb
{

Version::~Version()
{
  _pre->_next = _next;
  _next->_pre = _pre;
}

void Version::ref()
{++_ref;}

void Version::unRef()
{
  CERR_PRINT_WITH_CONDITIONAL(
    "Version: ref <= 0",
    _ref <= 0
  );

  CERR_PRINT_WITH_CONDITIONAL(
    "Version: unRef a dummyVersion",
    this == &_versionSet->_dummyVersion
  );
  _ref--;

  if (_ref == 0) delete this;
}

}