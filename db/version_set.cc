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

class VersionSet::Builder
{
 public:
  Builder(VersionSet* set, Version* version);
  ~Builder() = default;

  Builder(const Builder& other) = delete; 
  Builder& operator=(const Builder& other) = delete;
  void apply(VersionEdit* edit);
  void saveTo(Version* v);
 private:
  VersionSet* _set;
  Version* _curVersion;
};

VersionSet::Builder::Builder(VersionSet* set, Version* version)
      : _set(set),
        _curVersion(version) {}

VersionSet::VersionSet(const std::string dbName, const Options options,
                       std::shared_ptr<Comparator> InternalComparator)
      : _dbName(dbName),
        _options(options), 
        _comparator(InternalComparator),
        _cur(nullptr),
        _dummyVersion(this) {}

VersionSet::~VersionSet() {}

}