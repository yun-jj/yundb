#ifndef FILTER_POLICY_h
#define FILTER_POLICY_h

#include "yundb/slice.h"

#include <string>

namespace yundb
{

class FilterPolicy
{
 public:
  virtual ~FilterPolicy() = default;
  // Return policy name
  virtual const char* Name() const = 0;
  // Create filter and append dst
  virtual void createFilter(const Slice* keys,
                            int n, std::string* dst) const = 0;
  // Return true if the key was 
  // in the list of keys passed to createFilter().
  virtual bool keyMayMatch(const Slice& key, const Slice& filter) const = 0;
};

FilterPolicy* bloomPolicyFilter();

}

#endif