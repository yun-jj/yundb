#include "yundb/options.h"
#include "yundb/comparator.h"
#include "yundb/en.h"
#include "yundb/filter_policy.h"

namespace yundb
{
Options::Options()
    : comparator(BytewiseCmp()),
      env(Env::Default()),
      filter_policy(BloomPolicyFilter()) {}
}