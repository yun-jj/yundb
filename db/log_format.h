#ifndef LOG_FORMAT_H
#define LOG_FORMAT_H

#include "yundb/en.h"

namespace yundb
{
namespace log
{
constexpr size_t recordBlockSize = 32768; /* 32k */
constexpr size_t recordHeadSize = 3; /*length(2byte), type(1byte) */

enum RecordType
{
  /* Reserved for preallocated files? */
  ZeroType = 0,
  FullType = 1,
  /* For fragments */
  FirstType = 2,
  MiddleType = 3,
  LastType = 4
};
constexpr size_t maxRecordType = LastType;

}
}

#endif