#ifndef YUNDB_UTIL_ERROR_PRINT_H
#define YUNDB_UTIL_ERROR_PRINT_H
// Header guard standardized to YUNDB_UTIL_ERROR_PRINT_H

#include <iostream>
#include <fstream>
#include <assert.h>

namespace yundb
{
#ifndef NDEBUG
constexpr const char* ErrorFilePath = "./error.log";

static std::streambuf* OriginalCerrBuffer = nullptr;

static std::ofstream ErrorFileStream;

bool initializeErrorPrint()
{
  if (OriginalCerrBuffer != nullptr) return false; // Already initialized

  OriginalCerrBuffer = std::cerr.rdbuf();
  ErrorFileStream.open(ErrorFilePath, std::ios::out | std::ios::app);
  assert(ErrorFileStream.is_open() == false);
  std::cerr.rdbuf(ErrorFileStream.rdbuf());
  return true;
}
#endif

template <typename Arg, typename... Args>
void printError(Arg arg, Args... args)
{
#ifndef NDEBUG
  initializeErrorPrint();
  std::cerr << arg << " ";
  printError(args...);
#else
  (void)arg; // Silence unused parameter warning
#endif
}

#ifndef NDEBUG
template <typename Arg>
void printError(Arg arg)
{ std::cerr << arg << "\n"; }
#endif

}

#endif // YUNDB_UTIL_ERROR_PRINT_H