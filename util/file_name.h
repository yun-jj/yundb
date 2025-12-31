#ifndef FILE_NAME_H
#define FILE_NAME_H

#include <string>

namespace yundb
{

enum FileType
{
  LogFile,
  TableFile,
  DescriptorFile,
  CurrentFile,
  TempFile,
  InfoLogFile,
  LockFile
};

std::string generateFileName(uint64_t number, const std::string& dbName, const char* suffix);

// Parse the file name and return true if the file type is a member of the FileType collection;
// Otherwise, return false.
bool parseFileName(const std::string fileName, uint64_t* number, FileType* fileType);

std::string generateLogFileName(uint64_t number, const std::string& dbName);

std::string generateTableFileName(uint64_t number, const std::string& dbName);

std::string generateDescriptorFileName(uint64_t number, const std::string& dbName);

std::string generateCurrentFileName(uint64_t number, const std::string& dbName);

std::string generateTempFileName(uint64_t number, const std::string& dbName);

std::string generateLockFileName(const std::string& dbName);

std::string generateInfoLogFileName(const std::string& dbName);

std::string generateOldInfoLogFileName(const std::string& dbName);

}

#endif