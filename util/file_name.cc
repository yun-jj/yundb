#include "file_name.h"
#include "error_print.h"
#include "yundb/slice.h"

namespace yundb
{

std::string generateFileName(uint64_t number, const std::string& dbName,
                             const char* suffix)
{
  char buf[100];
  std::snprintf(buf, sizeof(buf), "/%06llu.%s",
                static_cast<unsigned long long>(number), suffix);
  return dbName + buf;
}

bool parseFileName(const std::string fileName, uint64_t* number,
                   FileType* fileType)
{
  CERR_PRINT_WITH_CONDITIONAL(
    "ParseFileName: nullptr", 
    number == nullptr || fileType == nullptr
  );

  size_t pos = fileName.find_last_of("/");
  Slice name(fileName.data(), fileName.size() - pos);  

  if (name == "CURRENT")
    *fileType = FileType::CurrentFile;
  else if (name == "LOCK")
    *fileType = FileType::LockFile;
  else if (name == "LOG" || name == "LOG.old")
    *fileType = FileType::InfoLogFile;
  else if (name.start_with("MANIFEST-"))
    *fileType = FileType::DescriptorFile;
  else if (name.end_with(".log")) 
    *fileType = FileType::LogFile;
  else if (name.end_with(".sst"))
    *fileType = FileType::TableFile;
  else if (name.end_with(".dbtmp"))
    *fileType = FileType::TempFile;
  else return false;

  if (*fileType == FileType::DescriptorFile)
    *number = static_cast<uint64_t>(std::strtoull(name.data() + 9, nullptr, 10));
  else
    *number = static_cast<uint64_t>(std::strtoull(name.data(), nullptr, 10));
  return true;
}

std::string generateLogFileName(uint64_t number, const std::string& dbName)
{
  CERR_PRINT_WITH_CONDITIONAL(
    "generateLogFileName: error number",
    number <= 0
  ); 
  return generateFileName(number, dbName, "log");
}

std::string generateTableFileName(uint64_t number, const std::string& dbName)
{
  CERR_PRINT_WITH_CONDITIONAL(
    "generateTableFileName: error number",
    number <= 0
  );
  return generateFileName(number, dbName, "sst");
}

std::string generateDescriptorFileName(uint64_t number, const std::string& dbName)
{ 
  CERR_PRINT_WITH_CONDITIONAL(
    "generateTableFileName: error number",
    number <= 0
  );
  char buf[20];
  std::snprintf(buf, sizeof(buf), "/MANIFEST-%06llu",
                static_cast<unsigned long long>(number));
  return dbName + buf;
}

std::string generateCurrentFileName(uint64_t number, const std::string& dbName)
{return dbName + "/CURRENT";}

std::string generateTempFileName(uint64_t number, const std::string& dbName)
{
  CERR_PRINT_WITH_CONDITIONAL(
    "generateTableFileName: error number",
    number <= 0
  );
  return generateFileName(number, dbName, "dbtmp");
}

std::string generateInfoLogFileName(const std::string& dbName)
{return dbName + "/LOG";}

std::string generateOldInfoLogFileName(const std::string& dbName)
{return dbName + "/LOG.old";}

std::string generateLockFileName(const std::string& dbName)
{return dbName + "/LOCK";}

std::string currentFileName(const std::string& dbname)
{return dbname + "/CURRENT";}

bool setCurrentFile(Env* env, const std::string& dbname,
                    uint64_t descriptorNumber)
{
  std::string manifestName = generateDescriptorFileName(descriptorNumber, dbname);
  Slice content = manifestName;

  content.removePrefix(dbname.size() + 1);
  std::string tmp = generateTempFileName(descriptorNumber, dbname);
  bool result = WriteStringToFileSync(content.toString() + "\n", tmp); 

  if (result)
  {
    bool result = env->RenameFile(tmp, currentFileName(dbname));

    if (!result)
    {
      env->RemoveFile(tmp);
      return false;
    }
  }
  else return false;

  return true;
}

}