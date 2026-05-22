#include "db/sstable_builder.h"
#include "db/memtable.h"
#include "db/dbformat.h"
#include "test_util.h"
#include "util/file_name.h"
#include "yundb/en.h"
#include "yundb/comparator.h"
#include "util/cache.h"
#include "db/table_cache.h"


#include <gtest/gtest.h>
#include <map>

class SstableBuilderTest : public testing::Test
{
 public:
  SstableBuilderTest();
 protected:
  yundb::Options options;
  std::shared_ptr<yundb::Arena> arena; 
  std::shared_ptr<yundb::MemTable> memTable;
  StringGenerater generater;
  std::string dbName;
  std::string fileName;
  std::map<std::string, std::string> kvMap; 
};

SstableBuilderTest::SstableBuilderTest()
      : arena(std::make_shared<yundb::Arena>())
{
  options.comparator = yundb::BytewiseCmp();
  memTable = std::make_shared<yundb::MemTable>(arena, options);
  dbName = TEST_TEMP_DIR;
  fileName = yundb::generateTableFileName(666666, dbName);
}

TEST_F(SstableBuilderTest, sstableGenerate)
{
  yundb::SequenceNumber seq;

  while (memTable->getMemoryUsage() <= options.write_buffer_size)
  {
    std::string key = generater.getRandString();
    std::string value = generater.getRandString();
    kvMap[key] = value;
    memTable->add(seq , yundb::ValueType::TypeValue, key, value);
    seq++;
  }

  yundb::WritableFile* file;
  options.env->newWritableFile(fileName, &file);
  yundb::SstableBuilder builder(options, file);
  builder.build(memTable.get());

  if (options.env->fileExists(dbName + fileName))
    options.env->removeFile(dbName + fileName);
}

TEST_F(SstableBuilderTest, sstableRead)
{
  yundb::SequenceNumber seq;
  yundb::WritableFile* writeFile;
  yundb::RandomAccessFile* randomAccessfile = nullptr;

  while (memTable->getMemoryUsage() <= options.write_buffer_size)
  {
    std::string key = generater.getRandString();
    std::string value = generater.getRandString();
    kvMap[key] = value;
    memTable->add(seq , yundb::ValueType::TypeValue, key, value);
    seq++;
  }

  options.env->newWritableFile(fileName, &writeFile);
  yundb::SstableBuilder builder(options, writeFile);
  builder.build(memTable.get());

  options.env->newRandomAccessFile(fileName, &randomAccessfile);

  yundb::TableCache tableCache(dbName, options,
                               std::make_shared<yundb::Cache>(options.max_cache_size));
  
  // Insert file into cache, so we can read data from cache when lookup
  tableCache.insert(666666, , 0, [](const yundb::Slice& key, void* value) {
    (void)value; // do nothing, just for test
  });

  size_t fileSize;
  options.env->getFileSize(fileName, &fileSize);

  for (const auto& kv : kvMap)
  {
    std::string value;
    bool found = tableCache.lookup(666666, fileSize, kv.first, &value);
    EXPECT_TRUE(found);
    EXPECT_EQ(value, kv.second);
  }
}
