#include "db/sstable_builder.h"
#include "db/memtable.h"
#include "db/dbformat.h"
#include "test_util.h"
#include "util/file_name.h"
#include "yundb/en.h"
#include "yundb/comparator.h"

#include <gtest/gtest.h>

class SstableBuilderTest : public testing::Test
{
 public:
  SstableBuilderTest();
 protected:
  yundb::Options options;
  std::shared_ptr<yundb::Arena> arena; 
  std::shared_ptr<yundb::MemTable> memTable;
  StringGenerater generater;
  
};

SstableBuilderTest::SstableBuilderTest()
      : arena(std::make_shared<yundb::Arena>())
{
  options.comparator = yundb::BytewiseCmp();
  memTable = std::make_shared<yundb::MemTable>(arena, options);
  
}

TEST_F(SstableBuilderTest, sstableGenerate)
{
  yundb::SequenceNumber seq;

  while (memTable->getKvSize() <= options.write_buffer_size)
  {
    memTable->add(seq , yundb::ValueType::TypeValue,
                  generater.getRandString(), generater.getRandString());
    seq++;
  }

  yundb::WritableFile* file;
  std::string dbName(TEST_TEMP_DIR);
  std::string fileName = yundb::generateTableFileName(666666, dbName);
  yundb::newWritableFile(fileName, &file);
  yundb::SstableBuilder builder(options, file);
  builder.build(memTable.get());
}