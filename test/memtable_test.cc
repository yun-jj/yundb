#include "yundb/en.h"
#include "yundb/slice.h"
#include "yundb/comparator.h"
#include "yundb/options.h"
#include "util/arena.h"
#include "db/memtable.h"
#include "db/dbformat.h"
#include "util/error_print.h"
#include "test_util.h"


#include <gtest/gtest.h>

static yundb::SequenceNumber seq = 0;

class MemTableTest : public testing::Test
{
 public:
  MemTableTest();
 protected:
  const int N = 5000;
  yundb::Options options;
  std::vector<std::string> keys, values;
  std::shared_ptr<yundb::Arena> arena;
  std::shared_ptr<yundb::MemTable> memTable;
  StringGenerater generater;
};

MemTableTest::MemTableTest() 
      : arena(std::make_shared<yundb::Arena>())
{
  options.comparator = yundb::BytewiseCmp();
  memTable = std::make_shared<yundb::MemTable>(arena, options);
}

TEST_F(MemTableTest, Insert)
{
  constexpr int N = 100000;

  for (int i = 0; N > i; i++, seq++)
  {
    memTable->add(seq, yundb::ValueType::TypeValue,
        generater.getRandString(), generater.getRandString());
  }
}

TEST_F(MemTableTest, Get)
{
  for (int i = 0; N > i; i++, seq++)
  {
    std::string key;
    do
    {
      key = generater.getRandString();
    } while (std::find(keys.begin(), keys.end(), key) != keys.end());
    
    keys.push_back(key);
    values.push_back(generater.getRandString()); 
    memTable->add(seq, yundb::ValueType::TypeValue, keys[i], values[i]);
  }

  std::string valueString;
  bool find = true;

  for (int i = 0; N > i; i++, seq++)
  {
    yundb::LookUpKey LUKey(yundb::Slice(keys[i]), seq);
    EXPECT_TRUE(memTable->get(LUKey, &valueString, find));
    EXPECT_TRUE(find);

    int rs = valueString.compare(values[i]);
    EXPECT_EQ(rs, 0);
    valueString.clear();
    find = true;
  }
}

/*
TEST_F(MemTableTest, Delete)
{
  for (int i = 0; N > i; i++, seq++)
    memTable->add(seq, yundb::ValueType::TypeDeletion, keys[i], "");
  for (int i = 0; N > i; i++)
  {
    yundb::LookUpKey LUKey(yundb::Slice(keys[i]), seq);
    memTable->get()
  }
}
*/