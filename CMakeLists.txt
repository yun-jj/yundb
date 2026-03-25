cmake_minimum_required(VERSION 3.10)
project(yundb)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(GTest QUIET)
find_package(Snappy QUIET)
find_package(Crc32c QUIET)

set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

set(YUNDB_TEST_DIR ${CMAKE_CURRENT_SOURCE_DIR}/test)
set(YUNDB_DB_DIR ${CMAKE_CURRENT_SOURCE_DIR}/db)
set(YUNDB_UTIL_DIR ${CMAKE_CURRENT_SOURCE_DIR}/util)
set(YUNDB_TEST_TEMP_DIR ${CMAKE_CURRENT_SOURCE_DIR}/test/tmp)

enable_testing()

file(GLOB_RECURSE SOURCES
    "${YUNDB_DB_DIR}/*.cc"
    "${YUNDB_UTIL_DIR}/*.cc"
)

add_library(yundb STATIC ${SOURCES})
target_link_libraries(yundb PRIVATE snappy crc32c)

# For project get head
target_include_directories(yundb
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/db
        ${CMAKE_CURRENT_SOURCE_DIR}/util
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_SOURCE_DIR}
)

add_executable(memtable_test ${YUNDB_TEST_DIR}/memtable_test.cc)
add_executable(sstable_builder_test ${YUNDB_TEST_DIR}/sstable_builder_test.cc)

target_compile_definitions(sstable_builder_test PUBLIC
    TEST_TEMP_DIR="${YUNDB_TEST_TEMP_DIR}"
)

  target_link_libraries(memtable_test
      PRIVATE 
          yundb
          GTest::gtest_main
  )

  target_link_libraries(sstable_builder_test
      PRIVATE 
          yundb
          GTest::gtest_main
  )