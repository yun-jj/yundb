cmake_minimum_required(VERSION 3.10)
project(yundb)

# 设置C++标准并强制启用
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 收集所有源文件
file(GLOB_RECURSE SOURCES
    "db/*.cc"
    "util/*.cc"
    "main.cc"
)

# 创建可执行文件
add_executable(yundb ${SOURCES})

# 包含头文件目录（关键修复）
target_include_directories(yundb
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/db      # 用于 db/log_writer.h 等
        ${CMAKE_CURRENT_SOURCE_DIR}/util    # 用于 util/arena.h 等
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include # 用于 yundb/slice.h
        ${CMAKE_CURRENT_SOURCE_DIR}         # 根目录（用于 "util/random.h"）
)