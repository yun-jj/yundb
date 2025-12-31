#ifndef EN_H
#define EN_H

#include <cstdint>
#include <string>
#include <cerrno>      
#include <cstring> 
#include <memory>

#include "slice.h"

namespace yundb
{

/* Sequentia read a file */
class SequentialFile
{
 public:
  SequentialFile() = default;
  SequentialFile(const SequentialFile& other) = delete;
  SequentialFile& operator=(const SequentialFile& other) = delete;
  virtual ~SequentialFile() = default;
  virtual void skip(uint64_t n) = 0;
  virtual bool read( Slice* str, uint64_t bytes) = 0;
};

/* Random read a file */
class RandomAccessFile
{
 public:
  RandomAccessFile() = default;
  RandomAccessFile(const RandomAccessFile& other) = delete;
  RandomAccessFile& operator=(const RandomAccessFile& other) = delete;
  virtual ~RandomAccessFile() = default;
  virtual size_t fileSize() const = 0;
  virtual bool read(uint64_t offset, Slice* str, uint64_t bytes) = 0;
};

/* A writable file abstract */
class WritableFile
{
 public:
  WritableFile() = default;
  WritableFile(const WritableFile& other) = delete;
  WritableFile& operator=(const WritableFile& other) = delete;
  virtual ~WritableFile() = default;

  virtual void append(const Slice& data) = 0;
  virtual void close() = 0;
  virtual void flush() = 0;
  virtual void sync() = 0;
};

class Env
{
 public:
  Env();

  Env(const Env& other) = delete;
  Env& operator=(const Env& other) = delete;

  virtual ~Env();

  static Env* Default();
};


void newWritableFile(std::string& file_name, WritableFile** result);

void newRandomAccessFile(std::string& file_name, RandomAccessFile** result);

}

#endif