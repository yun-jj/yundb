#ifndef YUNDB_INCLUDE_YUNDB_EN_H
#define YUNDB_INCLUDE_YUNDB_EN_H
// Header guard standardized to YUNDB_INCLUDE_YUNDB_EN_H

#include <cstdint>
#include <string>
#include <cerrno>      
#include <cstring> 
#include <memory>

#include "slice.h"

namespace yundb
{

class WritableFile;
class RandomAccessFile;

class Env
{
 public:
  Env();

  virtual ~Env();

  Env(const Env& other) = delete;

  Env& operator=(const Env& other) = delete;

  static Env* Default();

  Env& operator=(const Env& other) = delete;

  virtual bool removeFile(const std::string& fname) = 0;

  virtual bool renameFile(const std::string& src, const std::string& target) = 0;

  virtual void newWritableFile(std::string& file_name, WritableFile** result) = 0;

  virtual void newRandomAccessFile(std::string& file_name, RandomAccessFile** result) = 0;

};

/* Sequentia read a file */
class SequentialFile
{
 public:
  SequentialFile() = default;
  SequentialFile(const SequentialFile& other) = delete;
  SequentialFile& operator=(const SequentialFile& other) = delete;
  virtual ~SequentialFile() = default;
  virtual bool skip(uint64_t n) = 0;
  // Read up to "n" bytes from the file.  "scratch[0..n-1]" may be
  // written by this routine.  Sets "*result" to the data that was
  // read (including if fewer than "n" bytes were successfully read).
  // May set "*result" to point at data in "scratch[0..n-1]", so
  // "scratch[0..n-1]" must be live when "*result" is used.
  // If an error was encountered, returns a non-OK status.
  //
  // REQUIRES: External synchronization
  virtual bool read(Slice* str, char* scratch, uint64_t bytes) = 0;
};

/* Random read a file */
class RandomAccessFile
{
 public:
  RandomAccessFile() = default;
  RandomAccessFile(const RandomAccessFile& other) = delete;
  RandomAccessFile& operator=(const RandomAccessFile& other) = delete;
  virtual ~RandomAccessFile() = default;
  // Read up to "n" bytes from the file starting at "offset".
  // "scratch[0..n-1]" may be written by this routine.  Sets "*result"
  // to the data that was read (including if fewer than "n" bytes were
  // successfully read).  May set "*result" to point at data in
  // "scratch[0..n-1]", so "scratch[0..n-1]" must be live when
  // "*result" is used.  If an error was encountered, returns a non-OK
  // status.
  //
  // Safe for concurrent use by multiple threads.
  virtual bool read(uint64_t offset, Slice* str,
                    char* scratch, uint64_t bytes) const = 0;
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

// Identifies a locked file.
class LockFile
{
 public:
  LockFile() = default;
  LockFile(const LockFile& other) = delete;
  LockFile& operator=(const LockFile& other) = delete;
  virtual ~LockFile() = default;
};

bool writeStringToFile(const Slice& data, const std::string& fname);

bool writeStringToFileSync(const Slice& data, const std::string& fname);

bool readFileToString(const std::string& fname, std::string* data);

}

#endif // YUNDB_INCLUDE_YUNDB_EN_H