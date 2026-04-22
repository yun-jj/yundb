#ifndef YUNDB_INCLUDE_YUNDB_EN_H
#define YUNDB_INCLUDE_YUNDB_EN_H
// Header guard standardized to YUNDB_INCLUDE_YUNDB_EN_H

#include <cstdint>
#include <string>
#include <vector>
#include <cerrno>      
#include <cstring> 
#include <memory>

#include "slice.h"

namespace yundb
{

class WritableFile;
class SequentialFile;
class RandomAccessFile;
class FileLock;

class Env
{
 public:
  Env();

  virtual ~Env();

  Env(const Env& other) = delete;

  Env& operator=(const Env& other) = delete;

  static Env* Default();

  Env& operator=(const Env& other) = delete;

  virtual void newWritableFile(std::string& fileName, WritableFile** result) = 0;

  virtual void newAppendableFile(std::string& fileName, WritableFile** result) = 0;

  virtual void newSequentialFile(std::string& fileName, SequentialFile** result) = 0;

  virtual void newRandomAccessFile(std::string& fileName, RandomAccessFile** result) = 0;

  // Returns true iff the named file exists.
  virtual bool fileExists(const std::string& fileName) = 0;

  virtual bool getFileSize(const std::string& fileName, uint64_t* file_size) = 0;

  // Store in *result the names of the children of the specified directory.
  // The names are relative to "dir".
  // Original contents of *results are dropped.
  virtual bool getChildren(const std::string& dir, std::vector<std::string>* result) = 0;

  virtual bool removeFile(const std::string& fileName) = 0;

  virtual bool renameFile(const std::string& src, const std::string& target) = 0;

  virtual bool createDir(const std::string& fileName) = 0;

  virtual bool removeDir(const std::string& dirName) = 0;

  // Lock the specified file.  Used to prevent concurrent access to
  // the same db by multiple processes.  On failure, stores nullptr in
  // *lock and returns non-OK.
  //
  // On success, stores a pointer to the object that represents the
  // acquired lock in *lock and returns OK.  The caller should call
  // UnlockFile(*lock) to release the lock.  If the process exits,
  // the lock will be automatically released.
  //
  // If somebody else already holds the lock, finishes immediately
  // with a failure.  I.e., this call does not wait for existing locks
  // to go away.
  //
  // May create the named file if it does not already exist.
  virtual bool lockFile(const std::string& fileName, FileLock** lock) = 0;

  // Release the lock acquired by a previous successful call to LockFile.
  // REQUIRES: lock was returned by a successful LockFile() call
  // REQUIRES: lock has not already been unlocked.
  virtual bool unlockFile(FileLock* lock) = 0;

    // Arrange to run "(*function)(arg)" once in a background thread.
  //
  // "function" may run in an unspecified thread.  Multiple functions
  // added to the same Env may run concurrently in different threads.
  // I.e., the caller may not assume that background work items are
  // serialized.
  virtual void schedule(void (*function)(void* arg), void* arg) = 0;

  // Start a new thread, invoking "function(arg)" within the new thread.
  // When "function(arg)" returns, the thread will be destroyed.
  virtual void startThread(void (*function)(void* arg), void* arg) = 0;
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
class FileLock
{
 public:
  FileLock() = default;
  FileLock(const FileLock& other) = delete;
  FileLock& operator=(const FileLock& other) = delete;
  virtual ~FileLock() = default;
};

bool writeStringToFile(const Slice& data, const std::string& fname);

bool writeStringToFileSync(const Slice& data, const std::string& fname);

bool readFileToString(const std::string& fname, std::string* data);

}

#endif // YUNDB_INCLUDE_YUNDB_EN_H