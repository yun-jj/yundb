// Copyright (c) 2018 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#ifndef YUNDB_UTIL_SYNC_H
#define YUNDB_UTIL_SYNC_H
// Header guard standardized to YUNDB_UTIL_SYNC_H

#include <cassert>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>

namespace sync
{
// Thinly wraps std::mutex.
class Mutex {
 public:
  Mutex() = default;
  ~Mutex() = default;

  Mutex(const Mutex&) = delete;
  Mutex& operator=(const Mutex&) = delete;

  void Lock() {mu_.lock();}
  void unlock() { mu_.unlock(); }

 private:
  friend class CondVar;
  std::mutex mu_;
};

// Thinly wraps std::condition_variable.
class CondVar {
 public:
  explicit CondVar(Mutex* mu) : mu_(mu) { assert(mu != nullptr); }
  ~CondVar() = default;

  CondVar(const CondVar&) = delete;
  CondVar& operator=(const CondVar&) = delete;

  void wait() {
    std::unique_lock<std::mutex> lock(mu_->mu_, std::adopt_lock);
    cv_.wait(lock);
    lock.release();
  }
  void signal() { cv_.notify_one(); }
  void signalAll() { cv_.notify_all(); }

 private:
  std::condition_variable cv_;
  Mutex* const mu_;
};

template <typename LockType>
class LockGuard {
 public:
  explicit LockGuard(LockType& mu) : mu_(mu) {  mu_.Lock(); }
  ~LockGuard() { mu_.Unlock(); }

  LockGuard(const LockGuard&) = delete;
  LockGuard& operator=(const LockGuard&) = delete;

 private:
  LockType& mu_;
};

}

#endif // YUNDB_UTIL_SYNC_H