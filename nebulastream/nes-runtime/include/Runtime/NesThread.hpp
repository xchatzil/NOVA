/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#ifndef NES_RUNTIME_INCLUDE_RUNTIME_NESTHREAD_HPP_
#define NES_RUNTIME_INCLUDE_RUNTIME_NESTHREAD_HPP_

#include <Identifiers/Identifiers.hpp>
#include <Util/Logger/Logger.hpp>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <stdexcept>

/// Turn this on to have Thread::current_num_threads_ keep a count of currently-active threads.
#undef COUNT_ACTIVE_THREADS

// this class was inspired by FASTER (Microsoft (c))

namespace NES::Runtime {
/// Gives every thread a unique, numeric thread ID, and recycles IDs when threads exit.
/// do not use this class from components that do not belong to the worker execution engine!!!
class NesThread {
  public:
    /// The number of entries in table. Currently, this is fixed at 256 and never changes or grows.
    /// If the table runs out of entries, then the current implementation will throw a
    /// std::runtime_error.
    /// do not change this value
    static constexpr WorkerThreadId::Underlying MaxNumThreads = 256;

  private:
    /// Encapsulates a thread ID, getting a free ID from the Thread class when the thread starts, and
    /// releasing it back to the Thread class, when the thread exits.
    class ThreadId {
      public:
        static constexpr WorkerThreadId InvalidId = INVALID<WorkerThreadId>;

        inline ThreadId();
        inline ~ThreadId();

        [[nodiscard]] WorkerThreadId getId() const { return id; }

      private:
        WorkerThreadId id;
    };

  public:
    /// Call static method Thread::getId() to get the executing thread's ID.
    inline static WorkerThreadId getId() { return id.getId(); }

  private:
    /// Methods ReserveEntry() and ReleaseEntry() do the real work.
    inline static WorkerThreadId reserveEntry() {
        auto start = next_index++;
        auto end = start + 2 * MaxNumThreads;
        for (uint32_t id = start; id < end; ++id) {
            bool expected = false;
            if (id_used[id % MaxNumThreads].compare_exchange_strong(expected, true)) {
                return WorkerThreadId(id % MaxNumThreads);
            }
        }
        throw std::runtime_error("Too many threads!");
    }

    inline static void releaseEntry(WorkerThreadId workerThreadId) {
        NES_VERIFY(workerThreadId != ThreadId::InvalidId, "invalid thread id");
        auto threadWasInUse = id_used[workerThreadId.getRawValue() % MaxNumThreads].exchange(false);
        NES_VERIFY(threadWasInUse, "invalid thread id");
    }

    /// The current thread's index.
    static thread_local ThreadId id;

    /// Next thread index to consider.
    static std::atomic<uint32_t> next_index;
    /// Which thread IDs have already been taken.
    static std::atomic<bool> id_used[MaxNumThreads];

    friend class ThreadId;
};

inline NesThread::ThreadId::ThreadId() : id(InvalidId) { id = NesThread::reserveEntry(); }

inline NesThread::ThreadId::~ThreadId() { NesThread::releaseEntry(id); }

}// namespace NES::Runtime
#endif// NES_RUNTIME_INCLUDE_RUNTIME_NESTHREAD_HPP_
