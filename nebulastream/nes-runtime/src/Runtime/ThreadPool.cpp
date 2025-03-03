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
#include <Exceptions/TaskExecutionException.hpp>
#include <Network/NetworkChannel.hpp>
#include <Runtime/NesThread.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/Task.hpp>
#include <Runtime/ThreadPool.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/ThreadBarrier.hpp>
#include <Util/ThreadNaming.hpp>
#include <cstring>
#include <filesystem>
#include <functional>
#include <string>
#include <thread>
#include <utility>

#ifdef __linux__
#include <Runtime/HardwareManager.hpp>
#endif

#ifdef ENABLE_PAPI_PROFILER
#include <Runtime/Profiler/PAPIProfiler.hpp>
#endif
namespace NES::Runtime {

ThreadPool::ThreadPool(WorkerId nodeId,
                       QueryManagerPtr queryManager,
                       uint32_t numThreads,
                       std::vector<BufferManagerPtr> bufferManagers,
                       uint64_t numberOfBuffersPerWorker,
                       HardwareManagerPtr hardwareManager,
                       std::vector<uint64_t> workerPinningPositionList)
    : nodeId(nodeId), numThreads(numThreads), queryManager(std::move(queryManager)), bufferManagers(bufferManagers),
      numberOfBuffersPerWorker(numberOfBuffersPerWorker), workerPinningPositionList(workerPinningPositionList),
      hardwareManager(hardwareManager) {}

ThreadPool::~ThreadPool() {
    NES_DEBUG("Threadpool: Destroying Thread Pool");
    stop();
    NES_DEBUG("QueryManager: Destroy threads Queue");
    threads.clear();
}

void ThreadPool::runningRoutine(WorkerContext&& workerContext) {
    while (running) {
        try {
            switch (queryManager->processNextTask(running, workerContext)) {
                case ExecutionResult::Finished:
                case ExecutionResult::Ok:
                case ExecutionResult::Error: {
                    break;
                }
                case ExecutionResult::AllFinished: {
                    NES_DEBUG("Threadpool got poison pill - shutting down...");
                    running = false;
                    break;
                }
                default: {
                    NES_ASSERT(false, "Unsupported execution result");
                }
            }
        } catch (TaskExecutionException const& taskException) {
            NES_ERROR("Exception thrown during task execution on thread {}: {}", workerContext.getId(), taskException.what());
            queryManager->notifyTaskFailure(taskException.getExecutable(), std::string(taskException.what()));
        }
    }
    // to drain the queue for pending reconfigurations
    // after this no need to care for error handling
    try {
        queryManager->processNextTask(running, workerContext);
    } catch (std::exception const& error) {
        NES_ERROR("Got fatal error on thread {}: {}", workerContext.getId(), error.what());
        NES_THROW_RUNTIME_ERROR("Got fatal error on thread " << workerContext.getId() << ": " << error.what());
    }
    NES_DEBUG("Threadpool: end runningRoutine");
}

bool ThreadPool::start(const std::vector<uint64_t> threadToQueueMapping) {
    auto barrier = std::make_shared<ThreadBarrier>(numThreads + 1);
    std::unique_lock lock(reconfigLock);
    if (running) {
        NES_DEBUG("Threadpool:start already running, return false");
        return false;
    }
    running = true;

    /* spawn threads */
    NES_DEBUG("Threadpool: Spawning {} threads", numThreads);
    for (uint64_t i = 0; i < numThreads; ++i) {
        threads.emplace_back([this, i, barrier, &threadToQueueMapping]() {
            setThreadName("Wrk-%d-%d", nodeId, i);
            BufferManagerPtr localBufferManager;
            uint64_t queueIdx = threadToQueueMapping.size() ? threadToQueueMapping[i] : 0;
#if defined(__linux__)
            if (workerPinningPositionList.size() != 0) {
                NES_ASSERT(numThreads <= workerPinningPositionList.size(),
                           "Not enough worker positions for pinning are provided");
                uint64_t maxPosition = *std::max_element(workerPinningPositionList.begin(), workerPinningPositionList.end());
                NES_ASSERT(maxPosition < std::thread::hardware_concurrency(),
                           "pinning position thread is out of cpu range maxPosition=" << maxPosition);
                //pin core
                cpu_set_t cpuset;
                CPU_ZERO(&cpuset);
                CPU_SET(workerPinningPositionList[i], &cpuset);
                int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
                if (rc != 0) {
                    NES_ERROR("Error calling pthread_setaffinity_np: {}", rc);
                } else {
                    NES_WARNING("worker {} pins to core={}", i, workerPinningPositionList[i]);
                }
            }
#endif
            localBufferManager = bufferManagers[queueIdx];

            barrier->wait();
            NES_ASSERT(localBufferManager, "localBufferManager is null");
#ifdef ENABLE_PAPI_PROFILER
            auto path = std::filesystem::path("worker_" + std::to_string(NesThread::getId()) + ".csv");
            auto profiler = std::make_shared<Profiler::PapiCpuProfiler>(Profiler::PapiCpuProfiler::Presets::CachePresets,
                                                                        std::ofstream(path, std::ofstream::out),
                                                                        NesThread::getId(),
                                                                        NesThread::getId());
            queryManager->cpuProfilers[NesThread::getId() % queryManager->cpuProfilers.size()] = profiler;
#endif
            // TODO (2310) properly initialize the profiler with a file, thread, and core id
            auto workerId = NesThread::getId();
            NES_DEBUG("worker {} with workerId {} pins to queue {}", i, workerId, queueIdx);
            runningRoutine(WorkerContext(workerId, localBufferManager, numberOfBuffersPerWorker, queueIdx));
        });
    }
    barrier->wait();
    NES_DEBUG("Threadpool: start return from start");
    return true;
}

bool ThreadPool::stop() {
    std::unique_lock lock(reconfigLock);
    NES_DEBUG("ThreadPool: stop thread pool while {} with {} threads", (running.load() ? "running" : "not running"), numThreads);
    auto expected = true;
    if (!running.compare_exchange_strong(expected, false)) {
        return false;
    }
    /* wake up all threads in the query manager,
 * so they notice the change in the run variable */
    NES_DEBUG("Threadpool: Going to unblock {} threads", numThreads);
    queryManager->poisonWorkers();
    /* join all threads if possible */
    for (auto& thread : threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }
    threads.clear();
    NES_DEBUG("Threadpool: stop finished");
    return true;
}

uint32_t ThreadPool::getNumberOfThreads() const {
    std::unique_lock lock(reconfigLock);
    return numThreads;
}

}// namespace NES::Runtime
