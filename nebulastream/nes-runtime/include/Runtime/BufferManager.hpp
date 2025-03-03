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

#ifndef NES_RUNTIME_INCLUDE_RUNTIME_BUFFERMANAGER_HPP_
#define NES_RUNTIME_INCLUDE_RUNTIME_BUFFERMANAGER_HPP_

#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferRecycler.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>
#ifdef NES_USE_LATCH_FREE_BUFFER_MANAGER
#include <folly/MPMCQueue.h>
#include <folly/concurrency/UnboundedQueue.h>
#endif

namespace NES::Runtime {

class TupleBuffer;
namespace detail {
class MemorySegment;
}
/**
 * @brief The BufferManager is responsible for:
 * 1. Pooled Buffers: preallocated fixed-size buffers of memory that must be reference counted
 * 2. Unpooled Buffers: variable sized buffers that are allocated on-the-fly. They are also subject to reference
 * counting.
 *
 * The reference counting mechanism of the TupleBuffer is explained in TupleBuffer.hpp
 *
 * The BufferManager stores the pooled buffers as MemorySegment-s. When a component asks for a Pooled buffer,
 * then the BufferManager retrieves an available buffer (it blocks the calling thread, if no buffer is available).
 * It then hands out a TupleBuffer that is constructed through the pointer stored inside a MemorySegment.
 * This is necessary because the BufferManager must keep all buffers stored to ensure that when its
 * destructor is called, all buffers that it has ever created are deallocated. Note the BufferManager will check also
 * that no reference counter is non-zero and will throw a fatal exception, if a component hasnt returned every buffers.
 * This is necessary to avoid memory leaks.
 *
 * Unpooled buffers are either allocated on the spot or served via a previously allocated, unpooled buffer that has
 * been returned to the BufferManager by some component.
 *
 */
class BufferManager : public std::enable_shared_from_this<BufferManager>,
                      public BufferRecycler,
                      public AbstractBufferProvider,
                      public AbstractPoolProvider {
    friend class TupleBuffer;
    friend class detail::MemorySegment;

  private:
    class UnpooledBufferHolder {
      public:
        std::unique_ptr<detail::MemorySegment> segment;
        uint32_t size{0};
        bool free{false};

        UnpooledBufferHolder();

        explicit UnpooledBufferHolder(uint32_t size);

        UnpooledBufferHolder(std::unique_ptr<detail::MemorySegment>&& mem, uint32_t size);

        void markFree();

        friend bool operator<(const UnpooledBufferHolder& lhs, const UnpooledBufferHolder& rhs) { return lhs.size < rhs.size; }
    };

    static constexpr auto DEFAULT_BUFFER_SIZE = 8 * 1024;
    static constexpr auto DEFAULT_NUMBER_OF_BUFFERS = 1024;
    static constexpr auto DEFAULT_ALIGNMENT = 64;

  public:
    /**
     * @brief Creates a new global buffer manager
     * @param bufferSize the size of each buffer in bytes
     * @param numOfBuffers the total number of buffers in the pool
     * @param withAlignment the alignment of each buffer, default is 64 so ony cache line aligned buffers, This value must be a pow of two and smaller than page size
     */
    explicit BufferManager(
        uint32_t bufferSize = DEFAULT_BUFFER_SIZE,
        uint32_t numOfBuffers = DEFAULT_NUMBER_OF_BUFFERS,
        std::shared_ptr<std::pmr::memory_resource> memoryResource = std::make_shared<NesDefaultMemoryAllocator>(),
        uint32_t withAlignment = DEFAULT_ALIGNMENT);

    BufferManager(const BufferManager&) = delete;
    BufferManager& operator=(const BufferManager&) = delete;
    ~BufferManager() override;

    BufferManagerType getBufferManagerType() const override;

  private:
    /**
     * @brief Configure the BufferManager to use numOfBuffers buffers of size bufferSize bytes.
     * This is a one shot call. A second invocation of this call will fail
     * @param withAlignment
     */
    void initialize(uint32_t withAlignment);

  public:
    /**
     * @brief Provides a new TupleBuffer. This blocks until a buffer is available.
     * @return a new buffer
     */
    TupleBuffer getBufferBlocking() override;

    /**
     * @brief Provides a new TupleBuffer from the pool below a threshold. This blocks until a buffer is available.
     * @return a new buffer
     */
    TupleBuffer getThresholdBufferBlocking(double threshold);

    /**
     * @brief Returns a new TupleBuffer wrapped in an optional or an invalid option if there is no buffer.
     * @return a new buffer
     */
    std::optional<TupleBuffer> getBufferNoBlocking() override;

    /**
     * @brief Returns a new Buffer wrapped in an optional or an invalid option if there is no buffer available within
     * timeout_ms.
     * @param timeout_ms the amount of time to wait for a new buffer to be retuned
     * @return a new buffer
     */
    std::optional<TupleBuffer> getBufferTimeout(std::chrono::milliseconds timeout_ms) override;

    /**
     * @brief Returns an unpooled buffer of size bufferSize wrapped in an optional or an invalid option if an error
     * occurs.
     * @param bufferSize
     * @return a new buffer
     */
    std::optional<TupleBuffer> getUnpooledBuffer(size_t bufferSize) override;

    /**
     * @return Configured size of the buffers
     */
    size_t getBufferSize() const override;

    /**
     * @return Number of total buffers in the pool
     */
    size_t getNumOfPooledBuffers() const override;

    /**
     * @return number of unpooled buffers
     */
    size_t getNumOfUnpooledBuffers() const override;

    /**
     * @return Number of available buffers in the pool
     */
    size_t getAvailableBuffers() const override;

    /**
    * @return Number of available buffers in the fixed size pool
    */
    size_t getAvailableBuffersInFixedSizePools() const;

    /**
     * @brief Create a local buffer manager that is assigned to one pipeline or thread
     * @param numberOfReservedBuffers number of exclusive buffers to give to the pool
     * @return a local buffer manager with numberOfReservedBuffers exclusive buffer
     */
    LocalBufferPoolPtr createLocalBufferPool(size_t numberOfReservedBuffers) override;

    /**
      * @brief Create a local buffer manager that is assigned to one pipeline or thread
      * @param numberOfReservedBuffers number of exclusive buffers to give to the pool
      * @return a local buffer manager with numberOfReservedBuffers exclusive buffer
      */
    FixedSizeBufferPoolPtr createFixedSizeBufferPool(size_t numberOfReservedBuffers) override;

    /**
     * @brief Recycle a pooled buffer by making it available to others
     * @param buffer
     */
    void recyclePooledBuffer(detail::MemorySegment* segment) override;

    /**
    * @brief Recycle an unpooled buffer by making it available to others
    * @param buffer
    */
    void recycleUnpooledBuffer(detail::MemorySegment* segment) override;

    /**
     * @brief this method clears all local buffers pools and remove all buffers from the global buffer manager
     */
    void destroy() override;

  private:
    std::vector<detail::MemorySegment> allBuffers;
#ifndef NES_USE_LATCH_FREE_BUFFER_MANAGER
    std::deque<detail::MemorySegment*> availableBuffers;
#else
    folly::MPMCQueue<detail::MemorySegment*> availableBuffers;
    std::atomic<size_t> numOfAvailableBuffers;
#endif
    std::vector<UnpooledBufferHolder> unpooledBuffers;

    mutable std::recursive_mutex availableBuffersMutex;
    std::condition_variable_any availableBuffersCvar;

    mutable std::recursive_mutex unpooledBuffersMutex;

    uint32_t bufferSize;
    uint32_t numOfBuffers;

    uint8_t* basePointer{nullptr};
    size_t allocatedAreaSize;

    mutable std::recursive_mutex localBufferPoolsMutex;
    std::vector<std::shared_ptr<AbstractBufferProvider>> localBufferPools;
    std::shared_ptr<std::pmr::memory_resource> memoryResource;
    std::atomic<bool> isDestroyed{false};
};

}// namespace NES::Runtime

#endif// NES_RUNTIME_INCLUDE_RUNTIME_BUFFERMANAGER_HPP_
