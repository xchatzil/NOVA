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

#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/detail/TupleBufferImpl.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <bitset>

#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
#include <Util/Backward/backward.hpp>
#include <mutex>
#include <thread>
#endif

namespace NES::Runtime {

namespace detail {

// -----------------------------------------------------------------------------
// ------------------ Core Mechanism for Buffer recycling ----------------------
// -----------------------------------------------------------------------------

MemorySegment::MemorySegment(const MemorySegment& other) = default;

MemorySegment& MemorySegment::operator=(const MemorySegment& other) = default;

MemorySegment::MemorySegment(uint8_t* ptr,
                             uint32_t size,
                             BufferRecycler* recycler,
                             std::function<void(MemorySegment*, BufferRecycler*)>&& recycleFunction,
                             uint8_t* controlBlock)
    : size(size) {
    this->controlBlock = new (controlBlock) BufferControlBlock(this, recycler, std::move(recycleFunction));
    this->ptr = ptr;
    if (!this->ptr) {
        NES_THROW_RUNTIME_ERROR("[MemorySegment] invalid pointer");
    }
    if (!this->size) {
        NES_THROW_RUNTIME_ERROR("[MemorySegment] invalid size");
    }
}

MemorySegment::MemorySegment(uint8_t* ptr,
                             uint32_t size,
                             BufferRecycler* recycler,
                             std::function<void(MemorySegment*, BufferRecycler*)>&& recycleFunction,
                             bool)
    : ptr(ptr), size(size) {
    NES_ASSERT2_FMT(this->ptr, "invalid ptr");
    NES_ASSERT2_FMT(this->size, "invalid size");
    controlBlock.reset(new BufferControlBlock(this, recycler, std::move(recycleFunction)),
                       magic_enum::enum_integer(MemorySegmentType::Wrapped));
    controlBlock->prepare();
}

MemorySegment::~MemorySegment() {
    if (ptr) {

        /// XXX: If we want to make `release` noexcept as we discussed, we need to make sure that the
        ///      MemorySegment is noexcept destructible. I therefore transformed this error into an assertion
        ///      (I also consider this to be consistent with our handeling of the referenceCount in
        ///      the release function in general. Do you agree?).
        {
            auto const refCnt = controlBlock->getReferenceCount();
            NES_ASSERT(refCnt == 0, "[MemorySegment] invalid reference counter" << refCnt << " on mem segment dtor");
        }

        // Release the controlBlock, which is either allocated via 'new' or placement new. In the latter case, we only
        // have to call the destructor, as the memory segment that contains the controlBlock is managed separately.
        if (controlBlock.tag() == magic_enum::enum_integer(MemorySegmentType::Wrapped)) {
            delete controlBlock.get();
        } else {
            controlBlock->~BufferControlBlock();
        }

        std::exchange(controlBlock, nullptr);
        std::exchange(ptr, nullptr);
    }
}

BufferControlBlock::BufferControlBlock(MemorySegment* owner,
                                       BufferRecycler* recycler,
                                       std::function<void(MemorySegment*, BufferRecycler*)>&& recycleCallback)
    : owner(owner), owningBufferRecycler(recycler), recycleCallback(std::move(recycleCallback)) {
    // nop
}

BufferControlBlock::BufferControlBlock(const BufferControlBlock& that) {
    referenceCounter.store(that.referenceCounter.load());
    numberOfTuples = that.numberOfTuples;
    creationTimestamp = that.creationTimestamp;
    recycleCallback = that.recycleCallback;
    owner = that.owner;
    watermark = that.watermark;
    originId = that.originId;
}

BufferControlBlock& BufferControlBlock::operator=(const BufferControlBlock& that) {
    referenceCounter.store(that.referenceCounter.load());
    numberOfTuples = that.numberOfTuples;
    recycleCallback = that.recycleCallback;
    owner = that.owner;
    watermark = that.watermark;
    creationTimestamp = that.creationTimestamp;
    originId = that.originId;
    return *this;
}

MemorySegment* BufferControlBlock::getOwner() const { return owner; }

void BufferControlBlock::resetBufferRecycler(BufferRecycler* recycler) {
    NES_ASSERT2_FMT(recycler, "invalid recycler");
    auto* oldRecycler = owningBufferRecycler.exchange(recycler);
    NES_ASSERT2_FMT(recycler != oldRecycler, "invalid recycler");
}

void BufferControlBlock::addRecycleCallback(std::function<void(MemorySegment*, BufferRecycler*)>&& func) noexcept {
    auto oldRecycleCallback = this->recycleCallback;
    recycleCallback = [oldRecycleCallback, func](MemorySegment* memorySegment, BufferRecycler* bufferRecycler) {
        func(memorySegment, bufferRecycler);
        oldRecycleCallback(memorySegment, bufferRecycler);
    };
}

#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
/**
 * @brief This function collects the thread name and the callstack of the calling thread
 * @param threadName
 * @param callstack
 */
void fillThreadOwnershipInfo(std::string& threadName, std::string& callstack) {
    static constexpr int CALLSTACK_DEPTH = 16;

    backward::StackTrace st;
    backward::Printer p;
    st.load_here(CALLSTACK_DEPTH);
    std::stringbuf callStackBuffer;
    std::ostream os0(&callStackBuffer);
    p.print(st, os0);

    std::stringbuf threadNameBuffer;
    std::ostream os1(&threadNameBuffer);
    os1 << std::this_thread::get_id();

    threadName = threadNameBuffer.str();
    callstack = callStackBuffer.str();
}
#endif
bool BufferControlBlock::prepare() {
    int32_t expected = 0;
#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
    // store the current thread that owns the buffer and track which function obtained the buffer
    std::unique_lock lock(owningThreadsMutex);
    ThreadOwnershipInfo info;
    fillThreadOwnershipInfo(info.threadName, info.callstack);
    owningThreads[std::this_thread::get_id()].emplace_back(info);
#endif
    if (referenceCounter.compare_exchange_strong(expected, 1)) {
        return true;
    }
    NES_ERROR("Invalid reference counter: {}", expected);
    return false;
}

BufferControlBlock* BufferControlBlock::retain() {
#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
    // store the current thread that owns the buffer (shared) and track which function increased the coutner of the buffer
    std::unique_lock lock(owningThreadsMutex);
    ThreadOwnershipInfo info;
    fillThreadOwnershipInfo(info.threadName, info.callstack);
    owningThreads[std::this_thread::get_id()].emplace_back(info);
#endif
    referenceCounter++;
    return this;
}

#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
void BufferControlBlock::dumpOwningThreadInfo() {
    std::unique_lock lock(owningThreadsMutex);
    NES_FATAL_ERROR("Buffer {} has {} live references", getOwner(), referenceCounter.load());
    for (auto& item : owningThreads) {
        for (auto& v : item.second) {
            NES_FATAL_ERROR("Thread {} has buffer {} requested on callstack: {}", v.threadName, getOwner(), v.callstack);
        }
    }
}
#endif

int32_t BufferControlBlock::getReferenceCount() const noexcept { return referenceCounter.load(); }

bool BufferControlBlock::release() {
    if (uint32_t const prevRefCnt = referenceCounter.fetch_sub(1); prevRefCnt == 1) {
        numberOfTuples = 0;
        for (auto&& child : children) {
            child->controlBlock->release();
        }
        children.clear();
        recycleCallback(owner, owningBufferRecycler.load());
#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
        {
            std::unique_lock lock(owningThreadsMutex);
            owningThreads.clear();
        }
#endif
        return true;
    } else {
        NES_ASSERT(prevRefCnt != 0, "BufferControlBlock: releasing an already released buffer");
    }
#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
    {
        std::unique_lock lock(owningThreadsMutex);
        auto& v = owningThreads[std::this_thread::get_id()];
        if (!v.empty()) {
            v.pop_front();
        }
    }
#endif
    return false;
}

#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
BufferControlBlock::ThreadOwnershipInfo::ThreadOwnershipInfo(std::string&& threadName, std::string&& callstack)
    : threadName(threadName), callstack(callstack) {
    // nop
}

BufferControlBlock::ThreadOwnershipInfo::ThreadOwnershipInfo() : threadName("NOT-SAMPLED"), callstack("NOT-SAMPLED") {
    // nop
}
#endif

// -----------------------------------------------------------------------------
// ------------------ Utility functions for TupleBuffer ------------------------
// -----------------------------------------------------------------------------

uint64_t BufferControlBlock::getNumberOfTuples() const noexcept { return numberOfTuples; }

void BufferControlBlock::setNumberOfTuples(uint64_t numberOfTuples) { this->numberOfTuples = numberOfTuples; }

uint64_t BufferControlBlock::getWatermark() const noexcept { return watermark; }

void BufferControlBlock::setWatermark(uint64_t watermark) { this->watermark = watermark; }

uint64_t BufferControlBlock::getSequenceNumber() const noexcept { return sequenceNumber; }

void BufferControlBlock::setSequenceNumber(uint64_t sequenceNumber) { this->sequenceNumber = sequenceNumber; }

uint64_t BufferControlBlock::getChunkNumber() const noexcept { return chunkNumber; }

void BufferControlBlock::setChunkNumber(uint64_t chunkNumber) { this->chunkNumber = chunkNumber; }

bool BufferControlBlock::isLastChunk() const noexcept { return lastChunk; }

void BufferControlBlock::setLastChunk(bool lastChunk) { this->lastChunk = lastChunk; }

void BufferControlBlock::setCreationTimestamp(uint64_t ts) { this->creationTimestamp = ts; }

uint64_t BufferControlBlock::getCreationTimestamp() const noexcept { return creationTimestamp; }

OriginId BufferControlBlock::getOriginId() const noexcept { return originId; }

uint64_t BufferControlBlock::getStatisticId() const noexcept { return statisticId; }

void BufferControlBlock::setOriginId(OriginId originId) { this->originId = originId; }

void BufferControlBlock::setStatisticId(StatisticId statisticId) { this->statisticId = statisticId; }

void zmqBufferRecyclingCallback(void*, void* hint) {
    NES_VERIFY(hint != nullptr, "Hint cannot be null");
    auto* controlBlock = reinterpret_cast<BufferControlBlock*>(hint);
    controlBlock->release();
}

// -----------------------------------------------------------------------------
// ------------------ VarLen fields support for TupleBuffer --------------------
// -----------------------------------------------------------------------------

uint32_t BufferControlBlock::storeChildBuffer(BufferControlBlock* control) {
    control->retain();
    children.emplace_back(control->owner);
    return children.size() - 1;
}

bool BufferControlBlock::loadChildBuffer(uint16_t index, BufferControlBlock*& control, uint8_t*& ptr, uint32_t& size) const {
    NES_ASSERT2_FMT(index < children.size(), "Invalid index");

    auto* child = children[index];
    control = child->controlBlock->retain();
    ptr = child->ptr;
    size = child->size;

    return true;
}
}// namespace detail
}// namespace NES::Runtime
