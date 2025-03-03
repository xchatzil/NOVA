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

#include <Execution/Operators/Streaming/Aggregations/AbstractSlicePreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedThreadLocalSliceStore.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedThreadLocalSliceStore.hpp>
#include <Execution/Operators/Streaming/Aggregations/WindowProcessingTasks.hpp>
#include <Execution/Operators/Streaming/MultiOriginWatermarkProcessor.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/LocalBufferPool.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/VirtualEnableSharedFromThis.hpp>
#include <set>
#include <vector>
namespace NES::Runtime::Execution::Operators {

template<class SliceType, typename SliceStore>
AbstractSlicePreAggregationHandler<SliceType, SliceStore>::AbstractSlicePreAggregationHandler(
    uint64_t windowSize,
    uint64_t windowSlide,
    const std::vector<OriginId>& origins)
    : windowSize(windowSize), windowSlide(windowSlide),
      watermarkProcessor(std::make_unique<MultiOriginWatermarkProcessor>(origins)){};

template<class SliceType, typename SliceStore>
void AbstractSlicePreAggregationHandler<SliceType, SliceStore>::dispatchSliceMergingTasks(
    PipelineExecutionContext& ctx,
    std::shared_ptr<AbstractBufferProvider> bufferProvider,
    std::map<std::tuple<uint64_t, uint64_t>, std::vector<std::shared_ptr<SliceType>>>& collectedSlices) {
    // for all slices that have been collected, emit a merge task to combine this slices.
    // note: the sliceMetaData set is ordered implicitly by the slice start time as the std::map
    // is an associative container that contains a sorted set of unique objects of type Key.
    // Thus, we emit slice deployment tasks in increasing order.
    for (const auto& [metaData, slices] : collectedSlices) {
        NES_ERROR("Allocating buffer for slices {}-{}, seqNumber={}, buffers={}/{}",
                  std::get<0>(metaData),
                  std::get<1>(metaData),
                  resultSequenceNumber + 1, bufferProvider->getAvailableBuffers(), bufferProvider->getNumOfPooledBuffers());
        auto buffer = bufferProvider->getBufferBlocking();
        // allocate a slice merge task withing the buffer.
        auto task = allocateWithin<SliceMergeTask<SliceType>>(buffer);
        task->startSlice = std::get<0>(metaData);
        task->endSlice = std::get<1>(metaData);
        task->sequenceNumber = resultSequenceNumber++;
        task->chunkNumber = 1;
        task->lastChunk = true;
        task->slices = slices;
        NES_ERROR("{} Deploy merge task for slice {}-{} ", windowSize, task->startSlice, task->endSlice);
        ctx.dispatchBuffer(buffer);
    }
}

template<class SliceType, typename SliceStore>
void AbstractSlicePreAggregationHandler<SliceType, SliceStore>::trigger(WorkerContext& wctx,
                                                                        PipelineExecutionContext& ctx,
                                                                        OriginId originId,
                                                                        SequenceData sequenceData,
                                                                        uint64_t watermarkTs) {
    // the watermark update is an atomic process and returns the last and the current watermark.
    NES_ERROR("{} Trigger {}-{}-{}", windowSize, originId, sequenceData.toString(), watermarkTs);
    auto currentWatermark = watermarkProcessor->updateWatermark(watermarkTs, sequenceData, originId);

    if (lastTriggerWatermark == currentWatermark) {
        // if the current watermark has not changed, we don't have to trigger any windows and return.
        NES_ERROR("Window not triggering cause watermark did not update with value={}", watermarkTs);
        return;
    }

    // the watermark has changed get the lock to trigger
    std::lock_guard<std::mutex> lock(triggerMutex);
    // update currentWatermark, such that other threads to have to acquire the lock
    NES_ERROR("{} Trigger slices between {}-{}", windowSize, lastTriggerWatermark, currentWatermark);
    lastTriggerWatermark = currentWatermark;

    // collect all slices that end <= watermark from all thread local slice stores.
    std::map<std::tuple<uint64_t, uint64_t>, std::vector<std::shared_ptr<SliceType>>> collectedSlices;
    for (auto& threadLocalSliceStore : threadLocalSliceStores) {
        for (const auto& slice : threadLocalSliceStore->getSlices()) {
            NES_TRACE("Slices in slice store {}-{}", slice->getStart(), slice->getEnd());
        }
        auto slices = threadLocalSliceStore->extractSlicesUntilTs(lastTriggerWatermark);
        for (const auto& slice : slices) {
            NES_TRACE("Assign thread local slices {}-{}", slice->getStart(), slice->getEnd());
            auto sliceData = std::make_tuple(slice->getStart(), slice->getEnd());
            if (!collectedSlices.contains(sliceData)) {
                collectedSlices.emplace(std::make_tuple(slice->getStart(), slice->getEnd()),
                                        std::vector<std::shared_ptr<SliceType>>());
            }
            collectedSlices.find(sliceData)->second.emplace_back(slice);
        }
        threadLocalSliceStore->setLastWatermark(lastTriggerWatermark);
    }
    dispatchSliceMergingTasks(ctx, wctx.getBufferProvider(), collectedSlices);
};
template<class SliceType, typename SliceStore>
SliceStore* AbstractSlicePreAggregationHandler<SliceType, SliceStore>::getThreadLocalSliceStore(WorkerThreadId workerThreadId) {
    auto index = workerThreadId % threadLocalSliceStores.size();
    return threadLocalSliceStores[index].get();
}
template<class SliceType, typename SliceStore>
void AbstractSlicePreAggregationHandler<SliceType, SliceStore>::start(PipelineExecutionContextPtr, uint32_t) {
    NES_DEBUG("start AbstractSlicePreAggregationHandler");
}

template<class SliceType, typename SliceStore>
void AbstractSlicePreAggregationHandler<SliceType, SliceStore>::stop(QueryTerminationType queryTerminationType,
                                                                     PipelineExecutionContextPtr ctx) {
    NES_DEBUG("shutdown AbstractSlicePreAggregationHandler: {}", queryTerminationType);

    // get the lock to trigger -> this should actually not be necessary, as stop can not be called concurrently to the processing.
    std::lock_guard<std::mutex> lock(triggerMutex);

    if (queryTerminationType == Runtime::QueryTerminationType::Graceful) {
        // collect all remaining slices from all thread local slice stores.
        std::map<std::tuple<uint64_t, uint64_t>, std::vector<std::shared_ptr<SliceType>>> collectedSlices;
        for (auto& threadLocalSliceStore : threadLocalSliceStores) {
            // we can directly access the slices as no other worker can concurrently change them
            for (auto& slice : threadLocalSliceStore->getSlices()) {
                auto& sliceState = slice->getState();
                // each worker adds its local state to the staging area
                auto sliceData = std::make_tuple(slice->getStart(), slice->getEnd());
                if (!collectedSlices.contains(sliceData)) {
                    collectedSlices.emplace(std::make_tuple(slice->getStart(), slice->getEnd()),
                                            std::vector<std::shared_ptr<SliceType>>());
                }
                collectedSlices.find(sliceData)->second.emplace_back(std::move(slice));
            }
        }
        dispatchSliceMergingTasks(*ctx.get(), ctx->getBufferManager(), collectedSlices);
    }
}
template<class SliceType, typename SliceStore>
AbstractSlicePreAggregationHandler<SliceType, SliceStore>::~AbstractSlicePreAggregationHandler() {}

// Instantiate types
template class AbstractSlicePreAggregationHandler<NonKeyedSlice, NonKeyedThreadLocalSliceStore>;
template class AbstractSlicePreAggregationHandler<KeyedSlice, KeyedThreadLocalSliceStore>;

}// namespace NES::Runtime::Execution::Operators
