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

#include <Execution/Operators/Vectorization/StagingHandler.hpp>

#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES::Runtime::Execution::Operators {

StagingHandler::StagingHandler(uint64_t stageBufferSize, uint64_t schemaSize)
    : stageBufferSize(stageBufferSize), stageBufferCapacity(stageBufferSize / schemaSize), tupleBuffer(nullptr),
      currentWritePosition(0) {
    NES_ASSERT(schemaSize < stageBufferSize, "Stage buffer is too small for schema");
}

void StagingHandler::start(Runtime::Execution::PipelineExecutionContextPtr ctx, uint32_t) {
    auto bufferManager = ctx->getBufferManager();
    NES_INFO("Requesting unpooled buffer of size {} from buffer manager", stageBufferSize);
    auto bufferOpt = bufferManager->getUnpooledBuffer(stageBufferSize);
    NES_ASSERT(bufferOpt, "Request for unpooled buffer failed");
    auto buffer = bufferOpt.value();
    tupleBuffer = std::make_unique<Runtime::TupleBuffer>(buffer);
}

void StagingHandler::stop(Runtime::QueryTerminationType, Runtime::Execution::PipelineExecutionContextPtr) {
    if (tupleBuffer) {
        tupleBuffer->release();
    }
}

void StagingHandler::reset() { currentWritePosition = (uint64_t) 0; }

bool StagingHandler::full() const { return currentWritePosition >= stageBufferCapacity; }

TupleBuffer* StagingHandler::getTupleBuffer() const {
    if (currentWritePosition == 0) {
        auto ts =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch())
                .count();
        tupleBuffer->setCreationTimestampInMS(ts);
    }
    return tupleBuffer.get();
}

uint64_t StagingHandler::getCurrentWritePositionAndIncrement() {
    auto oldWritePosition = currentWritePosition;
    currentWritePosition = currentWritePosition + 1;
    if (tupleBuffer) {
        tupleBuffer->setNumberOfTuples(currentWritePosition);
    }
    return oldWritePosition;
}

}// namespace NES::Runtime::Execution::Operators
