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

#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/QueryManager.hpp>
#include <Sources/GeneratorSource.hpp>
#include <Sources/LambdaSource.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <chrono>
#include <utility>

namespace NES {

LambdaSource::LambdaSource(
    SchemaPtr schema,
    Runtime::BufferManagerPtr bufferManager,
    Runtime::QueryManagerPtr queryManager,
    uint64_t numbersOfBufferToProduce,
    uint64_t gatheringValue,
    std::function<void(NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce)>&& generationFunction,
    OperatorId operatorId,
    OriginId originId,
    StatisticId statisticId,
    size_t numSourceLocalBuffers,
    GatheringMode gatheringMode,
    uint64_t sourceAffinity,
    uint64_t taskQueueId,
    const std::string& physicalSourceName,
    std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors)
    : GeneratorSource(std::move(schema),
                      std::move(bufferManager),
                      std::move(queryManager),
                      numbersOfBufferToProduce,
                      operatorId,
                      originId,
                      statisticId,
                      numSourceLocalBuffers,
                      gatheringMode,
                      std::move(successors),
                      physicalSourceName),
      generationFunction(std::move(generationFunction)) {
    NES_DEBUG("Create LambdaSource with id={} func is {}", operatorId, (this->generationFunction ? "callable" : "not callable"));
    if (this->gatheringMode == GatheringMode::INTERVAL_MODE || this->gatheringMode == GatheringMode::ADAPTIVE_MODE) {
        this->gatheringInterval = std::chrono::milliseconds(gatheringValue);
    } else if (this->gatheringMode == GatheringMode::INGESTION_RATE_MODE) {
        this->gatheringIngestionRate = gatheringValue;
    } else {
        NES_THROW_RUNTIME_ERROR("Mode not implemented " << magic_enum::enum_name(gatheringMode));
    }
    numberOfTuplesToProduce = this->localBufferManager->getBufferSize() / this->schema->getSchemaSizeInBytes();
    this->sourceAffinity = sourceAffinity;
    this->taskQueueId = taskQueueId;
}

std::optional<Runtime::TupleBuffer> LambdaSource::receiveData() {
    NES_TRACE("LambdaSource::receiveData called on operatorId= {}", operatorId);
    using namespace std::chrono_literals;

    auto buffer = allocateBuffer();
    NES_ASSERT2_FMT(numberOfTuplesToProduce * schema->getSchemaSizeInBytes() <= buffer.getBuffer().getBufferSize(),
                    "value to write is larger than the buffer");

    auto rawBuffer = buffer.getBuffer();
    generationFunction(rawBuffer, numberOfTuplesToProduce);
    buffer.setNumberOfTuples(numberOfTuplesToProduce);
    generatedTuples += buffer.getNumberOfTuples();
    generatedBuffers++;

    NES_TRACE("LambdaSource: Current buffer content {}", buffer.toString(schema));
    NES_TRACE("LambdaSource: ReceiveData filled buffer with tuples={}, outOrgID={}",
              buffer.getNumberOfTuples(),
              rawBuffer.getOriginId());

    return rawBuffer;
}

std::string LambdaSource::toString() const { return "LambdaSource"; }

SourceType LambdaSource::getType() const { return SourceType::LAMBDA_SOURCE; }
}// namespace NES
