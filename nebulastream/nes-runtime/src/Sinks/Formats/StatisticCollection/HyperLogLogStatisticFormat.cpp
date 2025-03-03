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

#include <API/Schema.hpp>
#include <Measures/TimeMeasure.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Formats/StatisticCollection/HyperLogLogStatisticFormat.hpp>
#include <Statistics/Synopses/HyperLogLogStatistic.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <sstream>

namespace NES::Statistic {

StatisticFormatPtr HyperLogLogStatisticFormat::create(Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout,
                                                      std::function<std::string(const std::string&)> postProcessingData,
                                                      std::function<std::string(const std::string&)> preProcessingData) {
    const auto qualifierNameWithSeparator = memoryLayout->getSchema()->getQualifierNameForSystemGeneratedFieldsWithSeparator();
    return std::make_shared<HyperLogLogStatisticFormat>(
        HyperLogLogStatisticFormat(qualifierNameWithSeparator, std::move(memoryLayout), postProcessingData, preProcessingData));
}

std::vector<std::pair<StatisticHash, StatisticPtr>>
HyperLogLogStatisticFormat::readStatisticsFromBuffer(Runtime::TupleBuffer& buffer) {
    std::vector<HashStatisticPair> createdStatisticsWithTheirHash;

    // Each tuple represents one HyperLogLog-Sketch
    for (auto curTupleCnt = 0_u64; curTupleCnt < buffer.getNumberOfTuples(); ++curTupleCnt) {
        // Calculating / Getting the field offset for each of the fields we need
        const auto startTsFieldOffset = memoryLayout->getFieldOffset(curTupleCnt, startTsFieldName);
        const auto endTsFieldOffset = memoryLayout->getFieldOffset(curTupleCnt, endTsFieldName);
        const auto hashFieldOffset = memoryLayout->getFieldOffset(curTupleCnt, statisticHashFieldName);
        const auto observedTuplesFieldOffset = memoryLayout->getFieldOffset(curTupleCnt, observedTuplesFieldName);
        const auto widthFieldOffset = memoryLayout->getFieldOffset(curTupleCnt, widthFieldName);
        const auto estimateFieldOffset = memoryLayout->getFieldOffset(curTupleCnt, estimateFieldName);
        const auto hyperLogLogDataFieldOffset = memoryLayout->getFieldOffset(curTupleCnt, hyperLogLogDataFieldName);

        // Checking if all field offsets are valid and have a value
        if (!startTsFieldOffset.has_value() || !endTsFieldOffset.has_value() || !hashFieldOffset.has_value()
            || !observedTuplesFieldOffset.has_value() || !widthFieldOffset.has_value() || !estimateFieldOffset.has_value()
            || !hyperLogLogDataFieldOffset.has_value()) {
            NES_ERROR("Expected to receive an offset for all required fields! Skipping creating a statistic for "
                      "the {}th tuple in the buffer!",
                      curTupleCnt);
            continue;
        }

        // We choose to hardcode here the values. If we would to do it dynamically during the runtime, we would have to
        // do a lot of branches, as we do not have here the tracing from Nautilus.
        const auto startTs = *reinterpret_cast<uint64_t*>(buffer.getBuffer() + startTsFieldOffset.value());
        const auto endTs = *reinterpret_cast<uint64_t*>(buffer.getBuffer() + endTsFieldOffset.value());
        const auto hash = *reinterpret_cast<uint64_t*>(buffer.getBuffer() + hashFieldOffset.value());
        const auto observedTuples = *reinterpret_cast<uint64_t*>(buffer.getBuffer() + observedTuplesFieldOffset.value());
        const auto width = *reinterpret_cast<uint64_t*>(buffer.getBuffer() + widthFieldOffset.value());
        const auto estimate = *reinterpret_cast<double*>(buffer.getBuffer() + estimateFieldOffset.value());

        // Reading the HyperLogLogData that is stored as a string
        const auto hyperLogLogDataChildIdx =
            *reinterpret_cast<uint32_t*>(buffer.getBuffer() + hyperLogLogDataFieldOffset.value());
        const auto hyperLogLogDataString =
            postProcessingData(Runtime::MemoryLayouts::readVarSizedData(buffer, hyperLogLogDataChildIdx));

        // Creating now a HyperLogLogStatistic from this
        auto hyperLogLog = HyperLogLogStatistic::create(Windowing::TimeMeasure(startTs),
                                                        Windowing::TimeMeasure(endTs),
                                                        observedTuples,
                                                        width,
                                                        hyperLogLogDataString,
                                                        estimate);
        createdStatisticsWithTheirHash.emplace_back(hash, hyperLogLog);
    }

    return createdStatisticsWithTheirHash;
}

std::string HyperLogLogStatisticFormat::toString() const {
    std::ostringstream oss;
    oss << "startTsFieldName: " << startTsFieldName << " ";
    oss << "endTsFieldName: " << endTsFieldName << " ";
    oss << "statisticHashFieldName: " << statisticHashFieldName << " ";
    oss << "statisticTypeFieldName: " << statisticTypeFieldName << " ";
    oss << "observedTuplesFieldName: " << observedTuplesFieldName << " ";
    oss << "widthFieldName: " << widthFieldName << " ";
    oss << "estimateFieldName: " << estimateFieldName << " ";
    oss << "hyperLogLogDataFieldName: " << hyperLogLogDataFieldName << " ";
    return oss.str();
}

std::vector<Runtime::TupleBuffer>
HyperLogLogStatisticFormat::writeStatisticsIntoBuffers(const std::vector<HashStatisticPair>& statisticsPlusHashes,
                                                       Runtime::BufferManager& bufferManager) {

    std::vector<Runtime::TupleBuffer> createdTupleBuffers;
    uint64_t insertedStatistics = 0;
    const auto capacityOfTupleBuffer = memoryLayout->getCapacity();
    auto buffer = bufferManager.getBufferBlocking();
    for (auto& [statisticHash, statistic] : statisticsPlusHashes) {
        // 1. Calculating the offset for each field
        const auto startTsFieldOffset = memoryLayout->getFieldOffset(insertedStatistics, startTsFieldName);
        const auto endTsFieldOffset = memoryLayout->getFieldOffset(insertedStatistics, endTsFieldName);
        const auto hashFieldOffset = memoryLayout->getFieldOffset(insertedStatistics, statisticHashFieldName);
        const auto observedTuplesFieldOffset = memoryLayout->getFieldOffset(insertedStatistics, observedTuplesFieldName);
        const auto widthFieldOffset = memoryLayout->getFieldOffset(insertedStatistics, widthFieldName);
        const auto estimateFieldOffset = memoryLayout->getFieldOffset(insertedStatistics, estimateFieldName);
        const auto hyperLogLogDataFieldOffset = memoryLayout->getFieldOffset(insertedStatistics, hyperLogLogDataFieldName);

        // Checking if all field offsets are valid and have a value
        if (!startTsFieldOffset.has_value() || !endTsFieldOffset.has_value() || !hashFieldOffset.has_value()
            || !observedTuplesFieldOffset.has_value() || !widthFieldOffset.has_value() || !estimateFieldOffset.has_value()
            || !hyperLogLogDataFieldOffset.has_value()) {
            NES_ERROR("Expected to receive an offset for all required fields! Skipping creating a statistic for "
                      "the {}th tuple in the buffer!",
                      insertedStatistics);
            continue;
        }

        // 3. Getting all values from the statistic
        NES_ASSERT2_FMT(statistic->instanceOf<HyperLogLogStatistic>(),
                        "HyperLogLogStatisticFormat knows only how to write HyperLogLogStatistic!");
        const auto hyperLogLogStatistic = statistic->as<HyperLogLogStatistic>();
        const auto startTs = hyperLogLogStatistic->getStartTs();
        const auto endTs = hyperLogLogStatistic->getEndTs();
        const auto hash = statisticHash;
        const auto observedTuples = hyperLogLogStatistic->getObservedTuples();
        const auto width = hyperLogLogStatistic->getWidth();
        const auto estimate = hyperLogLogStatistic->getEstimate();
        const auto data = preProcessingData(hyperLogLogStatistic->getHyperLogLogDataAsString());

        // 4. We choose to hardcode here the values. If we would to do it dynamically during the runtime, we would have to
        // do a lot of branches, as we do not have here the tracing from Nautilus.
        *reinterpret_cast<uint64_t*>(buffer.getBuffer() + startTsFieldOffset.value()) = startTs.getTime();
        *reinterpret_cast<uint64_t*>(buffer.getBuffer() + endTsFieldOffset.value()) = endTs.getTime();
        *reinterpret_cast<uint64_t*>(buffer.getBuffer() + hashFieldOffset.value()) = hash;
        *reinterpret_cast<uint64_t*>(buffer.getBuffer() + observedTuplesFieldOffset.value()) = observedTuples;
        *reinterpret_cast<uint64_t*>(buffer.getBuffer() + widthFieldOffset.value()) = width;
        *reinterpret_cast<double*>(buffer.getBuffer() + estimateFieldOffset.value()) = estimate;

        // 5. Writing the HyperLogLogData that we store as a string
        const auto hyperLogLogDataChildIdx = Runtime::MemoryLayouts::writeVarSizedData(buffer, data, bufferManager);
        if (!hyperLogLogDataChildIdx.has_value()) {
            NES_ERROR("Expected to store the count min data as a string! Skipping creating a statistic for "
                      "the {}th tuple in the buffer!",
                      insertedStatistics);
            continue;
        }
        *reinterpret_cast<uint32_t*>(buffer.getBuffer() + hyperLogLogDataFieldOffset.value()) = hyperLogLogDataChildIdx.value();

        // 6. Incrementing the position in the tuple buffer and setting the number of tuples
        insertedStatistics += 1;

        if (insertedStatistics >= capacityOfTupleBuffer) {
            buffer.setNumberOfTuples(insertedStatistics);
            createdTupleBuffers.emplace_back(buffer);

            buffer = bufferManager.getBufferBlocking();
            insertedStatistics = 0;
        }
    }

    // In the loop, we only insert into the createdTupleBuffers, if the buffer is full. Thus, we need to check, if we
    // have created a buffer that has not been inserted into createdTupleBuffers
    if (insertedStatistics > 0) {
        buffer.setNumberOfTuples(insertedStatistics);
        createdTupleBuffers.emplace_back(buffer);
    }

    return createdTupleBuffers;
}

HyperLogLogStatisticFormat::HyperLogLogStatisticFormat(const std::string& qualifierNameWithSeparator,
                                                       Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout,
                                                       std::function<std::string(const std::string&)> postProcessingData,
                                                       std::function<std::string(const std::string&)> preProcessingData)
    : AbstractStatisticFormat(qualifierNameWithSeparator, std::move(memoryLayout), postProcessingData, preProcessingData),
      widthFieldName(qualifierNameWithSeparator + WIDTH_FIELD_NAME),
      estimateFieldName(qualifierNameWithSeparator + ESTIMATE_FIELD_NAME),
      hyperLogLogDataFieldName(qualifierNameWithSeparator + STATISTIC_DATA_FIELD_NAME) {}

HyperLogLogStatisticFormat::~HyperLogLogStatisticFormat() = default;

}// namespace NES::Statistic
