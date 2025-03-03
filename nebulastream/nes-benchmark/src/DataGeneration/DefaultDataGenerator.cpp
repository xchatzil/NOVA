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
#include <Configurations/Coordinator/SchemaType.hpp>
#include <DataGeneration/DefaultDataGenerator.hpp>
#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <random>

namespace NES::Benchmark::DataGeneration {
DefaultDataGenerator::DefaultDataGenerator(uint64_t minValue, uint64_t maxValue)
    : DataGenerator(), minValue(minValue), maxValue(maxValue) {}

std::vector<Runtime::TupleBuffer> DefaultDataGenerator::createData(size_t numberOfBuffers, size_t bufferSize) {
    std::vector<Runtime::TupleBuffer> createdBuffers;
    createdBuffers.reserve(numberOfBuffers);

    auto memoryLayout = this->getMemoryLayout(bufferSize);
    NES_INFO("Default source mode");

    // Prints every five percent the current progress
    uint64_t noTuplesInFivePercent = std::max(1UL, (numberOfBuffers * 5) / 100);
    for (uint64_t curBuffer = 0; curBuffer < numberOfBuffers; ++curBuffer) {

        Runtime::TupleBuffer bufferRef = allocateBuffer();
        auto testBuffer = Runtime::MemoryLayouts::TestTupleBuffer(memoryLayout, bufferRef);

        // using seed to generate a predictable sequence of values for deterministic behavior
        std::mt19937 generator(GENERATOR_SEED_DEFAULT);
        std::uniform_int_distribution<uint64_t> uniformIntDistribution(minValue, maxValue);

        /* This branch is solely for performance reasons.
             It still works with all layouts, for a RowLayout it is just magnitudes faster with this branch */
        if (memoryLayout->getSchema()->getLayoutType() == Schema::MemoryLayoutType::ROW_LAYOUT) {
            auto rowLayout = Runtime::MemoryLayouts::RowLayout::create(memoryLayout->getSchema(), bufferSize);
            auto testBuffer = std::make_unique<Runtime::MemoryLayouts::TestTupleBuffer>(rowLayout, bufferRef);

            for (uint64_t curRecord = 0; curRecord < testBuffer->getCapacity(); ++curRecord) {
                uint64_t value = uniformIntDistribution(generator);
                testBuffer->pushRecordToBuffer(
                    std::tuple<uint64_t, uint64_t, uint64_t, uint64_t>(curRecord, value, curRecord, curRecord));
            }

        } else {
            for (uint64_t curRecord = 0; curRecord < testBuffer.getCapacity(); ++curRecord) {
                auto value = uniformIntDistribution(generator);
                testBuffer[curRecord]["id"].write<uint64_t>(curRecord);
                testBuffer[curRecord]["value"].write<uint64_t>(value);
                testBuffer[curRecord]["payload"].write<uint64_t>(curRecord);
                testBuffer[curRecord]["timestamp"].write<uint64_t>(curRecord);
            }
        }

        if (curBuffer % noTuplesInFivePercent == 0) {
            NES_INFO("DefaultDataGenerator: currently at {}%", (((double) curBuffer / numberOfBuffers) * 100));
        }

        testBuffer.setNumberOfTuples(testBuffer.getCapacity());
        createdBuffers.emplace_back(bufferRef);
    }

    NES_INFO("Created all buffers!");
    return createdBuffers;
}

NES::SchemaPtr DefaultDataGenerator::getSchema() {
    return Schema::create()
        ->addField(createField("id", BasicType::UINT64))
        ->addField(createField("value", BasicType::UINT64))
        ->addField(createField("payload", BasicType::UINT64))
        ->addField(createField("timestamp", BasicType::UINT64));
}

Configurations::SchemaTypePtr DefaultDataGenerator::getSchemaType() {
    const char* dataType = "UINT64";
    std::vector<Configurations::SchemaFieldDetail> schemaFieldDetails;
    schemaFieldDetails.emplace_back("id", dataType);
    schemaFieldDetails.emplace_back("value", dataType);
    schemaFieldDetails.emplace_back("payload", dataType);
    schemaFieldDetails.emplace_back("timestamp", dataType);
    return Configurations::SchemaType::create(schemaFieldDetails);
}

std::string DefaultDataGenerator::getName() { return "Uniform"; }

std::string DefaultDataGenerator::toString() {
    std::ostringstream oss;

    oss << getName() << " (" << minValue << ", " << maxValue << ")";

    return oss.str();
}

}// namespace NES::Benchmark::DataGeneration
