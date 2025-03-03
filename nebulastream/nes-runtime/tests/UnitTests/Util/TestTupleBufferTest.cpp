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

#include "Common/DataTypes/DataTypeFactory.hpp"
#include <API/Schema.hpp>
#include <BaseIntegrationTest.hpp>
#include <Common/ExecutableType/Array.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <Util/magicenum/magic_enum.hpp>

namespace NES::Runtime::MemoryLayouts {

#define VAR_SIZED_DATA_TYPES uint16_t, std::string, double, std::string
#define FIXED_SIZED_DATA_TYPES uint16_t, bool, double

using VarSizeDataTuple = std::tuple<VAR_SIZED_DATA_TYPES>;
using FixedSizedDataTuple = std::tuple<FIXED_SIZED_DATA_TYPES>;

class TestTupleBufferTest : public Testing::BaseUnitTest, public testing::WithParamInterface<Schema::MemoryLayoutType> {
  public:
    BufferManagerPtr bufferManager;
    SchemaPtr schema, varSizedDataSchema;
    std::unique_ptr<TestTupleBuffer> testBuffer, testBufferVarSize;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("TestTupleBufferTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup TestTupleBufferTest test class.");
    }
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        const auto memoryLayout = GetParam();
        bufferManager = std::make_shared<BufferManager>(4096, 10);
        schema = Schema::create(memoryLayout)
                     ->addField("test$t1", BasicType::UINT16)
                     ->addField("test$t2", BasicType::BOOLEAN)
                     ->addField("test$t3", BasicType::FLOAT64);

        varSizedDataSchema = Schema::create(memoryLayout)
                                 ->addField("test$t1", BasicType::UINT16)
                                 ->addField("test$t2", DataTypeFactory::createText())
                                 ->addField("test$t3", BasicType::FLOAT64)
                                 ->addField("test$t4", DataTypeFactory::createText());

        auto tupleBuffer = bufferManager->getBufferBlocking();
        auto tupleBufferVarSizedData = bufferManager->getBufferBlocking();

        testBuffer = std::make_unique<TestTupleBuffer>(TestTupleBuffer::createTestTupleBuffer(tupleBuffer, schema));
        testBufferVarSize = std::make_unique<TestTupleBuffer>(
            TestTupleBuffer::createTestTupleBuffer(tupleBufferVarSizedData, varSizedDataSchema));
    }
};

TEST_P(TestTupleBufferTest, readWritetestBufferTest) {
    // Reading and writing a full tuple
    for (int i = 0; i < 10; ++i) {
        auto testTuple = std::make_tuple((uint16_t) i, true, i * 2.0);
        testBuffer->pushRecordToBuffer(testTuple);
        ASSERT_EQ((testBuffer->readRecordFromBuffer<FIXED_SIZED_DATA_TYPES>(i)), testTuple);
    }

    // Reading and writing a full tuple via DynamicTuple and DynamicField
    for (auto i = 0; i < 10; ++i) {
        (*testBuffer)[i]["test$t1"].write<uint16_t>(i);
        (*testBuffer)[i]["test$t2"].write<bool>(i % 2);
        (*testBuffer)[i]["test$t3"].write<double_t>(i * 42.0);

        ASSERT_EQ((*testBuffer)[i]["test$t1"].read<uint16_t>(), i);
        ASSERT_EQ((*testBuffer)[i]["test$t2"].read<bool>(), i % 2);
        ASSERT_EQ((*testBuffer)[i]["test$t3"].read<double_t>(), i * 42.0);
    }
}

TEST_P(TestTupleBufferTest, readWritetestBufferTestVarSizeData) {
    for (int i = 0; i < 10; ++i) {
        auto testTuple = std::make_tuple((uint16_t) i, "" + std::to_string(i) + std::to_string(i), i * 2.0, std::to_string(i));
        testBufferVarSize->pushRecordToBuffer(testTuple, bufferManager.get());
        ASSERT_EQ((testBufferVarSize->readRecordFromBuffer<VAR_SIZED_DATA_TYPES>(i)), testTuple);
    }

    // Reading and writing a full tuple via DynamicTuple and DynamicField
    for (auto i = 0_u64; i < 10; ++i) {
        (*testBufferVarSize)[i]["test$t1"].write<uint16_t>(i);
        (*testBufferVarSize)[i]["test$t3"].write<double_t>(i * 42.0);
        (*testBufferVarSize)[i].writeVarSized("test$t2", "" + std::to_string(i) + std::to_string(i), bufferManager.get());
        (*testBufferVarSize)[i].writeVarSized("test$t4", std::to_string(i), bufferManager.get());

        ASSERT_EQ((*testBufferVarSize)[i]["test$t1"].read<uint16_t>(), i);
        ASSERT_EQ((*testBufferVarSize)[i]["test$t3"].read<double_t>(), i * 42.0);
        ASSERT_EQ((*testBufferVarSize)[i].readVarSized("test$t2"), "" + std::to_string(i) + std::to_string(i));
        ASSERT_EQ((*testBufferVarSize)[i].readVarSized("test$t4"), std::to_string(i));
    }
}

TEST_P(TestTupleBufferTest, readWritetestBufferTestFullBuffer) {
    for (auto i = 0_u64; i < testBuffer->getCapacity(); ++i) {
        auto testTuple = std::make_tuple((uint16_t) i, true, i * 2.0);
        testBuffer->pushRecordToBuffer(testTuple);
        ASSERT_EQ((testBuffer->readRecordFromBuffer<FIXED_SIZED_DATA_TYPES>(i)), testTuple);
    }

    // Reading and writing a full tuple via DynamicTuple and DynamicField
    for (auto i = 0_u64; i < testBuffer->getCapacity(); ++i) {
        (*testBuffer)[i]["test$t1"].write<uint16_t>(i);
        (*testBuffer)[i]["test$t2"].write<bool>(i % 2);
        (*testBuffer)[i]["test$t3"].write<double_t>(i * 42.0);

        ASSERT_EQ((*testBuffer)[i]["test$t1"].read<uint16_t>(), i);
        ASSERT_EQ((*testBuffer)[i]["test$t2"].read<bool>(), i % 2);
        ASSERT_EQ((*testBuffer)[i]["test$t3"].read<double_t>(), i * 42.0);
    }
}

TEST_P(TestTupleBufferTest, readWritetestBufferTestFullBufferVarSizeData) {
    for (auto i = 0_u64; i < testBufferVarSize->getCapacity(); ++i) {
        auto testTuple = std::make_tuple((uint16_t) i, "" + std::to_string(i) + std::to_string(i), i * 2.0, std::to_string(i));
        testBufferVarSize->pushRecordToBuffer(testTuple, bufferManager.get());
        ASSERT_EQ((testBufferVarSize->readRecordFromBuffer<VAR_SIZED_DATA_TYPES>(i)), testTuple);
    }

    // Reading and writing a full tuple via DynamicTuple and DynamicField
    for (auto i = 0_u64; i < testBufferVarSize->getCapacity(); ++i) {
        (*testBufferVarSize)[i]["test$t1"].write<uint16_t>(i);
        (*testBufferVarSize)[i]["test$t3"].write<double_t>(i * 42.0);
        (*testBufferVarSize)[i].writeVarSized("test$t2", "" + std::to_string(i) + std::to_string(i), bufferManager.get());
        (*testBufferVarSize)[i].writeVarSized("test$t4", std::to_string(i), bufferManager.get());

        ASSERT_EQ((*testBufferVarSize)[i]["test$t1"].read<uint16_t>(), i);
        ASSERT_EQ((*testBufferVarSize)[i]["test$t3"].read<double_t>(), i * 42.0);
        ASSERT_EQ((*testBufferVarSize)[i].readVarSized("test$t2"), "" + std::to_string(i) + std::to_string(i));
        ASSERT_EQ((*testBufferVarSize)[i].readVarSized("test$t4"), std::to_string(i));
    }
}

TEST_P(TestTupleBufferTest, countOccurrencesTest) {
    struct TupleOccurrences {
        FixedSizedDataTuple tuple;
        uint64_t occurrences;
    };

    std::vector<TupleOccurrences> vec = {{{1, true, rand()}, 5},
                                         {{2, false, rand()}, 6},
                                         {{3, false, rand()}, 20},
                                         {{4, true, rand()}, 5}};

    auto posTuple = 0_u64;
    for (auto item : vec) {
        for (auto i = 0_u64; i < item.occurrences; ++i) {
            testBuffer->pushRecordToBuffer(item.tuple);
            ASSERT_EQ((testBuffer->readRecordFromBuffer<FIXED_SIZED_DATA_TYPES>(testBuffer->getNumberOfTuples() - 1)),
                      item.tuple);
        }

        auto dynamicTuple = (*testBuffer)[posTuple];
        ASSERT_EQ(item.occurrences, testBuffer->countOccurrences(dynamicTuple));
        posTuple += item.occurrences;
    }
}

TEST_P(TestTupleBufferTest, countOccurrencesTestVarSizeData) {
    struct TupleOccurrences {
        VarSizeDataTuple tuple;
        uint64_t occurrences;
    };

    std::vector<TupleOccurrences> vec = {{{1, "true", rand(), "aaaaa"}, 5},
                                         {{2, "false", rand(), "bbbbb"}, 6},
                                         {{4, "true", rand(), "ccccc"}, 20},
                                         {{3, "false", rand(), "ddddd"}, 5}};

    auto posTuple = 0_u64;
    for (auto item : vec) {
        for (auto i = 0_u64; i < item.occurrences; ++i) {
            testBufferVarSize->pushRecordToBuffer(item.tuple, bufferManager.get());
            ASSERT_EQ((testBufferVarSize->readRecordFromBuffer<VAR_SIZED_DATA_TYPES>(testBufferVarSize->getNumberOfTuples() - 1)),
                      item.tuple);
        }

        auto dynamicTuple = (*testBufferVarSize)[posTuple];
        ASSERT_EQ(item.occurrences, testBufferVarSize->countOccurrences(dynamicTuple));
        posTuple += item.occurrences;
    }
}

TEST_P(TestTupleBufferTest, DynamicTupleCompare) {
    std::vector<std::tuple<FIXED_SIZED_DATA_TYPES>> tuples = {
        {1, true, 42}, // 0
        {2, false, 43},// 1
        {1, true, 42}, // 2
        {2, false, 43},// 3

    };

    for (const auto& tuple : tuples) {
        testBuffer->pushRecordToBuffer(tuple);
        ASSERT_EQ((testBuffer->readRecordFromBuffer<FIXED_SIZED_DATA_TYPES>(testBuffer->getNumberOfTuples() - 1)), tuple);
    }

    // Check if the same tuple is equal to itself
    ASSERT_TRUE((*testBuffer)[0] == (*testBuffer)[0]);
    ASSERT_TRUE((*testBuffer)[1] == (*testBuffer)[1]);
    ASSERT_TRUE((*testBuffer)[2] == (*testBuffer)[2]);
    ASSERT_TRUE((*testBuffer)[3] == (*testBuffer)[3]);

    // Check that a tuple is not equal to another tuple that is different
    ASSERT_TRUE((*testBuffer)[0] != (*testBuffer)[1]);
    ASSERT_TRUE((*testBuffer)[1] != (*testBuffer)[2]);
    ASSERT_TRUE((*testBuffer)[2] != (*testBuffer)[3]);
    ASSERT_TRUE((*testBuffer)[3] != (*testBuffer)[0]);

    // Check if tuple have the same values but at different positions
    ASSERT_TRUE((*testBuffer)[0] == (*testBuffer)[2]);
    ASSERT_TRUE((*testBuffer)[1] == (*testBuffer)[3]);
}

TEST_P(TestTupleBufferTest, DynamicTupleCompareVarSizeData) {
    std::vector<std::tuple<VAR_SIZED_DATA_TYPES>> tuples = {
        {1, "true", 42, "aaaaa"}, // 0
        {2, "false", 43, "bbbbb"},// 1
        {1, "true", 42, "aaaaa"}, // 2
        {2, "false", 43, "bbbbb"},// 3
    };

    for (auto tuple : tuples) {
        testBufferVarSize->pushRecordToBuffer(tuple, bufferManager.get());
        ASSERT_EQ((testBufferVarSize->readRecordFromBuffer<VAR_SIZED_DATA_TYPES>(testBufferVarSize->getNumberOfTuples() - 1)),
                  tuple);
    }

    // Check if the same tuple is equal to itself
    ASSERT_TRUE((*testBufferVarSize)[0] == (*testBufferVarSize)[0]);
    ASSERT_TRUE((*testBufferVarSize)[1] == (*testBufferVarSize)[1]);
    ASSERT_TRUE((*testBufferVarSize)[2] == (*testBufferVarSize)[2]);
    ASSERT_TRUE((*testBufferVarSize)[3] == (*testBufferVarSize)[3]);

    // Check that a tuple is not equal to another tuple that is different
    ASSERT_TRUE((*testBufferVarSize)[0] != (*testBufferVarSize)[1]);
    ASSERT_TRUE((*testBufferVarSize)[1] != (*testBufferVarSize)[2]);
    ASSERT_TRUE((*testBufferVarSize)[2] != (*testBufferVarSize)[3]);
    ASSERT_TRUE((*testBufferVarSize)[3] != (*testBufferVarSize)[0]);

    // Check if tuple have the same values but at different positions
    ASSERT_TRUE((*testBufferVarSize)[0] == (*testBufferVarSize)[2]);
    ASSERT_TRUE((*testBufferVarSize)[1] == (*testBufferVarSize)[3]);
}

INSTANTIATE_TEST_CASE_P(TestInputs,
                        TestTupleBufferTest,
                        ::testing::Values(Schema::MemoryLayoutType::COLUMNAR_LAYOUT, Schema::MemoryLayoutType::ROW_LAYOUT),
                        [](const testing::TestParamInfo<TestTupleBufferTest::ParamType>& info) {
                            return std::string(magic_enum::enum_name(info.param));
                        });
}// namespace NES::Runtime::MemoryLayouts
