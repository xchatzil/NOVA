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
#include <BaseIntegrationTest.hpp>
#include <Common/ExecutableType/Array.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/MemoryLayout/RowLayoutField.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <cstdlib>
#include <gtest/gtest.h>
#include <iostream>
#include <vector>

namespace NES::Runtime::MemoryLayouts {
class RowMemoryLayoutTest : public Testing::BaseUnitTest {
  public:
    BufferManagerPtr bufferManager;
    static void SetUpTestCase() {
        NES::Logger::setupLogging("RowMemoryLayoutTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup RowMemoryLayoutTest test class.");
    }
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        bufferManager = std::make_shared<BufferManager>(4096, 10);
    }
};

/**
 * @brief Tests that we can construct a column layout.
 */
TEST_F(RowMemoryLayoutTest, rowLayoutCreateTest) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);

    RowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(rowLayout, nullptr);
}

/**
 * @brief Tests that the field offsets are are calculated correctly using a TestTupleBuffer.
 */
TEST_F(RowMemoryLayoutTest, rowLayoutMapCalcOffsetTest) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    RowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    auto testBuffer = std::make_unique<Runtime::MemoryLayouts::TestTupleBuffer>(rowLayout, tupleBuffer);

    ASSERT_EQ(testBuffer->getCapacity(), tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());
    ASSERT_EQ(testBuffer->getNumberOfTuples(), 0u);
    ASSERT_EQ(rowLayout->getFieldOffset(1, 2), schema->getSchemaSizeInBytes() * 1 + (1 + 2));
    ASSERT_EQ(rowLayout->getFieldOffset(4, 0), schema->getSchemaSizeInBytes() * 4 + 0);
}

/**
 * @brief Tests that we can write a single record to and read from a TestTupleBuffer correctly.
 */
TEST_F(RowMemoryLayoutTest, rowLayoutPushRecordAndReadRecordTestOneRecord) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    RowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    auto testBuffer = std::make_unique<Runtime::MemoryLayouts::TestTupleBuffer>(rowLayout, tupleBuffer);

    std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(1, 2, 3);
    testBuffer->pushRecordToBuffer(writeRecord);

    std::tuple<uint8_t, uint16_t, uint32_t> readRecord = testBuffer->readRecordFromBuffer<uint8_t, uint16_t, uint32_t>(0);

    ASSERT_EQ(writeRecord, readRecord);
    ASSERT_EQ(testBuffer->getNumberOfTuples(), 1UL);
}

/**
 * @brief Tests that we can write many records to and read from a TestTupleBuffer correctly.
 */
TEST_F(RowMemoryLayoutTest, rowLayoutPushRecordAndReadRecordTestMultipleRecord) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    RowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    auto testBuffer = std::make_unique<Runtime::MemoryLayouts::TestTupleBuffer>(rowLayout, tupleBuffer);

    size_t NUM_TUPLES = 230;//tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes();

    std::vector<std::tuple<uint8_t, uint16_t, uint32_t>> allTuples;
    for (size_t i = 0; i < NUM_TUPLES; i++) {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
        allTuples.emplace_back(writeRecord);
        testBuffer->pushRecordToBuffer(writeRecord);
    }

    for (size_t i = 0; i < NUM_TUPLES; i++) {
        std::tuple<uint8_t, uint16_t, uint32_t> readRecord = testBuffer->readRecordFromBuffer<uint8_t, uint16_t, uint32_t>(i);
        ASSERT_EQ(allTuples[i], readRecord);
    }

    ASSERT_EQ(testBuffer->getNumberOfTuples(), NUM_TUPLES);
}

/**
 * @brief Tests that we can access fields of a TupleBuffer that is used in a TestTupleBuffer correctly.
 */
TEST_F(RowMemoryLayoutTest, rowLayoutLayoutFieldSimple) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    RowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    auto testBuffer = std::make_unique<Runtime::MemoryLayouts::TestTupleBuffer>(rowLayout, tupleBuffer);

    size_t NUM_TUPLES = tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes();

    std::vector<std::tuple<uint8_t, uint16_t, uint32_t>> allTuples;
    for (size_t i = 0; i < NUM_TUPLES; ++i) {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
        allTuples.emplace_back(writeRecord);
        testBuffer->pushRecordToBuffer(writeRecord);
    }

    auto field0 = RowLayoutField<uint8_t, true>::create(0, rowLayout, tupleBuffer);
    auto field1 = RowLayoutField<uint16_t, true>::create(1, rowLayout, tupleBuffer);
    auto field2 = RowLayoutField<uint32_t, true>::create(2, rowLayout, tupleBuffer);

    for (size_t i = 0; i < NUM_TUPLES; ++i) {
        ASSERT_EQ(std::get<0>(allTuples[i]), field0[i]);
        ASSERT_EQ(std::get<1>(allTuples[i]), field1[i]);
        ASSERT_EQ(std::get<2>(allTuples[i]), field2[i]);
    }
}

/**
 * @brief Tests whether whether an error is thrown if we try to access non-existing fields of a TupleBuffer.
 */
TEST_F(RowMemoryLayoutTest, rowLayoutLayoutFieldBoundaryCheck) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    RowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    auto testBuffer = std::make_unique<Runtime::MemoryLayouts::TestTupleBuffer>(rowLayout, tupleBuffer);

    size_t NUM_TUPLES = tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes();

    std::vector<std::tuple<uint8_t, uint16_t, uint32_t>> allTuples;
    for (size_t i = 0; i < NUM_TUPLES; ++i) {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
        allTuples.emplace_back(writeRecord);
        testBuffer->pushRecordToBuffer(writeRecord);
    }

    auto field0 = RowLayoutField<uint8_t, true>::create(0, rowLayout, tupleBuffer);
    auto field1 = RowLayoutField<uint16_t, true>::create(1, rowLayout, tupleBuffer);
    auto field2 = RowLayoutField<uint32_t, true>::create(2, rowLayout, tupleBuffer);

    ASSERT_THROW((RowLayoutField<uint32_t, true>::create(3, rowLayout, tupleBuffer)), NES::Exceptions::RuntimeException);
    ASSERT_THROW((RowLayoutField<uint32_t, true>::create(4, rowLayout, tupleBuffer)), NES::Exceptions::RuntimeException);
    ASSERT_THROW((RowLayoutField<uint32_t, true>::create(5, rowLayout, tupleBuffer)), NES::Exceptions::RuntimeException);

    size_t i = 0;
    for (; i < NUM_TUPLES; ++i) {
        ASSERT_EQ(std::get<0>(allTuples[i]), field0[i]);
        ASSERT_EQ(std::get<1>(allTuples[i]), field1[i]);
        ASSERT_EQ(std::get<2>(allTuples[i]), field2[i]);
    }
    ASSERT_THROW(field0[i], NES::Exceptions::RuntimeException);
    ASSERT_THROW(field1[i], NES::Exceptions::RuntimeException);
    ASSERT_THROW(field2[i], NES::Exceptions::RuntimeException);

    ASSERT_THROW(field0[++i], NES::Exceptions::RuntimeException);
    ASSERT_THROW(field1[i], NES::Exceptions::RuntimeException);
    ASSERT_THROW(field2[i], NES::Exceptions::RuntimeException);
}

/**
 * @brief Tests whether we can only access the correct fields.
 */
TEST_F(RowMemoryLayoutTest, getFieldViaFieldNameRowLayout) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    RowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    ASSERT_NO_THROW((RowLayoutField<uint8_t, true>::create("t1", rowLayout, tupleBuffer)));
    ASSERT_NO_THROW((RowLayoutField<uint16_t, true>::create("t2", rowLayout, tupleBuffer)));
    ASSERT_NO_THROW((RowLayoutField<uint32_t, true>::create("t3", rowLayout, tupleBuffer)));

    ASSERT_THROW((RowLayoutField<uint32_t, true>::create("t4", rowLayout, tupleBuffer)), NES::Exceptions::RuntimeException);
    ASSERT_THROW((RowLayoutField<uint32_t, true>::create("t5", rowLayout, tupleBuffer)), NES::Exceptions::RuntimeException);
    ASSERT_THROW((RowLayoutField<uint32_t, true>::create("t6", rowLayout, tupleBuffer)), NES::Exceptions::RuntimeException);
}

/**
 * @brief Tests if an error is thrown if more tuples are added to a TupleBuffer than the TupleBuffer can store.
 */
TEST_F(RowMemoryLayoutTest, pushRecordTooManyRecordsRowLayout) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    RowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    auto testBuffer = std::make_unique<Runtime::MemoryLayouts::TestTupleBuffer>(rowLayout, tupleBuffer);

    size_t NUM_TUPLES = tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes();

    std::vector<std::tuple<uint8_t, uint16_t, uint32_t>> allTuples;
    size_t i = 0;
    for (; i < NUM_TUPLES; i++) {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
        allTuples.emplace_back(writeRecord);
        testBuffer->pushRecordToBuffer(writeRecord);
    }

    for (; i < NUM_TUPLES + 1; i++) {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
        allTuples.emplace_back(writeRecord);
        ASSERT_THROW(testBuffer->pushRecordToBuffer(writeRecord), NES::Exceptions::RuntimeException);
    }

    for (size_t i = 0; i < NUM_TUPLES; i++) {
        std::tuple<uint8_t, uint16_t, uint32_t> readRecord = testBuffer->readRecordFromBuffer<uint8_t, uint16_t, uint32_t>(i);
        ASSERT_EQ(allTuples[i], readRecord);
    }

    ASSERT_EQ(testBuffer->getNumberOfTuples(), NUM_TUPLES);
}

}// namespace NES::Runtime::MemoryLayouts
