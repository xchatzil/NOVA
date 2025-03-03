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

#include <API/Expressions/Expressions.hpp>
#include <BaseIntegrationTest.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <limits>

using namespace std;

namespace NES {

class DataTypeFactoryTests : public Testing::BaseUnitTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("DataTypeFactory.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup DataTypeFactory test class.");
    }
};

/**
 * @brief Input Query Plan:
 * Input:
 * | Physical Source |
 *
 * Expected Result:
 * | Physical Source |
 *
 */
TEST_F(DataTypeFactoryTests, stampModificationTest) {
    // increase lower bound
    {
        auto stamp = DataTypeFactory::copyTypeAndIncreaseLowerBound(DataTypeFactory::createInt32(), -200.0);
        stamp = DataTypeFactory::copyTypeAndIncreaseLowerBound(stamp, int64_t(-100L));
        ASSERT_TRUE(stamp->isInteger());
        const auto intStamp = DataType::as<Integer>(stamp);
        ASSERT_EQ(intStamp->getBits(), 32);
        ASSERT_EQ(intStamp->lowerBound, -100);
        ASSERT_EQ(intStamp->upperBound, INT32_MAX);
    }
    {
        auto stamp = DataTypeFactory::copyTypeAndIncreaseLowerBound(DataTypeFactory::createFloat(), -200.0);
        stamp = DataTypeFactory::copyTypeAndIncreaseLowerBound(stamp, int64_t(-100L));
        ASSERT_TRUE(stamp->isFloat());
        const auto floatStamp = DataType::as<Float>(stamp);
        ASSERT_EQ(floatStamp->getBits(), 32);
        ASSERT_EQ(floatStamp->lowerBound, -100);
        ASSERT_EQ(floatStamp->upperBound, std::numeric_limits<float>::max());
    }

    // decrease upper bound
    {
        auto stamp = DataTypeFactory::copyTypeAndDecreaseUpperBound(DataTypeFactory::createInt32(), 200.0);
        stamp = DataTypeFactory::copyTypeAndDecreaseUpperBound(stamp, int64_t(100L));
        ASSERT_TRUE(stamp->isInteger());
        const auto intStamp = DataType::as<Integer>(stamp);
        ASSERT_EQ(intStamp->getBits(), 32);
        ASSERT_EQ(intStamp->lowerBound, INT32_MIN);
        ASSERT_EQ(intStamp->upperBound, 100);
    }
    {
        auto stamp = DataTypeFactory::copyTypeAndDecreaseUpperBound(DataTypeFactory::createFloat(), 200.0);
        stamp = DataTypeFactory::copyTypeAndDecreaseUpperBound(stamp, int64_t(100L));
        ASSERT_TRUE(stamp->isFloat());
        const auto floatStamp = DataType::as<Float>(stamp);
        ASSERT_EQ(floatStamp->getBits(), 32);
        ASSERT_EQ(floatStamp->lowerBound, std::numeric_limits<float>::lowest());
        ASSERT_EQ(floatStamp->upperBound, 100);
    }

    // tighten both bounds at once
    // test behavior of integer stamp
    {
        auto stamp = DataTypeFactory::copyTypeAndTightenBounds(DataTypeFactory::createInt32(), int64_t(-100), int64_t(100));
        ASSERT_TRUE(stamp->isInteger());
        const auto intStamp = DataType::as<Integer>(stamp);
        ASSERT_EQ(intStamp->getBits(), 32);
        ASSERT_EQ(intStamp->lowerBound, -100);
        ASSERT_EQ(intStamp->upperBound, 100);
    }
    {
        auto stamp = DataTypeFactory::copyTypeAndTightenBounds(DataTypeFactory::createInt32(), -100.0, 100.0);
        ASSERT_TRUE(stamp->isInteger());
        const auto intStamp = DataType::as<Integer>(stamp);
        ASSERT_EQ(intStamp->getBits(), 32);
        ASSERT_EQ(intStamp->lowerBound, -100);
        ASSERT_EQ(intStamp->upperBound, 100);
    }
    {
        auto stamp = DataTypeFactory::copyTypeAndTightenBounds(DataTypeFactory::createInt32(), -99.9, 99.9);
        ASSERT_TRUE(stamp->isInteger());
        const auto intStamp = DataType::as<Integer>(stamp);
        ASSERT_EQ(intStamp->getBits(), 32);
        ASSERT_EQ(intStamp->lowerBound, -100);
        ASSERT_EQ(intStamp->upperBound, 100);
    }
    {
        // check if calls where only one of two bounds get tightened work
        auto stamp = DataTypeFactory::createInt32();
        ASSERT_TRUE(stamp->isInteger());
        stamp = DataTypeFactory::copyTypeAndTightenBounds(stamp, int64_t(-100), int64_t(100));
        stamp = DataTypeFactory::copyTypeAndTightenBounds(stamp, int64_t(-200LL), int64_t(99LL));
        stamp = DataTypeFactory::copyTypeAndTightenBounds(stamp, int64_t(-99LL), int64_t(200LL));
        ASSERT_TRUE(stamp->isInteger());
        const auto intStamp = DataType::as<Integer>(stamp);
        ASSERT_EQ(intStamp->getBits(), 32);
        ASSERT_EQ(intStamp->lowerBound, -99);
        ASSERT_EQ(intStamp->upperBound, 99);
    }

    // test behaviour of float stamp
    {
        auto stamp = DataTypeFactory::copyTypeAndTightenBounds(DataTypeFactory::createFloat(), -100.5, 100.5);
        ASSERT_TRUE(stamp->isFloat());
        const auto floatStamp = DataType::as<Float>(stamp);
        ASSERT_EQ(floatStamp->getBits(), 32);
        ASSERT_EQ(floatStamp->lowerBound, -100.5);
        ASSERT_EQ(floatStamp->upperBound, 100.5);
    }
    {
        auto stamp = DataTypeFactory::copyTypeAndTightenBounds(DataTypeFactory::createFloat(), int64_t(-100), int64_t(100));
        ASSERT_TRUE(stamp->isFloat());
        const auto floatStamp = DataType::as<Float>(stamp);
        ASSERT_EQ(floatStamp->getBits(), 32);
        ASSERT_EQ(floatStamp->lowerBound, -100);
        ASSERT_EQ(floatStamp->upperBound, 100);
    }
    {
        // check if calls where only one of two bounds get tightened works
        auto stamp = DataTypeFactory::createFloat();
        stamp = DataTypeFactory::copyTypeAndTightenBounds(stamp, int64_t(-100), int64_t(100));
        stamp = DataTypeFactory::copyTypeAndTightenBounds(stamp, -200, 99.9);
        stamp = DataTypeFactory::copyTypeAndTightenBounds(stamp, -99.9, 200);
        ASSERT_TRUE(stamp->isFloat());
        const auto floatStamp = DataType::as<Float>(stamp);
        ASSERT_EQ(floatStamp->getBits(), 32);
        ASSERT_EQ(floatStamp->lowerBound, -99.9);
        ASSERT_EQ(floatStamp->upperBound, 99.9);
    }

    // conversion of Integer to Stamp
    {
        auto const intStamp = DataTypeFactory::createInteger(-100, 100);
        auto const floatStamp = DataType::as<Float>(DataTypeFactory::createFloatFromInteger(intStamp));
        ASSERT_EQ(floatStamp->lowerBound, -100.0);
        ASSERT_EQ(floatStamp->upperBound, 100.0);
    }
}

}// namespace NES
