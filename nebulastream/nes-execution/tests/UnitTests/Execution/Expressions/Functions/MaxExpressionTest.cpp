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

#include <BaseIntegrationTest.hpp>
#include <Execution/Expressions/Functions/MaxExpression.hpp>
#include <TestUtils/ExpressionWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>
namespace NES::Runtime::Execution::Expressions {

class MaxExpressionTest : public Testing::BaseUnitTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MaxExpressionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup MaxExpressionTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down MaxExpressionTest test class."); }
};

TEST_F(MaxExpressionTest, evaluateMaxExpressionInteger) {
    auto expression = BinaryExpressionWrapper<MaxExpression>();
    // Int8
    {
        auto resultValue = expression.eval(Value<Int8>(17_s8), Value<Int8>(4_s8));
        ASSERT_EQ(resultValue, (float) 17);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }

    // Int16
    {
        auto resultValue = expression.eval(Value<Int16>(17_s16), Value<Int16>(4_s16));
        ASSERT_EQ(resultValue, (float) 17);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }

    // Int32
    {
        auto resultValue = expression.eval(Value<Int32>(17_s32), Value<Int32>(4_s32));
        ASSERT_EQ(resultValue, (float) 17);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Int64
    {
        auto resultValue = expression.eval(Value<Int64>(17_s64), Value<Int64>(4_s64));
        ASSERT_EQ(resultValue, (float) 17);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }

    // UInt8
    {
        auto resultValue = expression.eval(Value<UInt8>(17_u8), Value<UInt8>(4_u8));
        ASSERT_EQ(resultValue, (float) 17);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }

    // UInt16
    {
        auto resultValue = expression.eval(Value<UInt16>(17_u16), Value<UInt16>(4_u16));
        ASSERT_EQ(resultValue, (float) 17);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }

    // UInt32
    {
        auto resultValue = expression.eval(Value<UInt32>(17_u32), Value<UInt32>(4_u32));
        ASSERT_EQ(resultValue, (float) 17);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // UInt64
    {
        auto resultValue = expression.eval(Value<UInt64>(17_u64), Value<UInt64>(4_u64));
        ASSERT_EQ(resultValue, (float) 17);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}

TEST_F(MaxExpressionTest, evaluateMaxExpressionFloat) {
    auto expression = BinaryExpressionWrapper<MaxExpression>();
    // Float
    {
        auto resultValue = expression.eval(Value<Float>((float) 17), Value<Float>((float) 4));
        ASSERT_EQ(resultValue, (float) 17);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Double
    {
        auto resultValue = expression.eval(Value<Double>((double) 17), Value<Double>((double) 4));
        ASSERT_EQ(resultValue, (float) 17);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}

/**
 * @brief If we execute the expression on a boolean it should throw an exception.
 */
TEST_F(MaxExpressionTest, evaluateMaxExpressionOnWrongType) {
    auto expression = BinaryExpressionWrapper<MaxExpression>();
    ASSERT_ANY_THROW(expression.eval(Value<Boolean>(true), Value<Boolean>(false)););
}

}// namespace NES::Runtime::Execution::Expressions
