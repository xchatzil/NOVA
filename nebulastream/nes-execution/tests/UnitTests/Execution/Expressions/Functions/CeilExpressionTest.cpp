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
#include <Execution/Expressions/Functions/CeilExpression.hpp>
#include <TestUtils/ExpressionWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Expressions {

class CeilExpressionTest : public Testing::BaseUnitTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("CeilExpressionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup CeilExpressionTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down CeilExpressionTest test class."); }
};

TEST_F(CeilExpressionTest, evaluateCeilExpressionFloat) {
    auto expression = UnaryExpressionWrapper<CeilExpression>();
    // Double
    {
        auto resultValue = expression.eval(Value<Double>((double) 4.3));
        ASSERT_EQ(resultValue, (float) 5);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Float
    {
        auto resultValue = expression.eval(Value<Float>((float) 4.8));
        ASSERT_EQ(resultValue, (float) 5);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}

/**
* @brief If we execute the expression on a boolean it should throw an exception.
*/
TEST_F(CeilExpressionTest, evaluateCeilExpressionOnWrongType) {
    auto expression = UnaryExpressionWrapper<CeilExpression>();
    ASSERT_ANY_THROW(expression.eval(Value<Boolean>(true)););
}

}// namespace NES::Runtime::Execution::Expressions
