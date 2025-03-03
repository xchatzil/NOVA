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

#include <Exceptions/NotImplementedException.hpp>
#include <Execution/Expressions/Functions/AtanExpression.hpp>
#include <Execution/Expressions/Functions/ExecutableFunctionRegistry.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <cmath>
namespace NES::Runtime::Execution::Expressions {

AtanExpression::AtanExpression(const NES::Runtime::Execution::Expressions::ExpressionPtr& leftSubExpression)
    : leftSubExpression(leftSubExpression) {}

/**
 * @brief This method calculates the atan of x.
 * This function is basically a wrapper for std::atan and enables us to use it in our execution engine framework.
 * @param x double
 * @return double
 */
double calculateAtanDouble(double x) { return std::atan(x); }
double calculateAtanFloat(float x) { return std::atan(x); }

Value<> AtanExpression::execute(NES::Nautilus::Record& record) const {
    // Evaluate the left sub expression and retrieve the value.
    Value leftValue = leftSubExpression->execute(record);
    if (leftValue->isType<Float>()) {
        return FunctionCall<>("calculateAtanFloat", calculateAtanFloat, leftValue.as<Float>());
    } else if (leftValue->isType<Double>()) {
        return FunctionCall<>("calculateAtanDouble", calculateAtanDouble, leftValue.as<Double>());
    } else {
        // If no type was applicable we throw an exception.
        throw Exceptions::NotImplementedException(
            "This expression is only defined on numeric input arguments that are either Double or Float.");
    }
}
static ExecutableFunctionRegistry::Add<UnaryFunctionProvider<AtanExpression>> atanFunction("atan");
}// namespace NES::Runtime::Execution::Expressions
