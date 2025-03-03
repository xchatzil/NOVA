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
#include <Execution/Expressions/Functions/ExecutableFunctionRegistry.hpp>
#include <Execution/Expressions/Functions/RadiansExpression.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <cmath>
using namespace std;
namespace NES::Runtime::Execution::Expressions {

RadiansExpression::RadiansExpression(const NES::Runtime::Execution::Expressions::ExpressionPtr& SubExpression)
    : SubExpression(SubExpression) {}

double calculateRadians(double x) { return (x * M_PI) / 180; }
Value<> RadiansExpression::execute(NES::Nautilus::Record& record) const {
    // Evaluate the sub expression and retrieve the value.
    Value subValue = SubExpression->execute(record);
    //check the type and then call the function.
    if (subValue->isType<Int8>()) {
        // call the calculateMod function with the correct type
        return FunctionCall<>("calculateRadians", calculateRadians, subValue.as<Int8>());
    } else if (subValue->isType<Int16>()) {
        return FunctionCall<>("calculateRadians", calculateRadians, subValue.as<Int16>());
    } else if (subValue->isType<Int32>()) {
        return FunctionCall<>("calculateRadians", calculateRadians, subValue.as<Int32>());
    } else if (subValue->isType<Int64>()) {
        return FunctionCall<>("calculateRadians", calculateRadians, subValue.as<Int64>());
    } else if (subValue->isType<Float>()) {
        return FunctionCall<>("calculateRadians", calculateRadians, subValue.as<Float>());
    } else if (subValue->isType<Double>()) {
        return FunctionCall<>("calculateRadians", calculateRadians, subValue.as<Double>());
    } else {
        // If no type was applicable we throw an exception.
        throw Exceptions::NotImplementedException(
            "This expression is only defined on numeric input arguments that are either Integer or Float.");
    }
}
static ExecutableFunctionRegistry::Add<UnaryFunctionProvider<RadiansExpression>> radiansFunction("radians");
}// namespace NES::Runtime::Execution::Expressions
