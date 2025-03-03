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
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalBinaryOperator.hpp>
#include <sstream>
#include <utility>

namespace NES::QueryCompilation::PhysicalOperators {

PhysicalBinaryOperator::PhysicalBinaryOperator(OperatorId id,
                                               StatisticId statisticId,
                                               SchemaPtr leftSchema,
                                               SchemaPtr rightSchema,
                                               SchemaPtr outputSchema)
    : Operator(id, statisticId), PhysicalOperator(id, statisticId), BinaryOperator(id) {
    BinaryOperator::setLeftInputSchema(std::move(leftSchema));
    BinaryOperator::setRightInputSchema(std::move(rightSchema));
    BinaryOperator::setOutputSchema(std::move(outputSchema));
}

std::string PhysicalBinaryOperator::toString() const {
    std::stringstream out;
    out << BinaryOperator::toString();
    return out.str();
}

}// namespace NES::QueryCompilation::PhysicalOperators
