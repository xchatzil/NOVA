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
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalDemultiplexOperator.hpp>
#include <sstream>
#include <utility>
namespace NES::QueryCompilation::PhysicalOperators {

PhysicalOperatorPtr PhysicalDemultiplexOperator::create(OperatorId id, StatisticId statisticId, const SchemaPtr& inputSchema) {
    return std::make_shared<PhysicalDemultiplexOperator>(id, statisticId, inputSchema);
}
PhysicalOperatorPtr PhysicalDemultiplexOperator::create(StatisticId statisticId, SchemaPtr inputSchema) {
    return create(getNextOperatorId(), statisticId, std::move(inputSchema));
}

PhysicalDemultiplexOperator::PhysicalDemultiplexOperator(OperatorId id, StatisticId statisticId, const SchemaPtr& inputSchema)
    : Operator(id, statisticId), PhysicalUnaryOperator(id, statisticId, inputSchema, inputSchema) {}

std::string PhysicalDemultiplexOperator::toString() const {
    std::stringstream out;
    out << std::endl;
    out << "PhysicalDemultiplexOperator:\n";
    out << PhysicalUnaryOperator::toString();
    return out.str();
}

OperatorPtr PhysicalDemultiplexOperator::copy() { return create(id, statisticId, inputSchema); }

}// namespace NES::QueryCompilation::PhysicalOperators
