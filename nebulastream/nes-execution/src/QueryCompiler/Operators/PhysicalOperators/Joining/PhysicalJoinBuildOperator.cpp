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
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalJoinBuildOperator.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <sstream>
#include <utility>

namespace NES::QueryCompilation::PhysicalOperators {

PhysicalOperatorPtr PhysicalJoinBuildOperator::create(StatisticId statisticId,
                                                      const SchemaPtr& inputSchema,
                                                      const SchemaPtr& outputSchema,
                                                      const Join::JoinOperatorHandlerPtr& operatorHandler,
                                                      JoinBuildSideType buildSide) {
    return create(getNextOperatorId(), statisticId, inputSchema, outputSchema, operatorHandler, buildSide);
}

PhysicalOperatorPtr PhysicalJoinBuildOperator::create(OperatorId id,
                                                      StatisticId statisticId,
                                                      const SchemaPtr& inputSchema,
                                                      const SchemaPtr& outputSchema,
                                                      const Join::JoinOperatorHandlerPtr& operatorHandler,
                                                      JoinBuildSideType buildSide) {
    return std::make_shared<PhysicalJoinBuildOperator>(id, statisticId, inputSchema, outputSchema, operatorHandler, buildSide);
}

PhysicalJoinBuildOperator::PhysicalJoinBuildOperator(OperatorId id,
                                                     StatisticId statisticId,
                                                     SchemaPtr inputSchema,
                                                     SchemaPtr outputSchema,
                                                     Join::JoinOperatorHandlerPtr operatorHandler,
                                                     JoinBuildSideType buildSide)
    : Operator(id), PhysicalJoinOperator(std::move(operatorHandler)),
      PhysicalUnaryOperator(id, statisticId, std::move(inputSchema), std::move(outputSchema)), joinBuildSide(buildSide){};

std::string PhysicalJoinBuildOperator::toString() const {
    std::stringstream out;
    out << std::endl;
    out << "PhysicalJoinBuildOperator:\n";
    out << PhysicalUnaryOperator::toString();
    out << "joinBuildSide: " << magic_enum::enum_name(joinBuildSide);
    out << std::endl;
    return out.str();
}

OperatorPtr PhysicalJoinBuildOperator::copy() {
    auto result = create(id, statisticId, inputSchema, outputSchema, operatorHandler, joinBuildSide);
    result->addAllProperties(properties);
    return result;
}

JoinBuildSideType PhysicalJoinBuildOperator::getBuildSide() { return joinBuildSide; }

}// namespace NES::QueryCompilation::PhysicalOperators
