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
#ifndef NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_JOINING_PHYSICALJOINSINKOPERATOR_HPP_
#define NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_JOINING_PHYSICALJOINSINKOPERATOR_HPP_
#include <QueryCompiler/Operators/PhysicalOperators/AbstractScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalJoinOperator.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>

namespace NES::QueryCompilation::PhysicalOperators {
/**
 * @brief Physical operator for the join sink.
 * This operator queryIdAndCatalogEntryMapping the operator state and computes final join results.
 */
class PhysicalJoinSinkOperator : public PhysicalJoinOperator, public PhysicalBinaryOperator, public AbstractScanOperator {
  public:
    static PhysicalOperatorPtr create(OperatorId id,
                                      StatisticId statisticId,
                                      const SchemaPtr& leftInputSchema,
                                      const SchemaPtr& rightInputSchema,
                                      const SchemaPtr& outputSchema,
                                      const Join::JoinOperatorHandlerPtr& operatorHandler);
    static PhysicalOperatorPtr create(StatisticId statisticId,
                                      const SchemaPtr& leftInputSchema,
                                      const SchemaPtr& rightInputSchema,
                                      const SchemaPtr& outputSchema,
                                      const Join::JoinOperatorHandlerPtr& operatorHandler);
    PhysicalJoinSinkOperator(OperatorId id,
                             StatisticId statisticId,
                             SchemaPtr leftInputSchema,
                             SchemaPtr rightInputSchema,
                             SchemaPtr outputSchema,
                             Join::JoinOperatorHandlerPtr operatorHandler);
    [[nodiscard]] std::string toString() const override;
    OperatorPtr copy() override;
};
}// namespace NES::QueryCompilation::PhysicalOperators

#endif// NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_JOINING_PHYSICALJOINSINKOPERATOR_HPP_
