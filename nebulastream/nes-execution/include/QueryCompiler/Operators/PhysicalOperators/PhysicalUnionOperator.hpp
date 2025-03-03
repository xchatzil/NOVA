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
#ifndef NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALUNIONOPERATOR_HPP_
#define NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALUNIONOPERATOR_HPP_
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalBinaryOperator.hpp>
namespace NES::QueryCompilation::PhysicalOperators {
/**
 * @brief The physical multiplex operator
 * This operator has multiple upstream operators and forwards it to exactly one down-stream operator.
 * Thus it has multiple child nodes and one parent nodes.
 * Example query plan:
 *
 * DataSource --- OperatorX ---
 *                             \
 *                              --- Multiplex --- OperatorZ --- DataSink
 *                              /
 * DataSource --- OperatorY ---
 *
 */
class PhysicalUnionOperator : public PhysicalBinaryOperator {
  public:
    PhysicalUnionOperator(OperatorId id,
                          StatisticId statisticId,
                          const SchemaPtr& leftSchema,
                          const SchemaPtr& rightSchema,
                          const SchemaPtr& outputSchema);
    static PhysicalOperatorPtr create(OperatorId id,
                                      StatisticId statisticId,
                                      const SchemaPtr& leftSchema,
                                      const SchemaPtr& rightSchema,
                                      const SchemaPtr& outputSchema);
    static PhysicalOperatorPtr create(OperatorId id, StatisticId statisticId, const SchemaPtr& schema);
    static PhysicalOperatorPtr create(StatisticId statisticId, const SchemaPtr& schema);
    std::string toString() const override;
    OperatorPtr copy() override;
};

}// namespace NES::QueryCompilation::PhysicalOperators

#endif// NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALUNIONOPERATOR_HPP_
