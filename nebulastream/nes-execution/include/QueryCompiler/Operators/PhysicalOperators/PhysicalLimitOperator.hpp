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
#ifndef NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALLIMITOPERATOR_HPP_
#define NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALLIMITOPERATOR_HPP_

#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators {

/**
 * @brief Physical limit operator.
 */
class PhysicalLimitOperator : public PhysicalUnaryOperator {
  public:
    PhysicalLimitOperator(OperatorId id, StatisticId statisticId, SchemaPtr inputSchema, SchemaPtr outputSchema, uint64_t limit);
    static PhysicalOperatorPtr
    create(OperatorId id, StatisticId statisticId, const SchemaPtr& inputSchema, const SchemaPtr& outputSchema, uint64_t limit);
    static PhysicalOperatorPtr create(StatisticId statisticId, SchemaPtr inputSchema, SchemaPtr outputSchema, uint64_t limit);
    std::string toString() const override;
    OperatorPtr copy() override;

    /**
   * @brief get the limit.
   * @return limit
   */
    uint64_t getLimit();

  private:
    uint64_t limit;
};

}// namespace NES::QueryCompilation::PhysicalOperators

#endif// NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALLIMITOPERATOR_HPP_
