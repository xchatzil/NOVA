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
#ifndef NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALEMITOPERATOR_HPP_
#define NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALEMITOPERATOR_HPP_

#include <QueryCompiler/Operators/PhysicalOperators/AbstractEmitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators {

/**
 * @brief Physical Emit operator.
 */
class PhysicalEmitOperator : public PhysicalUnaryOperator, public AbstractEmitOperator {
  public:
    /**
     * @brief Constructor for the physical emit operator
     * @param id operator id
     * @param statisticId: represents the unique identifier of components that we can track statistics for
     * @param inputSchema input schema for the emit operator
     */
    PhysicalEmitOperator(OperatorId id, StatisticId statisticId, const SchemaPtr& inputSchema);

    /**
     * @brief Creates a physical emit operator
     * @param id operator id
     * @param statisticId: represents the unique identifier of components that we can track statistics for
     * @param inputSchema
     * @return PhysicalOperatorPtr
     */
    static PhysicalOperatorPtr create(OperatorId id, StatisticId statisticId, const SchemaPtr& inputSchema);

    /**
     * @brief Creates a physical emit operator
     * @param statisticId: represents the unique identifier of components that we can track statistics for
     * @param inputSchema
     * @return PhysicalOperatorPtr
     */
    static PhysicalOperatorPtr create(StatisticId statisticId, SchemaPtr inputSchema);

    std::string toString() const override;

    OperatorPtr copy() override;
};
}// namespace NES::QueryCompilation::PhysicalOperators

#endif// NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALEMITOPERATOR_HPP_
