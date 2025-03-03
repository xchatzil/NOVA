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
#ifndef NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_PHYSICALSLICEMERGINGOPERATOR_HPP_
#define NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_PHYSICALSLICEMERGINGOPERATOR_HPP_
#include <QueryCompiler/Operators/PhysicalOperators/AbstractEmitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/AbstractScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalWindowOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators {

/**
 * @brief Physical operator for slice merging.
 * This operator receives pre-aggregated slices, e.g., from the slice merger, and merges them in its local operator state.
 */
class PhysicalSliceMergingOperator : public PhysicalWindowOperator, public AbstractEmitOperator, public AbstractScanOperator {
  public:
    PhysicalSliceMergingOperator(OperatorId id,
                                 StatisticId statisticId,
                                 SchemaPtr inputSchema,
                                 SchemaPtr outputSchema,
                                 Windowing::LogicalWindowDescriptorPtr windowDefinition);
    static PhysicalOperatorPtr create(OperatorId id,
                                      StatisticId statisticId,
                                      const SchemaPtr& inputSchema,
                                      const SchemaPtr& outputSchema,
                                      const Windowing::LogicalWindowDescriptorPtr& windowDefinition);
    std::string toString() const override;
    OperatorPtr copy() override;
};
}// namespace NES::QueryCompilation::PhysicalOperators

#endif// NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_PHYSICALSLICEMERGINGOPERATOR_HPP_
