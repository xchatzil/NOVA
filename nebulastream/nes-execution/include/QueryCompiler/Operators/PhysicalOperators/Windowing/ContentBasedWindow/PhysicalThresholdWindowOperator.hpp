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

#ifndef NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_CONTENTBASEDWINDOW_PHYSICALTHRESHOLDWINDOWOPERATOR_HPP_
#define NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_CONTENTBASEDWINDOW_PHYSICALTHRESHOLDWINDOWOPERATOR_HPP_

#include <QueryCompiler/Operators/PhysicalOperators/AbstractScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators {
/**
 * @brief Physical operator ThresholdWindow.
 * This class represent physical operator for ThresholdWindow. It stores the operator handler which later can be used in the
 * executable operator.
 */
class PhysicalThresholdWindowOperator : public PhysicalUnaryOperator {
  public:
    /**
     * @brief constructor of the physical operator of threshold window
     * @param id of the operator
     * @param statisticId: represents the unique identifier of components that we can track statistics for
     * @param inputSchema input schema for the operator
     * @param outputSchema output schema for the operator
     * @param operatorHandler pointer to the operator handler of the threshold window (of type ThresholdWindowOperatorHandler)
     */
    PhysicalThresholdWindowOperator(OperatorId id,
                                    StatisticId statisticId,
                                    SchemaPtr inputSchema,
                                    SchemaPtr outputSchema,
                                    Windowing::LogicalWindowDescriptorPtr windowDefinition);

    static std::shared_ptr<PhysicalThresholdWindowOperator> create(StatisticId statisticId,
                                                                   SchemaPtr inputSchema,
                                                                   SchemaPtr outputSchema,
                                                                   Windowing::LogicalWindowDescriptorPtr windowDefinition);

    Windowing::LogicalWindowDescriptorPtr getWindowDefinition();

    std::string toString() const override;

    OperatorPtr copy() override;

  private:
    Windowing::LogicalWindowDescriptorPtr windowDefinition;
};

}// namespace NES::QueryCompilation::PhysicalOperators

#endif// NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_CONTENTBASEDWINDOW_PHYSICALTHRESHOLDWINDOWOPERATOR_HPP_
