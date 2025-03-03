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
#ifndef NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_PHYSICALWINDOWOPERATOR_HPP_
#define NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_PHYSICALWINDOWOPERATOR_HPP_

#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators {

/**
 * @brief Physical operator for all window sinks.
 * A window sink computes the final window result using the window slice store.
 */
class PhysicalWindowOperator : public PhysicalUnaryOperator {
  public:
    PhysicalWindowOperator(OperatorId id,
                           StatisticId statisticId,
                           SchemaPtr inputSchema,
                           SchemaPtr outputSchema,
                           Windowing::LogicalWindowDescriptorPtr windowDefinition);
    const Windowing::LogicalWindowDescriptorPtr& getWindowDefinition() const;

    /**
     * @brief returns the string representation of the class
     * @return the string representation of the class
     */
    std::string toString() const override;

  protected:
    Windowing::LogicalWindowDescriptorPtr windowDefinition;
};
}// namespace NES::QueryCompilation::PhysicalOperators

#endif// NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_PHYSICALWINDOWOPERATOR_HPP_
