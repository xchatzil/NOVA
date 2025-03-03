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
#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_LOGICALBINARYOPERATOR_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_LOGICALBINARYOPERATOR_HPP_

#include <Operators/AbstractOperators/Arity/BinaryOperator.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>

namespace NES {
/**
 * @brief Logical Binary operator, defines two output schemas
 */
class LogicalBinaryOperator : public LogicalOperator, public BinaryOperator {
  public:
    explicit LogicalBinaryOperator(OperatorId id);

    /**
    * @brief infers the input and out schema of this operator depending on its child.
    * @throws Exception if the schema could not be infers correctly or if the inferred types are not valid.
    * @param typeInferencePhaseContext needed for stamp inferring
    * @return true if schema was correctly inferred
    */
    bool inferSchema() override;

    void inferInputOrigins() override;

    /**
     * @brief Get all left input operators.
     * @return std::vector<OperatorPtr>
     */
    std::vector<OperatorPtr> getLeftOperators() const;

    /**
    * @brief Get all right input operators.
    * @return std::vector<OperatorPtr>
    */
    std::vector<OperatorPtr> getRightOperators() const;

  private:
    std::vector<OperatorPtr> getOperatorsBySchema(const SchemaPtr& schema) const;
};
}// namespace NES

#endif// NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_LOGICALBINARYOPERATOR_HPP_
