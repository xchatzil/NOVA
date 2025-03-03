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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_LOGICALPROJECTIONOPERATOR_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_LOGICALPROJECTIONOPERATOR_HPP_

#include <Operators/LogicalOperators/LogicalUnaryOperator.hpp>
#include <Operators/OperatorForwardDeclaration.hpp>

namespace NES {

/**
 * @brief projection operator, which contains an resets the output schema
 */
class LogicalProjectionOperator : public LogicalUnaryOperator {
  public:
    explicit LogicalProjectionOperator(std::vector<ExpressionNodePtr> expressions, OperatorId id);
    ~LogicalProjectionOperator() override = default;

    /**
     * @brief returns the list of fields that remain in the output schema.
     * @return  std::vector<ExpressionNodePtr>
     */
    std::vector<ExpressionNodePtr> getExpressions() const;

    /**
     * @brief check if two operators have the same output schema
     * @param rhs the operator to compare
     * @return bool true if they are the same otherwise false
     */
    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    [[nodiscard]] bool isIdentical(NodePtr const& rhs) const override;
    [[nodiscard]] std::string toString() const override;
    void inferStringSignature() override;

    /**
    * @brief infers the input and out schema of this operator depending on its child.
    * @param typeInferencePhaseContext needed for stamp inferring
    * @return true if schema was correctly inferred
    */
    bool inferSchema() override;
    OperatorPtr copy() override;

  private:
    std::vector<ExpressionNodePtr> expressions;
};

}// namespace NES
#endif// NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_LOGICALPROJECTIONOPERATOR_HPP_
