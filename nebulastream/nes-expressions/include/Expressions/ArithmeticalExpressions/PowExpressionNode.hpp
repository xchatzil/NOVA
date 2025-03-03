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

#ifndef NES_EXPRESSIONS_INCLUDE_EXPRESSIONS_ARITHMETICALEXPRESSIONS_POWEXPRESSIONNODE_HPP_
#define NES_EXPRESSIONS_INCLUDE_EXPRESSIONS_ARITHMETICALEXPRESSIONS_POWEXPRESSIONNODE_HPP_
#include <Expressions/ArithmeticalExpressions/ArithmeticalBinaryExpressionNode.hpp>
namespace NES {
/**
 * @brief This node represents an POWER expression.
 */
class PowExpressionNode final : public ArithmeticalBinaryExpressionNode {
  public:
    explicit PowExpressionNode(DataTypePtr stamp);
    ~PowExpressionNode() noexcept override = default;
    /**
     * @brief Create a new POWER expression
     */
    static ExpressionNodePtr create(ExpressionNodePtr const& left, ExpressionNodePtr const& right);

    [[nodiscard]] bool equal(NodePtr const& rhs) const override;

    [[nodiscard]] std::string toString() const override;

    /**
     * @brief Determine returned datatype (-> UInt64/Double/ Throw exception for invalid inputs). Override ArithmeticalBinaryExpressionNode::inferStamp to increase bounds.
     * @comment E.g. SQL Server has a very unintuitive behaviour of always returning the datatype of the base (Int/Float). C++ always returns a float. We decide to return a float, except when both base and exponent are an Integer; and we set high bounds as POWER is an exponential function.
     * @param typeInferencePhaseContext
     * @param schema: the current schema.
     */
    void inferStamp(SchemaPtr schema) override;

    /**
    * @brief Create a deep copy of this expression node.
    * @return ExpressionNodePtr
    */
    ExpressionNodePtr copy() override;

  private:
    explicit PowExpressionNode(PowExpressionNode* other);
};

}// namespace NES

#endif// NES_EXPRESSIONS_INCLUDE_EXPRESSIONS_ARITHMETICALEXPRESSIONS_POWEXPRESSIONNODE_HPP_
