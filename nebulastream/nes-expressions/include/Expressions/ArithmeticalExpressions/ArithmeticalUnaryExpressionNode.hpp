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

#ifndef NES_EXPRESSIONS_INCLUDE_EXPRESSIONS_ARITHMETICALEXPRESSIONS_ARITHMETICALUNARYEXPRESSIONNODE_HPP_
#define NES_EXPRESSIONS_INCLUDE_EXPRESSIONS_ARITHMETICALEXPRESSIONS_ARITHMETICALUNARYEXPRESSIONNODE_HPP_
#include <Expressions/ArithmeticalExpressions/ArithmeticalExpressionNode.hpp>
#include <Expressions/UnaryExpressionNode.hpp>
namespace NES {
/**
 * @brief This node represents an arithmetical expression.
 */
class ArithmeticalUnaryExpressionNode : public UnaryExpressionNode, public ArithmeticalExpressionNode {
  public:
    /**
     * @brief Infers the stamp of this arithmetical expression node.
     * Currently the type inference is equal for all arithmetical expression and expects numerical data types as operands.
     * @param typeInferencePhaseContext
     * @param schema the current schema.
     */
    void inferStamp(SchemaPtr schema) override;

    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    [[nodiscard]] std::string toString() const override;

  protected:
    explicit ArithmeticalUnaryExpressionNode(DataTypePtr stamp);
    explicit ArithmeticalUnaryExpressionNode(ArithmeticalUnaryExpressionNode* other);
    ~ArithmeticalUnaryExpressionNode() noexcept override = default;
};

}// namespace NES

#endif// NES_EXPRESSIONS_INCLUDE_EXPRESSIONS_ARITHMETICALEXPRESSIONS_ARITHMETICALUNARYEXPRESSIONNODE_HPP_
