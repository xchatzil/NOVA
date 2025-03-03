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

#ifndef NES_EXPRESSIONS_INCLUDE_EXPRESSIONS_LOGICALEXPRESSIONS_LOGICALUNARYEXPRESSIONNODE_HPP_
#define NES_EXPRESSIONS_INCLUDE_EXPRESSIONS_LOGICALEXPRESSIONS_LOGICALUNARYEXPRESSIONNODE_HPP_
#include <Expressions/LogicalExpressions/LogicalExpressionNode.hpp>
#include <Expressions/UnaryExpressionNode.hpp>
namespace NES {
/**
 * @brief This node represents a logical unary expression.
 */
class LogicalUnaryExpressionNode : public UnaryExpressionNode, public LogicalExpressionNode {
  protected:
    LogicalUnaryExpressionNode();

  public:
    [[nodiscard]] bool equal(NodePtr const& rhs) const override;

    /**
    * @brief Create a deep copy of this expression node.
    * @return ExpressionNodePtr
    */
    ExpressionNodePtr copy() override = 0;

  protected:
    explicit LogicalUnaryExpressionNode(LogicalUnaryExpressionNode* other);
};
}// namespace NES

#endif// NES_EXPRESSIONS_INCLUDE_EXPRESSIONS_LOGICALEXPRESSIONS_LOGICALUNARYEXPRESSIONNODE_HPP_
