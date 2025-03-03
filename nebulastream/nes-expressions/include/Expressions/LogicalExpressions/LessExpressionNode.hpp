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

#ifndef NES_EXPRESSIONS_INCLUDE_EXPRESSIONS_LOGICALEXPRESSIONS_LESSEXPRESSIONNODE_HPP_
#define NES_EXPRESSIONS_INCLUDE_EXPRESSIONS_LOGICALEXPRESSIONS_LESSEXPRESSIONNODE_HPP_
#include <Expressions/LogicalExpressions/LogicalBinaryExpressionNode.hpp>
namespace NES {

/**
 * @brief This node represents a less comparision between the two children.
 */
class LessExpressionNode : public LogicalBinaryExpressionNode {
  public:
    LessExpressionNode() = default;
    ~LessExpressionNode() override = default;
    /**
    * @brief Create a new less expression
    */
    static ExpressionNodePtr create(ExpressionNodePtr const& left, ExpressionNodePtr const& right);

    bool equal(NodePtr const& rhs) const override;

    std::string toString() const override;

    /**
    * @brief Create a deep copy of this expression node.
    * @return ExpressionNodePtr
    */
    ExpressionNodePtr copy() override;

  protected:
    explicit LessExpressionNode(LessExpressionNode* other);
};
}// namespace NES

#endif// NES_EXPRESSIONS_INCLUDE_EXPRESSIONS_LOGICALEXPRESSIONS_LESSEXPRESSIONNODE_HPP_
