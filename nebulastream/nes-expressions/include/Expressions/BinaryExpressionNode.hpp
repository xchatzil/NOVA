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

#ifndef NES_EXPRESSIONS_INCLUDE_EXPRESSIONS_BINARYEXPRESSIONNODE_HPP_
#define NES_EXPRESSIONS_INCLUDE_EXPRESSIONS_BINARYEXPRESSIONNODE_HPP_
#include <Expressions/ExpressionNode.hpp>
namespace NES {
/**
 * @brief A binary expression is represents expressions with two children.
 */
class BinaryExpressionNode : public ExpressionNode {
  public:
    ~BinaryExpressionNode() noexcept override = default;

    /**
     * @brief set the children node of this expression.
     */
    void setChildren(ExpressionNodePtr const& left, ExpressionNodePtr const& right);

    /**
     * @brief gets the left children.
     */
    ExpressionNodePtr getLeft() const;

    /**
     * @brief gets the right children.
     */
    ExpressionNodePtr getRight() const;

    /**
    * @brief Create a deep copy of this expression node.
    * @return ExpressionNodePtr
    */
    ExpressionNodePtr copy() override = 0;

  protected:
    explicit BinaryExpressionNode(DataTypePtr stamp);
    explicit BinaryExpressionNode(BinaryExpressionNode* other);
};

}// namespace NES
#endif// NES_EXPRESSIONS_INCLUDE_EXPRESSIONS_BINARYEXPRESSIONNODE_HPP_
