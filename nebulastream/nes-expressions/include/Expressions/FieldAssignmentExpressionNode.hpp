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

#ifndef NES_EXPRESSIONS_INCLUDE_EXPRESSIONS_FIELDASSIGNMENTEXPRESSIONNODE_HPP_
#define NES_EXPRESSIONS_INCLUDE_EXPRESSIONS_FIELDASSIGNMENTEXPRESSIONNODE_HPP_
#include <Expressions/BinaryExpressionNode.hpp>
#include <Expressions/FieldAccessExpressionNode.hpp>
namespace NES {

class FieldAssignmentExpressionNode;
using FieldAssignmentExpressionNodePtr = std::shared_ptr<FieldAssignmentExpressionNode>;
/**
 * @brief A FieldAssignmentExpression represents the assignment of an expression result to a specific field.
 */
class FieldAssignmentExpressionNode : public BinaryExpressionNode {
  public:
    explicit FieldAssignmentExpressionNode(DataTypePtr stamp);

    /**
     * @brief Create untyped field read.
     */
    static FieldAssignmentExpressionNodePtr create(const FieldAccessExpressionNodePtr& fieldAccess,
                                                   const ExpressionNodePtr& expressionNodePtr);

    [[nodiscard]] std::string toString() const override;
    [[nodiscard]] bool equal(NodePtr const& rhs) const override;

    /**
     * @brief return the field to which a new value is assigned.
     * @return FieldAccessExpressionNodePtr
     */
    FieldAccessExpressionNodePtr getField() const;

    /**
     * @brief returns the expressions, which calculates the new value.
     * @return ExpressionNodePtr
     */
    ExpressionNodePtr getAssignment() const;

    /**
     * @brief Infers the stamp of the expression given the current schema and the typeInferencePhaseContext.
     * @param typeInferencePhaseContext
     * @param schema
     */
    void inferStamp(SchemaPtr schema) override;

    /**
    * @brief Create a deep copy of this expression node.
    * @return ExpressionNodePtr
    */
    ExpressionNodePtr copy() override;

  protected:
    explicit FieldAssignmentExpressionNode(FieldAssignmentExpressionNode* other);
};
}// namespace NES

#endif// NES_EXPRESSIONS_INCLUDE_EXPRESSIONS_FIELDASSIGNMENTEXPRESSIONNODE_HPP_
