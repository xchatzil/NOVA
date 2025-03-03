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

#ifndef NES_EXPRESSIONS_INCLUDE_EXPRESSIONS_EXPRESSIONNODE_HPP_
#define NES_EXPRESSIONS_INCLUDE_EXPRESSIONS_EXPRESSIONNODE_HPP_

#include <Nodes/Node.hpp>
#include <memory>

namespace NES {

class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

class DataType;
using DataTypePtr = std::shared_ptr<DataType>;

class ExpressionNode;
using ExpressionNodePtr = std::shared_ptr<ExpressionNode>;

/**
 * @brief this indicates an expression, which is a parameter for a FilterOperator or a MapOperator.
 * Each expression declares a stamp, which expresses the data type of this expression.
 * A stamp can be of a concrete type or invalid if the data type was not yet inferred.
 */
class ExpressionNode : public Node {

  public:
    explicit ExpressionNode(DataTypePtr stamp);

    ~ExpressionNode() override = default;

    /**
     * @brief Indicates if this expression is a predicate -> if its result stamp is a boolean
     * @return
     */
    bool isPredicate() const;

    /**
     * @brief Infers the stamp of the expression given the current schema and the typeInferencePhaseContext.
     * @param typeInferencePhaseContext
     * @param schema
     */
    virtual void inferStamp(SchemaPtr schema);

    /**
     * @brief returns the stamp as the data type which is produced by this expression.
     * @return Stamp
     */
    DataTypePtr getStamp() const;

    /**
     * @brief sets the stamp of this expression.
     * @param stamp
     */
    void setStamp(DataTypePtr stamp);

    /**
     * @brief Create a deep copy of this expression node.
     * @return ExpressionNodePtr
     */
    virtual ExpressionNodePtr copy() = 0;

  protected:
    explicit ExpressionNode(const ExpressionNode* other);

    /**
     * @brief declares the type of this expression.
     * todo replace the direct usage of data types with a stamp abstraction.
     */
    DataTypePtr stamp;
};
}// namespace NES
#endif// NES_EXPRESSIONS_INCLUDE_EXPRESSIONS_EXPRESSIONNODE_HPP_
