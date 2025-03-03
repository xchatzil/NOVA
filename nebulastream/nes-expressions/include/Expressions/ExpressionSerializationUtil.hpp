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

#ifndef NES_EXPRESSIONS_INCLUDE_EXPRESSIONS_EXPRESSIONSERIALIZATIONUTIL_HPP_
#define NES_EXPRESSIONS_INCLUDE_EXPRESSIONS_EXPRESSIONSERIALIZATIONUTIL_HPP_

#include <memory>

namespace NES {

class ExpressionNode;
using ExpressionNodePtr = std::shared_ptr<ExpressionNode>;

class SerializableExpression;
class SerializableExpression_ConstantValueExpression;
class SerializableExpression_FieldAccessExpression;
class SerializableExpression_FieldRenameExpression;
class SerializableExpression_FieldAssignmentExpression;
class SerializableExpression_WhenExpression;
class SerializableExpression_CaseExpression;
class SerializableExpression_FunctionExpression;

/**
* @brief The ExpressionSerializationUtil offers functionality to serialize and de-serialize expression nodes to the
* corresponding protobuffer object.
*/
class ExpressionSerializationUtil {
  public:
    /**
    * @brief Serializes a expression node and all its children to a SerializableDataType object.
    * @param expressionNode The root expression node to serialize.
    * @param serializedExpression The corresponding protobuff object, which is used to capture the state of the object.
    * @return the modified serializedExpression
    */
    static SerializableExpression* serializeExpression(const ExpressionNodePtr& expressionNode,
                                                       SerializableExpression* serializedExpression);

    /**
    * @brief De-serializes the SerializableExpression and all its children to a corresponding ExpressionNodePtr
    * @param serializedExpression the serialized expression.
    * @return ExpressionNodePtr
    */
    static ExpressionNodePtr deserializeExpression(const SerializableExpression& serializedExpression);

  private:
    static void serializeLogicalExpressions(const ExpressionNodePtr& expression, SerializableExpression* serializedExpression);
    static void serializeArithmeticalExpressions(const ExpressionNodePtr& expression,
                                                 SerializableExpression* serializedExpression);
    static ExpressionNodePtr deserializeLogicalExpressions(const SerializableExpression& serializedExpression);
    static ExpressionNodePtr deserializeArithmeticalExpressions(const SerializableExpression& serializedExpression);
};
}// namespace NES

#endif// NES_EXPRESSIONS_INCLUDE_EXPRESSIONS_EXPRESSIONSERIALIZATIONUTIL_HPP_
