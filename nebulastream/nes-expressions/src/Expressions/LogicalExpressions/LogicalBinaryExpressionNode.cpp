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

#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Expressions/LogicalExpressions/LogicalBinaryExpressionNode.hpp>

namespace NES {
LogicalBinaryExpressionNode::LogicalBinaryExpressionNode()
    : BinaryExpressionNode(DataTypeFactory::createBoolean()), LogicalExpressionNode() {}

LogicalBinaryExpressionNode::LogicalBinaryExpressionNode(LogicalBinaryExpressionNode* other)
    : BinaryExpressionNode(other), LogicalExpressionNode() {}

bool LogicalBinaryExpressionNode::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<LogicalBinaryExpressionNode>()) {
        auto other = rhs->as<LogicalBinaryExpressionNode>();
        return this->getLeft()->equal(other->getLeft()) && this->getRight()->equal(other->getRight());
    }
    return false;
}

}// namespace NES
