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
#include <Expressions/ArithmeticalExpressions/AddExpressionNode.hpp>
#include <sstream>
#include <utility>
namespace NES {

AddExpressionNode::AddExpressionNode(DataTypePtr stamp) : ArithmeticalBinaryExpressionNode(std::move(stamp)){};

AddExpressionNode::AddExpressionNode(AddExpressionNode* other) : ArithmeticalBinaryExpressionNode(other) {}

ExpressionNodePtr AddExpressionNode::create(ExpressionNodePtr const& left, ExpressionNodePtr const& right) {
    auto addNode = std::make_shared<AddExpressionNode>(left->getStamp());
    addNode->setChildren(left, right);
    return addNode;
}

bool AddExpressionNode::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<AddExpressionNode>()) {
        auto otherAddNode = rhs->as<AddExpressionNode>();
        return getLeft()->equal(otherAddNode->getLeft()) && getRight()->equal(otherAddNode->getRight());
    }
    return false;
}

std::string AddExpressionNode::toString() const {
    std::stringstream ss;
    ss << children[0]->toString() << "+" << children[1]->toString();
    return ss.str();
}

ExpressionNodePtr AddExpressionNode::copy() {
    return AddExpressionNode::create(children[0]->as<ExpressionNode>()->copy(), children[1]->as<ExpressionNode>()->copy());
}

}// namespace NES
