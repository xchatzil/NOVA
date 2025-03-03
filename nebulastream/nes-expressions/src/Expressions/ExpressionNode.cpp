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
#include <Expressions/ExpressionNode.hpp>
#include <utility>
namespace NES {
ExpressionNode::ExpressionNode(DataTypePtr stamp) : stamp(std::move(stamp)) {}

bool ExpressionNode::isPredicate() const { return stamp->isBoolean(); }

DataTypePtr ExpressionNode::getStamp() const { return stamp; }

void ExpressionNode::setStamp(DataTypePtr stamp) { this->stamp = std::move(stamp); }

void ExpressionNode::inferStamp(SchemaPtr schema) {
    // infer stamp on all children nodes
    for (const auto& node : children) {
        node->as<ExpressionNode>()->inferStamp(schema);
    }
}

ExpressionNode::ExpressionNode(const ExpressionNode* other) : stamp(other->stamp) {}

}// namespace NES
