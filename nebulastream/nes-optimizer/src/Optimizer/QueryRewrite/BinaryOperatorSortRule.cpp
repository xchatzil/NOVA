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
#include <API/Schema.hpp>
#include <Catalogs/Exceptions/PhysicalSourceNotFoundException.hpp>
#include <Operators/LogicalOperators/LogicalUnionOperator.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinOperator.hpp>
#include <Optimizer/QueryRewrite/BinaryOperatorSortRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Optimizer {

BinaryOperatorSortRulePtr BinaryOperatorSortRule::create() {
    return std::make_shared<BinaryOperatorSortRule>(BinaryOperatorSortRule());
}

BinaryOperatorSortRule::BinaryOperatorSortRule() { NES_DEBUG("BinaryOperatorSortRule()"); };

QueryPlanPtr BinaryOperatorSortRule::apply(QueryPlanPtr queryPlanPtr) {
    NES_INFO("Apply BinaryOperatorSortRule ");
    //Find all join operators in the query plan and sort children individually.
    auto joinOperators = queryPlanPtr->getOperatorByType<LogicalJoinOperator>();
    for (const auto& joinOperator : joinOperators) {
        sortChildren(joinOperator);
    }
    //Find all Union operators in the query plan and sort children individually.
    auto unionOperators = queryPlanPtr->getOperatorByType<LogicalUnionOperator>();
    for (const auto& unionOperator : unionOperators) {
        sortChildren(unionOperator);
    }
    //Return the updated query plan
    return queryPlanPtr;
}

void BinaryOperatorSortRule::sortChildren(const BinaryOperatorPtr& binaryOperator) {
    //Extract the children operators
    auto children = binaryOperator->getChildren();
    NES_ASSERT(children.size() == 2, "Binary operator should have only 2 children");

    //Extract left and right children
    auto leftChild = children[0];
    auto rightChild = children[1];

    //Extract schema and qualifier name for left and right children
    auto leftInputSchema = binaryOperator->getLeftInputSchema();
    auto leftQualifierName = leftInputSchema->getQualifierNameForSystemGeneratedFields();
    auto rightInputSchema = binaryOperator->getRightInputSchema();
    auto rightQualifierName = rightInputSchema->getQualifierNameForSystemGeneratedFields();

    //Compare left and right children qualifier name
    if (leftQualifierName.compare(rightQualifierName) > 0) {
        //Remove the left children and insert it back to the binary operator
        // if right qualifier is smaller than left qualifier alphabetically
        binaryOperator->removeChild(leftChild);
        binaryOperator->addChild(leftChild);
        //Swap the schemas
        binaryOperator->setRightInputSchema(leftInputSchema);
        binaryOperator->setLeftInputSchema(rightInputSchema);
    }
}
}// namespace NES::Optimizer
