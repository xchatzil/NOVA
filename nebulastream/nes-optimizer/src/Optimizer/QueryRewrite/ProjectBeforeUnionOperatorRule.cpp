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
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Expressions/FieldAccessExpressionNode.hpp>
#include <Expressions/FieldRenameExpressionNode.hpp>
#include <Operators/LogicalOperators/LogicalProjectionOperator.hpp>
#include <Operators/LogicalOperators/LogicalUnionOperator.hpp>
#include <Optimizer/QueryRewrite/ProjectBeforeUnionOperatorRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Optimizer {

ProjectBeforeUnionOperatorRulePtr ProjectBeforeUnionOperatorRule::create() {
    return std::make_shared<ProjectBeforeUnionOperatorRule>(ProjectBeforeUnionOperatorRule());
}

QueryPlanPtr ProjectBeforeUnionOperatorRule::apply(QueryPlanPtr queryPlan) {

    NES_DEBUG("Before applying ProjectBeforeUnionOperatorRule to the query plan: {}", queryPlan->toString());
    auto unionOperators = queryPlan->getOperatorByType<LogicalUnionOperator>();
    for (auto& unionOperator : unionOperators) {
        auto rightInputSchema = unionOperator->getRightInputSchema();
        auto leftInputSchema = unionOperator->getLeftInputSchema();
        //Only apply the rule when right side and left side schema are different
        if (!rightInputSchema->equals(leftInputSchema, false)) {
            //Construct project operator for mapping rightInputSource To leftInputSource
            auto projectOperator = constructProjectOperator(rightInputSchema, leftInputSchema);
            auto childrenToUnionOperator = unionOperator->getChildren();
            for (auto& child : childrenToUnionOperator) {
                auto childOutputSchema = child->as<LogicalOperator>()->getOutputSchema();
                //Find the child that matches the right schema and inset the project operator there
                if (rightInputSchema->equals(childOutputSchema, false)) {
                    child->insertBetweenThisAndParentNodes(projectOperator);
                    break;
                }
            }
        }
    }
    NES_DEBUG("After applying ProjectBeforeUnionOperatorRule to the query plan: {}", queryPlan->toString());
    return queryPlan;
}

LogicalOperatorPtr ProjectBeforeUnionOperatorRule::constructProjectOperator(const SchemaPtr& sourceSchema,
                                                                            const SchemaPtr& destinationSchema) {
    NES_TRACE("Computing Projection operator for Source Schema{} and Destination schema {}",
              sourceSchema->toString(),
              destinationSchema->toString());
    //Fetch source and destination schema fields
    auto sourceFields = sourceSchema->fields;
    auto destinationFields = destinationSchema->fields;
    std::vector<ExpressionNodePtr> projectExpressions;
    //Compute projection expressions
    for (uint64_t i = 0; i < sourceSchema->getSize(); i++) {
        auto field = sourceFields[i];
        auto updatedFieldName = destinationFields[i]->getName();
        //Compute field access and field rename expression
        auto originalField = FieldAccessExpressionNode::create(field->getDataType(), field->getName());
        auto fieldRenameExpression =
            FieldRenameExpressionNode::create(originalField->as<FieldAccessExpressionNode>(), updatedFieldName);
        projectExpressions.push_back(fieldRenameExpression);
    }
    //Create Projection operator
    return LogicalOperatorFactory::createProjectionOperator(projectExpressions);
}

}// namespace NES::Optimizer
