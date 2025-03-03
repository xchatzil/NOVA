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

#ifndef NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYREWRITE_PROJECTBEFOREUNIONOPERATORRULE_HPP_
#define NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYREWRITE_PROJECTBEFOREUNIONOPERATORRULE_HPP_

#include <Optimizer/QueryRewrite/BaseRewriteRule.hpp>

namespace NES {

class LogicalOperator;
using LogicalOperatorPtr = std::shared_ptr<LogicalOperator>;

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

class Schema;
using SchemaPtr = std::shared_ptr<Schema>;
}// namespace NES

namespace NES::Optimizer {

class ProjectBeforeUnionOperatorRule;
using ProjectBeforeUnionOperatorRulePtr = std::shared_ptr<ProjectBeforeUnionOperatorRule>;

/**
 * @brief This rule is defined for adding a Projection Operator before a Union Operator. Following are the conditions:
 * - If there exists a Union operator with different schemas then add on one side a project operator to make both schema equal.
 */
class ProjectBeforeUnionOperatorRule : public BaseRewriteRule {

  public:
    static ProjectBeforeUnionOperatorRulePtr create();
    QueryPlanPtr apply(QueryPlanPtr queryPlan) override;
    virtual ~ProjectBeforeUnionOperatorRule() = default;

  private:
    ProjectBeforeUnionOperatorRule() = default;

    /**
     * @brief Construct the project operator to be added between union and one of the child.
     * @param sourceSchema : the source schema for project.
     * @param destinationSchema : the destination schema for project.
     * @return LogicalOperatorPtr: the project operator based on source and destination schema
     */
    static LogicalOperatorPtr constructProjectOperator(const SchemaPtr& sourceSchema, const SchemaPtr& destinationSchema);
};

}// namespace NES::Optimizer
#endif// NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYREWRITE_PROJECTBEFOREUNIONOPERATORRULE_HPP_
