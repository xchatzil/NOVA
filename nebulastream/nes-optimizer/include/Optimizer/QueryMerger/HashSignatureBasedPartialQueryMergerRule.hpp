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

#ifndef NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYMERGER_HASHSIGNATUREBASEDPARTIALQUERYMERGERRULE_HPP_
#define NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYMERGER_HASHSIGNATUREBASEDPARTIALQUERYMERGERRULE_HPP_

#include <Optimizer/QueryMerger/BaseQueryMergerRule.hpp>

namespace NES {
class LogicalOperator;
using LogicalOperatorPtr = std::shared_ptr<LogicalOperator>;

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;
}// namespace NES

namespace NES::Optimizer {

class SignatureEqualityUtil;
using SignatureEqualityUtilPtr = std::shared_ptr<SignatureEqualityUtil>;

class HashSignatureBasedPartialQueryMergerRule;
using HashSignatureBasedPartialQueryMergerRulePtr = std::shared_ptr<HashSignatureBasedPartialQueryMergerRule>;

/**
 * @brief HashSignatureBasedPartialQueryMergerRule is responsible for merging together all the equivalent chains of Global Query Nodes
 * within the Global Query Plan such that, after running this rule all equivalent operator chains should be merged together
 * and only a single representative operator chain should exists in the Global Query Plan for all of them.
 * Effectively this will prune the global query plan size.
 *
 * Following are the conditions for the two global query node chains to be equivalent:
 *  - For each global query node in the first chain, there should exists an equal global query node in the other chain (except for the node with the sink operator).
 *      - For two global query nodes to be equal, we check that for each logical operator in one global query node their is an equivalent logical operator in the other
 *      global query node.
 *  - The order of global query nodes in both the chains should be same.
 *
 * Following is the example:
 * Given a Global Query Plan with two Global Query Node chains as follow:
 *                                                         GQPRoot
 *                                                         /     \
 *                                                       /        \
 *                                                     /           \
 *                                         GQN1({Sink1},{Q1})  GQN5({Sink2},{Q2})
 *                                                |                 |
 *                                        GQN2({Map2},{Q1})    GQN6({Map1},{Q2})
 *                                                |                 |
 *                                     GQN3({Filter1},{Q1})    GQN7({Filter1},{Q2})
 *                                                |                 |
 *                                  GQN4({Source(Car)},{Q1})   GQN8({Source(Car)},{Q2})
 *
 *
 * After running the HashSignatureBasedPartialQueryMergerRule, the resulting Global Query Plan will look as follow:
 *
 *                                                         GQPRoot
 *                                                         /     \
 *                                                        /       \
 *                                           GQN1({Sink1},{Q1}) GQN5({Sink2},{Q2})
 *                                                        \      /
*                                           GQN1({Map2},{Q1}) GQN5({Map1},{Q2})
 *                                                         \   /
 *                                                  GQN3({Filter1},{Q1,Q2})
 *                                                           |
 *                                                GQN4({Source(Car)},{Q1,Q2})
 *
 */
class HashSignatureBasedPartialQueryMergerRule final : public BaseQueryMergerRule {

  public:
    static HashSignatureBasedPartialQueryMergerRulePtr create();

    bool apply(GlobalQueryPlanPtr globalQueryPlan) override;

    ~HashSignatureBasedPartialQueryMergerRule() final = default;

  private:
    /**
     * @brief identify if the query plans are equal or not
     * @param targetQueryPlan : target query plan
     * @param hostQueryPlan : host query plan
     * @return Map containing matching pair of target and host operators
     */
    std::map<LogicalOperatorPtr, LogicalOperatorPtr> areQueryPlansEqual(const QueryPlanPtr& targetQueryPlan,
                                                                        const QueryPlanPtr& hostQueryPlan);

    std::map<LogicalOperatorPtr, LogicalOperatorPtr> areOperatorEqual(const LogicalOperatorPtr& targetOperator,
                                                                      const LogicalOperatorPtr& hostOperator);
};
}// namespace NES::Optimizer

#endif// NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYMERGER_HASHSIGNATUREBASEDPARTIALQUERYMERGERRULE_HPP_
