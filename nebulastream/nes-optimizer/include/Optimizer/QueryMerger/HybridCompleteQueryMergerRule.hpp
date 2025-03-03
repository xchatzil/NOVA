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

#ifndef NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYMERGER_HYBRIDCOMPLETEQUERYMERGERRULE_HPP_
#define NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYMERGER_HYBRIDCOMPLETEQUERYMERGERRULE_HPP_

#include <Optimizer/QueryMerger/BaseQueryMergerRule.hpp>
#include <Util/QuerySignatures/Z3QuerySignatureContext.hpp>

namespace z3 {
class context;
using ContextPtr = std::shared_ptr<context>;
}// namespace z3

namespace NES::Optimizer {
class QuerySignatureContext;
using ContextPtr = std::shared_ptr<QuerySignatureContext>;
class SignatureEqualityUtil;
using SignatureEqualityUtilPtr = std::shared_ptr<SignatureEqualityUtil>;

class HybridCompleteQueryMergerRule;
using HybridCompleteQueryMergerRulePtr = std::shared_ptr<HybridCompleteQueryMergerRule>;

/**
 * @brief HybridCompleteQueryMergerRule is responsible for merging together all equal Queries within the Global Query Plan, such that, after running this rule
 * only a single representative operator chain should exists in the Global Query Plan for all of them.
 * Effectively this rule will prune the global query plan for duplicate operators.
 *
 * Following are the conditions for the two queryIdAndCatalogEntryMapping to be equivalent:
 *  - For each global query node for first query, there should exists an equal global query node in the other query (except for the node with the sink operator).
 *
 *
 * Following is the example:
 * Given a Global Query Plan with two Global Query Node chains as follow:
 *                                                         GQPRoot
 *                                                         /     \
 *                                                       /        \
 *                                                     /           \
 *                                         GQN1({Sink1},{Q1})  GQN5({Sink2},{Q2})
 *                                                |                 |
 *                                        GQN2({Map1},{Q1})    GQN6({Map1},{Q2})
 *                                                |                 |
 *                                     GQN3({Filter1},{Q1})    GQN7({Filter1},{Q2})
 *                                                |                 |
 *                                  GQN4({Source(Car)},{Q1})   GQN8({Source(Car)},{Q2})
 *
 *
 * After running the HybridCompleteQueryMergerRule, the resulting Global Query Plan will look as follow:
 *
 *                                                         GQPRoot
 *                                                         /     \
 *                                                        /       \
 *                                           GQN1({Sink1},{Q1}) GQN5({Sink2},{Q2})
 *                                                        \      /
 *                                                         \   /
 *                                                   GQN2({Map1},{Q1,Q2})
 *                                                           |
 *                                                  GQN3({Filter1},{Q1,Q2})
 *                                                           |
 *                                                GQN4({Source(Car)},{Q1,Q2})
 *
 */
class HybridCompleteQueryMergerRule final : public BaseQueryMergerRule {

  public:
    static HybridCompleteQueryMergerRulePtr create(z3::ContextPtr context);

    bool apply(GlobalQueryPlanPtr globalQueryPlan) override;

    ~HybridCompleteQueryMergerRule() noexcept final = default;

  private:
    explicit HybridCompleteQueryMergerRule(z3::ContextPtr context);
    SignatureEqualityUtilPtr signatureEqualityUtil;
    Z3QuerySignatureContext context;
};
}// namespace NES::Optimizer

#endif// NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYMERGER_HYBRIDCOMPLETEQUERYMERGERRULE_HPP_
