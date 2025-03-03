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

#include <Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Nodes/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/LogicalFilterOperator.hpp>
#include <Optimizer/QueryRewrite/FilterMergeRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Optimizer {

FilterMergeRulePtr FilterMergeRule::create() { return std::make_shared<FilterMergeRule>(); }

QueryPlanPtr FilterMergeRule::apply(NES::QueryPlanPtr queryPlan) {
    NES_INFO("Applying FilterMergeRule to query {}", queryPlan->toString());
    std::set<OperatorId> visitedOperators;
    auto filterOperators = queryPlan->getOperatorByType<LogicalFilterOperator>();
    NES_DEBUG("FilterMergeRule: Identified {} filter nodes in the query plan", filterOperators.size());
    NES_DEBUG("Query before applying the rule: {}", queryPlan->toString());
    for (auto& filter : filterOperators) {
        if (visitedOperators.find(filter->getId()) == visitedOperators.end()) {
            std::vector<LogicalFilterOperatorPtr> consecutiveFilters = getConsecutiveFilters(filter);
            NES_DEBUG("FilterMergeRule: Filter {} has {} consecutive filters as children",
                      filter->getId(),
                      consecutiveFilters.size());
            if (consecutiveFilters.size() >= 2) {
                NES_DEBUG("FilterMerge: Create combined predicate");
                auto combinedPredicate = consecutiveFilters.at(0)->getPredicate();
                NES_DEBUG("FilterMergeRule: The predicate of each consecutive filter should be appended to the conjunction");
                for (unsigned int i = 1; i < consecutiveFilters.size(); i++) {
                    auto predicate = consecutiveFilters.at(i)->getPredicate();
                    combinedPredicate = AndExpressionNode::create(combinedPredicate, predicate);
                }
                NES_DEBUG("FilterMergeRule: Create new combined filter with the conjunction of all filter predicates");
                auto combinedFilter = LogicalOperatorFactory::createFilterOperator(combinedPredicate);
                auto filterChainParents = consecutiveFilters.at(0)->getParents();
                auto filterChainChildren = consecutiveFilters.back()->getChildren();
                NES_DEBUG("FilterMergeRule: Start re-writing the new query plan");
                NES_DEBUG("FilterMergeRule: Remove parent/children references for the consecutive filters");
                for (auto& filterToRemove : consecutiveFilters) {
                    filterToRemove->removeAllParent();
                    filterToRemove->removeChildren();
                }
                NES_DEBUG("FilterMergeRule: Fix references, the parent of new filter are the parents of the filter chain");
                for (auto& filterChainParent : filterChainParents) {
                    combinedFilter->addParent(filterChainParent);
                }
                NES_DEBUG(
                    "FilterMergeRule: Fix references, the chain children have only one parent, which is the new combined filter");
                for (auto& filterChainChild : filterChainChildren) {
                    filterChainChild->removeAllParent();
                    filterChainChild->addParent(combinedFilter);
                }
                NES_DEBUG("FilterMergeRule: Mark the involved nodes as visited");
                for (auto& orderedFilter : consecutiveFilters) {
                    visitedOperators.insert(orderedFilter->getId());
                }
            } else {
                NES_DEBUG("FilterMergeRule: Only one filter was found, no optimization is possible")
            }
        } else {
            NES_DEBUG("FilterMergeRule: Filter node already visited");
        }
    }
    NES_DEBUG("Query after applying the rule: {}", queryPlan->toString());
    return queryPlan;
}

std::vector<LogicalFilterOperatorPtr> FilterMergeRule::getConsecutiveFilters(const NES::LogicalFilterOperatorPtr& filter) {
    std::vector<LogicalFilterOperatorPtr> consecutiveFilters = {};
    DepthFirstNodeIterator queryPlanNodeIterator(filter);
    auto nodeIterator = queryPlanNodeIterator.begin();
    auto node = (*nodeIterator);
    while (node->instanceOf<LogicalFilterOperator>()) {
        NES_DEBUG("Found consecutive filter in the chain, adding it the list");
        consecutiveFilters.push_back(node->as<LogicalFilterOperator>());
        ++nodeIterator;
        node = (*nodeIterator);
    }
    NES_DEBUG("Found {} consecutive filters", consecutiveFilters.size());
    return consecutiveFilters;
}

}// namespace NES::Optimizer
