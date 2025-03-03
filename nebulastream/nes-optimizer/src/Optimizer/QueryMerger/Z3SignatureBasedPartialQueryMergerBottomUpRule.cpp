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

#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Optimizer/QueryMerger/MatchedOperatorPair.hpp>
#include <Optimizer/QueryMerger/Z3SignatureBasedPartialQueryMergerBottomUpRule.hpp>
#include <Optimizer/QuerySignatures/SignatureEqualityUtil.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/QuerySignatures/QuerySignature.hpp>

namespace NES::Optimizer {

Z3SignatureBasedPartialQueryMergerBottomUpRule::Z3SignatureBasedPartialQueryMergerBottomUpRule(z3::ContextPtr context) {
    signatureEqualityUtil = SignatureEqualityUtil::create(std::move(context));
}

Z3SignatureBasedPartialQueryMergerBottomUpRulePtr Z3SignatureBasedPartialQueryMergerBottomUpRule::create(z3::ContextPtr context) {
    return std::make_shared<Z3SignatureBasedPartialQueryMergerBottomUpRule>(
        Z3SignatureBasedPartialQueryMergerBottomUpRule(std::move(context)));
}

bool Z3SignatureBasedPartialQueryMergerBottomUpRule::apply(GlobalQueryPlanPtr globalQueryPlan) {
    NES_INFO("Z3SignatureBasedPartialQueryMergerBottomUpRule: Applying Signature Based Equal Query Merger Rule to the Global "
             "Query Plan");
    std::vector<QueryPlanPtr> queryPlansToAdd = globalQueryPlan->getQueryPlansToAdd();
    if (queryPlansToAdd.empty()) {
        NES_WARNING("Z3SignatureBasedPartialQueryMergerBottomUpRule: Found only a single query metadata in the global query plan."
                    " Skipping the Signature Based Equal Query Merger Rule.");
        return true;
    }

    NES_DEBUG(
        "Z3SignatureBasedPartialQueryMergerBottomUpRule: Iterating over all Shared Query MetaData in the Global Query Plan");
    //Iterate over all shared query metadata to identify equal shared metadata
    for (const auto& targetQueryPlan : queryPlansToAdd) {
        bool matched = false;
        auto hostSharedQueryPlans =
            globalQueryPlan->getSharedQueryPlansConsumingSourcesAndPlacementStrategy(targetQueryPlan->getSourceConsumed(),
                                                                                     targetQueryPlan->getPlacementStrategy());
        for (auto& hostSharedQueryPlan : hostSharedQueryPlans) {

            //Fetch the host query plan to merge
            auto hostQueryPlan = hostSharedQueryPlan->getQueryPlan();

            //create a map of matching target to address operator id map
            auto matchedTargetToHostOperatorMap = areQueryPlansEqual(targetQueryPlan, hostQueryPlan);

            if (!matchedTargetToHostOperatorMap.empty()) {
                NES_TRACE("Z3SignatureBasedPartialQueryMergerBottomUpRule: Merge target Shared metadata into address metadata");

                // As we merge partially equivalent queryIdAndCatalogEntryMapping, we can potentially find matches across multiple operators.
                // As upstream matched operators are covered by downstream matched operators. We need to retain only the
                // downstream matched operator containing any upstream matched operator. This will prevent in computation
                // of inconsistent shared query plans.
                if (matchedTargetToHostOperatorMap.size() > 1) {
                    //Fetch all the matched target operators.
                    std::vector<LogicalOperatorPtr> matchedTargetOperators;
                    matchedTargetOperators.reserve(matchedTargetToHostOperatorMap.size());
                    for (auto& mapEntry : matchedTargetToHostOperatorMap) {
                        matchedTargetOperators.emplace_back(mapEntry.first);
                    }

                    //Iterate over the target operators and remove the upstream operators covered by downstream matched operators
                    for (uint64_t i = 0; i < matchedTargetOperators.size(); i++) {
                        for (uint64_t j = 0; j < matchedTargetOperators.size(); j++) {
                            if (i == j) {
                                continue;//Skip chk with itself
                            }

                            if (matchedTargetOperators[i]->containAsGrandChild(matchedTargetOperators[j])) {
                                matchedTargetToHostOperatorMap.erase(matchedTargetOperators[j]);
                            } else if (matchedTargetOperators[i]->containAsGrandParent(matchedTargetOperators[j])) {
                                matchedTargetToHostOperatorMap.erase(matchedTargetOperators[i]);
                                break;
                            }
                        }
                    }
                }

                std::vector<MatchedOperatorPairPtr> matchedOperatorPairs;
                matchedOperatorPairs.reserve(matchedTargetToHostOperatorMap.size());
                //Iterate over all matched pairs of operators and merge the query plan
                for (auto [targetOperator, hostOperator] : matchedTargetToHostOperatorMap) {
                    matchedOperatorPairs.emplace_back(
                        MatchedOperatorPair::create(hostOperator, targetOperator, ContainmentRelationship::EQUALITY));
                }

                //add matched operators to the host shared query plan
                hostSharedQueryPlan->addQuery(targetQueryPlan->getQueryId(), matchedOperatorPairs);

                //Update the shared query meta data
                globalQueryPlan->updateSharedQueryPlan(hostSharedQueryPlan);
                // exit the for loop as we found a matching address shared query meta data
                matched = true;
                break;
            }
        }

        if (!matched) {
            NES_DEBUG("Z3SignatureBasedPartialQueryMergerBottomUpRule: computing a new Shared Query Plan");
            globalQueryPlan->createNewSharedQueryPlan(targetQueryPlan);
        }
    }
    //Remove all empty shared query metadata
    globalQueryPlan->removeFailedOrStoppedSharedQueryPlans();
    return globalQueryPlan->clearQueryPlansToAdd();
}

std::map<LogicalOperatorPtr, LogicalOperatorPtr>
Z3SignatureBasedPartialQueryMergerBottomUpRule::areQueryPlansEqual(const QueryPlanPtr& targetQueryPlan,
                                                                   const QueryPlanPtr& hostQueryPlan) {

    std::map<LogicalOperatorPtr, LogicalOperatorPtr> targetHostOperatorMap;
    NES_DEBUG("Z3SignatureBasedPartialQueryMergerBottomUpRule: check if the target and address query plans are syntactically "
              "equal or not");
    auto targetSourceOperators = targetQueryPlan->getSourceOperators();
    auto hostSourceOperators = hostQueryPlan->getSourceOperators();

    if (targetSourceOperators.size() != hostSourceOperators.size()) {
        NES_WARNING(
            "Z3SignatureBasedPartialQueryMergerBottomUpRule: Not matched as number of Sources in target and host query plans are "
            "different.");
        return {};
    }

    //Fetch the first source operator and find a corresponding matching source operator in the address source operator list
    for (auto& targetSourceOperator : targetSourceOperators) {
        for (auto& hostSourceOperator : hostSourceOperators) {
            auto matchedOperators = areOperatorEqual(targetSourceOperator, hostSourceOperator);
            if (!matchedOperators.empty()) {
                targetHostOperatorMap.merge(matchedOperators);
                break;
            }
        }
    }
    return targetHostOperatorMap;
}

std::map<LogicalOperatorPtr, LogicalOperatorPtr>
Z3SignatureBasedPartialQueryMergerBottomUpRule::areOperatorEqual(const LogicalOperatorPtr& targetOperator,
                                                                 const LogicalOperatorPtr& hostOperator) {

    std::map<LogicalOperatorPtr, LogicalOperatorPtr> targetHostOperatorMap;
    if (targetOperator->instanceOf<SinkLogicalOperator>() && hostOperator->instanceOf<SinkLogicalOperator>()) {
        NES_TRACE("Z3SignatureBasedPartialQueryMergerBottomUpRule: Both target and host operators are of sink type.");
        return {};
    }

    NES_TRACE("HashSignatureBasedPartialQueryMergerRule: Compare target and host operators.");
    if (signatureEqualityUtil->checkEquality(targetOperator->getZ3Signature(), hostOperator->getZ3Signature())) {
        NES_TRACE("Z3SignatureBasedPartialQueryMergerBottomUpRule: Check if parents of target and address operators are equal.");
        uint16_t matchCount = 0;
        for (const auto& targetParent : targetOperator->getParents()) {
            for (const auto& hostParent : hostOperator->getParents()) {
                auto matchedOperators = areOperatorEqual(targetParent->as<LogicalOperator>(), hostParent->as<LogicalOperator>());
                if (!matchedOperators.empty()) {
                    targetHostOperatorMap.merge(matchedOperators);
                    matchCount++;
                    break;
                }
            }
        }

        if (matchCount < targetOperator->getParents().size()) {
            targetHostOperatorMap[targetOperator] = hostOperator;
        }
        return targetHostOperatorMap;
    }
    NES_WARNING("Z3SignatureBasedPartialQueryMergerBottomUpRule: Target and host operators are not matched.");
    return {};
}

}// namespace NES::Optimizer
