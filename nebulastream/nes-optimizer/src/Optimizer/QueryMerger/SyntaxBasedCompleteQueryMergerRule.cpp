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
#include <Optimizer/QueryMerger/SyntaxBasedCompleteQueryMergerRule.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Optimizer {

SyntaxBasedCompleteQueryMergerRulePtr SyntaxBasedCompleteQueryMergerRule::create() {
    return std::make_shared<SyntaxBasedCompleteQueryMergerRule>();
}

bool SyntaxBasedCompleteQueryMergerRule::apply(GlobalQueryPlanPtr globalQueryPlan) {

    NES_INFO("SyntaxBasedCompleteQueryMergerRule: Applying Syntax Based Equal Query Merger Rule to the Global Query Plan");
    std::vector<QueryPlanPtr> queryPlansToAdd = globalQueryPlan->getQueryPlansToAdd();
    if (queryPlansToAdd.empty()) {
        NES_WARNING("SyntaxBasedCompleteQueryMergerRule: Found no new query metadata in the global query plan."
                    " Skipping the Syntax Based Equal Query Merger Rule.");
        return true;
    }

    NES_DEBUG("SyntaxBasedCompleteQueryMergerRule: Iterating over all GQMs in the Global Query Plan");
    for (auto& targetQueryPlan : queryPlansToAdd) {
        bool merged = false;
        auto hostSharedQueryPlans =
            globalQueryPlan->getSharedQueryPlansConsumingSourcesAndPlacementStrategy(targetQueryPlan->getSourceConsumed(),
                                                                                     targetQueryPlan->getPlacementStrategy());
        for (auto& hostSharedQueryPlan : hostSharedQueryPlans) {
            //TODO: we need to check how this will pan out when we will have more than 1 sink
            auto hostQueryPlan = hostSharedQueryPlan->getQueryPlan();
            //create a map of matching target to address operator id map
            std::map<OperatorId, OperatorId> targetToHostSinkOperatorMap;
            //Check if the target and address query plan are equal and return the target and address operator mappings
            if (areQueryPlansEqual(targetQueryPlan, hostQueryPlan, targetToHostSinkOperatorMap)) {
                NES_TRACE("SyntaxBasedCompleteQueryMergerRule: Merge target Shared metadata into address metadata");

                //Compute matched operator pairs
                std::vector<MatchedOperatorPairPtr> matchedOperatorPairs;
                matchedOperatorPairs.reserve(targetToHostSinkOperatorMap.size());

                std::vector<LogicalOperatorPtr> hostSinkOperators = hostSharedQueryPlan->getSinkOperators();
                //Iterate over all target sink global query nodes and try to identify a matching address global query node
                // using the target address operator map
                for (auto& targetSinkOperator : targetQueryPlan->getSinkOperators()) {
                    auto hostSinkOperatorId = targetToHostSinkOperatorMap.at(targetSinkOperator->getId());

                    auto hostSinkOperator = std::find_if(hostSinkOperators.begin(),
                                                         hostSinkOperators.end(),
                                                         [hostSinkOperatorId](const LogicalOperatorPtr& hostOperator) {
                                                             return hostOperator->getId() == hostSinkOperatorId;
                                                         });

                    if (hostSinkOperator == hostSinkOperators.end()) {
                        NES_THROW_RUNTIME_ERROR("SyntaxBasedCompleteQueryMergerRule: Unexpected behaviour! matching host sink "
                                                "pair not found in the host query plan.");
                    }

                    //add to the matched pair
                    matchedOperatorPairs.emplace_back(
                        MatchedOperatorPair::create((*hostSinkOperator), targetSinkOperator, ContainmentRelationship::EQUALITY));
                }

                //add matched operators to the host shared query plan
                hostSharedQueryPlan->addQuery(targetQueryPlan->getQueryId(), matchedOperatorPairs);

                //Update the shared query meta data
                globalQueryPlan->updateSharedQueryPlan(hostSharedQueryPlan);
                // exit the for loop as we found a matching address shared query meta data
                merged = true;
                break;
            }
        }
        if (!merged) {
            NES_DEBUG("SyntaxBasedCompleteQueryMergerRule: computing a new Shared Query Plan");
            globalQueryPlan->createNewSharedQueryPlan(targetQueryPlan);
        }
    }
    //Remove all empty shared query metadata
    globalQueryPlan->removeFailedOrStoppedSharedQueryPlans();
    return globalQueryPlan->clearQueryPlansToAdd();
}

bool SyntaxBasedCompleteQueryMergerRule::areQueryPlansEqual(const QueryPlanPtr& targetQueryPlan,
                                                            const QueryPlanPtr& hostQueryPlan,
                                                            std::map<OperatorId, OperatorId>& targetHostOperatorMap) {

    NES_DEBUG("SyntaxBasedCompleteQueryMergerRule: check if the target and address query plans are syntactically equal or not");
    std::vector<OperatorPtr> targetSourceOperators = targetQueryPlan->getLeafOperators();
    std::vector<OperatorPtr> hostSourceOperators = hostQueryPlan->getLeafOperators();

    if (targetSourceOperators.size() != hostSourceOperators.size()) {
        NES_WARNING("SyntaxBasedCompleteQueryMergerRule: Not matched as number of sources in target and address query plans are "
                    "different.");
        return false;
    }

    //Fetch the first source operator and find a corresponding matching source operator in the address source operator list
    auto& targetSourceOperator = targetSourceOperators[0];
    for (auto& hostSourceOperator : hostSourceOperators) {
        targetHostOperatorMap.clear();
        if (areOperatorEqual(targetSourceOperator, hostSourceOperator, targetHostOperatorMap)) {
            return true;
        }
    }
    return false;
}

bool SyntaxBasedCompleteQueryMergerRule::areOperatorEqual(const OperatorPtr& targetOperator,
                                                          const OperatorPtr& hostOperator,
                                                          std::map<OperatorId, OperatorId>& targetHostOperatorMap) {

    NES_TRACE("SyntaxBasedCompleteQueryMergerRule: Check if the target and address operator are syntactically equal or not.");
    if (targetHostOperatorMap.contains(targetOperator->getId())) {
        if (targetHostOperatorMap.at(targetOperator->getId()) == hostOperator->getId()) {
            NES_TRACE("SyntaxBasedCompleteQueryMergerRule: Already matched so skipping rest of the check.");
            return true;
        }
        NES_WARNING("SyntaxBasedCompleteQueryMergerRule: Not matched as target operator was matched to another number of "
                    "sources in target and address query plans are different.");
        return false;
    }

    if (targetOperator->instanceOf<SinkLogicalOperator>() && hostOperator->instanceOf<SinkLogicalOperator>()) {
        NES_TRACE("SyntaxBasedCompleteQueryMergerRule: Both address and target operators are of sink type.");
        targetHostOperatorMap.insert_or_assign(targetOperator->getId(), hostOperator->getId());
        return true;
    }

    bool areParentsEqual;
    bool areChildrenEqual;

    NES_TRACE("SyntaxBasedCompleteQueryMergerRule: Compare address and target operators.");
    if (targetOperator->equal(hostOperator)) {
        targetHostOperatorMap.insert_or_assign(targetOperator->getId(), hostOperator->getId());

        NES_TRACE("SyntaxBasedCompleteQueryMergerRule: Check if parents of target and address operators are equal.");
        for (const auto& targetParent : targetOperator->getParents()) {
            areParentsEqual = false;
            for (const auto& hostParent : hostOperator->getParents()) {
                if (areOperatorEqual(targetParent->as<Operator>(), hostParent->as<Operator>(), targetHostOperatorMap)) {
                    areParentsEqual = true;
                    break;
                }
            }
            if (!areParentsEqual) {
                targetHostOperatorMap.erase(targetOperator->getId());
                return false;
            }
        }

        NES_TRACE("SyntaxBasedCompleteQueryMergerRule: Check if children of target and address operators are equal.");
        auto targetChildren = targetOperator->getChildren();
        for (const auto& targetChild : targetChildren) {
            areChildrenEqual = false;
            auto children = hostOperator->getChildren();
            for (const auto& hostChild : children) {
                if (areOperatorEqual(targetChild->as<Operator>(), hostChild->as<Operator>(), targetHostOperatorMap)) {
                    areChildrenEqual = true;
                    break;
                }
            }
            if (!areChildrenEqual) {
                targetHostOperatorMap.erase(targetOperator->getId());
                return false;
            }
        }
        return true;
    }
    NES_WARNING("SyntaxBasedCompleteQueryMergerRule: Target and address operators are not matched.");
    return false;
}
}// namespace NES::Optimizer
