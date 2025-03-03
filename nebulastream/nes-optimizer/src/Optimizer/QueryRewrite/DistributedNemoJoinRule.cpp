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
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalogEntry.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinOperator.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryRewrite/DistributedNemoJoinRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Placement/PlacementConstants.hpp>

namespace NES::Optimizer {

DistributedNemoJoinRule::DistributedNemoJoinRule(
    Configurations::OptimizerConfiguration configuration,
    TopologyPtr topology,
    const std::map<Catalogs::Source::SourceCatalogEntryPtr, std::set<uint64_t>>& keyDistributionMap)
    : topology(topology), configuration(configuration), keyDistributionMap(keyDistributionMap) {}

DistributedNemoJoinRulePtr
DistributedNemoJoinRule::create(Configurations::OptimizerConfiguration configuration,
                                TopologyPtr topology,
                                const std::map<Catalogs::Source::SourceCatalogEntryPtr, std::set<uint64_t>>& distributionMap) {
    NES_DEBUG("DistributedNemoJoinRule: Using distributed nemo join");
    return std::make_shared<DistributedNemoJoinRule>(DistributedNemoJoinRule(configuration, topology, distributionMap));
}

QueryPlanPtr DistributedNemoJoinRule::apply(QueryPlanPtr queryPlan) {
    NES_DEBUG("DistributedNemoJoinRule: Plan before placement\n{}", queryPlan->toString());
    auto joinOps = queryPlan->getOperatorByType<LogicalJoinOperator>();

    if (!joinOps.empty()) {
        std::map<uint64_t, std::set<std::pair<WorkerId, WorkerId>>> commonNodes = getNodesWithCommonKeys();

        for (const LogicalJoinOperatorPtr& joinOp : joinOps) {
            NES_DEBUG("DistributedNemoJoinRule::apply: join operator {}", joinOp->toString());
            auto parents = joinOp->getParents();
            // replace old central join with the new join replicas
            auto joinDefinition = joinOp->getJoinDefinition();

            for (const auto& pair : commonNodes) {
                LogicalJoinOperatorPtr newJoin =
                    LogicalOperatorFactory::createJoinOperator(joinDefinition)->as<LogicalJoinOperator>();
                newJoin->removeAllParent();
                newJoin->removeChildren();
                for (const auto& parent : parents) {
                    parent->addChild(newJoin);
                }

                uint64_t key = pair.first;
                auto nodePairs = pair.second;
                for (auto nodePair : nodePairs) {
                    NES_INFO("Adding pair to join {}: {}, {}", newJoin->getId(), nodePair.first, nodePair.second)
                    auto sourceOp1 = getSourceOperator(queryPlan, nodePair.first);
                    auto sourceOp2 = getSourceOperator(queryPlan, nodePair.second);
                    auto wmOp1 = sourceOp1->getParents()[0];
                    //wmOp1->removeAllParent();
                    auto wmOp2 = sourceOp2->getParents()[0];
                    //wmOp2->removeAllParent();

                    auto nodeId1 = std::any_cast<WorkerId>(sourceOp1->getProperty(PINNED_WORKER_ID));
                    auto nodeId2 = std::any_cast<WorkerId>(sourceOp2->getProperty(PINNED_WORKER_ID));
                    wmOp1->as_if<LogicalOperator>()->addProperty(PINNED_WORKER_ID, nodeId1);
                    wmOp2->as_if<LogicalOperator>()->addProperty(PINNED_WORKER_ID, nodeId2);
                    if (sourceOp1->getSourceDescriptor()->getPhysicalSourceName().ends_with("p")) {
                        newJoin->addProperty(PINNED_WORKER_ID, nodeId1);
                        newJoin->setOperatorState(OperatorState::PLACED);
                        NES_INFO("Pinning join {} to node {}", newJoin->getId(), nodeId1);
                    } else if (sourceOp2->getSourceDescriptor()->getPhysicalSourceName().ends_with("p")) {
                        newJoin->addProperty(PINNED_WORKER_ID, nodeId2);
                        newJoin->setOperatorState(OperatorState::PLACED);
                        NES_INFO("Pinning join {} to node {}", newJoin->getId(), nodeId2);
                    }

                    newJoin->addChild(wmOp1);
                    newJoin->addChild(wmOp2);
                }
            }
            joinOp->removeAllParent();
            joinOp->removeChildren();
        }
    } else {
        NES_DEBUG("DistributedNemoJoinRule: No join operator in query");
    }
    NES_DEBUG("DistributedNemoJoinRule: Plan after placement\n{}", queryPlan->toString());

    return queryPlan;
}

SourceLogicalOperatorPtr DistributedNemoJoinRule::getSourceOperator(const QueryPlanPtr& queryPlan, WorkerId workerId) {
    for (const SourceLogicalOperatorPtr& op : queryPlan->getSourceOperators()) {
        auto wId = std::any_cast<WorkerId>(op->getProperty(PINNED_WORKER_ID));
        if (workerId == wId) {
            return op;
        }
    }
    return nullptr;
}

std::map<uint64_t, std::set<SourceLogicalOperatorPtr>>
DistributedNemoJoinRule::getOperatorsWithCommonKeys(const QueryPlanPtr& queryPlan,
                                                    const std::map<uint64_t, std::set<WorkerId>>& commonKeys) {
    auto sourceOperators = queryPlan->getSourceOperators();
    std::map<uint64_t, std::set<SourceLogicalOperatorPtr>> commonOperators;
    for (const auto& pair : commonKeys) {
        auto key = pair.first;
        auto nodeIds = pair.second;
        for (const SourceLogicalOperatorPtr& op : sourceOperators) {
            auto wId = std::any_cast<WorkerId>(op->getProperty(PINNED_WORKER_ID));
            if (nodeIds.contains(wId)) {
                if (!commonOperators.contains(key)) {
                    commonOperators[key] = {};
                }
                commonOperators[key].insert(op);
            }
        }
    }
    return commonOperators;
}

std::map<uint64_t, std::set<std::pair<WorkerId, WorkerId>>> DistributedNemoJoinRule::getNodesWithCommonKeys() {
    std::map<uint64_t, std::set<std::pair<WorkerId, WorkerId>>> result;

    // Iterate through each pair in the map
    for (const auto& it1 : keyDistributionMap) {
        if (!it1.first->getLogicalSource()->getLogicalSourceName().ends_with("left")) {
            continue;
        }
        for (const auto& it2 : keyDistributionMap) {
            if (it2.first->getLogicalSource()->getLogicalSourceName().ends_with("right")) {
                // Iterate through integers in the first set
                for (auto num : it1.second) {
                    // Check if the second set contains the integer
                    if (it2.second.find(num) != it2.second.end()) {
                        NES_INFO("Adding It1: {}, It2: {}",
                                 it1.first->getPhysicalSource()->getPhysicalSourceName(),
                                 it2.first->getPhysicalSource()->getPhysicalSourceName());

                        // If yes, add it to the common integers set
                        if (!result.contains(num)) {
                            result[num] = {};
                        }
                        auto joinPair = std::pair(it1.first->getTopologyNodeId(), it2.first->getTopologyNodeId());
                        result[num].emplace(joinPair);
                    }
                }
            }
        }
    }
    return result;
}

}// namespace NES::Optimizer
