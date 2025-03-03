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

#include <Operators/LogicalOperators/LogicalOperatorForwardRefs.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Operators/Serialization/DecomposedQueryPlanSerializationUtil.hpp>
#include <Operators/Serialization/OperatorSerializationUtil.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Plans/Utils/PlanIterator.hpp>
#include <SerializableDecomposedQueryPlan.pb.h>
#include <Util/CompilerConstants.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES {

void DecomposedQueryPlanSerializationUtil::serializeDecomposedQueryPlan(
    const DecomposedQueryPlanPtr& decomposedQueryPlan,
    SerializableDecomposedQueryPlan* serializableDecomposedQueryPlan) {
    NES_DEBUG("QueryPlanSerializationUtil: serializing query plan {}", decomposedQueryPlan->toString());
    std::vector<OperatorPtr> rootOperators = decomposedQueryPlan->getRootOperators();
    NES_DEBUG("QueryPlanSerializationUtil: serializing the operator chain for each root operator independently");

    //Serialize Query Plan operators
    auto& serializedOperatorMap = *serializableDecomposedQueryPlan->mutable_operatormap();
    auto bfsIterator = PlanIterator(decomposedQueryPlan);
    for (auto itr = bfsIterator.begin(); itr != PlanIterator::end(); ++itr) {
        auto visitingOp = (*itr)->as<Operator>();
        if (serializedOperatorMap.find(visitingOp->getId().getRawValue()) != serializedOperatorMap.end()) {
            // skip rest of the steps as the operator is already serialized
            continue;
        }
        NES_TRACE("QueryPlan: Inserting operator in collection of already visited node.");
        SerializableOperator serializeOperator = OperatorSerializationUtil::serializeOperator(visitingOp, false);
        serializedOperatorMap[visitingOp->getId().getRawValue()] = serializeOperator;
    }

    //Serialize the root operator ids
    for (const auto& rootOperator : rootOperators) {
        auto rootOperatorId = rootOperator->getId();
        serializableDecomposedQueryPlan->add_rootoperatorids(rootOperatorId.getRawValue());
    }

    //Serialize the sub query plan and query plan id
    NES_TRACE("QueryPlanSerializationUtil: serializing the Query sub plan id and query id");
    serializableDecomposedQueryPlan->set_decomposedqueryid(decomposedQueryPlan->getDecomposedQueryId().getRawValue());
    serializableDecomposedQueryPlan->set_sharedqueryplanid(decomposedQueryPlan->getSharedQueryId().getRawValue());
    serializableDecomposedQueryPlan->set_state(serializeQueryState(decomposedQueryPlan->getState()));
}

DecomposedQueryPlanPtr DecomposedQueryPlanSerializationUtil::deserializeDecomposedQueryPlan(
    const NES::SerializableDecomposedQueryPlan* serializableDecomposedQueryPlan) {
    NES_TRACE("QueryPlanSerializationUtil: Deserializing query plan {}", serializableDecomposedQueryPlan->DebugString());
    std::vector<OperatorPtr> rootOperators;
    std::map<uint64_t, OperatorPtr> operatorIdToOperatorMap;

    //Deserialize all operators in the operator map
    for (const auto& operatorIdAndSerializedOperator : serializableDecomposedQueryPlan->operatormap()) {
        const auto& serializedOperator = operatorIdAndSerializedOperator.second;
        const OperatorPtr& deserializedOperator = OperatorSerializationUtil::deserializeOperator(serializedOperator);
        //Set the operator id sent by coordinator to the logical operator property
        const auto& operatorId = OperatorId(serializedOperator.operatorid());
        deserializedOperator->setId(operatorId);
        operatorIdToOperatorMap[serializedOperator.operatorid()] = deserializedOperator;
    }

    //Add deserialized children
    for (const auto& operatorIdAndSerializedOperator : serializableDecomposedQueryPlan->operatormap()) {
        const auto serializedOperator = operatorIdAndSerializedOperator.second;
        auto deserializedOperator = operatorIdToOperatorMap[serializedOperator.operatorid()];
        for (auto childId : serializedOperator.childrenids()) {
            deserializedOperator->addChild(operatorIdToOperatorMap[childId]);
        }
    }

    //add root operators
    for (auto rootOperatorId : serializableDecomposedQueryPlan->rootoperatorids()) {
        rootOperators.emplace_back(operatorIdToOperatorMap[rootOperatorId]);
    }

    //set properties of the query plan
    DecomposedQueryId decomposableQueryPlanId = DecomposedQueryId(serializableDecomposedQueryPlan->decomposedqueryid());
    SharedQueryId sharedQueryId = SharedQueryId(serializableDecomposedQueryPlan->sharedqueryplanid());

    auto decomposedQueryPlan =
        DecomposedQueryPlan::create(decomposableQueryPlanId, sharedQueryId, INVALID_WORKER_NODE_ID, rootOperators);
    if (serializableDecomposedQueryPlan->has_state()) {
        auto state = deserializeQueryState(serializableDecomposedQueryPlan->state());
        decomposedQueryPlan->setState(state);
    }
    return decomposedQueryPlan;
}

QueryState DecomposedQueryPlanSerializationUtil::deserializeQueryState(SerializableQueryState serializedQueryState) {
    using enum QueryState;
    switch (serializedQueryState) {
        case QUERY_STATE_REGISTERED: return REGISTERED;
        case QUERY_STATE_OPTIMIZING: return OPTIMIZING;
        case QUERY_STATE_DEPLOYED: return DEPLOYED;
        case QUERY_STATE_RUNNING: return RUNNING;
        case QUERY_STATE_MARKED_FOR_HARD_STOP: return MARKED_FOR_HARD_STOP;
        case QUERY_STATE_MARKED_FOR_SOFT_STOP: return MARKED_FOR_SOFT_STOP;
        case QUERY_STATE_SOFT_STOP_TRIGGERED: return SOFT_STOP_TRIGGERED;
        case QUERY_STATE_SOFT_STOP_COMPLETED: return SOFT_STOP_COMPLETED;
        case QUERY_STATE_STOPPED: return STOPPED;
        case QUERY_STATE_MARKED_FOR_FAILURE: return MARKED_FOR_FAILURE;
        case QUERY_STATE_FAILED: return FAILED;
        case QUERY_STATE_RESTARTING: return RESTARTING;
        case QUERY_STATE_MIGRATING: return MIGRATING;
        case QUERY_STATE_MIGRATION_COMPLETED: return MIGRATION_COMPLETED;
        case QUERY_STATE_EXPLAINED: return EXPLAINED;
        case QUERY_STATE_REDEPLOYED: return REDEPLOYED;
        case QUERY_STATE_MARKED_FOR_DEPLOYMENT: return MARKED_FOR_DEPLOYMENT;
        case QUERY_STATE_MARKED_FOR_REDEPLOYMENT: return MARKED_FOR_REDEPLOYMENT;
        case QUERY_STATE_MARKED_FOR_MIGRATION: return MARKED_FOR_MIGRATION;
        case SerializableQueryState_INT_MIN_SENTINEL_DO_NOT_USE_: return REGISTERED;
        case SerializableQueryState_INT_MAX_SENTINEL_DO_NOT_USE_: return REGISTERED;
    }
}

SerializableQueryState DecomposedQueryPlanSerializationUtil::serializeQueryState(QueryState queryState) {
    using enum QueryState;
    switch (queryState) {
        case REGISTERED: return QUERY_STATE_REGISTERED;
        case OPTIMIZING: return QUERY_STATE_OPTIMIZING;
        case DEPLOYED: return QUERY_STATE_DEPLOYED;
        case RUNNING: return QUERY_STATE_RUNNING;
        case MARKED_FOR_HARD_STOP: return QUERY_STATE_MARKED_FOR_HARD_STOP;
        case MARKED_FOR_SOFT_STOP: return QUERY_STATE_MARKED_FOR_SOFT_STOP;
        case SOFT_STOP_TRIGGERED: return QUERY_STATE_SOFT_STOP_TRIGGERED;
        case SOFT_STOP_COMPLETED: return QUERY_STATE_SOFT_STOP_COMPLETED;
        case STOPPED: return QUERY_STATE_STOPPED;
        case MARKED_FOR_FAILURE: return QUERY_STATE_MARKED_FOR_FAILURE;
        case FAILED: return QUERY_STATE_FAILED;
        case RESTARTING: return QUERY_STATE_RESTARTING;
        case MIGRATING: return QUERY_STATE_MIGRATING;
        case MIGRATION_COMPLETED: return QUERY_STATE_MIGRATION_COMPLETED;
        case EXPLAINED: return QUERY_STATE_EXPLAINED;
        case REDEPLOYED: return QUERY_STATE_REDEPLOYED;
        case MARKED_FOR_DEPLOYMENT: return QUERY_STATE_MARKED_FOR_DEPLOYMENT;
        case MARKED_FOR_REDEPLOYMENT: return QUERY_STATE_MARKED_FOR_REDEPLOYMENT;
        case MARKED_FOR_MIGRATION: return QUERY_STATE_MARKED_FOR_MIGRATION;
    }
}
}// namespace NES
