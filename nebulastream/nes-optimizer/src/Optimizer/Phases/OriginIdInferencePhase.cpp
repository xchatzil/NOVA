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

#include <Operators/AbstractOperators/OriginIdAssignmentOperator.hpp>
#include <Operators/Exceptions/InvalidLogicalOperatorException.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Optimizer/Phases/OriginIdInferencePhase.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>

namespace NES::Optimizer {

OriginIdInferencePhase::OriginIdInferencePhase() {}

OriginIdInferencePhasePtr OriginIdInferencePhase::create() {
    return std::make_shared<OriginIdInferencePhase>(OriginIdInferencePhase());
}

QueryPlanPtr OriginIdInferencePhase::execute(QueryPlanPtr queryPlan) {
    performInference(queryPlan->getOperatorByType<OriginIdAssignmentOperator>(), queryPlan->getRootOperators());
    return queryPlan;
}

DecomposedQueryPlanPtr OriginIdInferencePhase::execute(DecomposedQueryPlanPtr decomposedQueryPlan) {
    performInference(decomposedQueryPlan->getOperatorByType<OriginIdAssignmentOperator>(),
                     decomposedQueryPlan->getRootOperators());
    return decomposedQueryPlan;
}

void OriginIdInferencePhase::performInference(std::vector<OriginIdAssignmentOperatorPtr> originIdAssignmentOperator,
                                              std::vector<OperatorPtr> rootOperators) {
    // origin ids, always start from 1 to n, whereby n is the number of operators that assign new orin ids
    uint64_t originIdCounter = INITIAL_ORIGIN_ID.getRawValue();
    // set origin id for all operators of type OriginIdAssignmentOperator. For example, window, joins and sources.
    for (auto originIdAssignmentOperators : originIdAssignmentOperator) {
        originIdAssignmentOperators->setOriginId(OriginId(originIdCounter++));
    }

    // propagate origin ids through the complete query plan
    for (auto rootOperator : rootOperators) {
        if (auto logicalOperator = rootOperator->as_if<LogicalOperator>()) {
            logicalOperator->inferInputOrigins();
        } else {
            throw Exceptions::InvalidLogicalOperatorException(
                "During OriginIdInferencePhase all root operators have to be LogicalOperators");
        }
    }
}
}// namespace NES::Optimizer
