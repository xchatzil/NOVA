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

#include <Configurations/Coordinator/OptimizerConfiguration.hpp>
#include <Optimizer/Phases/QueryMergerPhase.hpp>
#include <Optimizer/QueryMerger/DefaultQueryMergerRule.hpp>
#include <Optimizer/QueryMerger/HashSignatureBasedCompleteQueryMergerRule.hpp>
#include <Optimizer/QueryMerger/HashSignatureBasedPartialQueryMergerRule.hpp>
#include <Optimizer/QueryMerger/HybridCompleteQueryMergerRule.hpp>
#include <Optimizer/QueryMerger/SyntaxBasedCompleteQueryMergerRule.hpp>
#include <Optimizer/QueryMerger/SyntaxBasedPartialQueryMergerRule.hpp>
#include <Optimizer/QueryMerger/Z3SignatureBasedBottomUpQueryContainmentRule.hpp>
#include <Optimizer/QueryMerger/Z3SignatureBasedCompleteQueryMergerRule.hpp>
#include <Optimizer/QueryMerger/Z3SignatureBasedPartialQueryMergerBottomUpRule.hpp>
#include <Optimizer/QueryMerger/Z3SignatureBasedPartialQueryMergerRule.hpp>
#include <Optimizer/QueryMerger/Z3SignatureBasedTreeBasedQueryContainmentMergerRule.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>

namespace NES::Optimizer {

QueryMergerPhasePtr QueryMergerPhase::create(z3::ContextPtr context,
                                             const Configurations::OptimizerConfiguration optimizerConfiguration) {
    return std::make_shared<QueryMergerPhase>(QueryMergerPhase(std::move(context), optimizerConfiguration));
}

QueryMergerPhase::QueryMergerPhase(z3::ContextPtr context, const Configurations::OptimizerConfiguration optimizerConfiguration) {

    switch (optimizerConfiguration.queryMergerRule) {
        case QueryMergerRule::SyntaxBasedCompleteQueryMergerRule:
            queryMergerRule = SyntaxBasedCompleteQueryMergerRule::create();
            break;
        case QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule:
            queryMergerRule = Z3SignatureBasedCompleteQueryMergerRule::create(context);
            break;
        case QueryMergerRule::HashSignatureBasedCompleteQueryMergerRule:
        case QueryMergerRule::ImprovedHashSignatureBasedCompleteQueryMergerRule:
            queryMergerRule = HashSignatureBasedCompleteQueryMergerRule::create();
            break;
        case QueryMergerRule::Z3SignatureBasedPartialQueryMergerRule:
            queryMergerRule = Z3SignatureBasedPartialQueryMergerRule::create(std::move(context));
            break;
        case QueryMergerRule::Z3SignatureBasedPartialQueryMergerBottomUpRule:
            queryMergerRule = Z3SignatureBasedPartialQueryMergerBottomUpRule::create(std::move(context));
            break;
        case QueryMergerRule::Z3SignatureBasedBottomUpQueryContainmentRule:
            queryMergerRule = Z3SignatureBasedBottomUpQueryContainmentRule::create(
                std::move(context),
                optimizerConfiguration.allowExhaustiveContainmentCheck.getValue());
            break;
        case QueryMergerRule::Z3SignatureBasedTopDownQueryContainmentMergerRule:
            queryMergerRule = Z3SignatureBasedTreeBasedQueryContainmentMergerRule::create(
                std::move(context),
                optimizerConfiguration.allowExhaustiveContainmentCheck.getValue());
            break;
        case QueryMergerRule::SyntaxBasedPartialQueryMergerRule:
            queryMergerRule = SyntaxBasedPartialQueryMergerRule::create();
            break;
        case QueryMergerRule::HashSignatureBasedPartialQueryMergerRule:
        case QueryMergerRule::ImprovedHashSignatureBasedPartialQueryMergerRule:
            queryMergerRule = HashSignatureBasedPartialQueryMergerRule::create();
            break;
        case QueryMergerRule::HybridCompleteQueryMergerRule:
            queryMergerRule = HybridCompleteQueryMergerRule::create(std::move(context));
            break;
        case QueryMergerRule::DefaultQueryMergerRule: queryMergerRule = DefaultQueryMergerRule::create();
    }
}

bool QueryMergerPhase::execute(GlobalQueryPlanPtr globalQueryPlan) {
    NES_DEBUG("QueryMergerPhase: Executing query merger phase.");
    return queryMergerRule->apply(std::move(globalQueryPlan));
}

}// namespace NES::Optimizer
