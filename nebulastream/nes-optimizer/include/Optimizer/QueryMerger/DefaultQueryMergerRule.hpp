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

#ifndef NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYMERGER_DEFAULTQUERYMERGERRULE_HPP_
#define NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYMERGER_DEFAULTQUERYMERGERRULE_HPP_

#include <Optimizer/QueryMerger/BaseQueryMergerRule.hpp>

namespace NES::Optimizer {

class DefaultQueryMergerRule;
using DefaultQueryMergerRulePtr = std::shared_ptr<DefaultQueryMergerRule>;

/**
 * @brief The default query merger rule creates a new Shared Query Plan in the global query plan for each new query
 */
class DefaultQueryMergerRule final : public BaseQueryMergerRule {

  public:
    static DefaultQueryMergerRulePtr create();

    bool apply(GlobalQueryPlanPtr globalQueryPlan) override;

    ~DefaultQueryMergerRule() noexcept override = default;

  private:
    DefaultQueryMergerRule() noexcept = default;
};
}// namespace NES::Optimizer

#endif// NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYMERGER_DEFAULTQUERYMERGERRULE_HPP_
