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
#ifndef NES_EXECUTION_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_LOWERLOGICALTOPHYSICALOPERATORS_HPP_
#define NES_EXECUTION_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_LOWERLOGICALTOPHYSICALOPERATORS_HPP_

#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <vector>

namespace NES::QueryCompilation {

/**
 * @brief This phase lowers a query plan of logical operators into a query plan of physical operators.
 * The lowering of individual operators is defined by the physical operator provider to improve extendability.
 */
class LowerLogicalToPhysicalOperators {
  public:
    explicit LowerLogicalToPhysicalOperators(PhysicalOperatorProviderPtr provider);
    static LowerLogicalToPhysicalOperatorsPtr create(const PhysicalOperatorProviderPtr& provider);
    DecomposedQueryPlanPtr apply(DecomposedQueryPlanPtr decomposedQueryPlan);

  private:
    PhysicalOperatorProviderPtr provider;
};
}// namespace NES::QueryCompilation

#endif// NES_EXECUTION_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_LOWERLOGICALTOPHYSICALOPERATORS_HPP_
