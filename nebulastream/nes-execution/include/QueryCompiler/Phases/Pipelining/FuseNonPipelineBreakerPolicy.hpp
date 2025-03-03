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
#ifndef NES_EXECUTION_INCLUDE_QUERYCOMPILER_PHASES_PIPELINING_FUSENONPIPELINEBREAKERPOLICY_HPP_
#define NES_EXECUTION_INCLUDE_QUERYCOMPILER_PHASES_PIPELINING_FUSENONPIPELINEBREAKERPOLICY_HPP_
#include <QueryCompiler/Phases/Pipelining/OperatorFusionPolicy.hpp>

namespace NES::QueryCompilation {

/**
 * @brief This operator fusion policy fuses all non pipeline breakers (operators, which do not materialize data).
 * Currently fusion is enabled for:
 * PhysicalMapOperator, PhysicalFilterOperator, PhysicalProjectOperator,
 * PhysicalWatermarkAssignmentOperator, PhysicalJoinBuildOperator,
 * PhysicalSlicePreAggregationOperator, PhysicalSliceMergingOperator.
 */
class FuseNonPipelineBreakerPolicy : public OperatorFusionPolicy {
  public:
    static OperatorFusionPolicyPtr create();
    bool isFusible(PhysicalOperators::PhysicalOperatorPtr physicalOperator) override;
};
}// namespace NES::QueryCompilation

#endif// NES_EXECUTION_INCLUDE_QUERYCOMPILER_PHASES_PIPELINING_FUSENONPIPELINEBREAKERPOLICY_HPP_
