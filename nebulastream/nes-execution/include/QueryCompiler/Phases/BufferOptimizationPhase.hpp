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
#ifndef NES_EXECUTION_INCLUDE_QUERYCOMPILER_PHASES_BUFFEROPTIMIZATIONPHASE_HPP_
#define NES_EXECUTION_INCLUDE_QUERYCOMPILER_PHASES_BUFFEROPTIMIZATIONPHASE_HPP_

#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/Phases/OutputBufferAllocationStrategies.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <vector>

namespace NES::QueryCompilation {

/**
 * @brief This phase scans all pipelines and determines if the OutputBufferOptimizationLevel (level) requested by the user can be applied.
 * It then notes the correct OutputBufferAllocationStrategy in the Emit operator of the pipeline.
 */
class BufferOptimizationPhase {
  public:
    /**
     * @brief Constructor to create a BufferOptimizationPhase
     */
    explicit BufferOptimizationPhase(OutputBufferOptimizationLevel level);

    /**
     * @brief Create a BufferOptimizationPhase
     */
    static BufferOptimizationPhasePtr create(OutputBufferOptimizationLevel level);

    /**
     * @brief Applies the phase on a pipelined query plan. Analyzes every pipeline to see if buffer optimization can be applied.
     * @param pipelined query plan
     * @return PipelineQueryPlanPtr
     */
    PipelineQueryPlanPtr apply(PipelineQueryPlanPtr pipelinedQueryPlan);

    /**
     * @brief Analyzes pipeline to see if buffer optimization can be applied.
     * @param pipeline
     * @return OperatorPipelinePtr
     */
    OperatorPipelinePtr apply(OperatorPipelinePtr pipeline);

  private:
    [[maybe_unused]] OutputBufferOptimizationLevel level;
};
}// namespace NES::QueryCompilation

#endif// NES_EXECUTION_INCLUDE_QUERYCOMPILER_PHASES_BUFFEROPTIMIZATIONPHASE_HPP_
