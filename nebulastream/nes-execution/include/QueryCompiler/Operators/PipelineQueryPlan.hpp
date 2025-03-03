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
#ifndef NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PIPELINEQUERYPLAN_HPP_
#define NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PIPELINEQUERYPLAN_HPP_

#include <Nodes/Node.hpp>

#include <Identifiers/Identifiers.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <memory>
#include <vector>

namespace NES::QueryCompilation {

/**
 * @brief Representation of a query plan, which consists of a set of OperatorPipelines.
 */
class PipelineQueryPlan {
  public:
    /**
     * @brief Creates a new pipelined query plan
     * @param sharedQueryId
     * @param decomposedQueryId
     * @return PipelineQueryPlanPtr
     */
    static PipelineQueryPlanPtr create(SharedQueryId sharedQueryId = INVALID_SHARED_QUERY_ID,
                                       DecomposedQueryId decomposedQueryId = INVALID_DECOMPOSED_QUERY_PLAN_ID);

    /**
     * @brief Add a pipeline to the query plan
     * @param pipeline
     */
    void addPipeline(const OperatorPipelinePtr& pipeline);

    /**
     * @brief Gets a list of source pipelines, which only contain a single physical source operator
     * @return std::vector<OperatorPipelinePtr>
     */
    [[nodiscard]] std::vector<OperatorPipelinePtr> getSourcePipelines() const;

    /**
     * @brief Gets a list of sink pipelines, which only contain a single physical sink operator
     * @return std::vector<OperatorPipelinePtr>
     */
    [[nodiscard]] std::vector<OperatorPipelinePtr> getSinkPipelines() const;

    /**
     * @brief Gets a list of all pipelines.
     * @return std::vector<OperatorPipelinePtr>
     */
    [[nodiscard]] std::vector<OperatorPipelinePtr> const& getPipelines() const;

    /**
     * @brief Remove a particular pipeline from the query plan
     * @param pipeline
     */
    void removePipeline(const OperatorPipelinePtr& pipeline);

    /**
     * @brief Gets the query id
     * @return QueryId
     */
    [[nodiscard]] SharedQueryId getQueryId() const;

    /**
     * @brief Gets the query sub plan id
     * @return QuerySubPlanId
     */
    [[nodiscard]] DecomposedQueryId getQuerySubPlanId() const;

    /**
     * @brief Creates a string representation of this PipelineQuery
     * @return std::string
     */
    std::string toString() const;

  private:
    PipelineQueryPlan(SharedQueryId sharedQueryId, DecomposedQueryId decomposedQueryId);
    const SharedQueryId sharedQueryId;
    const DecomposedQueryId decomposedQueryId;
    std::vector<OperatorPipelinePtr> pipelines;
};
}// namespace NES::QueryCompilation

#endif// NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PIPELINEQUERYPLAN_HPP_
