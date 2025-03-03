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

#include <API/QueryAPI.hpp>
#include <BaseIntegrationTest.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalJoinBuildOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalJoinSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalDemultiplexOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalFilterOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMapOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalProjectOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSourceOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnionOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalWatermarkAssignmentOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSlicePreAggregationOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalWindowSinkOperator.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/Phases/Pipelining/DefaultPipeliningPhase.hpp>
#include <QueryCompiler/Phases/Pipelining/FuseNonPipelineBreakerPolicy.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <iostream>

using namespace std;

namespace NES {

using namespace NES::API;
using namespace NES::QueryCompilation::PhysicalOperators;

class PipeliningPhaseTest : public Testing::BaseUnitTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("PipeliningPhaseTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup PipeliningPhaseTest test class.");
    }

  protected:
    ExpressionNodePtr pred1, pred2, pred3, pred4, pred5, pred6, pred7;
    LogicalOperatorPtr sourceOp1, sourceOp2, sourceOp3, sourceOp4, unionOp1;
    LogicalOperatorPtr watermarkAssigner1, centralWindowOperator, sliceCreationOperator, windowComputation, sliceMerging;
    LogicalOperatorPtr filterOp1, filterOp2, filterOp3, filterOp4, filterOp5, filterOp6, filterOp7;
    LogicalOperatorPtr sinkOp1, sinkOp2;
    LogicalOperatorPtr mapOp;
    LogicalOperatorPtr projectPp;
    LogicalJoinOperatorPtr joinOp1;
    static constexpr DecomposedQueryId defaultDecomposedQueryPlanId = INVALID_DECOMPOSED_QUERY_PLAN_ID;
    static constexpr SharedQueryId defaultSharedQueryId = INVALID_SHARED_QUERY_ID;
    std::atomic<StatisticId> statisticId = INVALID_STATISTIC_ID;
};

/**
 * @brief Input Query Plan:
 *
 * --- Physical Sink 1 --- Physical Filter --- Physical Source 1
 *
 * --- | Physical Sink 1 | --- | Physical Filter | --- | Physical Source 1 |
 *
 */
TEST_F(PipeliningPhaseTest, pipelineFilterQuery) {

    auto source = PhysicalSourceOperator::create(++statisticId, SchemaPtr(), SchemaPtr(), SourceDescriptorPtr());
    auto filter = PhysicalFilterOperator::create(++statisticId, SchemaPtr(), SchemaPtr(), ExpressionNodePtr());
    auto sink = PhysicalSinkOperator::create(++statisticId, SchemaPtr(), SchemaPtr(), SinkDescriptorPtr());
    auto queryPlan = QueryPlan::create(source);
    queryPlan->appendOperatorAsNewRoot(filter);
    queryPlan->appendOperatorAsNewRoot(sink);

    auto decomposedQueryPlan = DecomposedQueryPlan::create(defaultDecomposedQueryPlanId,
                                                           defaultSharedQueryId,
                                                           INVALID_WORKER_NODE_ID,
                                                           queryPlan->getRootOperators());

    NES_DEBUG("{}", queryPlan->toString());
    auto policy = QueryCompilation::FuseNonPipelineBreakerPolicy::create();
    auto phase = QueryCompilation::DefaultPipeliningPhase::create(policy);
    auto pipelinePlan = phase->apply(decomposedQueryPlan);

    auto sourcePipelines = pipelinePlan->getSourcePipelines();
    ASSERT_EQ(sourcePipelines.size(), 1u);
    auto sourcePipeline = sourcePipelines[0];

    ASSERT_INSTANCE_OF(sourcePipeline->getDecomposedQueryPlan()->getRootOperators()[0], PhysicalSourceOperator);
    auto filterPipe = sourcePipeline->getSuccessors()[0];
    ASSERT_INSTANCE_OF(filterPipe->getDecomposedQueryPlan()->getRootOperators()[0], PhysicalFilterOperator);
    auto sinkPipe = filterPipe->getSuccessors()[0];
    ASSERT_INSTANCE_OF(sinkPipe->getDecomposedQueryPlan()->getRootOperators()[0], PhysicalSinkOperator);
    ASSERT_EQ(sinkPipe->getSuccessors().size(), 0u);
    NES_DEBUG("{}", queryPlan->toString());
}

/**
 * @brief Input Query Plan:
 *
 * --- Physical Sink 1 --- Physical Filter --- Physical Map --- Physical Source 1
 *
 * --- | Physical Sink 1 | --- | Physical Filter - Physical Map | --- | Physical Source 1 |
 *
 */
TEST_F(PipeliningPhaseTest, pipelineFilterMapQuery) {
    auto source = PhysicalSourceOperator::create(++statisticId, SchemaPtr(), SchemaPtr(), SourceDescriptorPtr());
    auto filter = PhysicalFilterOperator::create(++statisticId, SchemaPtr(), SchemaPtr(), ExpressionNodePtr());
    auto map = PhysicalMapOperator::create(++statisticId, SchemaPtr(), SchemaPtr(), FieldAssignmentExpressionNodePtr());
    auto sink = PhysicalSinkOperator::create(++statisticId, SchemaPtr(), SchemaPtr(), SinkDescriptorPtr());
    auto queryPlan = QueryPlan::create(source);
    queryPlan->appendOperatorAsNewRoot(filter);
    queryPlan->appendOperatorAsNewRoot(map);
    queryPlan->appendOperatorAsNewRoot(sink);

    NES_DEBUG("{}", queryPlan->toString());

    auto decomposedQueryPlan = DecomposedQueryPlan::create(defaultDecomposedQueryPlanId,
                                                           defaultSharedQueryId,
                                                           INVALID_WORKER_NODE_ID,
                                                           queryPlan->getRootOperators());

    auto policy = QueryCompilation::FuseNonPipelineBreakerPolicy::create();
    auto phase = QueryCompilation::DefaultPipeliningPhase::create(policy);
    auto pipelinePlan = phase->apply(decomposedQueryPlan);

    auto sourcePipelines = pipelinePlan->getSourcePipelines();
    ASSERT_EQ(sourcePipelines.size(), 1U);
    auto sourcePipeline = sourcePipelines[0];
    ASSERT_INSTANCE_OF(sourcePipeline->getDecomposedQueryPlan()->getRootOperators()[0], PhysicalSourceOperator);
    auto filterPipe = sourcePipeline->getSuccessors()[0];
    ASSERT_INSTANCE_OF(filterPipe->getDecomposedQueryPlan()->getRootOperators()[0], PhysicalFilterOperator);
    auto sinkPipe = filterPipe->getSuccessors()[0];
    ASSERT_INSTANCE_OF(sinkPipe->getDecomposedQueryPlan()->getRootOperators()[0], PhysicalSinkOperator);
    ASSERT_EQ(sinkPipe->getSuccessors().size(), 0U);
}

/**
 * @brief Input Query Plan:
 *
 * --- Physical Sink 1 --- Physical Multiplex Operator --- Physical Source 1
 *                                                                        \
 *                                                                         --- Physical Source 2
 *
 * --- | Physical Sink 1 |  --- | Physical Source 1 |
 *                            \
 *                              --- | Physical Source 2 |
 *
 */
TEST_F(PipeliningPhaseTest, pipelineMultiplexQuery) {
    auto source1 = PhysicalSourceOperator::create(++statisticId, SchemaPtr(), SchemaPtr(), SourceDescriptorPtr());
    auto source2 = PhysicalSourceOperator::create(++statisticId, SchemaPtr(), SchemaPtr(), SourceDescriptorPtr());
    auto multiplex = PhysicalUnionOperator::create(++statisticId, SchemaPtr());
    auto sink = PhysicalSinkOperator::create(++statisticId, SchemaPtr(), SchemaPtr(), SinkDescriptorPtr());

    auto queryPlan = QueryPlan::create(source1);
    queryPlan->appendOperatorAsNewRoot(multiplex);
    queryPlan->appendOperatorAsNewRoot(sink);
    source2->addParent(multiplex);

    NES_DEBUG("{}", queryPlan->toString());
    auto decomposedQueryPlan = DecomposedQueryPlan::create(defaultDecomposedQueryPlanId,
                                                           defaultSharedQueryId,
                                                           INVALID_WORKER_NODE_ID,
                                                           queryPlan->getRootOperators());

    auto policy = QueryCompilation::FuseNonPipelineBreakerPolicy::create();
    auto phase = QueryCompilation::DefaultPipeliningPhase::create(policy);
    auto pipelinePlan = phase->apply(decomposedQueryPlan);

    auto sourcePipelines = pipelinePlan->getSourcePipelines();
    ASSERT_EQ(sourcePipelines.size(), 2U);
    auto sourcePipeline1 = sourcePipelines[0];

    ASSERT_INSTANCE_OF(sourcePipeline1->getDecomposedQueryPlan()->getRootOperators()[0], PhysicalSourceOperator);
    auto sourcePipeline2 = sourcePipelines[1];
    ASSERT_INSTANCE_OF(sourcePipeline2->getDecomposedQueryPlan()->getRootOperators()[0], PhysicalSourceOperator);
    ASSERT_EQ(sourcePipeline1->getSuccessors()[0], sourcePipeline2->getSuccessors()[0]);
    auto sinkPipe = sourcePipeline1->getSuccessors()[0];
    ASSERT_INSTANCE_OF(sinkPipe->getDecomposedQueryPlan()->getRootOperators()[0], PhysicalSinkOperator);
    ASSERT_EQ(sinkPipe->getSuccessors().size(), 0U);
}

/**
 * @brief Input Query Plan:
 *
 * --- Physical Sink 1 --- Physical Filter --- Physical Multiplex Operator --- Physical Source 1
 *                                                                        \
 *                                                                         --- Physical Source 2
 *
 * --- | Physical Sink 1 | --- | Physical Filter | --- | Physical Source 1 |
 *                                                \
 *                                                 --- | Physical Source 2 |
 *
 */
TEST_F(PipeliningPhaseTest, pipelineFilterMultiplexQuery) {
    auto source1 = PhysicalSourceOperator::create(++statisticId, SchemaPtr(), SchemaPtr(), SourceDescriptorPtr());
    auto source2 = PhysicalSourceOperator::create(++statisticId, SchemaPtr(), SchemaPtr(), SourceDescriptorPtr());
    auto multiplex = PhysicalUnionOperator::create(++statisticId, SchemaPtr());
    auto filter = PhysicalFilterOperator::create(++statisticId, SchemaPtr(), SchemaPtr(), ExpressionNodePtr());
    auto sink = PhysicalSinkOperator::create(++statisticId, SchemaPtr(), SchemaPtr(), SinkDescriptorPtr());

    auto queryPlan = QueryPlan::create(source1);
    queryPlan->appendOperatorAsNewRoot(multiplex);
    queryPlan->appendOperatorAsNewRoot(filter);
    queryPlan->appendOperatorAsNewRoot(sink);
    source2->addParent(multiplex);
    NES_DEBUG("{}", queryPlan->toString());

    auto decomposedQueryPlan = DecomposedQueryPlan::create(defaultDecomposedQueryPlanId,
                                                           defaultSharedQueryId,
                                                           INVALID_WORKER_NODE_ID,
                                                           queryPlan->getRootOperators());

    auto policy = QueryCompilation::FuseNonPipelineBreakerPolicy::create();
    auto phase = QueryCompilation::DefaultPipeliningPhase::create(policy);
    auto pipelinePlan = phase->apply(decomposedQueryPlan);

    auto sourcePipelines = pipelinePlan->getSourcePipelines();
    ASSERT_EQ(sourcePipelines.size(), 2U);
    auto sourcePipeline1 = sourcePipelines[0];

    ASSERT_INSTANCE_OF(sourcePipeline1->getDecomposedQueryPlan()->getRootOperators()[0], PhysicalSourceOperator);
    auto sourcePipeline2 = sourcePipelines[1];
    ASSERT_INSTANCE_OF(sourcePipeline2->getDecomposedQueryPlan()->getRootOperators()[0], PhysicalSourceOperator);
    ASSERT_EQ(sourcePipeline1->getSuccessors()[0], sourcePipeline2->getSuccessors()[0]);
    auto filterPipe = sourcePipeline1->getSuccessors()[0];
    ASSERT_INSTANCE_OF(filterPipe->getDecomposedQueryPlan()->getRootOperators()[0], PhysicalFilterOperator);
    auto sinkPipe = filterPipe->getSuccessors()[0];
    ASSERT_INSTANCE_OF(sinkPipe->getDecomposedQueryPlan()->getRootOperators()[0], PhysicalSinkOperator);
    ASSERT_EQ(sinkPipe->getSuccessors().size(), 0U);
}

/**
 * @brief Input Query Plan:
 *
 * --- Physical Sink 1  --- Physical Join Sink --- Physical Join Build --- Physical Source 1
 *                                             \
 *                                             --- Physical Join Build --- Physical Source 2
 *
 * --- | Physical Sink 1 | --- | Physical Join Sink | --- | Physical Join Build --- Physical Source 1 |
 *                                                  \
 *                                                    --- | Physical Join Build --- Physical Source 2 |
 *
 */
TEST_F(PipeliningPhaseTest, pipelineJoinQuery) {
    auto source1 = PhysicalSourceOperator::create(++statisticId, SchemaPtr(), SchemaPtr(), SourceDescriptorPtr());
    auto source2 = PhysicalSourceOperator::create(++statisticId, SchemaPtr(), SchemaPtr(), SourceDescriptorPtr());
    auto joinBuildLeft = PhysicalJoinBuildOperator::create(++statisticId,
                                                           SchemaPtr(),
                                                           SchemaPtr(),
                                                           Join::JoinOperatorHandlerPtr(),
                                                           QueryCompilation::JoinBuildSideType::Left);
    auto joinBuildRight = PhysicalJoinBuildOperator::create(++statisticId,
                                                            SchemaPtr(),
                                                            SchemaPtr(),
                                                            Join::JoinOperatorHandlerPtr(),
                                                            QueryCompilation::JoinBuildSideType::Right);
    auto joinSink =
        PhysicalJoinSinkOperator::create(++statisticId, SchemaPtr(), SchemaPtr(), SchemaPtr(), Join::JoinOperatorHandlerPtr());
    auto sink = PhysicalSinkOperator::create(++statisticId, SchemaPtr(), SchemaPtr(), SinkDescriptorPtr());

    auto queryPlan = QueryPlan::create(source1);
    queryPlan->appendOperatorAsNewRoot(joinBuildLeft);
    queryPlan->appendOperatorAsNewRoot(joinSink);
    queryPlan->appendOperatorAsNewRoot(sink);
    source2->addParent(joinBuildRight);
    joinBuildRight->addParent(joinSink);

    NES_DEBUG("{}", queryPlan->toString());

    auto decomposedQueryPlan = DecomposedQueryPlan::create(defaultDecomposedQueryPlanId,
                                                           defaultSharedQueryId,
                                                           INVALID_WORKER_NODE_ID,
                                                           queryPlan->getRootOperators());

    auto policy = QueryCompilation::FuseNonPipelineBreakerPolicy::create();
    auto phase = QueryCompilation::DefaultPipeliningPhase::create(policy);
    auto pipelinePlan = phase->apply(decomposedQueryPlan);
    auto sourcePipelines = pipelinePlan->getSourcePipelines();
    ASSERT_EQ(sourcePipelines.size(), 2U);
    auto sourcePipeline1 = sourcePipelines[0];
    auto sourcePipeline2 = sourcePipelines[1];
    ASSERT_INSTANCE_OF(sourcePipeline1->getDecomposedQueryPlan()->getRootOperators()[0], PhysicalSourceOperator);
    auto leftJoinBuildPipeline = sourcePipeline1->getSuccessors()[0];
    ASSERT_INSTANCE_OF(leftJoinBuildPipeline->getDecomposedQueryPlan()->getRootOperators()[0], PhysicalJoinBuildOperator);
    auto rightJoinBuildPipeline = sourcePipeline2->getSuccessors()[0];
    ASSERT_INSTANCE_OF(rightJoinBuildPipeline->getDecomposedQueryPlan()->getRootOperators()[0], PhysicalJoinBuildOperator);
    // check if both join pipelines have the same successor
    auto joinSinkPipeline = leftJoinBuildPipeline->getSuccessors()[0];
    ASSERT_EQ(joinSinkPipeline, rightJoinBuildPipeline->getSuccessors()[0]);
    // join build should have to predecessors
    ASSERT_EQ(joinSinkPipeline->getPredecessors().size(), 2U);
    ASSERT_INSTANCE_OF(joinSinkPipeline->getDecomposedQueryPlan()->getRootOperators()[0], PhysicalJoinSinkOperator);
    auto sinkPipe = joinSinkPipeline->getSuccessors()[0];
    ASSERT_INSTANCE_OF(sinkPipe->getDecomposedQueryPlan()->getRootOperators()[0], PhysicalSinkOperator);
    ASSERT_EQ(sinkPipe->getSuccessors().size(), 0U);
}

/**
 * @brief Input Query Plan:
 *
 * --- Physical Sink 1  --- Physical Join Sink --- Physical Join Build --- Physical Source 1
 *                                             \
 *                                             --- Physical Join Build --- Physical Multiplex Operator  --- Physical Source 2
 *                                                                                                      \
 *                                                                                                       --- Physical Source 3
 */
TEST_F(PipeliningPhaseTest, pipelineJoinWithMultiplexQuery) {
    auto source1 = PhysicalSourceOperator::create(++statisticId, SchemaPtr(), SchemaPtr(), SourceDescriptorPtr());
    auto source2 = PhysicalSourceOperator::create(++statisticId, SchemaPtr(), SchemaPtr(), SourceDescriptorPtr());
    auto source3 = PhysicalSourceOperator::create(++statisticId, SchemaPtr(), SchemaPtr(), SourceDescriptorPtr());
    auto multiplex = PhysicalUnionOperator::create(++statisticId, SchemaPtr());
    auto joinBuildLeft = PhysicalJoinBuildOperator::create(++statisticId,
                                                           SchemaPtr(),
                                                           SchemaPtr(),
                                                           Join::JoinOperatorHandlerPtr(),
                                                           QueryCompilation::JoinBuildSideType::Left);
    auto joinBuildRight = PhysicalJoinBuildOperator::create(++statisticId,
                                                            SchemaPtr(),
                                                            SchemaPtr(),
                                                            Join::JoinOperatorHandlerPtr(),
                                                            QueryCompilation::JoinBuildSideType::Right);
    auto joinSink =
        PhysicalJoinSinkOperator::create(++statisticId, SchemaPtr(), SchemaPtr(), SchemaPtr(), Join::JoinOperatorHandlerPtr());
    auto sink = PhysicalSinkOperator::create(++statisticId, SchemaPtr(), SchemaPtr(), SinkDescriptorPtr());

    auto queryPlan = QueryPlan::create(source1);
    queryPlan->appendOperatorAsNewRoot(joinBuildLeft);
    queryPlan->appendOperatorAsNewRoot(joinSink);
    queryPlan->appendOperatorAsNewRoot(sink);
    source2->addParent(multiplex);
    source3->addParent(multiplex);
    multiplex->addParent(joinBuildRight);
    joinBuildRight->addParent(joinSink);

    NES_DEBUG("{}", queryPlan->toString());

    auto decomposedQueryPlan = DecomposedQueryPlan::create(defaultDecomposedQueryPlanId,
                                                           defaultSharedQueryId,
                                                           INVALID_WORKER_NODE_ID,
                                                           queryPlan->getRootOperators());

    auto policy = QueryCompilation::FuseNonPipelineBreakerPolicy::create();
    auto phase = QueryCompilation::DefaultPipeliningPhase::create(policy);
    auto pipelinePlan = phase->apply(decomposedQueryPlan);
    auto sourcePipelines = pipelinePlan->getSourcePipelines();

    ASSERT_EQ(sourcePipelines.size(), 3U);
    auto sourcePipeline1 = sourcePipelines[0];
    auto sourcePipeline2 = sourcePipelines[1];
    auto sourcePipeline3 = sourcePipelines[2];
    ASSERT_INSTANCE_OF(sourcePipeline1->getDecomposedQueryPlan()->getRootOperators()[0], PhysicalSourceOperator);
    ASSERT_INSTANCE_OF(sourcePipeline2->getDecomposedQueryPlan()->getRootOperators()[0], PhysicalSourceOperator);
    ASSERT_INSTANCE_OF(sourcePipeline3->getDecomposedQueryPlan()->getRootOperators()[0], PhysicalSourceOperator);

    auto leftJoinBuildPipeline = sourcePipeline1->getSuccessors()[0];
    ASSERT_INSTANCE_OF(leftJoinBuildPipeline->getDecomposedQueryPlan()->getRootOperators()[0], PhysicalJoinBuildOperator);
    // check that source 2 and 3 have the same successors
    ASSERT_EQ(sourcePipeline2->getSuccessors()[0], sourcePipeline3->getSuccessors()[0]);
    auto rightJoinBuildPipeline = sourcePipeline2->getSuccessors()[0];
    // right build pipeline must have two predecessors
    ASSERT_EQ(rightJoinBuildPipeline->getPredecessors().size(), 2U);
    // check if both join pipelines have the same successor
    auto joinSinkPipeline = leftJoinBuildPipeline->getSuccessors()[0];
    ASSERT_EQ(joinSinkPipeline, rightJoinBuildPipeline->getSuccessors()[0]);
    ASSERT_EQ(joinSinkPipeline->getPredecessors().size(), 2U);
    ASSERT_INSTANCE_OF(joinSinkPipeline->getDecomposedQueryPlan()->getRootOperators()[0], PhysicalJoinSinkOperator);
    auto sinkPipe = joinSinkPipeline->getSuccessors()[0];
    ASSERT_INSTANCE_OF(sinkPipe->getDecomposedQueryPlan()->getRootOperators()[0], PhysicalSinkOperator);
    ASSERT_EQ(sinkPipe->getSuccessors().size(), 0U);
}

/**
 *  Result Query plan:
 *
 * --- Physical Sink 1  --- Physical Window Sink --- Physical Window Pre Aggregation Operator --- Physical Watermark Assigner --- Physical Source 1
 *
 * --- | Physical Sink 1 | --- | Physical Window Sink | --- | Physical Window Pre Aggregation --- Watermark Assigner | --- | Physical Source 1 |
 *
 */
TEST_F(PipeliningPhaseTest, pipelineWindowQuery) {
    auto source1 = PhysicalSourceOperator::create(++statisticId, SchemaPtr(), SchemaPtr(), SourceDescriptorPtr());
    auto windowAssignment =
        PhysicalWatermarkAssignmentOperator::create(++statisticId, SchemaPtr(), SchemaPtr(), WatermarkStrategyDescriptorPtr());
    auto slicePreAggregation = PhysicalSlicePreAggregationOperator::create(getNextOperatorId(),
                                                                           ++statisticId,
                                                                           SchemaPtr(),
                                                                           SchemaPtr(),
                                                                           LogicalWindowDescriptorPtr());
    auto windowSink = PhysicalWindowSinkOperator::create(getNextOperatorId(),
                                                         ++statisticId,
                                                         SchemaPtr(),
                                                         SchemaPtr(),
                                                         LogicalWindowDescriptorPtr());
    auto sink = PhysicalSinkOperator::create(++statisticId, SchemaPtr(), SchemaPtr(), SinkDescriptorPtr());

    auto queryPlan = QueryPlan::create(source1);
    queryPlan->appendOperatorAsNewRoot(windowAssignment);
    queryPlan->appendOperatorAsNewRoot(slicePreAggregation);
    queryPlan->appendOperatorAsNewRoot(windowSink);
    queryPlan->appendOperatorAsNewRoot(sink);

    NES_DEBUG("{}", queryPlan->toString());

    auto decomposedQueryPlan = DecomposedQueryPlan::create(defaultDecomposedQueryPlanId,
                                                           defaultSharedQueryId,
                                                           INVALID_WORKER_NODE_ID,
                                                           queryPlan->getRootOperators());

    auto policy = QueryCompilation::FuseNonPipelineBreakerPolicy::create();
    auto phase = QueryCompilation::DefaultPipeliningPhase::create(policy);
    auto pipelinePlan = phase->apply(decomposedQueryPlan);

    auto sourcePipelines = pipelinePlan->getSourcePipelines();
    ASSERT_EQ(sourcePipelines.size(), 1U);

    auto sourcePipeline1 = sourcePipelines[0];
    ASSERT_INSTANCE_OF(sourcePipeline1->getDecomposedQueryPlan()->getRootOperators()[0], PhysicalSourceOperator);

    auto preAggregationPipeline = sourcePipeline1->getSuccessors()[0];
    ASSERT_INSTANCE_OF(preAggregationPipeline->getDecomposedQueryPlan()->getRootOperators()[0],
                       PhysicalWatermarkAssignmentOperator);

    auto windowSinkPipeline = preAggregationPipeline->getSuccessors()[0];
    ASSERT_INSTANCE_OF(windowSinkPipeline->getDecomposedQueryPlan()->getRootOperators()[0], PhysicalWindowSinkOperator);

    auto sinkPipe = windowSinkPipeline->getSuccessors()[0];
    ASSERT_INSTANCE_OF(sinkPipe->getDecomposedQueryPlan()->getRootOperators()[0], PhysicalSinkOperator);
    ASSERT_EQ(sinkPipe->getSuccessors().size(), 0U);
}

/**
 *  Result Query plan:
 *
 * --- Physical Sink 1 --- Physical Map Operator --- Physical Filter Operator --- Physical Project Operator --- Physical Source 1
 *
 * --- | Physical Sink 1 | --- | Physical Map --- Physical Filter --- Physical Project |--- | Physical Source 1 |
 *
 */
TEST_F(PipeliningPhaseTest, pipelineMapFilterProjectQuery) {
    auto source = PhysicalSourceOperator::create(++statisticId, SchemaPtr(), SchemaPtr(), SourceDescriptorPtr());
    auto project = PhysicalProjectOperator::create(++statisticId, SchemaPtr(), SchemaPtr(), std::vector<ExpressionNodePtr>());
    auto filter = PhysicalFilterOperator::create(++statisticId, SchemaPtr(), SchemaPtr(), ExpressionNodePtr());
    auto map = PhysicalMapOperator::create(++statisticId, SchemaPtr(), SchemaPtr(), FieldAssignmentExpressionNodePtr());
    auto sink = PhysicalSinkOperator::create(++statisticId, SchemaPtr(), SchemaPtr(), SinkDescriptorPtr());

    auto queryPlan = QueryPlan::create(source);
    queryPlan->appendOperatorAsNewRoot(project);
    queryPlan->appendOperatorAsNewRoot(filter);
    queryPlan->appendOperatorAsNewRoot(map);
    queryPlan->appendOperatorAsNewRoot(sink);

    NES_DEBUG("{}", queryPlan->toString());

    auto decomposedQueryPlan = DecomposedQueryPlan::create(defaultDecomposedQueryPlanId,
                                                           defaultSharedQueryId,
                                                           INVALID_WORKER_NODE_ID,
                                                           queryPlan->getRootOperators());

    auto policy = QueryCompilation::FuseNonPipelineBreakerPolicy::create();
    auto phase = QueryCompilation::DefaultPipeliningPhase::create(policy);
    auto pipelinePlan = phase->apply(decomposedQueryPlan);

    auto sourcePipelines = pipelinePlan->getSourcePipelines();
    ASSERT_EQ(sourcePipelines.size(), 1U);

    auto sourcePipeline = sourcePipelines[0];
    ASSERT_INSTANCE_OF(sourcePipeline->getDecomposedQueryPlan()->getRootOperators()[0], PhysicalSourceOperator);

    auto projectFilterMapPipeline = sourcePipeline->getSuccessors()[0];
    ASSERT_INSTANCE_OF(projectFilterMapPipeline->getDecomposedQueryPlan()->getRootOperators()[0], PhysicalProjectOperator);

    auto sinkPipe = projectFilterMapPipeline->getSuccessors()[0];
    ASSERT_INSTANCE_OF(sinkPipe->getDecomposedQueryPlan()->getRootOperators()[0], PhysicalSinkOperator);
    ASSERT_EQ(sinkPipe->getSuccessors().size(), 0U);
}

/**
 * Result Query plan:
 *
 * --- Physical Sink 1 --- Physical Demultiplex Operator --- Physical Filter -- Physical Source 1
 *                      /
 *                     /
 * --- Physical Sink 2
 *
 *
 * --- | Physical Sink 1 | --- | Physical Filter |--- | Physical Source 1 |
 *                          /
 * --- | Physical Sink 2 | -
 *
 *
 */
TEST_F(PipeliningPhaseTest, pipelineDemultiplex) {
    auto source = PhysicalSourceOperator::create(++statisticId, SchemaPtr(), SchemaPtr(), SourceDescriptorPtr());
    auto filter = PhysicalFilterOperator::create(++statisticId, SchemaPtr(), SchemaPtr(), ExpressionNodePtr());
    auto demultiplex = PhysicalDemultiplexOperator::create(++statisticId, SchemaPtr());
    auto sink1 = PhysicalSinkOperator::create(++statisticId, SchemaPtr(), SchemaPtr(), SinkDescriptorPtr());
    auto sink2 = PhysicalSinkOperator::create(++statisticId, SchemaPtr(), SchemaPtr(), SinkDescriptorPtr());

    auto queryPlan = QueryPlan::create(source);
    queryPlan->appendOperatorAsNewRoot(filter);
    queryPlan->appendOperatorAsNewRoot(demultiplex);
    queryPlan->appendOperatorAsNewRoot(sink1);
    queryPlan->addRootOperator(sink2);
    sink2->addChild(demultiplex);

    NES_DEBUG("{}", queryPlan->toString());

    auto decomposedQueryPlan = DecomposedQueryPlan::create(defaultDecomposedQueryPlanId,
                                                           defaultSharedQueryId,
                                                           INVALID_WORKER_NODE_ID,
                                                           queryPlan->getRootOperators());

    auto policy = QueryCompilation::FuseNonPipelineBreakerPolicy::create();
    auto phase = QueryCompilation::DefaultPipeliningPhase::create(policy);
    auto pipelinePlan = phase->apply(decomposedQueryPlan);

    auto sourcePipelines = pipelinePlan->getSourcePipelines();
    ASSERT_EQ(sourcePipelines.size(), 1U);

    auto sourcePipeline = sourcePipelines[0];
    ASSERT_INSTANCE_OF(sourcePipeline->getDecomposedQueryPlan()->getRootOperators()[0], PhysicalSourceOperator);

    auto projectFilterPipeline = sourcePipeline->getSuccessors()[0];
    ASSERT_INSTANCE_OF(projectFilterPipeline->getDecomposedQueryPlan()->getRootOperators()[0], PhysicalFilterOperator);

    // The filter pipeline should have two successors
    ASSERT_EQ(projectFilterPipeline->getSuccessors().size(), 2U);

    auto sinkPipeline1 = projectFilterPipeline->getSuccessors()[0];
    ASSERT_INSTANCE_OF(sinkPipeline1->getDecomposedQueryPlan()->getRootOperators()[0], PhysicalSinkOperator);

    auto sinkPipeline2 = projectFilterPipeline->getSuccessors()[1];
    ASSERT_INSTANCE_OF(sinkPipeline2->getDecomposedQueryPlan()->getRootOperators()[0], PhysicalSinkOperator);
}

}// namespace NES
