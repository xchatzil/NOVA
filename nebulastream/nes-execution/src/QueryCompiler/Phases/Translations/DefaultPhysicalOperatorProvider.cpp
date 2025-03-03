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
#include <Execution/Operators/Streaming/Aggregations/Buckets/KeyedBucketPreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/Bucketing/HJOperatorHandlerBucketing.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/Slicing/HJOperatorHandlerSlicing.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/Bucketing/NLJOperatorHandlerBucketing.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/Slicing/NLJOperatorHandlerSlicing.hpp>
#include <Execution/Operators/Streaming/StatisticCollection/CountMin/CountMinBuild.hpp>
#include <Execution/Operators/Streaming/StatisticCollection/CountMin/CountMinOperatorHandler.hpp>
#include <Expressions/BinaryExpressionNode.hpp>
#include <Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Measures/TimeCharacteristic.hpp>
#include <Operators/LogicalOperators/LogicalFilterOperator.hpp>
#include <Operators/LogicalOperators/LogicalInferModelOperator.hpp>
#include <Operators/LogicalOperators/LogicalLimitOperator.hpp>
#include <Operators/LogicalOperators/LogicalMapOperator.hpp>
#include <Operators/LogicalOperators/LogicalProjectionOperator.hpp>
#include <Operators/LogicalOperators/LogicalUnionOperator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Descriptor/CountMinDescriptor.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Descriptor/HyperLogLogDescriptor.hpp>
#include <Operators/LogicalOperators/StatisticCollection/LogicalStatisticWindowOperator.hpp>
#include <Operators/LogicalOperators/UDFs/FlatMapUDF/FlatMapUDFLogicalOperator.hpp>
#include <Operators/LogicalOperators/UDFs/MapUDF/MapUDFLogicalOperator.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinOperator.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowOperator.hpp>
#include <Operators/LogicalOperators/Windows/WindowOperator.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <QueryCompiler/Exceptions/QueryCompilationException.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/Streaming/PhysicalStreamJoinBuildOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/Streaming/PhysicalStreamJoinProbeOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalDemultiplexOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalFilterOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalFlatMapUDFOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalInferModelOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalLimitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMapOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMapUDFOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalProjectOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSourceOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnionOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalWatermarkAssignmentOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/StatisticCollection/PhysicalCountMinBuildOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/StatisticCollection/PhysicalHyperLogLogBuildOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/ContentBasedWindow/PhysicalThresholdWindowOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSliceMergingOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSlicePreAggregationOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalWindowSinkOperator.hpp>
#include <QueryCompiler/Phases/Translations/DefaultPhysicalOperatorProvider.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Sinks/Formats/StatisticCollection/CountMinStatisticFormat.hpp>
#include <StatisticCollection/StatisticStorage/DefaultStatisticStore.hpp>
#include <Types/ContentBasedWindowType.hpp>
#include <Types/SlidingWindow.hpp>
#include <Types/ThresholdWindow.hpp>
#include <Types/TimeBasedWindowType.hpp>
#include <Types/TumblingWindow.hpp>
#include <Types/WindowType.hpp>
#include <Util/CompilerConstants.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UtilityFunction.hpp>
#include <utility>

namespace NES::QueryCompilation {

DefaultPhysicalOperatorProvider::DefaultPhysicalOperatorProvider(QueryCompilerOptionsPtr options)
    : PhysicalOperatorProvider(std::move(options)){};

PhysicalOperatorProviderPtr DefaultPhysicalOperatorProvider::create(const QueryCompilerOptionsPtr& options) {
    return std::make_shared<DefaultPhysicalOperatorProvider>(options);
}

bool DefaultPhysicalOperatorProvider::isDemultiplex(const LogicalOperatorPtr& operatorNode) {
    return operatorNode->getParents().size() > 1;
}

void DefaultPhysicalOperatorProvider::insertDemultiplexOperatorsBefore(const LogicalOperatorPtr& operatorNode) {
    auto operatorOutputSchema = operatorNode->getOutputSchema();
    // A demultiplex operator has the same output schema as its child operator.
    auto demultiplexOperator =
        PhysicalOperators::PhysicalDemultiplexOperator::create(operatorNode->getStatisticId(), operatorOutputSchema);
    demultiplexOperator->setOutputSchema(operatorNode->getOutputSchema());
    operatorNode->insertBetweenThisAndParentNodes(demultiplexOperator);
}

void DefaultPhysicalOperatorProvider::insertMultiplexOperatorsAfter(const LogicalOperatorPtr& operatorNode) {
    // the unionOperator operator has the same schema as the output schema of the operator node.
    auto unionOperator =
        PhysicalOperators::PhysicalUnionOperator::create(operatorNode->getStatisticId(), operatorNode->getOutputSchema());
    operatorNode->insertBetweenThisAndChildNodes(unionOperator);
}

void DefaultPhysicalOperatorProvider::lower(DecomposedQueryPlanPtr decomposedQueryPlan, LogicalOperatorPtr operatorNode) {
    if (isDemultiplex(operatorNode)) {
        insertDemultiplexOperatorsBefore(operatorNode);
    }

    if (operatorNode->instanceOf<UnaryOperator>()) {
        lowerUnaryOperator(decomposedQueryPlan, operatorNode);
    } else if (operatorNode->instanceOf<BinaryOperator>()) {
        lowerBinaryOperator(operatorNode);
    } else {
        NES_NOT_IMPLEMENTED();
    }

    NES_DEBUG("DefaultPhysicalOperatorProvider:: Plan after lowering \n{}", decomposedQueryPlan->toString());
}

void DefaultPhysicalOperatorProvider::lowerUnaryOperator(const DecomposedQueryPlanPtr& decomposedQueryPlan,
                                                         const LogicalOperatorPtr& operatorNode) {

    // If a unary operator has more than one parent, we introduce an implicit multiplex operator before.
    if (operatorNode->getChildren().size() > 1) {
        insertMultiplexOperatorsAfter(operatorNode);
    }

    if (operatorNode->getParents().size() > 1) {
        throw QueryCompilationException("A unary operator should only have at most one parent.");
    }

    if (operatorNode->instanceOf<SourceLogicalOperator>()) {
        auto logicalSourceOperator = operatorNode->as<SourceLogicalOperator>();
        auto physicalSourceOperator =
            PhysicalOperators::PhysicalSourceOperator::create(getNextOperatorId(),
                                                              operatorNode->getStatisticId(),
                                                              logicalSourceOperator->getOriginId(),
                                                              logicalSourceOperator->getInputSchema(),
                                                              logicalSourceOperator->getOutputSchema(),
                                                              logicalSourceOperator->getSourceDescriptor());
        physicalSourceOperator->addProperty(LOGICAL_OPERATOR_ID_KEY, operatorNode->getProperty(LOGICAL_OPERATOR_ID_KEY));
        operatorNode->replace(physicalSourceOperator);
    } else if (operatorNode->instanceOf<SinkLogicalOperator>()) {
        auto logicalSinkOperator = operatorNode->as<SinkLogicalOperator>();

        auto physicalSinkOperator = PhysicalOperators::PhysicalSinkOperator::create(operatorNode->getStatisticId(),
                                                                                    logicalSinkOperator->getInputSchema(),
                                                                                    logicalSinkOperator->getOutputSchema(),
                                                                                    logicalSinkOperator->getSinkDescriptor());
        physicalSinkOperator->addProperty(LOGICAL_OPERATOR_ID_KEY, operatorNode->getProperty(LOGICAL_OPERATOR_ID_KEY));
        operatorNode->replace(physicalSinkOperator);
        decomposedQueryPlan->replaceRootOperator(logicalSinkOperator, physicalSinkOperator);
    } else if (operatorNode->instanceOf<LogicalFilterOperator>()) {
        auto filterOperator = operatorNode->as<LogicalFilterOperator>();
        auto physicalFilterOperator = PhysicalOperators::PhysicalFilterOperator::create(operatorNode->getStatisticId(),
                                                                                        filterOperator->getInputSchema(),
                                                                                        filterOperator->getOutputSchema(),
                                                                                        filterOperator->getPredicate());
        physicalFilterOperator->addProperty(LOGICAL_OPERATOR_ID_KEY, operatorNode->getProperty(LOGICAL_OPERATOR_ID_KEY));
        operatorNode->replace(physicalFilterOperator);
    } else if (operatorNode->instanceOf<WindowOperator>()) {
        lowerWindowOperator(operatorNode);
    } else if (operatorNode->instanceOf<WatermarkAssignerLogicalOperator>()) {
        lowerWatermarkAssignmentOperator(operatorNode);
    } else if (operatorNode->instanceOf<LogicalMapOperator>()) {
        lowerMapOperator(operatorNode);
    } else if (operatorNode->instanceOf<InferModel::LogicalInferModelOperator>()) {
        lowerInferModelOperator(operatorNode);
    } else if (operatorNode->instanceOf<LogicalProjectionOperator>()) {
        lowerProjectOperator(operatorNode);
    } else if (operatorNode->instanceOf<MapUDFLogicalOperator>()) {
        lowerUDFMapOperator(operatorNode);
    } else if (operatorNode->instanceOf<FlatMapUDFLogicalOperator>()) {
        lowerUDFFlatMapOperator(operatorNode);
    } else if (operatorNode->instanceOf<LogicalLimitOperator>()) {
        auto limitOperator = operatorNode->as<LogicalLimitOperator>();
        auto physicalLimitOperator = PhysicalOperators::PhysicalLimitOperator::create(operatorNode->getStatisticId(),
                                                                                      limitOperator->getInputSchema(),
                                                                                      limitOperator->getOutputSchema(),
                                                                                      limitOperator->getLimit());
        physicalLimitOperator->addProperty(LOGICAL_OPERATOR_ID_KEY, operatorNode->getProperty(LOGICAL_OPERATOR_ID_KEY));
        operatorNode->replace(physicalLimitOperator);
    } else if (operatorNode->instanceOf<Statistic::LogicalStatisticWindowOperator>()) {
        lowerStatisticBuildOperator(*operatorNode->as<Statistic::LogicalStatisticWindowOperator>());
    } else {
        throw QueryCompilationException("No conversion for operator " + operatorNode->toString() + " was provided.");
    }
}

void DefaultPhysicalOperatorProvider::lowerStatisticBuildOperator(
    Statistic::LogicalStatisticWindowOperator& logicalStatisticWindowOperator) {
    const auto statisticDescriptor = logicalStatisticWindowOperator.getWindowStatisticDescriptor();
    if (statisticDescriptor->instanceOf<Statistic::CountMinDescriptor>()) {
        const auto countMinDescriptor = statisticDescriptor->as<Statistic::CountMinDescriptor>();
        auto physicalCountMinBuildOperator =
            PhysicalOperators::PhysicalCountMinBuildOperator::create(logicalStatisticWindowOperator.getStatisticId(),
                                                                     logicalStatisticWindowOperator.getInputSchema(),
                                                                     logicalStatisticWindowOperator.getOutputSchema(),
                                                                     countMinDescriptor->getField()->getFieldName(),
                                                                     countMinDescriptor->getWidth(),
                                                                     countMinDescriptor->getDepth(),
                                                                     logicalStatisticWindowOperator.getMetricHash(),
                                                                     logicalStatisticWindowOperator.getWindowType(),
                                                                     logicalStatisticWindowOperator.getSendingPolicy());
        physicalCountMinBuildOperator->as<UnaryOperator>()->setInputOriginIds(logicalStatisticWindowOperator.getInputOriginIds());
        logicalStatisticWindowOperator.replace(physicalCountMinBuildOperator);
    } else if (statisticDescriptor->instanceOf<Statistic::HyperLogLogDescriptor>()) {
        const auto hyperLogLogDescriptor = statisticDescriptor->as<Statistic::HyperLogLogDescriptor>();
        auto physicalHyperLogLogBuildOperator =
            PhysicalOperators::PhysicalHyperLogLogBuildOperator::create(logicalStatisticWindowOperator.getStatisticId(),
                                                                        logicalStatisticWindowOperator.getInputSchema(),
                                                                        logicalStatisticWindowOperator.getOutputSchema(),
                                                                        hyperLogLogDescriptor->getField()->getFieldName(),
                                                                        hyperLogLogDescriptor->getWidth(),
                                                                        logicalStatisticWindowOperator.getMetricHash(),
                                                                        logicalStatisticWindowOperator.getWindowType(),
                                                                        logicalStatisticWindowOperator.getSendingPolicy());
        physicalHyperLogLogBuildOperator->as<UnaryOperator>()->setInputOriginIds(
            logicalStatisticWindowOperator.getInputOriginIds());
        logicalStatisticWindowOperator.replace(physicalHyperLogLogBuildOperator);
    } else {
        NES_ERROR("We currently only support a CountMinStatisticDescriptor or HyperLogLogDescriptorStatisticDescriptor")
        NES_NOT_IMPLEMENTED();
    }
}

void DefaultPhysicalOperatorProvider::lowerBinaryOperator(const LogicalOperatorPtr& operatorNode) {
    if (operatorNode->instanceOf<LogicalUnionOperator>()) {
        lowerUnionOperator(operatorNode);
    } else if (operatorNode->instanceOf<LogicalJoinOperator>()) {
        lowerJoinOperator(operatorNode);
    } else {
        throw QueryCompilationException("No conversion for operator " + operatorNode->toString() + " was provided.");
    }
}

void DefaultPhysicalOperatorProvider::lowerUnionOperator(const LogicalOperatorPtr& operatorNode) {

    auto unionOperator = operatorNode->as<LogicalUnionOperator>();
    // this assumes that we apply the ProjectBeforeUnionRule and the input across all children is the same.
    if (!unionOperator->getLeftInputSchema()->equals(unionOperator->getRightInputSchema())) {
        throw QueryCompilationException("The children of a union operator should have the same schema. Left:"
                                        + unionOperator->getLeftInputSchema()->toString() + " but right "
                                        + unionOperator->getRightInputSchema()->toString());
    }
    auto physicalUnionOperator =
        PhysicalOperators::PhysicalUnionOperator::create(operatorNode->getStatisticId(), unionOperator->getLeftInputSchema());
    operatorNode->replace(physicalUnionOperator);
}

void DefaultPhysicalOperatorProvider::lowerProjectOperator(const LogicalOperatorPtr& operatorNode) {
    auto projectOperator = operatorNode->as<LogicalProjectionOperator>();
    auto physicalProjectOperator = PhysicalOperators::PhysicalProjectOperator::create(operatorNode->getStatisticId(),
                                                                                      projectOperator->getInputSchema(),
                                                                                      projectOperator->getOutputSchema(),
                                                                                      projectOperator->getExpressions());
    physicalProjectOperator->addProperty(LOGICAL_OPERATOR_ID_KEY, operatorNode->getProperty(LOGICAL_OPERATOR_ID_KEY));
    operatorNode->replace(physicalProjectOperator);
}

void DefaultPhysicalOperatorProvider::lowerInferModelOperator(LogicalOperatorPtr operatorNode) {
    auto inferModelOperator = operatorNode->as<InferModel::LogicalInferModelOperator>();
    auto physicalInferModelOperator =
        PhysicalOperators::PhysicalInferModelOperator::create(operatorNode->getStatisticId(),
                                                              inferModelOperator->getInputSchema(),
                                                              inferModelOperator->getOutputSchema(),
                                                              inferModelOperator->getModel(),
                                                              inferModelOperator->getInputFields(),
                                                              inferModelOperator->getOutputFields());
    operatorNode->replace(physicalInferModelOperator);
}

void DefaultPhysicalOperatorProvider::lowerMapOperator(const LogicalOperatorPtr& operatorNode) {
    auto mapOperator = operatorNode->as<LogicalMapOperator>();
    auto physicalMapOperator = PhysicalOperators::PhysicalMapOperator::create(operatorNode->getStatisticId(),
                                                                              mapOperator->getInputSchema(),
                                                                              mapOperator->getOutputSchema(),
                                                                              mapOperator->getMapExpression());
    physicalMapOperator->addProperty(LOGICAL_OPERATOR_ID_KEY, operatorNode->getProperty(LOGICAL_OPERATOR_ID_KEY));
    operatorNode->replace(physicalMapOperator);
}

void DefaultPhysicalOperatorProvider::lowerUDFMapOperator(const LogicalOperatorPtr& operatorNode) {
    auto mapUDFOperator = operatorNode->as<MapUDFLogicalOperator>();
    auto physicalMapOperator = PhysicalOperators::PhysicalMapUDFOperator::create(operatorNode->getStatisticId(),
                                                                                 mapUDFOperator->getInputSchema(),
                                                                                 mapUDFOperator->getOutputSchema(),
                                                                                 mapUDFOperator->getUDFDescriptor());
    physicalMapOperator->addProperty(LOGICAL_OPERATOR_ID_KEY, operatorNode->getProperty(LOGICAL_OPERATOR_ID_KEY));
    operatorNode->replace(physicalMapOperator);
}

void DefaultPhysicalOperatorProvider::lowerUDFFlatMapOperator(const LogicalOperatorPtr& operatorNode) {
    auto flatMapUDFOperator = operatorNode->as<FlatMapUDFLogicalOperator>();
    auto physicalMapOperator = PhysicalOperators::PhysicalFlatMapUDFOperator::create(operatorNode->getStatisticId(),
                                                                                     flatMapUDFOperator->getInputSchema(),
                                                                                     flatMapUDFOperator->getOutputSchema(),
                                                                                     flatMapUDFOperator->getUDFDescriptor());
    physicalMapOperator->addProperty(LOGICAL_OPERATOR_ID_KEY, operatorNode->getProperty(LOGICAL_OPERATOR_ID_KEY));
    operatorNode->replace(physicalMapOperator);
}

OperatorPtr DefaultPhysicalOperatorProvider::getJoinBuildInputOperator(const LogicalJoinOperatorPtr& joinOperator,
                                                                       SchemaPtr outputSchema,
                                                                       std::vector<OperatorPtr> children) {
    if (children.empty()) {
        throw QueryCompilationException("There should be at least one child for the join operator " + joinOperator->toString());
    }

    if (children.size() > 1) {
        auto demultiplexOperator =
            PhysicalOperators::PhysicalUnionOperator::create(joinOperator->getStatisticId(), std::move(outputSchema));
        demultiplexOperator->setOutputSchema(joinOperator->getOutputSchema());
        demultiplexOperator->addParent(joinOperator);
        for (const auto& child : children) {
            child->removeParent(joinOperator);
            child->addParent(demultiplexOperator);
        }
        return demultiplexOperator;
    }
    return children[0];
}

void DefaultPhysicalOperatorProvider::lowerJoinOperator(const LogicalOperatorPtr& operatorNode) {
    lowerNautilusJoin(operatorNode);
}

void DefaultPhysicalOperatorProvider::lowerNautilusJoin(const LogicalOperatorPtr& operatorNode) {
    using namespace Runtime::Execution::Operators;

    auto joinOperator = operatorNode->as<LogicalJoinOperator>();
    const auto& joinDefinition = joinOperator->getJoinDefinition();

    // returns the following pair:  std::make_pair(leftJoinKeyNameEqui,rightJoinKeyNameEqui);
    auto equiJoinKeyNames = NES::findEquiJoinKeyNames(joinDefinition->getJoinExpression());

    const auto windowType = joinDefinition->getWindowType()->as<Windowing::TimeBasedWindowType>();
    const auto& windowSize = windowType->getSize().getTime();
    const auto& windowSlide = windowType->getSlide().getTime();
    NES_ASSERT(windowType->instanceOf<Windowing::TumblingWindow>() || windowType->instanceOf<Windowing::SlidingWindow>(),
               "Only a tumbling or sliding window is currently supported for StreamJoin");

    const auto [timeStampFieldLeft, timeStampFieldRight] = getTimestampLeftAndRight(joinOperator, windowType);
    const auto leftInputOperator =
        getJoinBuildInputOperator(joinOperator, joinOperator->getLeftInputSchema(), joinOperator->getLeftOperators());
    const auto rightInputOperator =
        getJoinBuildInputOperator(joinOperator, joinOperator->getRightInputSchema(), joinOperator->getRightOperators());
    const auto joinStrategy = options->getStreamJoinStrategy();

    const StreamJoinOperators streamJoinOperators(operatorNode, leftInputOperator, rightInputOperator);
    const StreamJoinConfigs streamJoinConfig(equiJoinKeyNames.first,
                                             equiJoinKeyNames.second,
                                             windowSize,
                                             windowSlide,
                                             timeStampFieldLeft,
                                             timeStampFieldRight,
                                             joinStrategy);

    StreamJoinOperatorHandlerPtr joinOperatorHandler;
    switch (joinStrategy) {
        case StreamJoinStrategy::HASH_JOIN_LOCAL:
        case StreamJoinStrategy::HASH_JOIN_VAR_SIZED:
        case StreamJoinStrategy::HASH_JOIN_GLOBAL_LOCKING:
        case StreamJoinStrategy::HASH_JOIN_GLOBAL_LOCK_FREE:
            joinOperatorHandler = lowerStreamingHashJoin(streamJoinOperators, streamJoinConfig);
            break;
        case StreamJoinStrategy::NESTED_LOOP_JOIN:
            joinOperatorHandler = lowerStreamingNestedLoopJoin(streamJoinOperators, streamJoinConfig);
            break;
    }

    auto createBuildOperator = [&](const SchemaPtr& inputSchema,
                                   JoinBuildSideType buildSideType,
                                   const TimestampField& timeStampField,
                                   const std::string& joinFieldName) {
        return PhysicalOperators::PhysicalStreamJoinBuildOperator::create(operatorNode->getStatisticId(),
                                                                          inputSchema,
                                                                          joinOperator->getOutputSchema(),
                                                                          joinOperatorHandler,
                                                                          buildSideType,
                                                                          timeStampField,
                                                                          joinFieldName,
                                                                          options->getStreamJoinStrategy(),
                                                                          options->getWindowingStrategy());
    };

    const auto leftJoinBuildOperator = createBuildOperator(joinOperator->getLeftInputSchema(),
                                                           JoinBuildSideType::Left,
                                                           streamJoinConfig.timeStampFieldLeft,
                                                           streamJoinConfig.joinFieldNameLeft);
    const auto rightJoinBuildOperator = createBuildOperator(joinOperator->getRightInputSchema(),
                                                            JoinBuildSideType::Right,
                                                            streamJoinConfig.timeStampFieldRight,
                                                            streamJoinConfig.joinFieldNameRight);
    const auto joinProbeOperator =
        PhysicalOperators::PhysicalStreamJoinProbeOperator::create(operatorNode->getStatisticId(),
                                                                   joinOperator->getLeftInputSchema(),
                                                                   joinOperator->getRightInputSchema(),
                                                                   joinOperator->getOutputSchema(),
                                                                   joinOperator->getJoinExpression(),
                                                                   joinOperator->getWindowStartFieldName(),
                                                                   joinOperator->getWindowEndFieldName(),
                                                                   joinOperatorHandler,
                                                                   options->getStreamJoinStrategy(),
                                                                   options->getWindowingStrategy());

    streamJoinOperators.leftInputOperator->insertBetweenThisAndParentNodes(leftJoinBuildOperator);
    streamJoinOperators.rightInputOperator->insertBetweenThisAndParentNodes(rightJoinBuildOperator);
    streamJoinOperators.operatorNode->replace(joinProbeOperator);
}

Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr
DefaultPhysicalOperatorProvider::lowerStreamingNestedLoopJoin(const StreamJoinOperators& streamJoinOperators,
                                                              const StreamJoinConfigs& streamJoinConfig) {

    using namespace Runtime::Execution;
    const auto joinOperator = streamJoinOperators.operatorNode->as<LogicalJoinOperator>();

    Operators::NLJOperatorHandlerPtr joinOperatorHandler;
    if (options->getWindowingStrategy() == WindowingStrategy::SLICING) {
        return Operators::NLJOperatorHandlerSlicing::create(joinOperator->getAllInputOriginIds(),
                                                            joinOperator->getOutputOriginIds()[0],
                                                            streamJoinConfig.windowSize,
                                                            streamJoinConfig.windowSlide,
                                                            joinOperator->getLeftInputSchema(),
                                                            joinOperator->getRightInputSchema(),
                                                            Nautilus::Interface::PagedVectorVarSized::PAGE_SIZE,
                                                            Nautilus::Interface::PagedVectorVarSized::PAGE_SIZE);
    } else if (options->getWindowingStrategy() == WindowingStrategy::BUCKETING) {
        return Operators::NLJOperatorHandlerBucketing::create(joinOperator->getAllInputOriginIds(),
                                                              joinOperator->getOutputOriginIds()[0],
                                                              streamJoinConfig.windowSize,
                                                              streamJoinConfig.windowSlide,
                                                              joinOperator->getLeftInputSchema(),
                                                              joinOperator->getRightInputSchema(),
                                                              Nautilus::Interface::PagedVectorVarSized::PAGE_SIZE,
                                                              Nautilus::Interface::PagedVectorVarSized::PAGE_SIZE);
    } else {
        NES_NOT_IMPLEMENTED();
    }
}

Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr
DefaultPhysicalOperatorProvider::lowerStreamingHashJoin(const StreamJoinOperators& streamJoinOperators,
                                                        const StreamJoinConfigs& streamJoinConfig) {
    using namespace Runtime::Execution;
    const auto joinOperator = streamJoinOperators.operatorNode->as<LogicalJoinOperator>();
    Operators::HJOperatorHandlerPtr joinOperatorHandler;
    if (options->getWindowingStrategy() == WindowingStrategy::SLICING) {
        return Operators::HJOperatorHandlerSlicing::create(joinOperator->getAllInputOriginIds(),
                                                           joinOperator->getOutputOriginIds()[0],
                                                           streamJoinConfig.windowSize,
                                                           streamJoinConfig.windowSlide,
                                                           joinOperator->getLeftInputSchema(),
                                                           joinOperator->getRightInputSchema(),
                                                           streamJoinConfig.joinStrategy,
                                                           options->getHashJoinOptions()->getTotalSizeForDataStructures(),
                                                           options->getHashJoinOptions()->getPreAllocPageCnt(),
                                                           options->getHashJoinOptions()->getPageSize(),
                                                           options->getHashJoinOptions()->getNumberOfPartitions());
    } else if (options->getWindowingStrategy() == WindowingStrategy::BUCKETING) {
        return Operators::HJOperatorHandlerBucketing::create(joinOperator->getAllInputOriginIds(),
                                                             joinOperator->getOutputOriginIds()[0],
                                                             streamJoinConfig.windowSize,
                                                             streamJoinConfig.windowSlide,
                                                             joinOperator->getLeftInputSchema(),
                                                             joinOperator->getRightInputSchema(),
                                                             streamJoinConfig.joinStrategy,
                                                             options->getHashJoinOptions()->getTotalSizeForDataStructures(),
                                                             options->getHashJoinOptions()->getPreAllocPageCnt(),
                                                             options->getHashJoinOptions()->getPageSize(),
                                                             options->getHashJoinOptions()->getNumberOfPartitions());
    } else {
        NES_NOT_IMPLEMENTED();
    }
}

std::tuple<TimestampField, TimestampField>
DefaultPhysicalOperatorProvider::getTimestampLeftAndRight(const std::shared_ptr<LogicalJoinOperator>& joinOperator,
                                                          const Windowing::TimeBasedWindowTypePtr& windowType) const {
    if (windowType->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::Type::IngestionTime) {
        NES_DEBUG("Skip eventime identification as we use ingestion time");
        return {TimestampField::IngestionTime(), TimestampField::IngestionTime()};
    } else {

        // FIXME Once #3407 is done, we can change this to get the left and right fieldname
        auto timeStampFieldName = windowType->getTimeCharacteristic()->getField()->getName();
        auto timeStampFieldNameWithoutSourceName =
            timeStampFieldName.substr(timeStampFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR));

        // Lambda function for extracting the timestamp from a schema
        auto findTimeStampFieldName = [&](const SchemaPtr& schema) {
            for (const auto& field : schema->fields) {
                if (field->getName().find(timeStampFieldNameWithoutSourceName) != std::string::npos) {
                    return field->getName();
                }
            }
            return std::string();
        };

        // Extracting the left and right timestamp
        auto timeStampFieldNameLeft = findTimeStampFieldName(joinOperator->getLeftInputSchema());
        auto timeStampFieldNameRight = findTimeStampFieldName(joinOperator->getRightInputSchema());

        NES_ASSERT(!(timeStampFieldNameLeft.empty() || timeStampFieldNameRight.empty()),
                   "Could not find timestampfieldname " << timeStampFieldNameWithoutSourceName << " in both streams!");
        NES_DEBUG("timeStampFieldNameLeft:{}  timeStampFieldNameRight:{} ", timeStampFieldNameLeft, timeStampFieldNameRight);

        return {TimestampField::EventTime(timeStampFieldNameLeft, windowType->getTimeCharacteristic()->getTimeUnit()),
                TimestampField::EventTime(timeStampFieldNameRight, windowType->getTimeCharacteristic()->getTimeUnit())};
    }
}

void DefaultPhysicalOperatorProvider::lowerWatermarkAssignmentOperator(const LogicalOperatorPtr& operatorNode) {
    auto logicalWatermarkAssignment = operatorNode->as<WatermarkAssignerLogicalOperator>();
    auto physicalWatermarkAssignment = PhysicalOperators::PhysicalWatermarkAssignmentOperator::create(
        operatorNode->getStatisticId(),
        logicalWatermarkAssignment->getInputSchema(),
        logicalWatermarkAssignment->getOutputSchema(),
        logicalWatermarkAssignment->getWatermarkStrategyDescriptor());
    physicalWatermarkAssignment->setOutputSchema(logicalWatermarkAssignment->getOutputSchema());
    operatorNode->replace(physicalWatermarkAssignment);
}

void DefaultPhysicalOperatorProvider::lowerTimeBasedWindowOperator(const LogicalOperatorPtr& operatorNode) {

    NES_DEBUG("Create Thread local window aggregation");
    auto windowOperator = operatorNode->as<WindowOperator>();
    auto windowInputSchema = windowOperator->getInputSchema();
    auto windowOutputSchema = windowOperator->getOutputSchema();
    auto windowDefinition = windowOperator->getWindowDefinition();

    auto timeBasedWindowType = windowDefinition->getWindowType()->as<Windowing::TimeBasedWindowType>();
    auto windowOperatorProperties =
        WindowOperatorProperties(windowOperator, windowInputSchema, windowOutputSchema, windowDefinition);

    if (windowOperator->getInputOriginIds().empty()) {
        throw QueryCompilationException("The number of input origin IDs for an window operator should not be zero.");
    }

    // TODO this currently just mimics the old usage of the set of input origins.
    windowDefinition->setNumberOfInputEdges(windowOperator->getInputOriginIds().size());
    windowDefinition->setInputOriginIds(windowOperator->getInputOriginIds());

    auto preAggregationOperator = PhysicalOperators::PhysicalSlicePreAggregationOperator::create(getNextOperatorId(),
                                                                                                 operatorNode->getStatisticId(),
                                                                                                 windowInputSchema,
                                                                                                 windowOutputSchema,
                                                                                                 windowDefinition);
    operatorNode->insertBetweenThisAndChildNodes(preAggregationOperator);

    // if we have a sliding window and use slicing we have to create another slice merge operator
    if (timeBasedWindowType->instanceOf<Windowing::SlidingWindow>()
        && options->getWindowingStrategy() == WindowingStrategy::SLICING) {
        auto mergingOperator = PhysicalOperators::PhysicalSliceMergingOperator::create(getNextOperatorId(),
                                                                                       operatorNode->getStatisticId(),
                                                                                       windowInputSchema,
                                                                                       windowOutputSchema,
                                                                                       windowDefinition);
        operatorNode->insertBetweenThisAndChildNodes(mergingOperator);
    }
    auto windowSink = PhysicalOperators::PhysicalWindowSinkOperator::create(getNextOperatorId(),
                                                                            operatorNode->getStatisticId(),
                                                                            windowInputSchema,
                                                                            windowOutputSchema,
                                                                            windowDefinition);
    operatorNode->replace(windowSink);
}

void DefaultPhysicalOperatorProvider::lowerWindowOperator(const LogicalOperatorPtr& operatorNode) {
    auto windowOperator = operatorNode->as<WindowOperator>();
    auto windowInputSchema = windowOperator->getInputSchema();
    auto windowOutputSchema = windowOperator->getOutputSchema();
    auto windowDefinition = windowOperator->getWindowDefinition();
    if (windowOperator->getInputOriginIds().empty()) {
        throw QueryCompilationException("The number of input origin IDs for an window operator should not be zero.");
    }
    // TODO this currently just mimics the old usage of the set of input origins.
    windowDefinition->setNumberOfInputEdges(windowOperator->getInputOriginIds().size());

    // create window operator handler, to establish a common Runtime object for aggregation and trigger phase.
    if (operatorNode->instanceOf<LogicalWindowOperator>()) {
        // handle if threshold window
        //TODO: At this point we are already a central window, we do not want the threshold window to become a Gentral Window in the first place
        auto windowType = operatorNode->as<WindowOperator>()->getWindowDefinition()->getWindowType();
        if (windowType->instanceOf<Windowing::ContentBasedWindowType>()) {
            auto contentBasedWindowType = windowType->as<Windowing::ContentBasedWindowType>();
            // check different content-based window types
            if (contentBasedWindowType->getContentBasedSubWindowType()
                == Windowing::ContentBasedWindowType::ContentBasedSubWindowType::THRESHOLDWINDOW) {
                NES_INFO("Lower ThresholdWindow");
                auto thresholdWindowPhysicalOperator =
                    PhysicalOperators::PhysicalThresholdWindowOperator::create(operatorNode->getStatisticId(),
                                                                               windowInputSchema,
                                                                               windowOutputSchema,
                                                                               windowDefinition);
                thresholdWindowPhysicalOperator->addProperty(LOGICAL_OPERATOR_ID_KEY,
                                                             operatorNode->getProperty(LOGICAL_OPERATOR_ID_KEY));

                operatorNode->replace(thresholdWindowPhysicalOperator);
                return;
            } else {
                throw QueryCompilationException("No support for this window type."
                                                + windowDefinition->getWindowType()->toString());
            }
        } else if (windowType->instanceOf<Windowing::TimeBasedWindowType>()) {
            lowerTimeBasedWindowOperator(operatorNode);
        }
    } else {
        throw QueryCompilationException("No conversion for operator " + operatorNode->toString() + " was provided.");
    }
}

}// namespace NES::QueryCompilation
