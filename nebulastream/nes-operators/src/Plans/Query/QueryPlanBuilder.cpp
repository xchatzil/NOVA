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

#include <API/AttributeField.hpp>
#include <Expressions/FieldAssignmentExpressionNode.hpp>
#include <Expressions/FieldRenameExpressionNode.hpp>
#include <Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Measures/TimeCharacteristic.hpp>
#include <Operators/LogicalOperators/LogicalBatchJoinDescriptor.hpp>
#include <Operators/LogicalOperators/LogicalBinaryOperator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/UDFs/UDFDescriptor.hpp>
#include <Operators/LogicalOperators/Watermarks/EventTimeWatermarkStrategyDescriptor.hpp>
#include <Operators/LogicalOperators/Watermarks/IngestionTimeWatermarkStrategyDescriptor.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDescriptor.hpp>
#include <Plans/Query/QueryPlanBuilder.hpp>
#include <Types/TimeBasedWindowType.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <iostream>
#include <utility>

namespace NES {

QueryPlanPtr QueryPlanBuilder::createQueryPlan(std::string sourceName) {
    NES_DEBUG("QueryPlanBuilder: create query plan for input source  {}", sourceName);
    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create(sourceName));
    auto queryPlanPtr = QueryPlan::create(sourceOperator);
    queryPlanPtr->setSourceConsumed(sourceName);
    return queryPlanPtr;
}

QueryPlanPtr QueryPlanBuilder::addProjection(const std::vector<ExpressionNodePtr>& expressions, QueryPlanPtr queryPlan) {
    NES_DEBUG("QueryPlanBuilder: add projection operator to query plan");
    OperatorPtr op = LogicalOperatorFactory::createProjectionOperator(expressions);
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

QueryPlanPtr QueryPlanBuilder::addRename(std::string const& newSourceName, QueryPlanPtr queryPlan) {
    NES_DEBUG("QueryPlanBuilder: add rename operator to query plan");
    auto op = LogicalOperatorFactory::createRenameSourceOperator(newSourceName);
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

QueryPlanPtr QueryPlanBuilder::addFilter(ExpressionNodePtr const& filterExpression, QueryPlanPtr queryPlan) {
    NES_DEBUG("QueryPlanBuilder: add filter operator to query plan");
    if (!filterExpression->getNodesByType<FieldRenameExpressionNode>().empty()) {
        NES_THROW_RUNTIME_ERROR("QueryPlanBuilder: Filter predicate cannot have a FieldRenameExpression");
    }
    OperatorPtr op = LogicalOperatorFactory::createFilterOperator(filterExpression);
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

QueryPlanPtr QueryPlanBuilder::addLimit(const uint64_t limit, QueryPlanPtr queryPlan) {
    NES_DEBUG("QueryPlanBuilder: add limit operator to query plan");
    OperatorPtr op = LogicalOperatorFactory::createLimitOperator(limit);
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

QueryPlanPtr QueryPlanBuilder::addMapUDF(Catalogs::UDF::UDFDescriptorPtr const& descriptor, QueryPlanPtr queryPlan) {
    NES_DEBUG("QueryPlanBuilder: add map java udf operator to query plan");
    auto op = LogicalOperatorFactory::createMapUDFLogicalOperator(descriptor);
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

QueryPlanPtr QueryPlanBuilder::addFlatMapUDF(Catalogs::UDF::UDFDescriptorPtr const& descriptor, QueryPlanPtr queryPlan) {
    NES_DEBUG("QueryPlanBuilder: add flat map java udf operator to query plan");
    auto op = LogicalOperatorFactory::createFlatMapUDFLogicalOperator(descriptor);
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

QueryPlanPtr QueryPlanBuilder::addMap(FieldAssignmentExpressionNodePtr const& mapExpression, QueryPlanPtr queryPlan) {
    NES_DEBUG("QueryPlanBuilder: add map operator to query plan");
    if (!mapExpression->getNodesByType<FieldRenameExpressionNode>().empty()) {
        NES_THROW_RUNTIME_ERROR("QueryPlanBuilder: Map expression cannot have a FieldRenameExpression");
    }
    OperatorPtr op = LogicalOperatorFactory::createMapOperator(mapExpression);
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

QueryPlanPtr QueryPlanBuilder::addUnion(QueryPlanPtr leftQueryPlan, QueryPlanPtr rightQueryPlan) {
    NES_DEBUG("QueryPlanBuilder: unionWith the subQuery to current query plan");
    OperatorPtr op = LogicalOperatorFactory::createUnionOperator();
    leftQueryPlan = addBinaryOperatorAndUpdateSource(op, leftQueryPlan, rightQueryPlan);
    return leftQueryPlan;
}

QueryPlanPtr QueryPlanBuilder::addStatisticBuildOperator(Windowing::WindowTypePtr window,
                                                         Statistic::WindowStatisticDescriptorPtr statisticDescriptor,
                                                         Statistic::StatisticMetricHash metricHash,
                                                         Statistic::SendingPolicyPtr sendingPolicy,
                                                         Statistic::TriggerConditionPtr triggerCondition,
                                                         QueryPlanPtr queryPlan) {
    queryPlan = checkAndAddWatermarkAssignment(queryPlan, window);
    auto op = LogicalOperatorFactory::createStatisticBuildOperator(window,
                                                                   std::move(statisticDescriptor),
                                                                   metricHash,
                                                                   sendingPolicy,
                                                                   triggerCondition);
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

QueryPlanPtr QueryPlanBuilder::addJoin(
    QueryPlanPtr leftQueryPlan,
    QueryPlanPtr rightQueryPlan,
    ExpressionNodePtr joinExpression,
    const Windowing::WindowTypePtr& windowType,
    Join::LogicalJoinDescriptor::JoinType joinType = Join::LogicalJoinDescriptor::JoinType::CARTESIAN_PRODUCT) {
    NES_DEBUG("QueryPlanBuilder: joinWith the subQuery to current query");

    NES_DEBUG("QueryPlanBuilder: Iterate over all ExpressionNode to check join field.");
    std::unordered_set<std::shared_ptr<BinaryExpressionNode>> visitedExpressions;
    auto bfsIterator = BreadthFirstNodeIterator(joinExpression);
    for (auto itr = bfsIterator.begin(); itr != BreadthFirstNodeIterator::end(); ++itr) {
        if ((*itr)->instanceOf<BinaryExpressionNode>()
            && !(*itr)->as<BinaryExpressionNode>()->getLeft()->instanceOf<BinaryExpressionNode>()) {
            auto visitingOp = (*itr)->as<BinaryExpressionNode>();
            if (visitedExpressions.contains(visitingOp)) {
                // skip rest of the steps as the node found in already visited node list
                continue;
            } else {
                visitedExpressions.insert(visitingOp);
                auto onLeftKey = (*itr)->as<BinaryExpressionNode>()->getLeft();
                auto onRightKey = (*itr)->as<BinaryExpressionNode>()->getRight();
                // ensure that the child nodes are not binary
                if (!onLeftKey->instanceOf<BinaryExpressionNode>() && !onRightKey->instanceOf<BinaryExpressionNode>()) {
                    if (onLeftKey->instanceOf<ConstantValueExpressionNode>()
                        || onRightKey->instanceOf<ConstantValueExpressionNode>()) {
                        NES_THROW_RUNTIME_ERROR("use .filter() for your expression.");
                    }
                    auto leftKeyFieldAccess = checkExpression(onLeftKey, "leftSide");
                    auto rightQueryPlanKeyFieldAccess = checkExpression(onRightKey, "rightSide");
                }
            }
        }
    }

    NES_ASSERT(rightQueryPlan && !rightQueryPlan->getRootOperators().empty(), "invalid rightQueryPlan query plan");
    auto rootOperatorRhs = rightQueryPlan->getRootOperators()[0];
    auto leftJoinType = leftQueryPlan->getRootOperators()[0]->getOutputSchema();
    auto rightQueryPlanJoinType = rootOperatorRhs->getOutputSchema();

    // check if query contain watermark assigner, and add if missing (as default behaviour)
    leftQueryPlan = checkAndAddWatermarkAssignment(leftQueryPlan, windowType);
    rightQueryPlan = checkAndAddWatermarkAssignment(rightQueryPlan, windowType);

    //TODO 1,1 should be replaced once we have distributed joins with the number of child input edges
    //TODO(Ventura?>Steffen) can we know this at this query submission time?
    auto joinDefinition = Join::LogicalJoinDescriptor::create(joinExpression, windowType, 1, 1, joinType);

    NES_DEBUG("QueryPlanBuilder: add join operator to query plan");
    auto op = LogicalOperatorFactory::createJoinOperator(joinDefinition);
    leftQueryPlan = addBinaryOperatorAndUpdateSource(op, leftQueryPlan, rightQueryPlan);
    return leftQueryPlan;
}

QueryPlanPtr QueryPlanBuilder::addBatchJoin(QueryPlanPtr leftQueryPlan,
                                            QueryPlanPtr rightQueryPlan,
                                            ExpressionNodePtr onProbeKey,
                                            ExpressionNodePtr onBuildKey) {
    NES_DEBUG("Query: joinWith the subQuery to current query");
    auto probeKeyFieldAccess = checkExpression(onProbeKey, "onProbeKey");
    auto buildKeyFieldAccess = checkExpression(onBuildKey, "onBuildKey");

    NES_ASSERT(rightQueryPlan && !rightQueryPlan->getRootOperators().empty(), "invalid rightQueryPlan query plan");
    auto rootOperatorRhs = rightQueryPlan->getRootOperators()[0];
    auto leftJoinType = leftQueryPlan->getRootOperators()[0]->getOutputSchema();
    auto rightQueryPlanJoinType = rootOperatorRhs->getOutputSchema();

    // todo here again we wan't to extend to distributed joins:
    //TODO 1,1 should be replaced once we have distributed joins with the number of child input edges
    //TODO(Ventura?>Steffen) can we know this at this query submission time?
    auto joinDefinition = Join::Experimental::LogicalBatchJoinDescriptor::create(buildKeyFieldAccess, probeKeyFieldAccess, 1, 1);

    auto op = LogicalOperatorFactory::createBatchJoinOperator(joinDefinition);
    leftQueryPlan = addBinaryOperatorAndUpdateSource(op, leftQueryPlan, rightQueryPlan);
    return leftQueryPlan;
}

QueryPlanPtr QueryPlanBuilder::addSink(QueryPlanPtr queryPlan, SinkDescriptorPtr sinkDescriptor, WorkerId workerId) {
    OperatorPtr op = LogicalOperatorFactory::createSinkOperator(sinkDescriptor, workerId);
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

QueryPlanPtr QueryPlanBuilder::assignWatermark(QueryPlanPtr queryPlan,
                                               Windowing::WatermarkStrategyDescriptorPtr const& watermarkStrategyDescriptor) {
    OperatorPtr op = LogicalOperatorFactory::createWatermarkAssignerOperator(watermarkStrategyDescriptor);
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

QueryPlanPtr QueryPlanBuilder::checkAndAddWatermarkAssignment(QueryPlanPtr queryPlan, const Windowing::WindowTypePtr windowType) {
    NES_DEBUG("QueryPlanBuilder: checkAndAddWatermarkAssignment for a (sub)query plan");
    auto timeBasedWindowType = windowType->as<Windowing::TimeBasedWindowType>();

    if (queryPlan->getOperatorByType<WatermarkAssignerLogicalOperator>().empty()) {
        if (timeBasedWindowType->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::Type::IngestionTime) {
            return assignWatermark(queryPlan, Windowing::IngestionTimeWatermarkStrategyDescriptor::create());
        } else if (timeBasedWindowType->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::Type::EventTime) {
            return assignWatermark(
                queryPlan,
                Windowing::EventTimeWatermarkStrategyDescriptor::create(
                    FieldAccessExpressionNode::create(timeBasedWindowType->getTimeCharacteristic()->getField()->getName()),
                    Windowing::TimeMeasure(0),
                    timeBasedWindowType->getTimeCharacteristic()->getTimeUnit()));
        }
    }
    return queryPlan;
}

QueryPlanPtr QueryPlanBuilder::addBinaryOperatorAndUpdateSource(OperatorPtr operatorNode,
                                                                QueryPlanPtr leftQueryPlan,
                                                                QueryPlanPtr rightQueryPlan) {
    //we add the root of th right query plan as root of the left query plan
    // thus, children of the binary join operator are
    // [0] left and [1] right
    leftQueryPlan->addRootOperator(rightQueryPlan->getRootOperators()[0]);
    leftQueryPlan->appendOperatorAsNewRoot(operatorNode);
    NES_DEBUG("QueryPlanBuilder: addBinaryOperatorAndUpdateSource: update the source names");
    auto newSourceName = Util::updateSourceName(leftQueryPlan->getSourceConsumed(), rightQueryPlan->getSourceConsumed());
    leftQueryPlan->setSourceConsumed(newSourceName);
    return leftQueryPlan;
}

std::shared_ptr<FieldAccessExpressionNode> QueryPlanBuilder::checkExpression(ExpressionNodePtr expression, std::string side) {
    if (!expression->instanceOf<FieldAccessExpressionNode>()) {
        NES_ERROR("QueryPlanBuilder: window key ({}) has to be an FieldAccessExpression but it was a  {}",
                  side,
                  expression->toString());
        NES_THROW_RUNTIME_ERROR("QueryPlanBuilder: window key has to be an FieldAccessExpression");
    }
    return expression->as<FieldAccessExpressionNode>();
}
}// namespace NES
