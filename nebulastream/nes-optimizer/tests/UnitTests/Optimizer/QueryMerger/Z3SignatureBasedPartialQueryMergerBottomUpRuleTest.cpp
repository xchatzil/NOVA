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

// clang-format off
#include <gtest/gtest.h>
#include <BaseIntegrationTest.hpp>
// clang-format on
#include <API/QueryAPI.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Operators/LogicalOperators/LogicalFilterOperator.hpp>
#include <Operators/LogicalOperators/LogicalMapOperator.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryMerger/Z3SignatureBasedPartialQueryMergerBottomUpRule.hpp>
#include <Optimizer/QuerySignatures/SignatureEqualityUtil.hpp>
#include <Plans/ChangeLog/ChangeLogEntry.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Plans/Utils/PlanIterator.hpp>
#include <StatisticCollection/StatisticCache/DefaultStatisticCache.hpp>
#include <StatisticCollection/StatisticProbeHandling/DefaultStatisticProbeGenerator.hpp>
#include <StatisticCollection/StatisticProbeHandling/StatisticProbeHandler.hpp>
#include <StatisticCollection/StatisticRegistry/StatisticRegistry.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Mobility/SpatialType.hpp>
#include <z3++.h>

using namespace NES;
using namespace Configurations;

class Z3SignatureBasedPartialQueryMergerBottomUpRuleTest : public Testing::BaseUnitTest {

  public:
    SchemaPtr schema;
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    std::shared_ptr<Catalogs::UDF::UDFCatalog> udfCatalog;
    Statistic::StatisticProbeHandlerPtr statisticProbeHandler;

    /* Will be called before all tests in this class are started. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("Z3SignatureBasedPartialQueryMergerBottomUpRuleTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup Z3SignatureBasedPartialQueryMergerBottomUpRuleTest test case.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        schema = Schema::create()
                     ->addField("ts", BasicType::UINT32)
                     ->addField("type", BasicType::UINT32)
                     ->addField("id", BasicType::UINT32)
                     ->addField("value", BasicType::UINT64)
                     ->addField("id1", BasicType::UINT32)
                     ->addField("value1", BasicType::UINT64);
        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
        sourceCatalog->addLogicalSource("car", schema);
        sourceCatalog->addLogicalSource("bike", schema);
        sourceCatalog->addLogicalSource("truck", schema);

        std::map<std::string, std::any> properties;
        properties[NES::Worker::Properties::MAINTENANCE] = false;
        properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

        TopologyNodePtr sourceNode1 = TopologyNode::create(WorkerId(2), "localhost", 123, 124, 4, properties);

        auto logicalSourceCar = sourceCatalog->getLogicalSource("car");
        auto physicalSourceCar = PhysicalSource::create(DefaultSourceType::create("car", "testCar"));
        auto sourceCatalogEntry1 =
            Catalogs::Source::SourceCatalogEntry::create(physicalSourceCar, logicalSourceCar, sourceNode1->getId());
        sourceCatalog->addPhysicalSource("car", sourceCatalogEntry1);

        auto logicalSourceBike = sourceCatalog->getLogicalSource("bike");
        auto physicalSourceBike = PhysicalSource::create(DefaultSourceType::create("bike", "testBike"));
        auto sourceCatalogEntry2 =
            Catalogs::Source::SourceCatalogEntry::create(physicalSourceBike, logicalSourceBike, sourceNode1->getId());
        sourceCatalog->addPhysicalSource("bike", sourceCatalogEntry2);

        auto logicalSourceTruck = sourceCatalog->getLogicalSource("truck");
        auto physicalSourceTruck = PhysicalSource::create(DefaultSourceType::create("truck", "testTruck"));
        auto sourceCatalogEntry3 =
            Catalogs::Source::SourceCatalogEntry::create(physicalSourceCar, logicalSourceCar, sourceNode1->getId());
        sourceCatalog->addPhysicalSource("truck", sourceCatalogEntry3);
        udfCatalog = Catalogs::UDF::UDFCatalog::create();
        statisticProbeHandler = Statistic::StatisticProbeHandler::create(Statistic::StatisticRegistry::create(),
                                                                         Statistic::DefaultStatisticProbeGenerator::create(),
                                                                         Statistic::DefaultStatisticCache::create(),
                                                                         Topology::create());
    }
};

/**
 * @brief Test applying SignatureBasedPartialQueryMergerRuleTest on Global query plan with same queryIdAndCatalogEntryMapping
 */
TEST_F(Z3SignatureBasedPartialQueryMergerBottomUpRuleTest, testMergingEqualQueries) {

    auto topologySpecificReWrite = Optimizer::TopologySpecificQueryRewritePhase::create(Topology::create(),
                                                                                        sourceCatalog,
                                                                                        Configurations::OptimizerConfiguration(),
                                                                                        statisticProbeHandler);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("car")
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorPtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query query2 = Query::from("car")
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorPtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase =
        Optimizer::SignatureInferencePhase::create(context, Optimizer::QueryMergerRule::Z3SignatureBasedPartialQueryMergerRule);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    queryPlan1 = topologySpecificReWrite->execute(queryPlan1);
    queryPlan2 = topologySpecificReWrite->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedPartialQueryMergerBottomUpRule::create(context);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 1);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    EXPECT_TRUE(updatedSharedQueryPlan1);

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 2);

    // assert children equality
    for (const auto& sink1Child : updatedRootOperators1[0]->getChildren()) {
        bool found = false;
        for (const auto& sink2Child : updatedRootOperators1[1]->getChildren()) {
            if (sink1Child->equal(sink2Child)) {
                found = true;
                break;
            }
        }
        EXPECT_TRUE(found);
    }
}

/**
 * @brief Test applying SignatureBasedPartialQueryMergerRuleTest on Global query plan with partially same queryIdAndCatalogEntryMapping
 */
TEST_F(Z3SignatureBasedPartialQueryMergerBottomUpRuleTest, testMergingPartiallyEqualQueries) {

    auto topologySpecificReWrite = Optimizer::TopologySpecificQueryRewritePhase::create(Topology::create(),
                                                                                        sourceCatalog,
                                                                                        Configurations::OptimizerConfiguration(),
                                                                                        statisticProbeHandler);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("car")
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id1") < 45)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorPtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query query2 = Query::from("car")
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("value1") < 45)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorPtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    queryPlan1 = topologySpecificReWrite->execute(queryPlan1);
    queryPlan2 = topologySpecificReWrite->execute(queryPlan2);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase =
        Optimizer::SignatureInferencePhase::create(context, Optimizer::QueryMergerRule::Z3SignatureBasedPartialQueryMergerRule);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedPartialQueryMergerBottomUpRule::create(context);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 1);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    EXPECT_TRUE(updatedSharedQueryPlan1);

    NES_INFO("{}", updatedSharedQueryPlan1->toString());

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 2);

    for (const auto& sink1Child : updatedRootOperators1[0]->getChildren()) {
        bool found = false;
        for (const auto& sink2Child : updatedRootOperators1[1]->getChildren()) {
            EXPECT_NE(sink1Child, sink2Child);
            auto sink1ChildGrandChild = sink1Child->getChildren();
            auto sink2ChildGrandChild = sink2Child->getChildren();
            EXPECT_TRUE(sink1ChildGrandChild.size() == 1);
            EXPECT_TRUE(sink2ChildGrandChild.size() == 1);
            if (sink1ChildGrandChild[0]->equal(sink2ChildGrandChild[0])) {
                found = true;
                break;
            }
        }
        EXPECT_TRUE(found);
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with two queryIdAndCatalogEntryMapping with different source
 */
TEST_F(Z3SignatureBasedPartialQueryMergerBottomUpRuleTest, testMergingQueriesWithDifferentSources) {

    // Prepare
    auto topologySpecificReWrite = Optimizer::TopologySpecificQueryRewritePhase::create(Topology::create(),
                                                                                        sourceCatalog,
                                                                                        Configurations::OptimizerConfiguration(),
                                                                                        statisticProbeHandler);

    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("car").map(Attribute("value") = 40).filter(Attribute("id") < 45).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorPtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query query2 = Query::from("truck").map(Attribute("value") = 40).filter(Attribute("id") < 45).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorPtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    queryPlan1 = topologySpecificReWrite->execute(queryPlan1);
    queryPlan2 = topologySpecificReWrite->execute(queryPlan2);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase =
        Optimizer::SignatureInferencePhase::create(context, Optimizer::QueryMergerRule::Z3SignatureBasedPartialQueryMergerRule);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedPartialQueryMergerBottomUpRule::create(context);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 2);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    auto updatedSharedQueryPlan2 = updatedSharedQMToDeploy[1]->getQueryPlan();

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 1);
    auto updatedRootOperators2 = updatedSharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(updatedRootOperators2.size() == 1);

    //assert
    for (const auto& sink1ChildOperator : updatedRootOperators1[0]->getChildren()) {
        for (const auto& sink2ChildOperator : updatedRootOperators2[0]->getChildren()) {
            EXPECT_NE(sink1ChildOperator, sink2ChildOperator);
        }
    }
}

TEST_F(Z3SignatureBasedPartialQueryMergerBottomUpRuleTest, testMergingMoreThanTwoPartiallyEqualQueries) {

    auto topologySpecificReWrite = Optimizer::TopologySpecificQueryRewritePhase::create(Topology::create(),
                                                                                        sourceCatalog,
                                                                                        Configurations::OptimizerConfiguration(),
                                                                                        statisticProbeHandler);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("car").filter(Attribute("id") < 45).map(Attribute("queryId") = 1).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorPtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query query2 = Query::from("car").filter(Attribute("id") < 45).map(Attribute("queryId") = 2).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorPtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    Query query3 = Query::from("car").filter(Attribute("id") < 45).map(Attribute("queryId") = 3).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan3 = query3.getQueryPlan();
    SinkLogicalOperatorPtr sinkOperator3 = queryPlan3->getSinkOperators()[0];
    QueryId queryId3 = PlanIdGenerator::getNextQueryId();
    queryPlan3->setQueryId(queryId3);

    queryPlan1 = topologySpecificReWrite->execute(queryPlan1);
    queryPlan2 = topologySpecificReWrite->execute(queryPlan2);
    queryPlan3 = topologySpecificReWrite->execute(queryPlan3);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);
    typeInferencePhase->execute(queryPlan3);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase =
        Optimizer::SignatureInferencePhase::create(context, Optimizer::QueryMergerRule::Z3SignatureBasedPartialQueryMergerRule);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);
    z3InferencePhase->execute(queryPlan3);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //execute merging shared query plan 1 and 2
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedPartialQueryMergerBottomUpRule::create(context);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    auto updatedSharedQueryPlansDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    NES_INFO("Shared Plan After Merging with Query 2\n{}", updatedSharedQueryPlansDeploy[0]->getQueryPlan()->toString());
    updatedSharedQueryPlansDeploy[0]->setStatus(SharedQueryPlanStatus::DEPLOYED);

    //Clear old change log entries by updating the processed till timestamp
    auto now = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    updatedSharedQueryPlansDeploy[0]->updateProcessedChangeLogTimestamp(now);

    // execute merging shared query plan 1 and 3
    globalQueryPlan->addQueryPlan(queryPlan3);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    updatedSharedQueryPlansDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_EQ(updatedSharedQueryPlansDeploy.size(), 1U);

    auto updatedSharedQueryPlan1 = updatedSharedQueryPlansDeploy[0]->getQueryPlan();
    EXPECT_TRUE(updatedSharedQueryPlan1);

    NES_INFO("Shared Plan After Merging with Query 3\n{}", updatedSharedQueryPlan1->toString());

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_EQ(updatedRootOperators1.size(), 3U);
    EXPECT_EQ(updatedSharedQueryPlan1->getSourceOperators().size(), 1U);

    now = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    auto changeLogEntries = updatedSharedQueryPlansDeploy[0]->getChangeLogEntries(now);
    EXPECT_EQ(changeLogEntries.size(), 1U);

    for (const auto& upstreamOperator : changeLogEntries[0].second->upstreamOperators) {
        EXPECT_TRUE(upstreamOperator->instanceOf<LogicalFilterOperator>());
        // Three different map operators are added
        EXPECT_TRUE(upstreamOperator->getParents().size() == 3U);
        for (const auto& parent : upstreamOperator->getParents()) {
            EXPECT_TRUE(parent->instanceOf<LogicalMapOperator>());
        }
        // There are one physical sources
        EXPECT_TRUE(upstreamOperator->getChildren().size() == 1U);
        for (const auto& child : upstreamOperator->getChildren()) {
            EXPECT_TRUE(child->instanceOf<SourceLogicalOperator>());
        }
    }
}

TEST_F(Z3SignatureBasedPartialQueryMergerBottomUpRuleTest, testMergingPartiallyEqualQueriesWithQueryStopChangeLog) {

    auto topologySpecificReWrite = Optimizer::TopologySpecificQueryRewritePhase::create(Topology::create(),
                                                                                        sourceCatalog,
                                                                                        Configurations::OptimizerConfiguration(),
                                                                                        statisticProbeHandler);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("car").sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorPtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query query2 = Query::from("car").sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorPtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    queryPlan1 = topologySpecificReWrite->execute(queryPlan1);
    queryPlan2 = topologySpecificReWrite->execute(queryPlan2);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase =
        Optimizer::SignatureInferencePhase::create(context, Optimizer::QueryMergerRule::Z3SignatureBasedPartialQueryMergerRule);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedPartialQueryMergerBottomUpRule::create(context);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 1);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    EXPECT_TRUE(updatedSharedQueryPlan1);

    NES_INFO("{}", updatedSharedQueryPlan1->toString());

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 2);

    // stop query 1
    globalQueryPlan->removeQuery(queryPlan1->getQueryId(), RequestType::StopQuery);

    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    auto updatedSharedPlanAfterStopToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_TRUE(updatedSharedPlanAfterStopToDeploy.size() == 1);

    auto updatedSharedPlanAfterStop = updatedSharedPlanAfterStopToDeploy[0]->getQueryPlan();
    auto now = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    auto changeLogEntries = updatedSharedPlanAfterStopToDeploy[0]->getChangeLogEntries(now);

    EXPECT_TRUE(updatedSharedPlanAfterStop);

    ASSERT_EQ(changeLogEntries.size(), 1UL);

    //assert that the sink operators have same up-stream operator
    auto rootOperatorsAfterStop = updatedSharedPlanAfterStop->getRootOperators();
    EXPECT_EQ(rootOperatorsAfterStop.size(), 2);

    //assert that the at least one sink operator is in the state TO_BE_REMOVED
    EXPECT_TRUE(std::any_of(rootOperatorsAfterStop.begin(), rootOperatorsAfterStop.end(), [](const OperatorPtr operatorToCheck) {
        return operatorToCheck->as_if<LogicalOperator>()->getOperatorState() == OperatorState::TO_BE_REMOVED;
    }));
}

TEST_F(Z3SignatureBasedPartialQueryMergerBottomUpRuleTest, testMergingPartiallyEqualQueriesWithQueryStop) {

    auto topologySpecificReWrite = Optimizer::TopologySpecificQueryRewritePhase::create(Topology::create(),
                                                                                        sourceCatalog,
                                                                                        Configurations::OptimizerConfiguration(),
                                                                                        statisticProbeHandler);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("car")
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id1") < 45)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorPtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query query2 = Query::from("car")
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("value1") < 45)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorPtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    queryPlan1 = topologySpecificReWrite->execute(queryPlan1);
    queryPlan2 = topologySpecificReWrite->execute(queryPlan2);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase =
        Optimizer::SignatureInferencePhase::create(context, Optimizer::QueryMergerRule::Z3SignatureBasedPartialQueryMergerRule);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedPartialQueryMergerBottomUpRule::create(context);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 1);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    EXPECT_TRUE(updatedSharedQueryPlan1);

    NES_INFO("{}", updatedSharedQueryPlan1->toString());

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 2);

    for (const auto& sink1Child : updatedRootOperators1[0]->getChildren()) {
        bool found = false;
        for (const auto& sink2Child : updatedRootOperators1[1]->getChildren()) {
            EXPECT_NE(sink1Child, sink2Child);
            auto sink1ChildGrandChild = sink1Child->getChildren();
            auto sink2ChildGrandChild = sink2Child->getChildren();
            EXPECT_TRUE(sink1ChildGrandChild.size() == 1);
            EXPECT_TRUE(sink2ChildGrandChild.size() == 1);
            if (sink1ChildGrandChild[0]->equal(sink2ChildGrandChild[0])) {
                found = true;
                break;
            }
        }
        EXPECT_TRUE(found);
    }

    // stop query 1
    globalQueryPlan->removeQuery(queryPlan1->getQueryId(), RequestType::StopQuery);

    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    auto updatedSharedPlanAfterStopToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_TRUE(updatedSharedPlanAfterStopToDeploy.size() == 1);

    auto updatedSharedPlanAfterStop = updatedSharedPlanAfterStopToDeploy[0]->getQueryPlan();
    EXPECT_TRUE(updatedSharedQueryPlan1);

    NES_INFO("{}", updatedSharedQueryPlan1->toString());

    //assert that the sink operators have same up-stream operator
    auto rootOperatorsAfterStop = updatedSharedPlanAfterStop->getRootOperators();
    EXPECT_EQ(rootOperatorsAfterStop.size(), 2);

    //assert that the at least one sink operator is in the state TO_BE_REMOVED
    EXPECT_TRUE(std::any_of(rootOperatorsAfterStop.begin(), rootOperatorsAfterStop.end(), [](const OperatorPtr operatorToCheck) {
        return operatorToCheck->as_if<LogicalOperator>()->getOperatorState() == OperatorState::TO_BE_REMOVED;
    }));
}
