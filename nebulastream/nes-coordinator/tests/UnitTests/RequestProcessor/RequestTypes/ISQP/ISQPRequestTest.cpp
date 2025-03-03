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
#include <API/TestSchemas.hpp>
#include <BaseUnitTest.hpp>
#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Query/QueryCatalogEntry.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Exceptions/RPCQueryUndeploymentException.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkAssignerLogicalOperator.hpp>
#include <Optimizer/Phases/QueryMergerPhase.hpp>
#include <Optimizer/Phases/QueryPlacementAmendmentPhase.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <RequestProcessor/RequestTypes/AddQueryRequest.hpp>
#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPAddLinkEvent.hpp>
#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPAddLinkPropertyEvent.hpp>
#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPAddNodeEvent.hpp>
#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPAddQueryEvent.hpp>
#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPRemoveLinkEvent.hpp>
#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPRemoveNodeEvent.hpp>
#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPRemoveQueryEvent.hpp>
#include <RequestProcessor/RequestTypes/ISQP/ISQPRequest.hpp>
#include <RequestProcessor/StorageHandles/StorageDataStructures.hpp>
#include <RequestProcessor/StorageHandles/TwoPhaseLockingStorageHandler.hpp>
#include <Services/PlacementAmendment/PlacementAmendmentHandler.hpp>
#include <Services/PlacementAmendment/PlacementAmendmentInstance.hpp>
#include <StatisticCollection/StatisticCache/DefaultStatisticCache.hpp>
#include <StatisticCollection/StatisticProbeHandling/DefaultStatisticProbeGenerator.hpp>
#include <StatisticCollection/StatisticProbeHandling/StatisticProbeHandler.hpp>
#include <StatisticCollection/StatisticRegistry/StatisticRegistry.hpp>
#include <Util/IncrementalPlacementUtils.hpp>
#include <Util/Logger/Logger.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <z3++.h>

using namespace testing;

namespace NES::Optimizer {

class MockedPlacementAmendmentHandler : public NES::Optimizer::PlacementAmendmentHandler {
  public:
    MockedPlacementAmendmentHandler(uint16_t numOfHandler) : PlacementAmendmentHandler(numOfHandler){};
    MOCK_METHOD(void, enqueueRequest, (const NES::Optimizer::PlacementAmendmentInstancePtr&), (override));
};

}// namespace NES::Optimizer

namespace NES::RequestProcessor {

class ISQPRequestTest : public Testing::BaseUnitTest {
  public:
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    std::shared_ptr<Catalogs::UDF::UDFCatalog> udfCatalog;
    Optimizer::PlacementStrategy TEST_PLACEMENT_STRATEGY = Optimizer::PlacementStrategy::BottomUp;
    uint8_t ZERO_RETRIES = 0;
    std::shared_ptr<Catalogs::Query::QueryCatalog> queryCatalog;
    TopologyPtr topology;
    GlobalQueryPlanPtr globalQueryPlan;
    Optimizer::GlobalExecutionPlanPtr globalExecutionPlan;
    Configurations::CoordinatorConfigurationPtr coordinatorConfiguration;
    Statistic::StatisticProbeHandlerPtr statisticProbeHandler;
    z3::ContextPtr z3Context;

    /* Will be called before all tests in this class are started. */
    static void SetUpTestCase() { NES::Logger::setupLogging("QueryFailureTest.log", NES::LogLevel::LOG_DEBUG); }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        topology = Topology::create();
        globalQueryPlan = GlobalQueryPlan::create();
        globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();
        queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
        udfCatalog = std::make_shared<Catalogs::UDF::UDFCatalog>();
        coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
        statisticProbeHandler = Statistic::StatisticProbeHandler::create(Statistic::StatisticRegistry::create(),
                                                                         Statistic::DefaultStatisticProbeGenerator::create(),
                                                                         Statistic::DefaultStatisticCache::create(),
                                                                         topology);
        z3::config cfg;
        cfg.set("timeout", 50000);
        cfg.set("model", false);
        cfg.set("type_check", false);
        z3Context = std::make_shared<z3::context>(cfg);
    }
};

void updateCapturedValue(const NES::Optimizer::PlacementAmendmentInstancePtr& placementAmendmentInstance) {
    placementAmendmentInstance->setPromise(true);
}

//test adding topology nodes and links
TEST_F(ISQPRequestTest, testISQPAddNodeAndLinkEvents) {

    //Create mocked handler
    auto mockedPlacementAmendmentHandler = std::make_shared<NES::Optimizer::MockedPlacementAmendmentHandler>(1);
    // No need to change placements as ISQP request contains only topology change events. Therefore, asser that no call
    // to mocked placement amendment handler are made.
    EXPECT_CALL(*mockedPlacementAmendmentHandler, enqueueRequest(_)).Times(0);

    // init topology nodes by computing ISQP events
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;
    auto nodeId1 = WorkerId(1);
    auto addNodeEvent1 = ISQPAddNodeEvent::create(WorkerType::CLOUD, nodeId1, "localhost", 4000, 4002, 4, properties);
    auto nodeId2 = WorkerId(2);
    auto addNodeEvent2 = ISQPAddNodeEvent::create(WorkerType::SENSOR, nodeId2, "localhost", 4000, 4002, 4, properties);
    auto nodeId3 = WorkerId(3);
    auto addNodeEvent3 = ISQPAddNodeEvent::create(WorkerType::SENSOR, nodeId3, "localhost", 4000, 4002, 4, properties);
    auto nodeId4 = WorkerId(4);
    auto addNodeEvent4 = ISQPAddNodeEvent::create(WorkerType::SENSOR, nodeId4, "localhost", 4000, 4002, 4, properties);
    auto isqpRemoveLink14 = ISQPRemoveLinkEvent::create(nodeId1, nodeId4);
    auto isqpAddLink34 = ISQPAddLinkEvent::create(nodeId3, nodeId4);

    std::vector<ISQPEventPtr> isqpEvents;
    isqpEvents.emplace_back(addNodeEvent1);
    isqpEvents.emplace_back(addNodeEvent2);
    isqpEvents.emplace_back(addNodeEvent3);
    isqpEvents.emplace_back(addNodeEvent4);
    isqpEvents.emplace_back(isqpRemoveLink14);
    isqpEvents.emplace_back(isqpAddLink34);

    // Prepare the system
    auto storageHandler = TwoPhaseLockingStorageHandler::create({coordinatorConfiguration,
                                                                 topology,
                                                                 globalExecutionPlan,
                                                                 globalQueryPlan,
                                                                 queryCatalog,
                                                                 sourceCatalog,
                                                                 udfCatalog,
                                                                 statisticProbeHandler});
    // Create ISQP request
    auto isqpRequest = ISQPRequest::create(mockedPlacementAmendmentHandler, z3Context, isqpEvents, ZERO_RETRIES);
    constexpr auto requestId = RequestId(1);
    isqpRequest->setId(requestId);

    // Execute add request until deployment phase
    try {
        isqpRequest->execute(storageHandler);
    } catch (Exceptions::RPCQueryUndeploymentException& e) {
        FAIL();
    }

    // Assertions
    EXPECT_TRUE(topology->nodeWithWorkerIdExists(nodeId1));
    EXPECT_TRUE(topology->nodeWithWorkerIdExists(nodeId2));
    EXPECT_TRUE(topology->nodeWithWorkerIdExists(nodeId3));
    EXPECT_TRUE(topology->nodeWithWorkerIdExists(nodeId4));
}

//test adding two requests to first add topology nodes and then remove topology nodes
TEST_F(ISQPRequestTest, testFirstISQPAddNodeThenRemoveNodes) {

    //Create mocked handler
    auto mockedPlacementAmendmentHandler = std::make_shared<NES::Optimizer::MockedPlacementAmendmentHandler>(1);
    // No need to change placements as ISQP request contains only topology change events. Therefore, asser that no call
    // to mocked placement amendment handler are made.
    EXPECT_CALL(*mockedPlacementAmendmentHandler, enqueueRequest(_)).Times(0);

    // init topology nodes
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;
    auto nodeId1 = WorkerId(1);
    auto addNodeEvent1 = ISQPAddNodeEvent::create(WorkerType::CLOUD, nodeId1, "localhost", 4000, 4002, 4, properties);
    auto nodeId2 = WorkerId(2);
    auto addNodeEvent2 = ISQPAddNodeEvent::create(WorkerType::SENSOR, nodeId2, "localhost", 4000, 4002, 4, properties);
    auto nodeId3 = WorkerId(3);
    auto addNodeEvent3 = ISQPAddNodeEvent::create(WorkerType::SENSOR, nodeId3, "localhost", 4000, 4002, 4, properties);
    auto nodeId4 = WorkerId(4);
    auto addNodeEvent4 = ISQPAddNodeEvent::create(WorkerType::SENSOR, nodeId4, "localhost", 4000, 4002, 4, properties);

    auto isqpRemoveLink14 = ISQPRemoveLinkEvent::create(nodeId1, nodeId4);
    auto isqpAddLink34 = ISQPAddLinkEvent::create(nodeId3, nodeId4);

    std::vector<ISQPEventPtr> isqpEventsForRequest1;
    isqpEventsForRequest1.emplace_back(addNodeEvent1);
    isqpEventsForRequest1.emplace_back(addNodeEvent2);
    isqpEventsForRequest1.emplace_back(addNodeEvent3);
    isqpEventsForRequest1.emplace_back(addNodeEvent4);
    isqpEventsForRequest1.emplace_back(isqpRemoveLink14);
    isqpEventsForRequest1.emplace_back(isqpAddLink34);

    // Prepare the system
    auto storageHandler = TwoPhaseLockingStorageHandler::create({coordinatorConfiguration,
                                                                 topology,
                                                                 globalExecutionPlan,
                                                                 globalQueryPlan,
                                                                 queryCatalog,
                                                                 sourceCatalog,
                                                                 udfCatalog,
                                                                 statisticProbeHandler});
    // Create ISQP request
    auto isqpRequest1 = ISQPRequest::create(mockedPlacementAmendmentHandler, z3Context, isqpEventsForRequest1, ZERO_RETRIES);
    constexpr auto requestId1 = RequestId(1);
    isqpRequest1->setId(requestId1);

    // Execute add request until deployment phase
    try {
        isqpRequest1->execute(storageHandler);
    } catch (Exceptions::RPCQueryUndeploymentException& e) {
        FAIL();
    }
    EXPECT_TRUE(topology->nodeWithWorkerIdExists(nodeId4));

    auto parentNodeIds = topology->getParentTopologyNodeIds(nodeId4);
    auto childNodeIds = topology->getChildTopologyNodeIds(nodeId4);

    auto nodeRemovalEvent = ISQPRemoveNodeEvent::create(nodeId4, parentNodeIds, childNodeIds);

    std::vector<ISQPEventPtr> isqpEventsForRequest2;
    isqpEventsForRequest2.emplace_back(nodeRemovalEvent);

    // Prepare
    auto isqpRequest2 = ISQPRequest::create(mockedPlacementAmendmentHandler, z3Context, isqpEventsForRequest2, ZERO_RETRIES);
    constexpr auto requestId2 = RequestId(2);
    isqpRequest2->setId(requestId2);
    // Execute add request until deployment phase
    try {
        isqpRequest2->execute(storageHandler);
    } catch (Exceptions::RPCQueryUndeploymentException& e) {
        FAIL();
    }
    EXPECT_FALSE(topology->nodeWithWorkerIdExists(nodeId4));
}

//test adding a single
TEST_F(ISQPRequestTest, testAddQueryEvents) {

    //Create mocked handler and define the mock assertions
    auto mockedPlacementAmendmentHandler = std::make_shared<NES::Optimizer::MockedPlacementAmendmentHandler>(1);
    // Two ISQP requests are issues in this test. One contains only topology change events and other contains an add query event.
    // Therefore, assert that only one call ot the mocked placement amendment handler are made.
    EXPECT_CALL(*mockedPlacementAmendmentHandler, enqueueRequest(_)).Times(1).WillRepeatedly(Invoke(updateCapturedValue));

    // init topology nodes
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;
    auto nodeId1 = WorkerId(1);
    auto addNodeEvent1 = ISQPAddNodeEvent::create(WorkerType::CLOUD, nodeId1, "localhost", 4000, 4002, 4, properties);
    auto nodeId2 = WorkerId(2);
    auto addNodeEvent2 = ISQPAddNodeEvent::create(WorkerType::SENSOR, nodeId2, "localhost", 4000, 4002, 4, properties);
    auto nodeId3 = WorkerId(3);
    auto addNodeEvent3 = ISQPAddNodeEvent::create(WorkerType::SENSOR, nodeId3, "localhost", 4000, 4002, 4, properties);
    auto nodeId4 = WorkerId(4);
    auto addNodeEvent4 = ISQPAddNodeEvent::create(WorkerType::SENSOR, nodeId4, "localhost", 4000, 4002, 4, properties);

    auto isqpRemoveLink14 = ISQPRemoveLinkEvent::create(nodeId1, nodeId4);
    auto isqpRemoveLink13 = ISQPRemoveLinkEvent::create(nodeId1, nodeId3);
    auto isqpAddLink34 = ISQPAddLinkEvent::create(nodeId3, nodeId4);
    auto isqpAddLink23 = ISQPAddLinkEvent::create(nodeId2, nodeId3);
    auto isqpAddLinkProperty34 = ISQPAddLinkPropertyEvent::create(nodeId3, nodeId4, 1, 1);
    auto isqpAddLinkProperty23 = ISQPAddLinkPropertyEvent::create(nodeId2, nodeId3, 1, 1);
    auto isqpAddLinkProperty12 = ISQPAddLinkPropertyEvent::create(nodeId1, nodeId2, 1, 1);

    std::vector<ISQPEventPtr> isqpEventsForRequest1;
    isqpEventsForRequest1.emplace_back(addNodeEvent1);
    isqpEventsForRequest1.emplace_back(addNodeEvent2);
    isqpEventsForRequest1.emplace_back(addNodeEvent3);
    isqpEventsForRequest1.emplace_back(addNodeEvent4);
    isqpEventsForRequest1.emplace_back(isqpRemoveLink14);
    isqpEventsForRequest1.emplace_back(isqpRemoveLink13);
    isqpEventsForRequest1.emplace_back(isqpAddLink34);
    isqpEventsForRequest1.emplace_back(isqpAddLink23);
    isqpEventsForRequest1.emplace_back(isqpAddLinkProperty34);
    isqpEventsForRequest1.emplace_back(isqpAddLinkProperty23);
    isqpEventsForRequest1.emplace_back(isqpAddLinkProperty12);

    // Enable query merging
    coordinatorConfiguration->optimizer.queryMergerRule = Optimizer::QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule;
    // Enable Incremental placement
    coordinatorConfiguration->optimizer.enableIncrementalPlacement = true;

    // Prepare the system
    auto storageHandler = TwoPhaseLockingStorageHandler::create({coordinatorConfiguration,
                                                                 topology,
                                                                 globalExecutionPlan,
                                                                 globalQueryPlan,
                                                                 queryCatalog,
                                                                 sourceCatalog,
                                                                 udfCatalog,
                                                                 statisticProbeHandler});

    // Create ISQP request
    auto isqpRequest1 = ISQPRequest::create(mockedPlacementAmendmentHandler, z3Context, isqpEventsForRequest1, ZERO_RETRIES);
    constexpr auto requestId1 = RequestId(1);
    isqpRequest1->setId(requestId1);
    // Execute add request until deployment phase
    try {
        isqpRequest1->execute(storageHandler);
    } catch (Exceptions::RPCQueryUndeploymentException& e) {
        FAIL();
    }
    EXPECT_TRUE(topology->nodeWithWorkerIdExists(nodeId4));

    // Register physical and logical sources
    std::string logicalSourceName = "test";
    auto defaultSourceType = DefaultSourceType::create(logicalSourceName, "pTest1");
    auto physicalSource = PhysicalSource::create(defaultSourceType);
    auto schema = TestSchemas::getSchemaTemplate("id_val_u32");
    auto logicalSource = LogicalSource::create(logicalSourceName, schema);
    sourceCatalog->addLogicalSource(logicalSource->getLogicalSourceName(), logicalSource->getSchema());
    auto sce = Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, nodeId4);
    sourceCatalog->addPhysicalSource(logicalSourceName, sce);

    auto query1 = Query::from(logicalSourceName).sink(NullOutputSinkDescriptor::create());
    const QueryPlanPtr& queryPlan1 = query1.getQueryPlan();
    queryPlan1->setQueryId(QueryId(1));
    auto queryAddEvent1 = ISQPAddQueryEvent::create(queryPlan1, TEST_PLACEMENT_STRATEGY);

    std::vector<ISQPEventPtr> isqpEventsForRequest2;
    isqpEventsForRequest2.emplace_back(queryAddEvent1);

    // Prepare
    auto isqpRequest2 = ISQPRequest::create(mockedPlacementAmendmentHandler, z3Context, isqpEventsForRequest2, ZERO_RETRIES);
    constexpr auto requestId2 = RequestId(2);
    isqpRequest2->setId(requestId2);

    // Execute add request until deployment phase
    try {
        isqpRequest2->execute(storageHandler);
    } catch (Exceptions::RPCQueryUndeploymentException& e) {
        FAIL();
    }
}

//test adding a single
TEST_F(ISQPRequestTest, testMultipleAddQueryEventsInaSingleBatch) {

    //Create mocked handler and define the mock assertions
    auto mockedPlacementAmendmentHandler = std::make_shared<NES::Optimizer::MockedPlacementAmendmentHandler>(1);
    EXPECT_CALL(*mockedPlacementAmendmentHandler, enqueueRequest(_)).Times(2).WillRepeatedly(Invoke(updateCapturedValue));

    // init topology nodes
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;
    auto nodeId1 = WorkerId(1);
    auto addNodeEvent1 = ISQPAddNodeEvent::create(WorkerType::CLOUD, nodeId1, "localhost", 4000, 4002, 4, properties);
    auto nodeId2 = WorkerId(2);
    auto addNodeEvent2 = ISQPAddNodeEvent::create(WorkerType::SENSOR, nodeId2, "localhost", 4000, 4002, 4, properties);
    auto nodeId3 = WorkerId(3);
    auto addNodeEvent3 = ISQPAddNodeEvent::create(WorkerType::SENSOR, nodeId3, "localhost", 4000, 4002, 4, properties);
    auto nodeId4 = WorkerId(4);
    auto addNodeEvent4 = ISQPAddNodeEvent::create(WorkerType::SENSOR, nodeId4, "localhost", 4000, 4002, 4, properties);

    auto isqpRemoveLink14 = ISQPRemoveLinkEvent::create(nodeId1, nodeId4);
    auto isqpRemoveLink13 = ISQPRemoveLinkEvent::create(nodeId1, nodeId3);
    auto isqpAddLink34 = ISQPAddLinkEvent::create(nodeId3, nodeId4);
    auto isqpAddLink23 = ISQPAddLinkEvent::create(nodeId2, nodeId3);
    auto isqpAddLinkProperty34 = ISQPAddLinkPropertyEvent::create(nodeId3, nodeId4, 1, 1);
    auto isqpAddLinkProperty23 = ISQPAddLinkPropertyEvent::create(nodeId2, nodeId3, 1, 1);
    auto isqpAddLinkProperty12 = ISQPAddLinkPropertyEvent::create(nodeId1, nodeId2, 1, 1);

    std::vector<ISQPEventPtr> isqpEventsForRequest1;
    isqpEventsForRequest1.emplace_back(addNodeEvent1);
    isqpEventsForRequest1.emplace_back(addNodeEvent2);
    isqpEventsForRequest1.emplace_back(addNodeEvent3);
    isqpEventsForRequest1.emplace_back(addNodeEvent4);
    isqpEventsForRequest1.emplace_back(isqpRemoveLink14);
    isqpEventsForRequest1.emplace_back(isqpRemoveLink13);
    isqpEventsForRequest1.emplace_back(isqpAddLink34);
    isqpEventsForRequest1.emplace_back(isqpAddLink23);
    isqpEventsForRequest1.emplace_back(isqpAddLinkProperty34);
    isqpEventsForRequest1.emplace_back(isqpAddLinkProperty23);
    isqpEventsForRequest1.emplace_back(isqpAddLinkProperty12);

    // Enable query merging
    coordinatorConfiguration->optimizer.queryMergerRule = Optimizer::QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule;
    // Enable incremental placement
    coordinatorConfiguration->optimizer.enableIncrementalPlacement = true;
    // Number of amender threads
    coordinatorConfiguration->optimizer.placementAmendmentThreadCount = 2;

    // Prepare the system
    auto storageHandler = TwoPhaseLockingStorageHandler::create({coordinatorConfiguration,
                                                                 topology,
                                                                 globalExecutionPlan,
                                                                 globalQueryPlan,
                                                                 queryCatalog,
                                                                 sourceCatalog,
                                                                 udfCatalog,
                                                                 statisticProbeHandler});
    auto isqpRequest1 = ISQPRequest::create(mockedPlacementAmendmentHandler, z3Context, isqpEventsForRequest1, ZERO_RETRIES);
    constexpr auto requestId1 = RequestId(1);
    isqpRequest1->setId(requestId1);
    // Execute add request until deployment phase
    try {
        isqpRequest1->execute(storageHandler);
    } catch (Exceptions::RPCQueryUndeploymentException& e) {
        FAIL();
    }
    EXPECT_TRUE(topology->nodeWithWorkerIdExists(nodeId4));

    // Register physical and logical sources
    std::string logicalSourceName = "test";
    auto defaultSourceType = DefaultSourceType::create(logicalSourceName, "pTest1");
    auto physicalSource = PhysicalSource::create(defaultSourceType);
    auto schema = TestSchemas::getSchemaTemplate("id_val_u32");
    auto logicalSource = LogicalSource::create(logicalSourceName, schema);
    sourceCatalog->addLogicalSource(logicalSource->getLogicalSourceName(), logicalSource->getSchema());
    auto sce = Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, nodeId4);
    sourceCatalog->addPhysicalSource(logicalSourceName, sce);

    auto query1 = Query::from(logicalSourceName).sink(NullOutputSinkDescriptor::create());
    const QueryPlanPtr& queryPlan1 = query1.getQueryPlan();
    queryPlan1->setQueryId(QueryId(1));
    auto queryAddEvent1 = ISQPAddQueryEvent::create(queryPlan1, TEST_PLACEMENT_STRATEGY);

    auto query2 = Query::from(logicalSourceName).sink(NullOutputSinkDescriptor::create());
    const QueryPlanPtr& queryPlan2 = query2.getQueryPlan();
    queryPlan2->setQueryId(QueryId(2));
    auto queryAddEvent2 = ISQPAddQueryEvent::create(queryPlan2, TEST_PLACEMENT_STRATEGY);

    std::vector<ISQPEventPtr> isqpEventsForRequest2;
    isqpEventsForRequest2.emplace_back(queryAddEvent1);
    isqpEventsForRequest2.emplace_back(queryAddEvent2);

    // Prepare
    auto isqpRequest2 = ISQPRequest::create(mockedPlacementAmendmentHandler, z3Context, isqpEventsForRequest2, ZERO_RETRIES);
    constexpr auto requestId2 = RequestId(2);
    isqpRequest2->setId(requestId2);
    // Execute add request until deployment phase
    try {
        isqpRequest2->execute(storageHandler);
    } catch (Exceptions::RPCQueryUndeploymentException& e) {
        FAIL();
    }
    auto response1 = queryAddEvent1->getResponse().get();
    auto queryId1 = std::static_pointer_cast<RequestProcessor::ISQPAddQueryResponse>(response1)->queryId;
    EXPECT_NE(queryId1, INVALID_QUERY_ID);
    auto response2 = queryAddEvent2->getResponse().get();
    auto queryId2 = std::static_pointer_cast<RequestProcessor::ISQPAddQueryResponse>(response2)->queryId;
    EXPECT_NE(queryId2, INVALID_QUERY_ID);

    // Prepare
    auto queryRemoveEvent = ISQPRemoveQueryEvent::create(queryId2);
    std::vector<ISQPEventPtr> isqpEventsForRequest3;
    isqpEventsForRequest3.emplace_back(queryRemoveEvent);
    auto isqpRequest3 = ISQPRequest::create(mockedPlacementAmendmentHandler, z3Context, isqpEventsForRequest3, ZERO_RETRIES);
    constexpr RequestId requestId3(3);
    isqpRequest3->setId(requestId3);
    try {
        isqpRequest3->execute(storageHandler);
    } catch (Exceptions::RPCQueryUndeploymentException& e) {
        FAIL();
    }
}

//test adding multiple queries
TEST_F(ISQPRequestTest, testMultipleAddQueryEventsInDifferentBatchWithMerging) {

    //Create mocked handler and define the mock assertions
    auto mockedPlacementAmendmentHandler = std::make_shared<NES::Optimizer::MockedPlacementAmendmentHandler>(1);
    EXPECT_CALL(*mockedPlacementAmendmentHandler, enqueueRequest(_)).Times(2).WillRepeatedly(Invoke(updateCapturedValue));

    // init topology nodes
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;
    auto nodeId1 = WorkerId(1);
    auto addNodeEvent1 = ISQPAddNodeEvent::create(WorkerType::CLOUD, nodeId1, "localhost", 4000, 4002, 4, properties);
    auto nodeId2 = WorkerId(2);
    auto addNodeEvent2 = ISQPAddNodeEvent::create(WorkerType::SENSOR, nodeId2, "localhost", 4000, 4002, 4, properties);
    auto nodeId3 = WorkerId(3);
    auto addNodeEvent3 = ISQPAddNodeEvent::create(WorkerType::SENSOR, nodeId3, "localhost", 4000, 4002, 4, properties);
    auto nodeId4 = WorkerId(4);
    auto addNodeEvent4 = ISQPAddNodeEvent::create(WorkerType::SENSOR, nodeId4, "localhost", 4000, 4002, 4, properties);

    auto isqpRemoveLink14 = ISQPRemoveLinkEvent::create(nodeId1, nodeId4);
    auto isqpAddLink34 = ISQPAddLinkEvent::create(nodeId3, nodeId4);

    std::vector<ISQPEventPtr> isqpEventsForRequest1;
    isqpEventsForRequest1.emplace_back(addNodeEvent1);
    isqpEventsForRequest1.emplace_back(addNodeEvent2);
    isqpEventsForRequest1.emplace_back(addNodeEvent3);
    isqpEventsForRequest1.emplace_back(addNodeEvent4);
    isqpEventsForRequest1.emplace_back(isqpRemoveLink14);
    isqpEventsForRequest1.emplace_back(isqpAddLink34);

    // Enable query merging
    coordinatorConfiguration->optimizer.queryMergerRule = Optimizer::QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule;
    // Enable incremental placement
    coordinatorConfiguration->optimizer.enableIncrementalPlacement = true;
    // Number of amender threads
    coordinatorConfiguration->optimizer.placementAmendmentThreadCount = 2;
    // placement amendment mode
    coordinatorConfiguration->optimizer.placementAmendmentMode = Optimizer::PlacementAmendmentMode::OPTIMISTIC;

    // Prepare the system
    auto storageHandler = TwoPhaseLockingStorageHandler::create({coordinatorConfiguration,
                                                                 topology,
                                                                 globalExecutionPlan,
                                                                 globalQueryPlan,
                                                                 queryCatalog,
                                                                 sourceCatalog,
                                                                 udfCatalog,
                                                                 statisticProbeHandler});
    auto isqpRequest1 = ISQPRequest::create(mockedPlacementAmendmentHandler, z3Context, isqpEventsForRequest1, ZERO_RETRIES);
    constexpr auto requestId1 = RequestId(1);
    isqpRequest1->setId(requestId1);
    // Execute add request until deployment phase
    try {
        isqpRequest1->execute(storageHandler);
    } catch (Exceptions::RPCQueryUndeploymentException& e) {
        FAIL();
    }
    EXPECT_TRUE(topology->nodeWithWorkerIdExists(nodeId4));

    // Register physical and logical sources
    std::string logicalSourceName = "test";
    auto defaultSourceType = DefaultSourceType::create(logicalSourceName, "pTest1");
    auto physicalSource = PhysicalSource::create(defaultSourceType);
    auto schema = TestSchemas::getSchemaTemplate("id_val_u32");
    auto logicalSource = LogicalSource::create(logicalSourceName, schema);
    sourceCatalog->addLogicalSource(logicalSource->getLogicalSourceName(), logicalSource->getSchema());
    auto sce = Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, nodeId4);
    sourceCatalog->addPhysicalSource(logicalSourceName, sce);

    auto query1 = Query::from(logicalSourceName).sink(NullOutputSinkDescriptor::create());
    const QueryPlanPtr& queryPlan1 = query1.getQueryPlan();
    auto queryAddEvent1 = ISQPAddQueryEvent::create(queryPlan1, TEST_PLACEMENT_STRATEGY);

    std::vector<ISQPEventPtr> isqpEventsForRequest2;
    isqpEventsForRequest2.emplace_back(queryAddEvent1);

    // Prepare
    auto isqpRequest2 = ISQPRequest::create(mockedPlacementAmendmentHandler, z3Context, isqpEventsForRequest2, ZERO_RETRIES);
    constexpr auto requestId2 = RequestId(2);
    isqpRequest2->setId(requestId2);
    // Execute add request until deployment phase
    try {
        isqpRequest2->execute(storageHandler);
    } catch (Exceptions::RPCQueryUndeploymentException& e) {
        FAIL();
    }

    auto query2 = Query::from(logicalSourceName).sink(NullOutputSinkDescriptor::create());
    const QueryPlanPtr& queryPlan2 = query2.getQueryPlan();
    auto queryAddEvent2 = ISQPAddQueryEvent::create(queryPlan2, TEST_PLACEMENT_STRATEGY);

    std::vector<ISQPEventPtr> isqpEventsForRequest3;
    isqpEventsForRequest3.emplace_back(queryAddEvent2);

    // Prepare
    auto isqpRequest3 = ISQPRequest::create(mockedPlacementAmendmentHandler, z3Context, isqpEventsForRequest3, ZERO_RETRIES);
    constexpr auto requestId3 = RequestId(3);
    isqpRequest3->setId(requestId3);
    // Execute add request until deployment phase
    try {
        isqpRequest3->execute(storageHandler);
    } catch (Exceptions::RPCQueryUndeploymentException& e) {
        FAIL();
    }
}

//test adding multiple queries
TEST_F(ISQPRequestTest, testMultipleAddQueryEventsInDifferentBatchWithoutMerging) {

    //Create mocked handler and define the mock assertions
    auto mockedPlacementAmendmentHandler = std::make_shared<NES::Optimizer::MockedPlacementAmendmentHandler>(1);
    EXPECT_CALL(*mockedPlacementAmendmentHandler, enqueueRequest(_)).Times(3).WillRepeatedly(Invoke(updateCapturedValue));

    // init topology nodes
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;
    auto nodeId1 = WorkerId(1);
    auto addNodeEvent1 = ISQPAddNodeEvent::create(WorkerType::CLOUD, nodeId1, "localhost", 4000, 4002, 4, properties);
    auto nodeId2 = WorkerId(2);
    auto addNodeEvent2 = ISQPAddNodeEvent::create(WorkerType::SENSOR, nodeId2, "localhost", 4000, 4002, 4, properties);
    auto nodeId3 = WorkerId(3);
    auto addNodeEvent3 = ISQPAddNodeEvent::create(WorkerType::SENSOR, nodeId3, "localhost", 4000, 4002, 4, properties);
    auto nodeId4 = WorkerId(4);
    auto addNodeEvent4 = ISQPAddNodeEvent::create(WorkerType::SENSOR, nodeId4, "localhost", 4000, 4002, 4, properties);

    auto isqpRemoveLink14 = ISQPRemoveLinkEvent::create(nodeId1, nodeId4);
    auto isqpAddLink34 = ISQPAddLinkEvent::create(nodeId3, nodeId4);

    std::vector<ISQPEventPtr> isqpEventsForRequest1;
    isqpEventsForRequest1.emplace_back(addNodeEvent1);
    isqpEventsForRequest1.emplace_back(addNodeEvent2);
    isqpEventsForRequest1.emplace_back(addNodeEvent3);
    isqpEventsForRequest1.emplace_back(addNodeEvent4);
    isqpEventsForRequest1.emplace_back(isqpRemoveLink14);
    isqpEventsForRequest1.emplace_back(isqpAddLink34);

    // Disable query merging
    coordinatorConfiguration->optimizer.queryMergerRule = Optimizer::QueryMergerRule::DefaultQueryMergerRule;
    // Disable incremental placement
    coordinatorConfiguration->optimizer.enableIncrementalPlacement = false;
    // Number of amender threads
    coordinatorConfiguration->optimizer.placementAmendmentThreadCount = 2;

    // Prepare the system
    auto storageHandler = TwoPhaseLockingStorageHandler::create({coordinatorConfiguration,
                                                                 topology,
                                                                 globalExecutionPlan,
                                                                 globalQueryPlan,
                                                                 queryCatalog,
                                                                 sourceCatalog,
                                                                 udfCatalog,
                                                                 statisticProbeHandler});
    auto isqpRequest1 = ISQPRequest::create(mockedPlacementAmendmentHandler, z3Context, isqpEventsForRequest1, ZERO_RETRIES);
    constexpr auto requestId1 = RequestId(1);
    isqpRequest1->setId(requestId1);
    // Execute add request until deployment phase
    try {
        isqpRequest1->execute(storageHandler);
    } catch (Exceptions::RPCQueryUndeploymentException& e) {
        FAIL();
    }
    EXPECT_TRUE(topology->nodeWithWorkerIdExists(nodeId4));

    // Register physical and logical sources
    std::string logicalSourceName = "test";
    auto defaultSourceType = DefaultSourceType::create(logicalSourceName, "pTest1");
    auto physicalSource = PhysicalSource::create(defaultSourceType);
    auto schema = TestSchemas::getSchemaTemplate("id_val_u32");
    auto logicalSource = LogicalSource::create(logicalSourceName, schema);
    sourceCatalog->addLogicalSource(logicalSource->getLogicalSourceName(), logicalSource->getSchema());
    auto sce = Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, nodeId4);
    sourceCatalog->addPhysicalSource(logicalSourceName, sce);

    auto query1 = Query::from(logicalSourceName).sink(NullOutputSinkDescriptor::create());
    const QueryPlanPtr& queryPlan1 = query1.getQueryPlan();
    auto queryAddEvent1 = ISQPAddQueryEvent::create(queryPlan1, TEST_PLACEMENT_STRATEGY);

    std::vector<ISQPEventPtr> isqpEventsForRequest2;
    isqpEventsForRequest2.emplace_back(queryAddEvent1);

    // Prepare
    auto isqpRequest2 = ISQPRequest::create(mockedPlacementAmendmentHandler, z3Context, isqpEventsForRequest2, ZERO_RETRIES);
    constexpr auto requestId2 = RequestId(2);
    isqpRequest2->setId(requestId2);
    // Execute add request until deployment phase
    try {
        isqpRequest2->execute(storageHandler);
    } catch (Exceptions::RPCQueryUndeploymentException& e) {
        FAIL();
    }
    auto response = queryAddEvent1->getResponse().get();
    auto queryId1 = std::static_pointer_cast<RequestProcessor::ISQPAddQueryResponse>(response)->queryId;
    EXPECT_NE(queryId1, INVALID_QUERY_ID);

    auto query2 = Query::from(logicalSourceName).sink(NullOutputSinkDescriptor::create());
    const QueryPlanPtr& queryPlan2 = query2.getQueryPlan();
    auto queryAddEvent2 = ISQPAddQueryEvent::create(queryPlan2, TEST_PLACEMENT_STRATEGY);

    std::vector<ISQPEventPtr> isqpEventsForRequest3;
    isqpEventsForRequest3.emplace_back(queryAddEvent2);

    // Prepare
    auto isqpRequest3 = ISQPRequest::create(mockedPlacementAmendmentHandler, z3Context, isqpEventsForRequest3, ZERO_RETRIES);
    constexpr auto requestId3 = RequestId(3);
    isqpRequest3->setId(requestId3);
    // Execute add request until deployment phase
    try {
        isqpRequest3->execute(storageHandler);
    } catch (Exceptions::RPCQueryUndeploymentException& e) {
        FAIL();
    }
    auto response2 = queryAddEvent2->getResponse().get();
    auto queryId2 = std::static_pointer_cast<RequestProcessor::ISQPAddQueryResponse>(response2)->queryId;
    EXPECT_NE(queryId2, INVALID_QUERY_ID);
}

//test adding multiple queries
TEST_F(ISQPRequestTest, testTopologyChangeEventsInaSingleBatchWithMergingWithoutIncrementalPlacement) {

    //Create mocked handler and define the mock assertions
    auto mockedPlacementAmendmentHandler = std::make_shared<NES::Optimizer::MockedPlacementAmendmentHandler>(1);
    EXPECT_CALL(*mockedPlacementAmendmentHandler, enqueueRequest(_)).Times(2).WillRepeatedly(Invoke(updateCapturedValue));

    // init topology nodes
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;
    WorkerId nodeId1(1);
    auto addNodeEvent1 = ISQPAddNodeEvent::create(WorkerType::CLOUD, nodeId1, "localhost", 4000, 4002, 4, properties);
    WorkerId nodeId2(2);
    auto addNodeEvent2 = ISQPAddNodeEvent::create(WorkerType::SENSOR, nodeId2, "localhost", 4000, 4002, 4, properties);
    WorkerId nodeId3(3);
    auto addNodeEvent3 = ISQPAddNodeEvent::create(WorkerType::SENSOR, nodeId3, "localhost", 4000, 4002, 4, properties);
    WorkerId nodeId4(4);
    auto addNodeEvent4 = ISQPAddNodeEvent::create(WorkerType::SENSOR, nodeId4, "localhost", 4000, 4002, 4, properties);
    WorkerId nodeId5(5);
    auto addNodeEvent5 = ISQPAddNodeEvent::create(WorkerType::SENSOR, nodeId5, "localhost", 4000, 4002, 4, properties);

    auto isqpRemoveLink14 = ISQPRemoveLinkEvent::create(nodeId1, nodeId4);
    auto isqpRemoveLink15 = ISQPRemoveLinkEvent::create(nodeId1, nodeId5);
    auto isqpAddLink24 = ISQPAddLinkEvent::create(nodeId2, nodeId4);
    auto isqpAddLink45 = ISQPAddLinkEvent::create(nodeId4, nodeId5);
    auto isqpAddLinkProperty24 = ISQPAddLinkPropertyEvent::create(nodeId2, nodeId4, 1, 1);
    auto isqpAddLinkProperty45 = ISQPAddLinkPropertyEvent::create(nodeId4, nodeId5, 1, 1);
    auto isqpAddLinkProperty12 = ISQPAddLinkPropertyEvent::create(nodeId1, nodeId2, 1, 1);
    auto isqpAddLinkProperty13 = ISQPAddLinkPropertyEvent::create(nodeId1, nodeId3, 1, 1);

    std::vector<ISQPEventPtr> isqpEventsForRequest1;
    isqpEventsForRequest1.emplace_back(addNodeEvent1);
    isqpEventsForRequest1.emplace_back(addNodeEvent2);
    isqpEventsForRequest1.emplace_back(addNodeEvent3);
    isqpEventsForRequest1.emplace_back(addNodeEvent4);
    isqpEventsForRequest1.emplace_back(addNodeEvent5);
    isqpEventsForRequest1.emplace_back(isqpRemoveLink14);
    isqpEventsForRequest1.emplace_back(isqpRemoveLink15);
    isqpEventsForRequest1.emplace_back(isqpAddLink24);
    isqpEventsForRequest1.emplace_back(isqpAddLink45);
    isqpEventsForRequest1.emplace_back(isqpAddLinkProperty24);
    isqpEventsForRequest1.emplace_back(isqpAddLinkProperty45);
    isqpEventsForRequest1.emplace_back(isqpAddLinkProperty12);
    isqpEventsForRequest1.emplace_back(isqpAddLinkProperty13);

    // Enable query merging
    coordinatorConfiguration->optimizer.queryMergerRule = Optimizer::QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule;
    // Disable incremental placement
    coordinatorConfiguration->optimizer.enableIncrementalPlacement = false;
    // Number of amender threads
    coordinatorConfiguration->optimizer.placementAmendmentThreadCount = 4;

    // Prepare the system
    auto storageHandler = TwoPhaseLockingStorageHandler::create({coordinatorConfiguration,
                                                                 topology,
                                                                 globalExecutionPlan,
                                                                 globalQueryPlan,
                                                                 queryCatalog,
                                                                 sourceCatalog,
                                                                 udfCatalog,
                                                                 statisticProbeHandler});
    auto isqpRequest1 = ISQPRequest::create(mockedPlacementAmendmentHandler, z3Context, isqpEventsForRequest1, ZERO_RETRIES);
    constexpr RequestId requestId1(1);
    isqpRequest1->setId(requestId1);
    // Execute add request until deployment phase
    try {
        isqpRequest1->execute(storageHandler);
    } catch (Exceptions::RPCQueryUndeploymentException& e) {
        FAIL();
    }
    EXPECT_TRUE(topology->nodeWithWorkerIdExists(nodeId4));

    // Register physical and logical sources
    std::string logicalSourceName = "test";
    auto defaultSourceType = DefaultSourceType::create(logicalSourceName, "pTest1");
    auto physicalSource = PhysicalSource::create(defaultSourceType);
    auto schema = TestSchemas::getSchemaTemplate("id_val_u32");
    auto logicalSource = LogicalSource::create(logicalSourceName, schema);
    sourceCatalog->addLogicalSource(logicalSource->getLogicalSourceName(), logicalSource->getSchema());
    auto sce = Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, nodeId5);
    sourceCatalog->addPhysicalSource(logicalSourceName, sce);

    auto query1 =
        Query::from(logicalSourceName).map(Attribute("value") = Attribute("value") + 1).sink(NullOutputSinkDescriptor::create());
    const QueryPlanPtr& queryPlan1 = query1.getQueryPlan();
    queryPlan1->setQueryId(QueryId(1));
    auto queryAddEvent1 = ISQPAddQueryEvent::create(queryPlan1, TEST_PLACEMENT_STRATEGY);

    auto query2 =
        Query::from(logicalSourceName).map(Attribute("value") = Attribute("value") + 1).sink(NullOutputSinkDescriptor::create());
    const QueryPlanPtr& queryPlan2 = query2.getQueryPlan();
    queryPlan2->setQueryId(QueryId(2));
    auto queryAddEvent2 = ISQPAddQueryEvent::create(queryPlan2, TEST_PLACEMENT_STRATEGY);

    std::vector<ISQPEventPtr> isqpEventsForRequest2;
    isqpEventsForRequest2.emplace_back(queryAddEvent1);
    isqpEventsForRequest2.emplace_back(queryAddEvent2);

    // Prepare
    auto isqpRequest2 = ISQPRequest::create(mockedPlacementAmendmentHandler, z3Context, isqpEventsForRequest2, ZERO_RETRIES);
    constexpr RequestId requestId2(2);
    isqpRequest2->setId(requestId2);
    // Execute add request until deployment phase
    try {
        isqpRequest2->execute(storageHandler);
    } catch (Exceptions::RPCQueryUndeploymentException& e) {
        FAIL();
    }
    auto response1 = queryAddEvent1->getResponse().get();
    auto queryId1 = std::static_pointer_cast<RequestProcessor::ISQPAddQueryResponse>(response1)->queryId;
    EXPECT_NE(queryId1, INVALID_QUERY_ID);
    auto response2 = queryAddEvent2->getResponse().get();
    auto queryId2 = std::static_pointer_cast<RequestProcessor::ISQPAddQueryResponse>(response2)->queryId;
    EXPECT_NE(queryId2, INVALID_QUERY_ID);

    // Prepare
    auto addLink34 = ISQPAddLinkEvent::create(nodeId3, nodeId4);
    auto removeLink24 = ISQPRemoveLinkEvent::create(nodeId2, nodeId4);
    std::vector<ISQPEventPtr> isqpEventsForRequest3;
    isqpEventsForRequest3.emplace_back(addLink34);
    isqpEventsForRequest3.emplace_back(removeLink24);
    auto isqpRequest3 = ISQPRequest::create(mockedPlacementAmendmentHandler, z3Context, isqpEventsForRequest3, ZERO_RETRIES);
    constexpr RequestId requestId3(3);
    isqpRequest3->setId(requestId3);
    try {
        isqpRequest3->execute(storageHandler);
    } catch (Exceptions::RPCQueryUndeploymentException& e) {
        FAIL();
    }

    // Verify if removal happened
    auto sharedQueryId = globalQueryPlan->getSharedQueryId(queryId1);
    auto sharedQueryPlan = globalQueryPlan->getSharedQueryPlan(sharedQueryId);
    auto numOfSinks = sharedQueryPlan->getQueryPlan()->getSinkOperators().size();
    EXPECT_EQ(numOfSinks, 2);
}
}// namespace NES::RequestProcessor
