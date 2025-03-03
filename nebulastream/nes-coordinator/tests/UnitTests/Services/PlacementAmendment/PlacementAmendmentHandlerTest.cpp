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
#include <Phases/DeploymentPhase.hpp>
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
#include <Util/DeploymentContext.hpp>
#include <Util/IncrementalPlacementUtils.hpp>
#include <Util/Logger/Logger.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <z3++.h>

using namespace testing;

namespace z3 {
class expr;
using ExprPtr = std::shared_ptr<expr>;
class context;
using ContextPtr = std::shared_ptr<context>;
}// namespace z3

namespace NES::Optimizer {

class MockedPlacementAmendmentHandler : public NES::Optimizer::PlacementAmendmentHandler {
  public:
    MockedPlacementAmendmentHandler(uint16_t numOfHandler) : PlacementAmendmentHandler(numOfHandler){};
    MOCK_METHOD(void, enqueueRequest, (const NES::Optimizer::PlacementAmendmentInstancePtr&), (override));
};

}// namespace NES::Optimizer

namespace NES::RequestProcessor {

class MockedDeploymentPhase : public DeploymentPhase {
  public:
    MockedDeploymentPhase(Catalogs::Query::QueryCatalogPtr queryCatalog)
        : DeploymentPhase(queryCatalog), queryCatalog(queryCatalog){};

    void execute(const std::set<Optimizer::DeploymentContextPtr>& deploymentContexts, RequestType requestType) override {
        if (!deploymentContexts.empty()) {
            QueryState sharedQueryState = QueryState::REGISTERED;
            SharedQueryId sharedQueryId = INVALID_SHARED_QUERY_ID;
            for (const auto& deploymentContext : deploymentContexts) {
                sharedQueryId = deploymentContext->getSharedQueryId();
                auto decomposedQueryId = deploymentContext->getDecomposedQueryId();
                auto decomposedQueryPlanVersion = deploymentContext->getDecomposedQueryPlanVersion();
                auto workerId = deploymentContext->getWorkerId();
                auto decomposedQueryPlanState = deploymentContext->getDecomposedQueryPlanState();
                switch (decomposedQueryPlanState) {
                    case QueryState::MARKED_FOR_DEPLOYMENT: {
                        if (sharedQueryState != QueryState::FAILED && sharedQueryState != QueryState::STOPPED) {
                            sharedQueryState = QueryState::RUNNING;
                        }
                    }
                    case QueryState::MARKED_FOR_REDEPLOYMENT: {
                        if (sharedQueryState != QueryState::FAILED && sharedQueryState != QueryState::STOPPED
                            && decomposedQueryPlanState == QueryState::MARKED_FOR_REDEPLOYMENT) {
                            sharedQueryState = QueryState::RUNNING;
                        }
                        // Update decomposed query plan status
                        queryCatalog->updateDecomposedQueryPlanStatus(sharedQueryId,
                                                                      decomposedQueryId,
                                                                      decomposedQueryPlanVersion,
                                                                      decomposedQueryPlanState,
                                                                      workerId);
                        break;
                    }
                    case QueryState::MARKED_FOR_MIGRATION: {
                        if (requestType == RequestType::AddQuery) {
                            // Update decomposed query plan status
                            queryCatalog->updateDecomposedQueryPlanStatus(sharedQueryId,
                                                                          decomposedQueryId,
                                                                          decomposedQueryPlanVersion,
                                                                          decomposedQueryPlanState,
                                                                          workerId);
                        } else if (requestType == RequestType::StopQuery || requestType == RequestType::RestartQuery) {
                            sharedQueryState = QueryState::STOPPED;
                            // Update decomposed query plan status
                            queryCatalog->updateDecomposedQueryPlanStatus(sharedQueryId,
                                                                          decomposedQueryId,
                                                                          decomposedQueryPlanVersion,
                                                                          QueryState::MARKED_FOR_HARD_STOP,
                                                                          workerId);
                        } else if (requestType == RequestType::FailQuery) {
                            sharedQueryState = QueryState::FAILED;
                            // Update decomposed query plan status
                            queryCatalog->updateDecomposedQueryPlanStatus(sharedQueryId,
                                                                          decomposedQueryId,
                                                                          decomposedQueryPlanVersion,
                                                                          QueryState::MARKED_FOR_FAILURE,
                                                                          workerId);
                        } else {
                            NES_ERROR("Unhandled request type {} for decomposed query plan in status MARKED_FOR_MIGRATION",
                                      magic_enum::enum_name(requestType));
                        }
                        break;
                    }
                    default: {
                        NES_WARNING("Can not handle decomposed query plan in the state {}",
                                    magic_enum::enum_name(decomposedQueryPlanState));
                    }
                }
            }
            queryCatalog->updateSharedQueryStatus(sharedQueryId, sharedQueryState, "");
        }
    }

  private:
    Catalogs::Query::QueryCatalogPtr queryCatalog;
};

class PlacementAmendmentHandlerTest : public Testing::BaseUnitTest {
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

    std::vector<ISQPEventPtr> computeTopologySetupRequests() {
        // init topology nodes using mocked amendment handler and real ISQP request
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

        std::vector<ISQPEventPtr> isqpEventsForRequest;
        isqpEventsForRequest.emplace_back(addNodeEvent1);
        isqpEventsForRequest.emplace_back(addNodeEvent2);
        isqpEventsForRequest.emplace_back(addNodeEvent3);
        isqpEventsForRequest.emplace_back(addNodeEvent4);
        isqpEventsForRequest.emplace_back(isqpRemoveLink14);
        isqpEventsForRequest.emplace_back(isqpRemoveLink13);
        isqpEventsForRequest.emplace_back(isqpAddLink34);
        isqpEventsForRequest.emplace_back(isqpAddLink23);
        isqpEventsForRequest.emplace_back(isqpAddLinkProperty34);
        isqpEventsForRequest.emplace_back(isqpAddLinkProperty23);
        isqpEventsForRequest.emplace_back(isqpAddLinkProperty12);
        return isqpEventsForRequest;
    }

    void
    executeMockISQPRequest(const std::vector<ISQPEventPtr>& isqpEvents,
                           const StorageHandlerPtr& storageHandler,
                           const std::shared_ptr<Optimizer::MockedPlacementAmendmentHandler>& mockedPlacementAmendmentHandler) {

        auto isqpRequest1 = ISQPRequest::create(mockedPlacementAmendmentHandler, z3Context, isqpEvents, ZERO_RETRIES);
        constexpr auto requestId1 = RequestId(1);
        isqpRequest1->setId(requestId1);
        // Execute add request until deployment phase
        try {
            isqpRequest1->execute(storageHandler);
        } catch (Exceptions::RPCQueryUndeploymentException& e) {
            FAIL();
        }
    }

    void performPlacementAmendment(const Optimizer::PlacementAmendmentHandlerPtr& placementAmendmentHandler) {
        // Fetch affected SQPs and call in parallel operator placement amendment phase
        auto sharedQueryPlans = globalQueryPlan->getSharedQueryPlansToDeploy();
        auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
        std::vector<std::future<bool>> completedAmendments;
        //Note: deployment phase is mocked so nothing is actually getting deployed
        auto mockedDeploymentPhase = std::make_shared<MockedDeploymentPhase>(queryCatalog);
        for (const auto& sharedQueryPlan : sharedQueryPlans) {
            const auto& amendmentInstance = Optimizer::PlacementAmendmentInstance::create(sharedQueryPlan,
                                                                                          globalExecutionPlan,
                                                                                          topology,
                                                                                          typeInferencePhase,
                                                                                          coordinatorConfiguration,
                                                                                          mockedDeploymentPhase);
            completedAmendments.emplace_back(amendmentInstance->getFuture());
            placementAmendmentHandler->enqueueRequest(amendmentInstance);
        }

        uint64_t numOfFailedPlacements = 0;
        // Wait for all amendment runners to finish processing
        for (auto& completedAmendment : completedAmendments) {
            if (!completedAmendment.get()) {
                numOfFailedPlacements++;
            }
        }

        // No placement amendment should fail
        EXPECT_EQ(numOfFailedPlacements, 0);
    }
};

// Function to run ISQP request without performing placement amendment
void handleMockedPlacementAmendmentCall(const NES::Optimizer::PlacementAmendmentInstancePtr& placementAmendmentInstance) {
    placementAmendmentInstance->setPromise(true);
}

//test adding a single
TEST_F(PlacementAmendmentHandlerTest, testAddQueryEvents) {

    //1. Create mocked amendment handler and ISQP request to setup topology and queries to be processed
    auto mockedPlacementAmendmentHandler = std::make_shared<NES::Optimizer::MockedPlacementAmendmentHandler>(1);
    // Two ISQP requests are issues in this test. One contains only topology change events and other contains an add query event.
    // Therefore, assert that only one call ot the mocked placement amendment handler are made.
    EXPECT_CALL(*mockedPlacementAmendmentHandler, enqueueRequest(_))
        .Times(1)
        .WillRepeatedly(Invoke(handleMockedPlacementAmendmentCall));

    // Prepare the storage handler
    // Enable query merging
    coordinatorConfiguration->optimizer.queryMergerRule = Optimizer::QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule;
    // Enable Incremental placement
    coordinatorConfiguration->optimizer.enableIncrementalPlacement = true;
    auto storageHandler = TwoPhaseLockingStorageHandler::create({coordinatorConfiguration,
                                                                 topology,
                                                                 globalExecutionPlan,
                                                                 globalQueryPlan,
                                                                 queryCatalog,
                                                                 sourceCatalog,
                                                                 udfCatalog,
                                                                 statisticProbeHandler});

    // 1. Setup topology by creating the ISQP topology events
    auto isqpEventsForRequest1 = computeTopologySetupRequests();
    // Execute the ISQP events
    executeMockISQPRequest(isqpEventsForRequest1, storageHandler, mockedPlacementAmendmentHandler);
    // Assert that o worker node exists
    auto nodeId4 = WorkerId(4);
    EXPECT_TRUE(topology->nodeWithWorkerIdExists(nodeId4));

    // 2. Register physical, logical sources, and a query using ISQP request without performing operator placement
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
    executeMockISQPRequest(isqpEventsForRequest2, storageHandler, mockedPlacementAmendmentHandler);
    auto response1 = queryAddEvent1->getResponse().get();
    auto queryId1 = std::static_pointer_cast<RequestProcessor::ISQPAddQueryResponse>(response1)->queryId;
    EXPECT_NE(queryId1, INVALID_QUERY_ID);

    // 3. Initiate the real placement amendment handler by fetching the shared query plans generated by adding the query by
    // previous ISQP request.
    auto placementAmendmentHandler =
        std::make_shared<Optimizer::PlacementAmendmentHandler>(coordinatorConfiguration->optimizer.placementAmendmentThreadCount);
    placementAmendmentHandler->start();
    performPlacementAmendment(placementAmendmentHandler);
    // Assertions
    EXPECT_EQ(queryCatalog->getQueryState(queryId1), QueryState::RUNNING);
    placementAmendmentHandler->shutDown();
}

//test placement amendment of multiple queries merged together
TEST_F(PlacementAmendmentHandlerTest, testMultipleAddQueryEventsInaSingleBatchWithMergingWithIncrementalPlacement2PL) {

    // 1. Create mocked amendment handler and ISQP request to setup topology and queries to be processed
    auto mockedPlacementAmendmentHandler = std::make_shared<NES::Optimizer::MockedPlacementAmendmentHandler>(1);
    // Two ISQP requests are issues in this test. One contains only topology change events and other contains an add query event.
    // Therefore, assert that only one call ot the mocked placement amendment handler are made.
    EXPECT_CALL(*mockedPlacementAmendmentHandler, enqueueRequest(_))
        .Times(2)
        .WillRepeatedly(Invoke(handleMockedPlacementAmendmentCall));

    // Prepare the storage handler
    // Enable query merging
    coordinatorConfiguration->optimizer.queryMergerRule = Optimizer::QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule;
    // Enable Incremental placement
    coordinatorConfiguration->optimizer.enableIncrementalPlacement = true;
    // Set number of amendment instance handlers
    coordinatorConfiguration->optimizer.placementAmendmentThreadCount = 2;
    auto storageHandler = TwoPhaseLockingStorageHandler::create({coordinatorConfiguration,
                                                                 topology,
                                                                 globalExecutionPlan,
                                                                 globalQueryPlan,
                                                                 queryCatalog,
                                                                 sourceCatalog,
                                                                 udfCatalog,
                                                                 statisticProbeHandler});
    // 2. Setup topology
    auto isqpEventsForRequest1 = computeTopologySetupRequests();
    executeMockISQPRequest(isqpEventsForRequest1, storageHandler, mockedPlacementAmendmentHandler);
    auto nodeId4 = WorkerId(4);
    EXPECT_TRUE(topology->nodeWithWorkerIdExists(nodeId4));

    // 3. Register physical, logical sources, and queries
    std::string logicalSourceName = "test";
    auto defaultSourceType = DefaultSourceType::create(logicalSourceName, "pTest1");
    auto physicalSource = PhysicalSource::create(defaultSourceType);
    auto schema = TestSchemas::getSchemaTemplate("id_val_u32");
    auto logicalSource = LogicalSource::create(logicalSourceName, schema);
    sourceCatalog->addLogicalSource(logicalSource->getLogicalSourceName(), logicalSource->getSchema());
    auto sce = Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, WorkerId(4));
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
    executeMockISQPRequest(isqpEventsForRequest2, storageHandler, mockedPlacementAmendmentHandler);

    // 4. Initiate the placement amendment handler to place the added queries
    auto placementAmendmentHandler =
        std::make_shared<Optimizer::PlacementAmendmentHandler>(coordinatorConfiguration->optimizer.placementAmendmentThreadCount);
    placementAmendmentHandler->start();
    performPlacementAmendment(placementAmendmentHandler);
    // Assertions
    auto response1 = queryAddEvent1->getResponse().get();
    auto queryId1 = std::static_pointer_cast<RequestProcessor::ISQPAddQueryResponse>(response1)->queryId;
    EXPECT_EQ(queryCatalog->getQueryState(queryId1), QueryState::RUNNING);
    auto response2 = queryAddEvent2->getResponse().get();
    auto queryId2 = std::static_pointer_cast<RequestProcessor::ISQPAddQueryResponse>(response2)->queryId;
    EXPECT_EQ(queryCatalog->getQueryState(queryId2), QueryState::RUNNING);

    // 5. Prepare mock ISQP request to stop the query 2
    auto queryRemoveEvent = ISQPRemoveQueryEvent::create(queryId2);
    std::vector<ISQPEventPtr> isqpEventsForRequest3;
    isqpEventsForRequest3.emplace_back(queryRemoveEvent);
    executeMockISQPRequest(isqpEventsForRequest3, storageHandler, mockedPlacementAmendmentHandler);

    // 6. Initiate placement amendment handler to update the placements for affected queries
    performPlacementAmendment(placementAmendmentHandler);
    // Verify if removal happened
    auto sharedQueryId = globalQueryPlan->getSharedQueryId(queryId1);
    auto sharedQueryPlan = globalQueryPlan->getSharedQueryPlan(sharedQueryId);
    auto numOfSinks = sharedQueryPlan->getQueryPlan()->getSinkOperators().size();
    EXPECT_EQ(numOfSinks, 1);
    placementAmendmentHandler->shutDown();
}

//test adding multiple queries
TEST_F(PlacementAmendmentHandlerTest, testMultipleAddQueryEventsInaSingleBatchWithMergingWithoutIncrementalPlacement2PL) {

    // 1. Create mocked amendment handler and ISQP request to setup topology and queries to be processed
    auto mockedPlacementAmendmentHandler = std::make_shared<NES::Optimizer::MockedPlacementAmendmentHandler>(1);
    // Two ISQP requests are issues in this test. One contains only topology change events and other contains an add query event.
    // Therefore, assert that only one call ot the mocked placement amendment handler are made.
    EXPECT_CALL(*mockedPlacementAmendmentHandler, enqueueRequest(_))
        .Times(2)
        .WillRepeatedly(Invoke(handleMockedPlacementAmendmentCall));

    // Prepare the storage handler
    // Enable query merging
    coordinatorConfiguration->optimizer.queryMergerRule = Optimizer::QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule;
    // Disable incremental placement
    coordinatorConfiguration->optimizer.enableIncrementalPlacement = false;
    // Number of amender threads
    coordinatorConfiguration->optimizer.placementAmendmentThreadCount = 4;
    auto storageHandler = TwoPhaseLockingStorageHandler::create({coordinatorConfiguration,
                                                                 topology,
                                                                 globalExecutionPlan,
                                                                 globalQueryPlan,
                                                                 queryCatalog,
                                                                 sourceCatalog,
                                                                 udfCatalog,
                                                                 statisticProbeHandler});

    // 2. Setup topology using mocked ISQP request
    auto isqpEventsForRequest1 = computeTopologySetupRequests();
    executeMockISQPRequest(isqpEventsForRequest1, storageHandler, mockedPlacementAmendmentHandler);
    auto nodeId4 = WorkerId(4);
    EXPECT_TRUE(topology->nodeWithWorkerIdExists(nodeId4));

    // 3. Register physical and logical sources and queries using mocked ISQP request
    std::string logicalSourceName = "test";
    auto defaultSourceType = DefaultSourceType::create(logicalSourceName, "pTest1");
    auto physicalSource = PhysicalSource::create(defaultSourceType);
    auto schema = TestSchemas::getSchemaTemplate("id_val_u32");
    auto logicalSource = LogicalSource::create(logicalSourceName, schema);
    sourceCatalog->addLogicalSource(logicalSource->getLogicalSourceName(), logicalSource->getSchema());
    auto sce = Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, nodeId4);
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
    executeMockISQPRequest(isqpEventsForRequest2, storageHandler, mockedPlacementAmendmentHandler);

    // 4. Initiate the placement amendment handler to place the added queries
    auto placementAmendmentHandler =
        std::make_shared<Optimizer::PlacementAmendmentHandler>(coordinatorConfiguration->optimizer.placementAmendmentThreadCount);
    placementAmendmentHandler->start();
    performPlacementAmendment(placementAmendmentHandler);
    // Assertions
    auto response1 = queryAddEvent1->getResponse().get();
    auto queryId1 = std::static_pointer_cast<RequestProcessor::ISQPAddQueryResponse>(response1)->queryId;
    EXPECT_EQ(queryCatalog->getQueryState(queryId1), QueryState::RUNNING);
    auto response2 = queryAddEvent2->getResponse().get();
    auto queryId2 = std::static_pointer_cast<RequestProcessor::ISQPAddQueryResponse>(response2)->queryId;
    EXPECT_EQ(queryCatalog->getQueryState(queryId2), QueryState::RUNNING);

    // 5. stop query 2 using mocked ISQP request
    auto queryRemoveEvent = ISQPRemoveQueryEvent::create(queryId2);
    std::vector<ISQPEventPtr> isqpEventsForRequest3;
    isqpEventsForRequest3.emplace_back(queryRemoveEvent);
    executeMockISQPRequest(isqpEventsForRequest3, storageHandler, mockedPlacementAmendmentHandler);

    // 6. Remove the placement of stopped query
    performPlacementAmendment(placementAmendmentHandler);
    // Verify if removal happened
    auto sharedQueryId = globalQueryPlan->getSharedQueryId(queryId1);
    auto sharedQueryPlan = globalQueryPlan->getSharedQueryPlan(sharedQueryId);
    auto numOfSinks = sharedQueryPlan->getQueryPlan()->getSinkOperators().size();
    EXPECT_EQ(numOfSinks, 1);
    placementAmendmentHandler->shutDown();
}

//test adding multiple queries without merging and perform placement using 2PL strategy
TEST_F(PlacementAmendmentHandlerTest, testMultipleAddQueryEventsInaSingleBatchWithoutMergingWithoutIncrementalPlacementWith2PL) {

    // 1. Create mocked amendment handler and ISQP request to setup topology and queries to be processed
    auto mockedPlacementAmendmentHandler = std::make_shared<NES::Optimizer::MockedPlacementAmendmentHandler>(1);
    // Two ISQP requests are issues in this test. One contains only topology change events and other contains an add query event.
    // Therefore, assert that only one call ot the mocked placement amendment handler are made.
    EXPECT_CALL(*mockedPlacementAmendmentHandler, enqueueRequest(_))
        .Times(3)
        .WillRepeatedly(Invoke(handleMockedPlacementAmendmentCall));

    // Prepare the storage handler
    // Disable query merging
    coordinatorConfiguration->optimizer.queryMergerRule = Optimizer::QueryMergerRule::DefaultQueryMergerRule;
    // Disable incremental placement
    coordinatorConfiguration->optimizer.enableIncrementalPlacement = false;
    // Number of amender threads
    coordinatorConfiguration->optimizer.placementAmendmentThreadCount = 2;
    auto storageHandler = TwoPhaseLockingStorageHandler::create({coordinatorConfiguration,
                                                                 topology,
                                                                 globalExecutionPlan,
                                                                 globalQueryPlan,
                                                                 queryCatalog,
                                                                 sourceCatalog,
                                                                 udfCatalog,
                                                                 statisticProbeHandler});

    // 2. Setup topology using mocked ISQP request
    auto isqpEventsForRequest1 = computeTopologySetupRequests();
    executeMockISQPRequest(isqpEventsForRequest1, storageHandler, mockedPlacementAmendmentHandler);
    auto nodeId4 = WorkerId(4);
    EXPECT_TRUE(topology->nodeWithWorkerIdExists(nodeId4));

    // 3. Register physical and logical sources and queries using mocked ISQP request
    // Register physical and logical sources
    std::string logicalSourceName = "test";
    auto defaultSourceType = DefaultSourceType::create(logicalSourceName, "pTest1");
    auto physicalSource = PhysicalSource::create(defaultSourceType);
    auto schema = TestSchemas::getSchemaTemplate("id_val_u32");
    auto logicalSource = LogicalSource::create(logicalSourceName, schema);
    sourceCatalog->addLogicalSource(logicalSource->getLogicalSourceName(), logicalSource->getSchema());
    auto sce = Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, nodeId4);
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
    executeMockISQPRequest(isqpEventsForRequest2, storageHandler, mockedPlacementAmendmentHandler);

    // 4. Initiate the placement amendment handler to place the added queries
    auto placementAmendmentHandler =
        std::make_shared<Optimizer::PlacementAmendmentHandler>(coordinatorConfiguration->optimizer.placementAmendmentThreadCount);
    placementAmendmentHandler->start();
    performPlacementAmendment(placementAmendmentHandler);
    //Assertion
    auto response1 = queryAddEvent1->getResponse().get();
    auto queryId1 = std::static_pointer_cast<RequestProcessor::ISQPAddQueryResponse>(response1)->queryId;
    EXPECT_EQ(queryCatalog->getQueryState(queryId1), QueryState::RUNNING);
    auto response2 = queryAddEvent2->getResponse().get();
    auto queryId2 = std::static_pointer_cast<RequestProcessor::ISQPAddQueryResponse>(response2)->queryId;
    EXPECT_EQ(queryCatalog->getQueryState(queryId2), QueryState::RUNNING);

    // 5. stop query 2 using mocked ISQP request
    auto queryRemoveEvent = ISQPRemoveQueryEvent::create(queryId2);
    std::vector<ISQPEventPtr> isqpEventsForRequest3;
    isqpEventsForRequest3.emplace_back(queryRemoveEvent);
    executeMockISQPRequest(isqpEventsForRequest3, storageHandler, mockedPlacementAmendmentHandler);

    // 6. Remove the placement of stopped query
    performPlacementAmendment(placementAmendmentHandler);
    placementAmendmentHandler->shutDown();
}

//test adding multiple queries without merging and perform placement using OCC startegy
TEST_F(PlacementAmendmentHandlerTest, testMultipleAddQueryEventsInaSingleBatchWithoutMergingWithoutIncrementalPlacementWithOCC) {

    // 1. Create mocked amendment handler and ISQP request to setup topology and queries to be processed
    auto mockedPlacementAmendmentHandler = std::make_shared<NES::Optimizer::MockedPlacementAmendmentHandler>(1);
    // Two ISQP requests are issues in this test. One contains only topology change events and other contains an add query event.
    // Therefore, assert that only one call ot the mocked placement amendment handler are made.
    EXPECT_CALL(*mockedPlacementAmendmentHandler, enqueueRequest(_))
        .Times(2)
        .WillRepeatedly(Invoke(handleMockedPlacementAmendmentCall));

    // Disable query merging
    coordinatorConfiguration->optimizer.queryMergerRule = Optimizer::QueryMergerRule::DefaultQueryMergerRule;
    // Enable incremental placement
    coordinatorConfiguration->optimizer.enableIncrementalPlacement = true;
    // Number of amender threads
    coordinatorConfiguration->optimizer.placementAmendmentThreadCount = 2;
    // placement amendment mode
    coordinatorConfiguration->optimizer.placementAmendmentMode = Optimizer::PlacementAmendmentMode::OPTIMISTIC;
    // Prepare the storage handler
    auto storageHandler = TwoPhaseLockingStorageHandler::create({coordinatorConfiguration,
                                                                 topology,
                                                                 globalExecutionPlan,
                                                                 globalQueryPlan,
                                                                 queryCatalog,
                                                                 sourceCatalog,
                                                                 udfCatalog,
                                                                 statisticProbeHandler});

    //2. Setup the
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
    auto addNodeEvent4 = ISQPAddNodeEvent::create(WorkerType::SENSOR, nodeId4, "localhost", 4000, 4002, 6, properties);

    auto isqpRemoveLink14 = ISQPRemoveLinkEvent::create(nodeId1, nodeId4);
    auto isqpAddLink34 = ISQPAddLinkEvent::create(nodeId3, nodeId4);

    std::vector<ISQPEventPtr> isqpEventsForRequest1;
    isqpEventsForRequest1.emplace_back(addNodeEvent1);
    isqpEventsForRequest1.emplace_back(addNodeEvent2);
    isqpEventsForRequest1.emplace_back(addNodeEvent3);
    isqpEventsForRequest1.emplace_back(addNodeEvent4);
    isqpEventsForRequest1.emplace_back(isqpRemoveLink14);
    isqpEventsForRequest1.emplace_back(isqpAddLink34);
    executeMockISQPRequest(isqpEventsForRequest1, storageHandler, mockedPlacementAmendmentHandler);
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

    auto query1 = Query::from(logicalSourceName)
                      .map(Attribute("value") = Attribute("value") + 1)
                      .map(Attribute("value") = Attribute("value") + 1)
                      .sink(NullOutputSinkDescriptor::create());
    const QueryPlanPtr& queryPlan1 = query1.getQueryPlan();
    queryPlan1->setQueryId(QueryId(1));
    auto queryAddEvent1 = ISQPAddQueryEvent::create(queryPlan1, TEST_PLACEMENT_STRATEGY);

    auto query2 = Query::from(logicalSourceName)
                      .map(Attribute("value") = Attribute("value") + 1)
                      .map(Attribute("value") = Attribute("value") + 1)
                      .sink(NullOutputSinkDescriptor::create());
    const QueryPlanPtr& queryPlan2 = query2.getQueryPlan();
    queryPlan2->setQueryId(QueryId(2));
    auto queryAddEvent2 = ISQPAddQueryEvent::create(queryPlan2, TEST_PLACEMENT_STRATEGY);

    std::vector<ISQPEventPtr> isqpEventsForRequest2;
    isqpEventsForRequest2.emplace_back(queryAddEvent1);
    isqpEventsForRequest2.emplace_back(queryAddEvent2);
    executeMockISQPRequest(isqpEventsForRequest2, storageHandler, mockedPlacementAmendmentHandler);

    // 4. Initiate the placement amendment handler to place the added queries
    auto placementAmendmentHandler =
        std::make_shared<Optimizer::PlacementAmendmentHandler>(coordinatorConfiguration->optimizer.placementAmendmentThreadCount);
    placementAmendmentHandler->start();
    performPlacementAmendment(placementAmendmentHandler);
    // Assertion
    auto response1 = queryAddEvent1->getResponse().get();
    auto queryId1 = std::static_pointer_cast<RequestProcessor::ISQPAddQueryResponse>(response1)->queryId;
    EXPECT_EQ(queryCatalog->getQueryState(queryId1), QueryState::RUNNING);
    auto response2 = queryAddEvent2->getResponse().get();
    auto queryId2 = std::static_pointer_cast<RequestProcessor::ISQPAddQueryResponse>(response2)->queryId;
    EXPECT_EQ(queryCatalog->getQueryState(queryId2), QueryState::RUNNING);
    placementAmendmentHandler->shutDown();
}

//test adding multiple queries without merging and perform placement using OCC startegy
TEST_F(PlacementAmendmentHandlerTest, testMultipleAddQueryEventsInaSingleBatchWithMergingWithoutIncrementalPlacementWithOCC) {

    // 1. Create mocked amendment handler and ISQP request to setup topology and queries to be processed
    auto mockedPlacementAmendmentHandler = std::make_shared<NES::Optimizer::MockedPlacementAmendmentHandler>(1);
    // Two ISQP requests are issues in this test. One contains only topology change events and other contains an add query event.
    // Therefore, assert that only one call ot the mocked placement amendment handler are made.
    EXPECT_CALL(*mockedPlacementAmendmentHandler, enqueueRequest(_))
        .Times(2)
        .WillRepeatedly(Invoke(handleMockedPlacementAmendmentCall));

    // Disable query merging
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
    auto addNodeEvent4 = ISQPAddNodeEvent::create(WorkerType::SENSOR, nodeId4, "localhost", 4000, 4002, 6, properties);

    auto isqpRemoveLink14 = ISQPRemoveLinkEvent::create(nodeId1, nodeId4);
    auto isqpRemoveLink13 = ISQPRemoveLinkEvent::create(nodeId1, nodeId3);
    auto isqpAddLink23 = ISQPAddLinkEvent::create(nodeId2, nodeId3);
    auto isqpAddLink24 = ISQPAddLinkEvent::create(nodeId2, nodeId4);

    std::vector<ISQPEventPtr> isqpEventsForRequest1;
    isqpEventsForRequest1.emplace_back(addNodeEvent1);
    isqpEventsForRequest1.emplace_back(addNodeEvent2);
    isqpEventsForRequest1.emplace_back(addNodeEvent3);
    isqpEventsForRequest1.emplace_back(addNodeEvent4);
    isqpEventsForRequest1.emplace_back(isqpRemoveLink13);
    isqpEventsForRequest1.emplace_back(isqpRemoveLink14);
    isqpEventsForRequest1.emplace_back(isqpAddLink23);
    isqpEventsForRequest1.emplace_back(isqpAddLink24);
    executeMockISQPRequest(isqpEventsForRequest1, storageHandler, mockedPlacementAmendmentHandler);
    EXPECT_TRUE(topology->nodeWithWorkerIdExists(nodeId4));

    // Register physical and logical sources
    auto schema = TestSchemas::getSchemaTemplate("id_val_u32");
    std::string logicalSourceName1 = "test1";
    auto defaultSourceType1 = DefaultSourceType::create(logicalSourceName1, "pTest1");
    auto physicalSource1 = PhysicalSource::create(defaultSourceType1);
    auto logicalSource1 = LogicalSource::create(logicalSourceName1, schema);
    sourceCatalog->addLogicalSource(logicalSource1->getLogicalSourceName(), logicalSource1->getSchema());
    auto sce1 = Catalogs::Source::SourceCatalogEntry::create(physicalSource1, logicalSource1, nodeId4);
    sourceCatalog->addPhysicalSource(logicalSourceName1, sce1);
    std::string logicalSourceName2 = "test2";
    auto defaultSourceType2 = DefaultSourceType::create(logicalSourceName2, "pTest2");
    auto physicalSource2 = PhysicalSource::create(defaultSourceType2);
    auto logicalSource2 = LogicalSource::create(logicalSourceName2, schema);
    sourceCatalog->addLogicalSource(logicalSource2->getLogicalSourceName(), logicalSource2->getSchema());
    auto sce2 = Catalogs::Source::SourceCatalogEntry::create(physicalSource2, logicalSource2, nodeId3);
    sourceCatalog->addPhysicalSource(logicalSourceName2, sce2);

    auto query1 = Query::from(logicalSourceName1)
                      .map(Attribute("value") = Attribute("value") + 1)
                      .map(Attribute("value") = Attribute("value") + 1)
                      .sink(NullOutputSinkDescriptor::create());
    const QueryPlanPtr& queryPlan1 = query1.getQueryPlan();
    queryPlan1->setQueryId(QueryId(1));
    auto queryAddEvent1 = ISQPAddQueryEvent::create(queryPlan1, TEST_PLACEMENT_STRATEGY);

    auto query2 = Query::from(logicalSourceName2)
                      .map(Attribute("value") = Attribute("value") + 1)
                      .map(Attribute("value") = Attribute("value") + 1)
                      .sink(NullOutputSinkDescriptor::create());
    const QueryPlanPtr& queryPlan2 = query2.getQueryPlan();
    queryPlan2->setQueryId(QueryId(2));
    auto queryAddEvent2 = ISQPAddQueryEvent::create(queryPlan2, TEST_PLACEMENT_STRATEGY);

    std::vector<ISQPEventPtr> isqpEventsForRequest2;
    isqpEventsForRequest2.emplace_back(queryAddEvent1);
    isqpEventsForRequest2.emplace_back(queryAddEvent2);

    // Prepare
    executeMockISQPRequest(isqpEventsForRequest2, storageHandler, mockedPlacementAmendmentHandler);

    auto placementAmendmentHandler =
        std::make_shared<Optimizer::PlacementAmendmentHandler>(coordinatorConfiguration->optimizer.placementAmendmentThreadCount);
    placementAmendmentHandler->start();
    performPlacementAmendment(placementAmendmentHandler);
    auto response1 = queryAddEvent1->getResponse().get();
    auto queryId1 = std::static_pointer_cast<RequestProcessor::ISQPAddQueryResponse>(response1)->queryId;
    EXPECT_EQ(queryCatalog->getQueryState(queryId1), QueryState::RUNNING);
    auto response2 = queryAddEvent2->getResponse().get();
    auto queryId2 = std::static_pointer_cast<RequestProcessor::ISQPAddQueryResponse>(response2)->queryId;
    EXPECT_EQ(queryCatalog->getQueryState(queryId2), QueryState::RUNNING);
    placementAmendmentHandler->shutDown();
}

//test adding multiple queries
TEST_F(PlacementAmendmentHandlerTest, testMultipleAddQueryEventsInDifferentBatchWithMergingWithIncrementalPlacementOCC) {

    // 1. Create mocked amendment handler and ISQP request to setup topology and queries to be processed
    auto mockedPlacementAmendmentHandler = std::make_shared<NES::Optimizer::MockedPlacementAmendmentHandler>(1);
    // Two ISQP requests are issues in this test. One contains only topology change events and other contains an add query event.
    // Therefore, assert that only one call ot the mocked placement amendment handler are made.
    EXPECT_CALL(*mockedPlacementAmendmentHandler, enqueueRequest(_))
        .Times(2)
        .WillRepeatedly(Invoke(handleMockedPlacementAmendmentCall));

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

    executeMockISQPRequest(isqpEventsForRequest1, storageHandler, mockedPlacementAmendmentHandler);
    EXPECT_TRUE(topology->nodeWithWorkerIdExists(nodeId4));

    // Register physical, logical sources, and registering first query
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
    executeMockISQPRequest(isqpEventsForRequest2, storageHandler, mockedPlacementAmendmentHandler);

    // Perform placement of the added query
    auto placementAmendmentHandler =
        std::make_shared<Optimizer::PlacementAmendmentHandler>(coordinatorConfiguration->optimizer.placementAmendmentThreadCount);
    placementAmendmentHandler->start();
    performPlacementAmendment(placementAmendmentHandler);
    auto response = queryAddEvent1->getResponse().get();
    auto queryId1 = std::static_pointer_cast<RequestProcessor::ISQPAddQueryResponse>(response)->queryId;
    EXPECT_EQ(queryCatalog->getQueryState(queryId1), QueryState::RUNNING);

    // Add second query
    auto query2 = Query::from(logicalSourceName).sink(NullOutputSinkDescriptor::create());
    const QueryPlanPtr& queryPlan2 = query2.getQueryPlan();
    auto queryAddEvent2 = ISQPAddQueryEvent::create(queryPlan2, TEST_PLACEMENT_STRATEGY);
    std::vector<ISQPEventPtr> isqpEventsForRequest3;
    isqpEventsForRequest3.emplace_back(queryAddEvent2);
    executeMockISQPRequest(isqpEventsForRequest3, storageHandler, mockedPlacementAmendmentHandler);

    // Perform placement of the second query
    performPlacementAmendment(placementAmendmentHandler);
    auto response2 = queryAddEvent2->getResponse().get();
    auto queryId2 = std::static_pointer_cast<RequestProcessor::ISQPAddQueryResponse>(response2)->queryId;
    EXPECT_EQ(queryCatalog->getQueryState(queryId2), QueryState::RUNNING);
    placementAmendmentHandler->shutDown();
}

//test adding multiple queries
TEST_F(PlacementAmendmentHandlerTest, testMultipleAddQueryEventsInDifferentBatchWithMergingWithoutIncrementalPlacement) {

    // 1. Create mocked amendment handler and ISQP request to setup topology and queries to be processed
    auto mockedPlacementAmendmentHandler = std::make_shared<NES::Optimizer::MockedPlacementAmendmentHandler>(1);
    // Two ISQP requests are issues in this test. One contains only topology change events and other contains an add query event.
    // Therefore, assert that only one call ot the mocked placement amendment handler are made.
    EXPECT_CALL(*mockedPlacementAmendmentHandler, enqueueRequest(_))
        .Times(2)
        .WillRepeatedly(Invoke(handleMockedPlacementAmendmentCall));

    // Enable query merging
    coordinatorConfiguration->optimizer.queryMergerRule = Optimizer::QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule;
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
    auto isqpAddLink23 = ISQPAddLinkEvent::create(nodeId2, nodeId3);
    auto isqpAddLink24 = ISQPAddLinkEvent::create(nodeId2, nodeId4);

    std::vector<ISQPEventPtr> isqpEventsForRequest1;
    isqpEventsForRequest1.emplace_back(addNodeEvent1);
    isqpEventsForRequest1.emplace_back(addNodeEvent2);
    isqpEventsForRequest1.emplace_back(addNodeEvent3);
    isqpEventsForRequest1.emplace_back(addNodeEvent4);
    isqpEventsForRequest1.emplace_back(isqpRemoveLink13);
    isqpEventsForRequest1.emplace_back(isqpRemoveLink14);
    isqpEventsForRequest1.emplace_back(isqpAddLink23);
    isqpEventsForRequest1.emplace_back(isqpAddLink24);
    executeMockISQPRequest(isqpEventsForRequest1, storageHandler, mockedPlacementAmendmentHandler);
    EXPECT_TRUE(topology->nodeWithWorkerIdExists(nodeId4));

    // Register physical and logical sources
    auto schema = TestSchemas::getSchemaTemplate("id_val_u32");
    std::string logicalSourceName1 = "test1";
    auto defaultSourceType1 = DefaultSourceType::create(logicalSourceName1, "pTest1");
    auto physicalSource1 = PhysicalSource::create(defaultSourceType1);
    auto logicalSource1 = LogicalSource::create(logicalSourceName1, schema);
    sourceCatalog->addLogicalSource(logicalSource1->getLogicalSourceName(), logicalSource1->getSchema());
    auto sce1 = Catalogs::Source::SourceCatalogEntry::create(physicalSource1, logicalSource1, nodeId4);
    sourceCatalog->addPhysicalSource(logicalSourceName1, sce1);
    std::string logicalSourceName2 = "test2";
    auto defaultSourceType2 = DefaultSourceType::create(logicalSourceName2, "pTest2");
    auto physicalSource2 = PhysicalSource::create(defaultSourceType2);
    auto logicalSource2 = LogicalSource::create(logicalSourceName2, schema);
    sourceCatalog->addLogicalSource(logicalSource2->getLogicalSourceName(), logicalSource2->getSchema());
    auto sce2 = Catalogs::Source::SourceCatalogEntry::create(physicalSource2, logicalSource2, nodeId3);
    sourceCatalog->addPhysicalSource(logicalSourceName2, sce2);

    auto query1 =
        Query::from(logicalSourceName1).unionWith(Query::from(logicalSourceName2)).sink(NullOutputSinkDescriptor::create());
    const QueryPlanPtr& queryPlan1 = query1.getQueryPlan();
    auto queryAddEvent1 = ISQPAddQueryEvent::create(queryPlan1, TEST_PLACEMENT_STRATEGY);

    std::vector<ISQPEventPtr> isqpEventsForRequest2;
    isqpEventsForRequest2.emplace_back(queryAddEvent1);

    // Prepare
    executeMockISQPRequest(isqpEventsForRequest2, storageHandler, mockedPlacementAmendmentHandler);

    auto placementAmendmentHandler =
        std::make_shared<Optimizer::PlacementAmendmentHandler>(coordinatorConfiguration->optimizer.placementAmendmentThreadCount);
    placementAmendmentHandler->start();
    performPlacementAmendment(placementAmendmentHandler);
    auto response = queryAddEvent1->getResponse().get();
    auto queryId1 = std::static_pointer_cast<RequestProcessor::ISQPAddQueryResponse>(response)->queryId;
    EXPECT_EQ(queryCatalog->getQueryState(queryId1), QueryState::RUNNING);

    auto query2 =
        Query::from(logicalSourceName1).unionWith(Query::from(logicalSourceName2)).sink(NullOutputSinkDescriptor::create());
    const QueryPlanPtr& queryPlan2 = query2.getQueryPlan();
    auto queryAddEvent2 = ISQPAddQueryEvent::create(queryPlan2, TEST_PLACEMENT_STRATEGY);

    std::vector<ISQPEventPtr> isqpEventsForRequest3;
    isqpEventsForRequest3.emplace_back(queryAddEvent2);
    executeMockISQPRequest(isqpEventsForRequest3, storageHandler, mockedPlacementAmendmentHandler);

    performPlacementAmendment(placementAmendmentHandler);
    auto response2 = queryAddEvent2->getResponse().get();
    auto queryId2 = std::static_pointer_cast<RequestProcessor::ISQPAddQueryResponse>(response2)->queryId;
    EXPECT_EQ(queryCatalog->getQueryState(queryId2), QueryState::RUNNING);
    placementAmendmentHandler->shutDown();
}

//test adding multiple queries
TEST_F(PlacementAmendmentHandlerTest, testMultipleAddQueryEventsInDifferentBatchWithoutMergingWithoutIncrementalPlacement) {

    // 1. Create mocked amendment handler and ISQP request to setup topology and queries to be processed
    auto mockedPlacementAmendmentHandler = std::make_shared<NES::Optimizer::MockedPlacementAmendmentHandler>(1);
    // Two ISQP requests are issues in this test. One contains only topology change events and other contains an add query event.
    // Therefore, assert that only one call ot the mocked placement amendment handler are made.
    EXPECT_CALL(*mockedPlacementAmendmentHandler, enqueueRequest(_))
        .Times(2)
        .WillRepeatedly(Invoke(handleMockedPlacementAmendmentCall));

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
    executeMockISQPRequest(isqpEventsForRequest1, storageHandler, mockedPlacementAmendmentHandler);
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
    executeMockISQPRequest(isqpEventsForRequest2, storageHandler, mockedPlacementAmendmentHandler);

    auto placementAmendmentHandler =
        std::make_shared<Optimizer::PlacementAmendmentHandler>(coordinatorConfiguration->optimizer.placementAmendmentThreadCount);
    placementAmendmentHandler->start();
    performPlacementAmendment(placementAmendmentHandler);
    auto response = queryAddEvent1->getResponse().get();
    auto queryId1 = std::static_pointer_cast<RequestProcessor::ISQPAddQueryResponse>(response)->queryId;
    EXPECT_EQ(queryCatalog->getQueryState(queryId1), QueryState::RUNNING);

    auto query2 = Query::from(logicalSourceName).sink(NullOutputSinkDescriptor::create());
    const QueryPlanPtr& queryPlan2 = query2.getQueryPlan();
    auto queryAddEvent2 = ISQPAddQueryEvent::create(queryPlan2, TEST_PLACEMENT_STRATEGY);

    std::vector<ISQPEventPtr> isqpEventsForRequest3;
    isqpEventsForRequest3.emplace_back(queryAddEvent2);
    executeMockISQPRequest(isqpEventsForRequest3, storageHandler, mockedPlacementAmendmentHandler);

    performPlacementAmendment(placementAmendmentHandler);
    auto response2 = queryAddEvent2->getResponse().get();
    auto queryId2 = std::static_pointer_cast<RequestProcessor::ISQPAddQueryResponse>(response2)->queryId;
    EXPECT_EQ(queryCatalog->getQueryState(queryId2), QueryState::RUNNING);
    placementAmendmentHandler->shutDown();
}

//test adding multiple queries without merging and perform placement using OCC startegy
TEST_F(PlacementAmendmentHandlerTest, testFailureDuringPlacementOfMultipleQueriesUsingOptimisticStrategy) {

    // 1. Create mocked amendment handler and ISQP request to setup topology and queries to be processed
    auto mockedPlacementAmendmentHandler = std::make_shared<NES::Optimizer::MockedPlacementAmendmentHandler>(1);
    // Two ISQP requests are issues in this test. One contains only topology change events and other contains an add query event.
    // Therefore, assert that only one call ot the mocked placement amendment handler are made.
    EXPECT_CALL(*mockedPlacementAmendmentHandler, enqueueRequest(_))
        .Times(4)
        .WillRepeatedly(Invoke(handleMockedPlacementAmendmentCall));

    // Disable query merging
    coordinatorConfiguration->optimizer.queryMergerRule = Optimizer::QueryMergerRule::DefaultQueryMergerRule;
    // Enable incremental placement
    coordinatorConfiguration->optimizer.enableIncrementalPlacement = true;
    // Number of amender threads
    coordinatorConfiguration->optimizer.placementAmendmentThreadCount = 4;
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

    // init topology nodes
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;
    uint16_t numberOfSlots = 6;
    auto nodeId1 = WorkerId(1);
    auto addNodeEvent1 = ISQPAddNodeEvent::create(WorkerType::CLOUD, nodeId1, "localhost", 4000, 4002, numberOfSlots, properties);
    auto nodeId2 = WorkerId(2);
    auto addNodeEvent2 =
        ISQPAddNodeEvent::create(WorkerType::SENSOR, nodeId2, "localhost", 4000, 4002, numberOfSlots, properties);
    auto nodeId3 = WorkerId(3);
    auto addNodeEvent3 =
        ISQPAddNodeEvent::create(WorkerType::SENSOR, nodeId3, "localhost", 4000, 4002, numberOfSlots, properties);
    auto nodeId4 = WorkerId(4);
    auto addNodeEvent4 =
        ISQPAddNodeEvent::create(WorkerType::SENSOR, nodeId4, "localhost", 4000, 4002, numberOfSlots, properties);
    auto isqpRemoveLink14 = ISQPRemoveLinkEvent::create(nodeId1, nodeId4);
    auto isqpAddLink34 = ISQPAddLinkEvent::create(nodeId3, nodeId4);
    std::vector<ISQPEventPtr> isqpEventsForRequest1;
    isqpEventsForRequest1.emplace_back(addNodeEvent1);
    isqpEventsForRequest1.emplace_back(addNodeEvent2);
    isqpEventsForRequest1.emplace_back(addNodeEvent3);
    isqpEventsForRequest1.emplace_back(addNodeEvent4);
    isqpEventsForRequest1.emplace_back(isqpRemoveLink14);
    isqpEventsForRequest1.emplace_back(isqpAddLink34);
    executeMockISQPRequest(isqpEventsForRequest1, storageHandler, mockedPlacementAmendmentHandler);
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
    auto query1 = Query::from(logicalSourceName)
                      .map(Attribute("value") = Attribute("value") + 1)
                      .map(Attribute("value") = Attribute("value") + 1)
                      .sink(NullOutputSinkDescriptor::create());
    const QueryPlanPtr& queryPlan1 = query1.getQueryPlan();
    queryPlan1->setQueryId(QueryId(1));
    auto queryAddEvent1 = ISQPAddQueryEvent::create(queryPlan1, TEST_PLACEMENT_STRATEGY);
    auto query2 = Query::from(logicalSourceName)
                      .map(Attribute("value") = Attribute("value") + 1)
                      .map(Attribute("value") = Attribute("value") + 1)
                      .sink(NullOutputSinkDescriptor::create());
    const QueryPlanPtr& queryPlan2 = query2.getQueryPlan();
    queryPlan2->setQueryId(QueryId(2));
    auto queryAddEvent2 = ISQPAddQueryEvent::create(queryPlan2, TEST_PLACEMENT_STRATEGY);
    auto query3 = Query::from(logicalSourceName)
                      .map(Attribute("value") = Attribute("value") + 1)
                      .map(Attribute("value") = Attribute("value") + 1)
                      .sink(NullOutputSinkDescriptor::create());
    const QueryPlanPtr& queryPlan3 = query3.getQueryPlan();
    queryPlan3->setQueryId(QueryId(3));
    auto queryAddEvent3 = ISQPAddQueryEvent::create(queryPlan3, TEST_PLACEMENT_STRATEGY);
    auto query4 = Query::from(logicalSourceName)
                      .map(Attribute("value") = Attribute("value") + 1)
                      .map(Attribute("value") = Attribute("value") + 1)
                      .sink(NullOutputSinkDescriptor::create());
    const QueryPlanPtr& queryPlan4 = query4.getQueryPlan();
    queryPlan4->setQueryId(QueryId(4));
    auto queryAddEvent4 = ISQPAddQueryEvent::create(queryPlan4, TEST_PLACEMENT_STRATEGY);
    std::vector<ISQPEventPtr> isqpEventsForRequest2;
    isqpEventsForRequest2.emplace_back(queryAddEvent1);
    isqpEventsForRequest2.emplace_back(queryAddEvent2);
    isqpEventsForRequest2.emplace_back(queryAddEvent3);
    isqpEventsForRequest2.emplace_back(queryAddEvent4);
    // Perform ISQP request to create shared query plans for the added queries
    executeMockISQPRequest(isqpEventsForRequest2, storageHandler, mockedPlacementAmendmentHandler);

    auto placementAmendmentHandler =
        std::make_shared<Optimizer::PlacementAmendmentHandler>(coordinatorConfiguration->optimizer.placementAmendmentThreadCount);
    placementAmendmentHandler->start();
    performPlacementAmendment(placementAmendmentHandler);
    auto sharedQueryPlanToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    // Only two requests should be processed
    EXPECT_EQ(sharedQueryPlanToDeploy.size(), 2);

    // Assert that the change log still exists in the failed shared query plans and the status of the query is in optimizing
    /*    for (const auto& unDeployedSharedQueryPlan : sharedQueryPlanToDeploy) {
        auto hostedQueryIds = unDeployedSharedQueryPlan->getQueryIds();
        EXPECT_EQ(hostedQueryIds.size(), 1);
        EXPECT_EQ(queryCatalog->getQueryState(hostedQueryIds[0]), QueryState::OPTIMIZING);
        uint64_t nowInMicroSec =
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        const ChangeLogEntries& changeLogEntries = unDeployedSharedQueryPlan->getChangeLogEntries(nowInMicroSec);
        EXPECT_EQ(changeLogEntries.size(), 1);
    }*/
    placementAmendmentHandler->shutDown();
}

//test adding multiple queries
TEST_F(PlacementAmendmentHandlerTest, testTopologyChangeEventsInaSingleBatchWithMergingWithoutIncrementalPlacement2PL) {

    // 1. Create mocked amendment handler and ISQP request to setup topology and queries to be processed
    auto mockedPlacementAmendmentHandler = std::make_shared<NES::Optimizer::MockedPlacementAmendmentHandler>(1);
    // Two ISQP requests are issues in this test. One contains only topology change events and other contains an add query event.
    // Therefore, assert that only one call ot the mocked placement amendment handler are made.
    EXPECT_CALL(*mockedPlacementAmendmentHandler, enqueueRequest(_))
        .Times(2)
        .WillRepeatedly(Invoke(handleMockedPlacementAmendmentCall));

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
    executeMockISQPRequest(isqpEventsForRequest1, storageHandler, mockedPlacementAmendmentHandler);
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

    executeMockISQPRequest(isqpEventsForRequest2, storageHandler, mockedPlacementAmendmentHandler);

    auto placementAmendmentHandler =
        std::make_shared<Optimizer::PlacementAmendmentHandler>(coordinatorConfiguration->optimizer.placementAmendmentThreadCount);
    placementAmendmentHandler->start();
    performPlacementAmendment(placementAmendmentHandler);
    auto response1 = queryAddEvent1->getResponse().get();
    auto queryId1 = std::static_pointer_cast<RequestProcessor::ISQPAddQueryResponse>(response1)->queryId;
    EXPECT_EQ(queryCatalog->getQueryState(queryId1), QueryState::RUNNING);
    auto response2 = queryAddEvent2->getResponse().get();
    auto queryId2 = std::static_pointer_cast<RequestProcessor::ISQPAddQueryResponse>(response2)->queryId;
    EXPECT_EQ(queryCatalog->getQueryState(queryId2), QueryState::RUNNING);

    // Prepare
    auto addLink34 = ISQPAddLinkEvent::create(nodeId3, nodeId4);
    auto removeLink24 = ISQPRemoveLinkEvent::create(nodeId2, nodeId4);
    std::vector<ISQPEventPtr> isqpEventsForRequest3;
    isqpEventsForRequest3.emplace_back(addLink34);
    isqpEventsForRequest3.emplace_back(removeLink24);
    executeMockISQPRequest(isqpEventsForRequest3, storageHandler, mockedPlacementAmendmentHandler);

    performPlacementAmendment(placementAmendmentHandler);
    // Verify if removal happened
    auto sharedQueryId = globalQueryPlan->getSharedQueryId(queryId1);
    auto sharedQueryPlan = globalQueryPlan->getSharedQueryPlan(sharedQueryId);
    auto numOfSinks = sharedQueryPlan->getQueryPlan()->getSinkOperators().size();
    EXPECT_EQ(numOfSinks, 2);
    placementAmendmentHandler->shutDown();
}

//test adding multiple queries
TEST_F(PlacementAmendmentHandlerTest,
       testTopologyChangeEventsFourUnionQueryInaSingleBatchWithMergingWithoutIncrementalPlacement2PL) {

    // 1. Create mocked amendment handler and ISQP request to setup topology and queries to be processed
    auto mockedPlacementAmendmentHandler = std::make_shared<NES::Optimizer::MockedPlacementAmendmentHandler>(1);
    // Two ISQP requests are issues in this test. One contains only topology change events and other contains an add query event.
    // Therefore, assert that only one call ot the mocked placement amendment handler are made.
    EXPECT_CALL(*mockedPlacementAmendmentHandler, enqueueRequest(_))
        .Times(2)
        .WillRepeatedly(Invoke(handleMockedPlacementAmendmentCall));

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
    auto isqpAddLink25 = ISQPAddLinkEvent::create(nodeId2, nodeId5);
    auto isqpAddLinkProperty24 = ISQPAddLinkPropertyEvent::create(nodeId2, nodeId4, 1, 1);
    auto isqpAddLinkProperty25 = ISQPAddLinkPropertyEvent::create(nodeId2, nodeId5, 1, 1);
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
    isqpEventsForRequest1.emplace_back(isqpAddLink25);
    isqpEventsForRequest1.emplace_back(isqpAddLinkProperty24);
    isqpEventsForRequest1.emplace_back(isqpAddLinkProperty25);
    isqpEventsForRequest1.emplace_back(isqpAddLinkProperty12);
    isqpEventsForRequest1.emplace_back(isqpAddLinkProperty13);
    executeMockISQPRequest(isqpEventsForRequest1, storageHandler, mockedPlacementAmendmentHandler);
    EXPECT_TRUE(topology->nodeWithWorkerIdExists(nodeId4));

    // Register physical and logical sources
    auto schema = TestSchemas::getSchemaTemplate("id_val_u32");
    std::string logicalSourceName1 = "test1";
    auto defaultSourceType1 = DefaultSourceType::create(logicalSourceName1, "pTest1");
    auto physicalSource1 = PhysicalSource::create(defaultSourceType1);
    auto logicalSource1 = LogicalSource::create(logicalSourceName1, schema);
    auto sce1 = Catalogs::Source::SourceCatalogEntry::create(physicalSource1, logicalSource1, nodeId5);
    std::string logicalSourceName2 = "test2";
    auto defaultSourceType2 = DefaultSourceType::create(logicalSourceName2, "pTest2");
    auto physicalSource2 = PhysicalSource::create(defaultSourceType2);
    auto logicalSource2 = LogicalSource::create(logicalSourceName2, schema);
    auto sce2 = Catalogs::Source::SourceCatalogEntry::create(physicalSource2, logicalSource2, nodeId4);
    sourceCatalog->addLogicalSource(logicalSource1->getLogicalSourceName(), logicalSource1->getSchema());
    sourceCatalog->addLogicalSource(logicalSource2->getLogicalSourceName(), logicalSource2->getSchema());
    sourceCatalog->addPhysicalSource(logicalSourceName1, sce1);
    sourceCatalog->addPhysicalSource(logicalSourceName2, sce2);

    auto query1 = Query::from(logicalSourceName1)
                      .map(Attribute("value") = Attribute("value") + 1)
                      .unionWith(Query::from(logicalSourceName2).map(Attribute("value") = Attribute("value") + 1))
                      .sink(NullOutputSinkDescriptor::create());
    const QueryPlanPtr& queryPlan1 = query1.getQueryPlan();
    queryPlan1->setQueryId(QueryId(1));
    auto queryAddEvent1 = ISQPAddQueryEvent::create(queryPlan1, TEST_PLACEMENT_STRATEGY);

    std::vector<ISQPEventPtr> isqpEventsForRequest2;
    isqpEventsForRequest2.emplace_back(queryAddEvent1);
    executeMockISQPRequest(isqpEventsForRequest2, storageHandler, mockedPlacementAmendmentHandler);

    auto placementAmendmentHandler =
        std::make_shared<Optimizer::PlacementAmendmentHandler>(coordinatorConfiguration->optimizer.placementAmendmentThreadCount);
    placementAmendmentHandler->start();
    performPlacementAmendment(placementAmendmentHandler);
    auto response1 = queryAddEvent1->getResponse().get();
    auto queryId1 = std::static_pointer_cast<RequestProcessor::ISQPAddQueryResponse>(response1)->queryId;
    EXPECT_EQ(queryCatalog->getQueryState(queryId1), QueryState::RUNNING);

    topology->print();

    // Prepare
    auto addLink34 = ISQPAddLinkEvent::create(nodeId3, nodeId4);
    auto removeLink24 = ISQPRemoveLinkEvent::create(nodeId2, nodeId4);
    std::vector<ISQPEventPtr> isqpEventsForRequest3;
    isqpEventsForRequest3.emplace_back(addLink34);
    isqpEventsForRequest3.emplace_back(removeLink24);
    executeMockISQPRequest(isqpEventsForRequest3, storageHandler, mockedPlacementAmendmentHandler);

    performPlacementAmendment(placementAmendmentHandler);
    // Verify if removal happened
    auto sharedQueryId = globalQueryPlan->getSharedQueryId(queryId1);
    auto sharedQueryPlan = globalQueryPlan->getSharedQueryPlan(sharedQueryId);
    auto numOfSinks = sharedQueryPlan->getQueryPlan()->getSinkOperators().size();
    EXPECT_EQ(numOfSinks, 1);
    placementAmendmentHandler->shutDown();
}

TEST_F(PlacementAmendmentHandlerTest,
       testTopologyChangeEventsFourUnionQueryInaSingleBatchWithMergingWithIncrementalPlacement2PL) {

    // 1. Create mocked amendment handler and ISQP request to setup topology and queries to be processed
    auto mockedPlacementAmendmentHandler = std::make_shared<NES::Optimizer::MockedPlacementAmendmentHandler>(1);
    // Two ISQP requests are issues in this test. One contains only topology change events and other contains an add query event.
    // Therefore, assert that only one call ot the mocked placement amendment handler are made.
    EXPECT_CALL(*mockedPlacementAmendmentHandler, enqueueRequest(_))
        .Times(2)
        .WillRepeatedly(Invoke(handleMockedPlacementAmendmentCall));

    // Enable query merging
    coordinatorConfiguration->optimizer.queryMergerRule = Optimizer::QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule;
    // Disable incremental placement
    coordinatorConfiguration->optimizer.enableIncrementalPlacement = true;
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
    auto isqpAddLink25 = ISQPAddLinkEvent::create(nodeId2, nodeId5);
    auto isqpAddLinkProperty24 = ISQPAddLinkPropertyEvent::create(nodeId2, nodeId4, 1, 1);
    auto isqpAddLinkProperty25 = ISQPAddLinkPropertyEvent::create(nodeId2, nodeId5, 1, 1);
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
    isqpEventsForRequest1.emplace_back(isqpAddLink25);
    isqpEventsForRequest1.emplace_back(isqpAddLinkProperty24);
    isqpEventsForRequest1.emplace_back(isqpAddLinkProperty25);
    isqpEventsForRequest1.emplace_back(isqpAddLinkProperty12);
    isqpEventsForRequest1.emplace_back(isqpAddLinkProperty13);
    executeMockISQPRequest(isqpEventsForRequest1, storageHandler, mockedPlacementAmendmentHandler);
    EXPECT_TRUE(topology->nodeWithWorkerIdExists(WorkerId(nodeId4)));

    // Register physical and logical sources
    auto schema = TestSchemas::getSchemaTemplate("id_val_u32");
    std::string logicalSourceName1 = "test1";
    auto defaultSourceType1 = DefaultSourceType::create(logicalSourceName1, "pTest1");
    auto physicalSource1 = PhysicalSource::create(defaultSourceType1);
    auto logicalSource1 = LogicalSource::create(logicalSourceName1, schema);
    auto sce1 = Catalogs::Source::SourceCatalogEntry::create(physicalSource1, logicalSource1, nodeId5);
    std::string logicalSourceName2 = "test2";
    auto defaultSourceType2 = DefaultSourceType::create(logicalSourceName2, "pTest2");
    auto physicalSource2 = PhysicalSource::create(defaultSourceType2);
    auto logicalSource2 = LogicalSource::create(logicalSourceName2, schema);
    auto sce2 = Catalogs::Source::SourceCatalogEntry::create(physicalSource2, logicalSource2, nodeId4);
    sourceCatalog->addLogicalSource(logicalSource1->getLogicalSourceName(), logicalSource1->getSchema());
    sourceCatalog->addLogicalSource(logicalSource2->getLogicalSourceName(), logicalSource2->getSchema());
    sourceCatalog->addPhysicalSource(logicalSourceName1, sce1);
    sourceCatalog->addPhysicalSource(logicalSourceName2, sce2);

    auto query1 = Query::from(logicalSourceName1)
                      .map(Attribute("value") = Attribute("value") + 1)
                      .unionWith(Query::from(logicalSourceName2).map(Attribute("value") = Attribute("value") + 1))
                      .sink(NullOutputSinkDescriptor::create());
    const QueryPlanPtr& queryPlan1 = query1.getQueryPlan();
    queryPlan1->setQueryId(QueryId(1));
    auto queryAddEvent1 = ISQPAddQueryEvent::create(queryPlan1, TEST_PLACEMENT_STRATEGY);

    std::vector<ISQPEventPtr> isqpEventsForRequest2;
    isqpEventsForRequest2.emplace_back(queryAddEvent1);
    executeMockISQPRequest(isqpEventsForRequest2, storageHandler, mockedPlacementAmendmentHandler);

    auto placementAmendmentHandler =
        std::make_shared<Optimizer::PlacementAmendmentHandler>(coordinatorConfiguration->optimizer.placementAmendmentThreadCount);
    placementAmendmentHandler->start();
    performPlacementAmendment(placementAmendmentHandler);
    auto response1 = queryAddEvent1->getResponse().get();
    auto queryId1 = std::static_pointer_cast<RequestProcessor::ISQPAddQueryResponse>(response1)->queryId;
    EXPECT_EQ(queryCatalog->getQueryState(queryId1), QueryState::RUNNING);

    topology->print();

    // Prepare
    auto addLink34 = ISQPAddLinkEvent::create(nodeId3, nodeId4);
    auto removeLink24 = ISQPRemoveLinkEvent::create(nodeId2, nodeId4);
    std::vector<ISQPEventPtr> isqpEventsForRequest3;
    isqpEventsForRequest3.emplace_back(addLink34);
    isqpEventsForRequest3.emplace_back(removeLink24);
    executeMockISQPRequest(isqpEventsForRequest3, storageHandler, mockedPlacementAmendmentHandler);

    performPlacementAmendment(placementAmendmentHandler);
    // Verify if removal happened
    auto sharedQueryId = globalQueryPlan->getSharedQueryId(queryId1);
    auto sharedQueryPlan = globalQueryPlan->getSharedQueryPlan(sharedQueryId);
    auto numOfSinks = sharedQueryPlan->getQueryPlan()->getSinkOperators().size();
    EXPECT_EQ(numOfSinks, 1);
    placementAmendmentHandler->shutDown();
}

}// namespace NES::RequestProcessor
