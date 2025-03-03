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
#include <GRPC/WorkerRPCClient.hpp>
#include <Operators/LogicalOperators/LogicalMapOperator.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <RequestProcessor/RequestTypes/AddQueryRequest.hpp>
#include <RequestProcessor/StorageHandles/StorageDataStructures.hpp>
#include <RequestProcessor/StorageHandles/TwoPhaseLockingStorageHandler.hpp>
#include <Services/PlacementAmendment/PlacementAmendmentHandler.hpp>
#include <StatisticCollection/StatisticCache/DefaultStatisticCache.hpp>
#include <StatisticCollection/StatisticProbeHandling/DefaultStatisticProbeGenerator.hpp>
#include <StatisticCollection/StatisticProbeHandling/StatisticProbeHandler.hpp>
#include <StatisticCollection/StatisticRegistry/StatisticRegistry.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <iostream>
#include <z3++.h>

namespace NES::RequestProcessor {

class AddQueryRequestTest : public Testing::BaseUnitTest {
  public:
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    std::shared_ptr<Catalogs::UDF::UDFCatalog> udfCatalog;
    Optimizer::PlacementStrategy TEST_PLACEMENT_STRATEGY = Optimizer::PlacementStrategy::TopDown;
    uint8_t ZERO_RETRIES = 0;
    std::shared_ptr<Catalogs::Query::QueryCatalog> queryCatalog;
    TopologyPtr topology;
    GlobalQueryPlanPtr globalQueryPlan;
    Optimizer::GlobalExecutionPlanPtr globalExecutionPlan;
    Configurations::CoordinatorConfigurationPtr coordinatorConfiguration;
    z3::ContextPtr z3Context;
    Statistic::StatisticProbeHandlerPtr statisticProbeHandler;

    /* Will be called before all tests in this class are started. */
    static void SetUpTestCase() { NES::Logger::setupLogging("QueryFailureTest.log", NES::LogLevel::LOG_DEBUG); }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        // init topology node for physical source
        std::map<std::string, std::any> properties;
        properties[NES::Worker::Properties::MAINTENANCE] = false;
        properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;
        auto rootNodeId = WorkerId(1);
        topology = Topology::create();
        topology->registerWorker(rootNodeId, "localhost", 4000, 4002, 4, properties, 0, 0);
        topology->addAsRootWorkerId(rootNodeId);
        auto defaultSourceType = DefaultSourceType::create("test2", "test_source");
        auto physicalSource = PhysicalSource::create(defaultSourceType);
        auto logicalSource = LogicalSource::create("test2", Schema::create());
        // add source to source catalog
        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
        sourceCatalog->addLogicalSource(logicalSource->getLogicalSourceName(), logicalSource->getSchema());
        auto sce = Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, rootNodeId);
        sourceCatalog->addPhysicalSource("default_logical", sce);
        queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
        coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
        globalQueryPlan = GlobalQueryPlan::create();
        globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();
        udfCatalog = Catalogs::UDF::UDFCatalog::create();
        z3Context = std::make_shared<z3::context>();
        statisticProbeHandler = Statistic::StatisticProbeHandler::create(Statistic::StatisticRegistry::create(),
                                                                         Statistic::DefaultStatisticProbeGenerator::create(),
                                                                         Statistic::DefaultStatisticCache::create(),
                                                                         topology);
    }
};

//test adding a single query until the deployment step, which cannot be done in a unit test
TEST_F(AddQueryRequestTest, testAddQueryRequestWithOneQuery) {

    // Prepare
    constexpr auto requestId = RequestId(1);
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical").sink(printSinkDescriptor);
    QueryPlanPtr queryPlan = query.getQueryPlan();
    SinkLogicalOperatorPtr sinkOperator1 = queryPlan->getSinkOperators()[0];
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);
    auto storageHandler = TwoPhaseLockingStorageHandler::create({coordinatorConfiguration,
                                                                 topology,
                                                                 globalExecutionPlan,
                                                                 globalQueryPlan,
                                                                 queryCatalog,
                                                                 sourceCatalog,
                                                                 udfCatalog,
                                                                 statisticProbeHandler});

    //Create new entry in query catalog service
    queryCatalog->createQueryCatalogEntry("query string",
                                          queryPlan,
                                          Optimizer::PlacementStrategy::TopDown,
                                          QueryState::REGISTERED);

    EXPECT_EQ(queryCatalog->getQueryState(queryId), QueryState::REGISTERED);

    // initialize but not start the placement amendment handler
    auto placementAmendmentHandler = std::make_shared<Optimizer::PlacementAmendmentHandler>(0);
    // Create add request
    auto addQueryRequest = RequestProcessor::AddQueryRequest::create(queryPlan,
                                                                     TEST_PLACEMENT_STRATEGY,
                                                                     ZERO_RETRIES,
                                                                     z3Context,
                                                                     placementAmendmentHandler);
    addQueryRequest->setId(requestId);

    // Execute add request until deployment phase
    try {
        addQueryRequest->execute(storageHandler);
    } catch (Exceptions::RPCQueryUndeploymentException& e) {
        EXPECT_EQ(e.getMode(), RpcClientMode::Register);
    }
}
}// namespace NES::RequestProcessor
