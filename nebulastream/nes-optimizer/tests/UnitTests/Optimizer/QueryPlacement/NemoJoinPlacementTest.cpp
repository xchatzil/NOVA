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
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Source/SourceCatalogEntry.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Operators/LogicalOperators/LogicalInferModelOperator.hpp>
#include <Operators/LogicalOperators/Network/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinOperator.hpp>
#include <Optimizer/Phases/QueryPlacementAmendmentPhase.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryPlacementAddition/BasePlacementAdditionStrategy.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <StatisticCollection/StatisticCache/DefaultStatisticCache.hpp>
#include <StatisticCollection/StatisticProbeHandling/DefaultStatisticProbeGenerator.hpp>
#include <StatisticCollection/StatisticProbeHandling/StatisticProbeHandler.hpp>
#include <StatisticCollection/StatisticRegistry/StatisticRegistry.hpp>
#include <Util/Logger/Logger.hpp>

#include <gtest/gtest.h>
#include <z3++.h>

using namespace NES;
using namespace z3;
using namespace Configurations;
using namespace Optimizer;

static const std::string logSourceNameLeft = "log_left";
static const std::string logSourceNameRight = "log_right";

class NemoJoinPlacementTest : public Testing::BaseUnitTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("NemoJoinPlacementTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup NemoJoinPlacementTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        NES_DEBUG("Setup NemoJoinPlacementTest test case.");
    }

    /**
     * Create topology based on given layers, intermediate nodes, and source nodes
     */
    static TopologyPtr setupTopology(uint64_t layers, uint64_t nodesPerNode, uint64_t leafNodesPerNode) {
        uint64_t resources = 100;
        auto rootId = WorkerId(1);
        auto nodeId = rootId.getRawValue();
        uint64_t leafNodes = 0;

        std::vector<WorkerId> nodes;
        std::vector<WorkerId> parents;

        std::map<std::string, std::any> properties;
        properties[NES::Worker::Properties::MAINTENANCE] = false;

        // Setup the topology
        TopologyPtr topology = Topology::create();
        topology->registerWorker(rootId, "localhost", 4000, 5000, resources, properties, 0, 0);
        topology->addAsRootWorkerId(rootId);
        nodes.emplace_back(rootId);
        parents.emplace_back(rootId);

        for (uint64_t i = 2; i <= layers; i++) {
            std::vector<WorkerId> newParents;
            for (auto parent : parents) {
                uint64_t nodeCnt = nodesPerNode;
                if (i == layers) {
                    nodeCnt = leafNodesPerNode;
                }
                for (uint64_t j = 0; j < nodeCnt; j++) {
                    if (i == layers) {
                        leafNodes++;
                    }
                    nodeId++;
                    topology->registerWorker(WorkerId(nodeId),
                                             "localhost",
                                             4000 + nodeId,
                                             5000 + nodeId,
                                             resources,
                                             properties,
                                             0,
                                             0);
                    topology->removeTopologyNodeAsChild(rootId, WorkerId(nodeId));
                    topology->addTopologyNodeAsChild(parent, WorkerId(nodeId));
                    nodes.emplace_back(nodeId);
                    newParents.emplace_back(nodeId);
                }
            }
            parents = newParents;
        }

        NES_DEBUG("NemoJoinPlacementTest: topology: {}", topology->toString());
        return topology;
    }

    /**
     * Creates a SourceCatalog where all even sources are part of the left join and odd nodes part of the right join.
     */
    static Catalogs::Source::SourceCatalogPtr setupJoinSourceCatalog(const std::vector<WorkerId>& sourceNodes,
                                                                     const uint64_t groupSize = 2) {
        // Prepare the source and schema
        auto schema = Schema::create()
                          ->addField("id", BasicType::UINT32)
                          ->addField("value", BasicType::UINT64)
                          ->addField("timestamp", DataTypeFactory::createUInt64());
        Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
        sourceCatalog->addLogicalSource(logSourceNameLeft, schema);
        sourceCatalog->addLogicalSource(logSourceNameRight, schema);

        std::map<NES::Catalogs::Source::SourceCatalogEntryPtr, std::set<uint64_t>> distributionMap;
        uint64_t keyVal = 10;
        uint64_t cnt = 0;
        for (auto nodeId : sourceNodes) {
            const auto isEven = nodeId.getRawValue() % 2 == 0;
            std::string logSourceName = isEven ? logSourceNameLeft : logSourceNameRight;
            std::string phySourceName = isEven ? "left_" + nodeId.toString() : "right_" + nodeId.toString();
            CSVSourceTypePtr csvSourceType = CSVSourceType::create(logSourceName, phySourceName);

            auto physicalSource = PhysicalSource::create(csvSourceType);
            auto logicalSource = sourceCatalog->getLogicalSource(logSourceName);

            auto entry = Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, nodeId);
            sourceCatalog->addPhysicalSource(logSourceName, entry);

            std::set<uint64_t> values = {keyVal};
            NES_INFO("Adding dist: {},{},{}",
                     logicalSource->getLogicalSourceName(),
                     physicalSource->getPhysicalSourceName(),
                     keyVal);
            distributionMap.emplace(entry, std::move(values));
            if ((cnt + 1) % groupSize == 0) {
                keyVal += 10;
            }
            cnt++;
        }
        sourceCatalog->setKeyDistributionMap(distributionMap);
        return sourceCatalog;
    }

    /**
     * Creates a SourceCatalog where all even sources are part of the left join and odd nodes part of the right join.
     */
    static Catalogs::Source::SourceCatalogPtr setupJoinSourceCatalogChain(const std::vector<WorkerId>& leftNodes,
                                                                          const std::vector<WorkerId>& rightNodes) {
        // Prepare the source and schema
        auto schema = Schema::create()
                          ->addField("id", BasicType::UINT32)
                          ->addField("value", BasicType::UINT64)
                          ->addField("timestamp", DataTypeFactory::createUInt64());
        Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
        sourceCatalog->addLogicalSource(logSourceNameLeft, schema);
        sourceCatalog->addLogicalSource(logSourceNameRight, schema);

        std::map<NES::Catalogs::Source::SourceCatalogEntryPtr, std::set<uint64_t>> distributionMap;
        uint64_t keyVal = 10;
        uint64_t cnt = 0;
        for (unsigned long i = 0; i < leftNodes.size(); i++) {
            auto leftId = leftNodes[i];
            std::string phySourceNameLeft = "left_" + leftId.toString() + "_p";
            CSVSourceTypePtr csvSourceTypeLeft = CSVSourceType::create(logSourceNameLeft, phySourceNameLeft);
            auto physicalSourceLeft = PhysicalSource::create(csvSourceTypeLeft);
            auto logicalSourceLeft = sourceCatalog->getLogicalSource(logSourceNameLeft);
            auto entryLeft = Catalogs::Source::SourceCatalogEntry::create(physicalSourceLeft, logicalSourceLeft, leftId);
            sourceCatalog->addPhysicalSource(logSourceNameLeft, entryLeft);

            std::set<uint64_t> valuesLeft = {keyVal};
            NES_INFO("Adding dist: {},{},{}",
                     logicalSourceLeft->getLogicalSourceName(),
                     physicalSourceLeft->getPhysicalSourceName(),
                     keyVal);
            distributionMap.emplace(entryLeft, std::move(valuesLeft));

            auto rightId = rightNodes[i];
            std::string phySourceNameRight = "right_" + rightId.toString();
            CSVSourceTypePtr csvSourceTypeRight = CSVSourceType::create(logSourceNameRight, phySourceNameRight);
            auto physicalSourceRight = PhysicalSource::create(csvSourceTypeRight);
            auto logicalSourceRight = sourceCatalog->getLogicalSource(logSourceNameRight);
            auto entryRight = Catalogs::Source::SourceCatalogEntry::create(physicalSourceRight, logicalSourceRight, rightId);
            sourceCatalog->addPhysicalSource(logSourceNameRight, entryRight);

            std::set<uint64_t> valuesRight = {keyVal};
            NES_INFO("Adding dist: {},{},{}",
                     logicalSourceRight->getLogicalSourceName(),
                     physicalSourceRight->getPhysicalSourceName(),
                     keyVal);
            distributionMap.emplace(entryRight, std::move(valuesRight));

            keyVal += 10;
        }

        sourceCatalog->setKeyDistributionMap(distributionMap);
        return sourceCatalog;
    }

    /**
     * Performs an operator placement based on the given query, topology, source catalog and optimizer config.
     */
    static std::tuple<SharedQueryId, GlobalExecutionPlanPtr>
    runPlacement(const Query& query,
                 const TopologyPtr& topology,
                 const Catalogs::Source::SourceCatalogPtr& sourceCatalog,
                 const OptimizerConfiguration& optimizerConfig,
                 const Statistic::StatisticProbeHandlerPtr& statisticProbeHandler,
                 Optimizer::PlacementStrategy placementStrategy = Optimizer::PlacementStrategy::BottomUp) {
        std::shared_ptr<Catalogs::UDF::UDFCatalog> udfCatalog = Catalogs::UDF::UDFCatalog::create();
        auto testQueryPlan = query.getQueryPlan();
        testQueryPlan->setPlacementStrategy(placementStrategy);

        // Prepare the placement
        GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
        auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

        // Execute optimization phases prior to placement
        testQueryPlan = typeInferencePhase->execute(testQueryPlan);
        auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
        auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
        testQueryPlan = queryReWritePhase->execute(testQueryPlan);
        typeInferencePhase->execute(testQueryPlan);

        auto topologySpecificQueryRewrite =
            Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, optimizerConfig, statisticProbeHandler);
        topologySpecificQueryRewrite->execute(testQueryPlan);
        typeInferencePhase->execute(testQueryPlan);

        // Execute the placement
        auto sharedQueryPlan = SharedQueryPlan::create(testQueryPlan);
        auto sharedQueryId = sharedQueryPlan->getId();
        auto queryPlacementPhase = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                   topology,
                                                                                   typeInferencePhase,
                                                                                   coordinatorConfiguration);
        queryPlacementPhase->execute(sharedQueryPlan);
        return {sharedQueryId, globalExecutionPlan};
    }

    template<typename T>
    static void verifyChildrenOfType(const std::vector<DecomposedQueryPlanPtr>& querySubPlans,
                                     uint64_t expectedSubPlanSize = 1,
                                     uint64_t expectedRootOperators = 1) {
        ASSERT_EQ(querySubPlans.size(), expectedSubPlanSize);
        for (auto& querySubPlan : querySubPlans) {
            std::vector<OperatorPtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), expectedRootOperators);
            OperatorPtr actualRootOperator = actualRootOperators[0];
            ASSERT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperator>());
            auto children = actualRootOperator->getChildren();
            for (const auto& child : children) {
                ASSERT_TRUE(child->instanceOf<T>());
            }
        }
    }

    template<typename T>
    static void verifySourceOperators(std::vector<DecomposedQueryPlanPtr>& querySubPlans,
                                      uint64_t expectedSubPlanSize,
                                      uint64_t expectedSourceOperatorSize) {
        ASSERT_EQ(querySubPlans.size(), expectedSubPlanSize);
        auto querySubPlan = querySubPlans[0];
        std::vector<SourceLogicalOperatorPtr> sourceOperators = querySubPlan->getSourceOperators();
        ASSERT_EQ(sourceOperators.size(), expectedSourceOperatorSize);
        for (const auto& sourceOperator : sourceOperators) {
            EXPECT_TRUE(sourceOperator->instanceOf<SourceLogicalOperator>());
            auto sourceDescriptor = sourceOperator->as_if<SourceLogicalOperator>()->getSourceDescriptor();
            ASSERT_TRUE(sourceDescriptor->instanceOf<T>());
        }
    }
};

TEST_F(NemoJoinPlacementTest, testNemoJoin) {
    // create flat topology with 1 coordinator, 3 workers and 6 sources
    const std::vector<WorkerId>& sourceNodes = {WorkerId(5), WorkerId(6), WorkerId(7), WorkerId(8), WorkerId(9), WorkerId(10)};
    auto topology = setupTopology(3, 3, 2);
    auto sourceCatalog = setupJoinSourceCatalog(sourceNodes);
    auto statisticProbeHandler = Statistic::StatisticProbeHandler::create(Statistic::StatisticRegistry::create(),
                                                                          Statistic::DefaultStatisticProbeGenerator::create(),
                                                                          Statistic::DefaultStatisticCache::create(),
                                                                          Topology::create());

    auto optimizerConfig = Configurations::OptimizerConfiguration();
    optimizerConfig.joinOptimizationMode = DistributedJoinOptimizationMode::NEMO;

    //run the placement
    Query query = Query::from(logSourceNameLeft)
                      .joinWith(Query::from(logSourceNameRight))
                      .where(Attribute("id") == Attribute("id"))
                      .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                      .sink(NullOutputSinkDescriptor::create());

    auto outputTuple = runPlacement(query, topology, sourceCatalog, optimizerConfig, statisticProbeHandler);
    auto queryId = std::get<0>(outputTuple);
    auto executionPlan = std::get<1>(outputTuple);
    auto executionNodes = executionPlan->getLockedExecutionNodesHostingSharedQueryId(queryId);
    EXPECT_EQ(executionNodes.size(), 10);// topology contains 1 coordinator, 3 workers, 6 sources

    for (const auto& executionNode : executionNodes) {
        std::vector<DecomposedQueryPlanPtr> querySubPlans = executionNode->operator*()->getAllDecomposedQueryPlans(queryId);
        auto isSourceIt = std::find(sourceNodes.begin(), sourceNodes.end(), executionNode->operator*()->getId());
        if (executionNode->operator*()->getId() == WorkerId(1)) {
            // coordinator should have 3 network sinks for the three joins at intermediate nodes
            verifyChildrenOfType<SourceLogicalOperator>(querySubPlans, 1, 1);
            verifySourceOperators<NES::Network::NetworkSourceDescriptor>(querySubPlans, 1, 3);
        } else if (isSourceIt == sourceNodes.end()) {
            // node is an intermediate worker and not a source node which contains the joins
            verifyChildrenOfType<LogicalJoinOperator>(querySubPlans, 1, 1);
            verifySourceOperators<NES::Network::NetworkSourceDescriptor>(querySubPlans, 1, 2);
        } else {
            // node is a source node and only contains the watermark and source operator
            verifyChildrenOfType<WatermarkAssignerLogicalOperator>(querySubPlans, 1, 1);
            verifySourceOperators<LogicalSourceDescriptor>(querySubPlans, 1, 1);
        }
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(1));
}

TEST_F(NemoJoinPlacementTest, testNemoJoin2) {
    // create flat topology with 1 coordinator, 3 workers and 6 sources
    const std::vector<WorkerId>& sourceNodes =
        {WorkerId(8), WorkerId(9), WorkerId(10), WorkerId(11), WorkerId(12), WorkerId(13), WorkerId(14), WorkerId(15)};
    auto topology = setupTopology(4, 2, 2);
    auto sourceCatalog = setupJoinSourceCatalog(sourceNodes, 4);
    auto statisticProbeHandler = Statistic::StatisticProbeHandler::create(Statistic::StatisticRegistry::create(),
                                                                          Statistic::DefaultStatisticProbeGenerator::create(),
                                                                          Statistic::DefaultStatisticCache::create(),
                                                                          Topology::create());

    auto optimizerConfig = Configurations::OptimizerConfiguration();
    optimizerConfig.joinOptimizationMode = DistributedJoinOptimizationMode::NEMO;

    //run the placement
    Query query = Query::from(logSourceNameLeft)
                      .joinWith(Query::from(logSourceNameRight))
                      .where(Attribute("id") == Attribute("id"))
                      .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                      .sink(NullOutputSinkDescriptor::create());

    auto outputTuple = runPlacement(query, topology, sourceCatalog, optimizerConfig, statisticProbeHandler, Optimizer::PlacementStrategy::BottomUp);
    auto queryId = std::get<0>(outputTuple);
    auto executionPlan = std::get<1>(outputTuple);
    auto executionNodes = executionPlan->getLockedExecutionNodesHostingSharedQueryId(queryId);
    EXPECT_EQ(executionNodes.size(), 10);// topology contains 1 coordinator, 3 workers, 6 sources

    for (const auto& executionNode : executionNodes) {
        std::vector<DecomposedQueryPlanPtr> querySubPlans = executionNode->operator*()->getAllDecomposedQueryPlans(queryId);
        auto isSourceIt = std::find(sourceNodes.begin(), sourceNodes.end(), executionNode->operator*()->getId());
        if (executionNode->operator*()->getId() == WorkerId(1)) {
            // coordinator should have 3 network sinks for the three joins at intermediate nodes
            verifyChildrenOfType<SourceLogicalOperator>(querySubPlans, 1, 1);
            verifySourceOperators<NES::Network::NetworkSourceDescriptor>(querySubPlans, 1, 3);
        } else if (isSourceIt == sourceNodes.end()) {
            // node is an intermediate worker and not a source node which contains the joins
            verifyChildrenOfType<LogicalJoinOperator>(querySubPlans, 1, 1);
            verifySourceOperators<NES::Network::NetworkSourceDescriptor>(querySubPlans, 1, 2);
        } else {
            // node is a source node and only contains the watermark and source operator
            verifyChildrenOfType<WatermarkAssignerLogicalOperator>(querySubPlans, 1, 1);
            verifySourceOperators<LogicalSourceDescriptor>(querySubPlans, 1, 1);
        }
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(1));
}

TEST_F(NemoJoinPlacementTest, testNemoJoinSource) {
    // create flat topology with 1 coordinator, 3 workers and 6 sources
    const std::vector<WorkerId>& sourceNodesLeft = {WorkerId(2), WorkerId(3)};
    const std::vector<WorkerId>& sourceNodesRight = {WorkerId(4), WorkerId(5)};

    auto topology = setupTopology(3, 2, 1);
    auto sourceCatalog = setupJoinSourceCatalogChain(sourceNodesLeft, sourceNodesRight);
    auto statisticProbeHandler = Statistic::StatisticProbeHandler::create(Statistic::StatisticRegistry::create(),
                                                                          Statistic::DefaultStatisticProbeGenerator::create(),
                                                                          Statistic::DefaultStatisticCache::create(),
                                                                          Topology::create());

    auto optimizerConfig = Configurations::OptimizerConfiguration();
    optimizerConfig.joinOptimizationMode = DistributedJoinOptimizationMode::NEMO;

    //run the placement
    Query query = Query::from(logSourceNameLeft)
                      .joinWith(Query::from(logSourceNameRight))
                      .where(Attribute("id") == Attribute("id"))
                      .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                      .sink(NullOutputSinkDescriptor::create());

    auto outputTuple = runPlacement(query, topology, sourceCatalog, optimizerConfig, statisticProbeHandler, Optimizer::PlacementStrategy::TopDown);
    auto queryId = std::get<0>(outputTuple);
    auto executionPlan = std::get<1>(outputTuple);
    auto executionNodes = executionPlan->getLockedExecutionNodesHostingSharedQueryId(queryId);
    EXPECT_EQ(executionNodes.size(), 5);// topology contains 1 coordinator, 3 workers, 6 sources

    for (const auto& executionNode : executionNodes) {
        std::vector<DecomposedQueryPlanPtr> querySubPlans = executionNode->operator*()->getAllDecomposedQueryPlans(queryId);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(1));
}
