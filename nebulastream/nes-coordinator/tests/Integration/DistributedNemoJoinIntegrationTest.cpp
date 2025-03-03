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
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/TCPSourceType.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <gtest/gtest.h>

namespace NES {
using namespace Configurations;
using namespace Runtime;

static const std::string sourceNameLeft = "log_left";
static const std::string sourceNameRight = "log_right";

/**
 * @brief Test of distributed NEMO join
 */
class DistributedNemoJoinIntegrationTest : public Testing::BaseIntegrationTest {
  public:
    BufferManagerPtr bufferManager;
    QueryCompilation::StreamJoinStrategy joinStrategy = QueryCompilation::StreamJoinStrategy::NESTED_LOOP_JOIN;
    QueryCompilation::WindowingStrategy windowingStrategy = QueryCompilation::WindowingStrategy::SLICING;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("DistributedNemoJoinIntegrationTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup DistributedNemoJoinIntegrationTest test class.");
    }

    void SetUp() override {
        Testing::BaseIntegrationTest::SetUp();
        bufferManager = std::make_shared<BufferManager>();
    }

    static CSVSourceTypePtr createCSVSourceType(const std::string& logicalSourceNAme,
                                                const std::string& physicalSourceName,
                                                const std::string& inputPath) {
        CSVSourceTypePtr csvSourceType = CSVSourceType::create(logicalSourceNAme, physicalSourceName);
        csvSourceType->setFilePath(inputPath);
        csvSourceType->setNumberOfTuplesToProducePerBuffer(50);
        csvSourceType->setNumberOfBuffersToProduce(1);
        csvSourceType->setSkipHeader(false);
        csvSourceType->setGatheringInterval(1000);
        return csvSourceType;
    }

    /**
     * Creates a TestHarness with a given query and topology parameters. The leafs of the topology are sources.
     * @param query the query
     * @param crdFunctor functor to pass coordinator params
     * @param layers number of layers of the topology tree
     * @param nodesPerNode number of children per node
     * @param leafNodesPerNode number of leaf nodes for the parents of last layer
     * @return the TestHarness
     */
    TestHarness createTestHarness(const Query& query,
                                  std::function<void(CoordinatorConfigurationPtr)> crdFunctor,
                                  uint64_t layers,
                                  uint64_t nodesPerNode,
                                  uint64_t leafNodesPerNode) {
        auto inputSchema = Schema::create()
                               ->addField("key", DataTypeFactory::createUInt32())
                               ->addField("value", DataTypeFactory::createUInt32())
                               ->addField("timestamp", DataTypeFactory::createUInt64());

        TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                      .setJoinStrategy(joinStrategy)
                                      .setWindowingStrategy(windowingStrategy)
                                      .addLogicalSource(sourceNameLeft, inputSchema)
                                      .addLogicalSource(sourceNameRight, inputSchema);

        std::vector<uint64_t> nodes;
        std::vector<uint64_t> parents;
        uint64_t nodeId = 1;
        uint64_t leafNodes = 0;
        nodes.emplace_back(1);
        parents.emplace_back(1);

        std::vector<nlohmann::json> distributionList;
        uint64_t value = 0;

        // create the additional layers
        for (uint64_t i = 2; i <= layers; i++) {
            std::vector<uint64_t> newParents;
            std::string logSource;
            // iterate and set the parents of the current layer
            for (auto parent : parents) {
                uint64_t nodeCnt = nodesPerNode;
                if (i == layers) {
                    nodeCnt = leafNodesPerNode;
                }
                // set the children of the current parents
                for (uint64_t j = 0; j < nodeCnt; j++) {
                    nodeId++;
                    // if children are leaf nodes create the sources, else just create intermediate workers
                    if (i == layers) {
                        leafNodes++;
                        // even leaf nodes are part of the LHS join, uneven of the RHS join
                        if (leafNodes % 2 == 0) {
                            logSource = sourceNameLeft;
                        } else {
                            logSource = sourceNameRight;
                            value += 10;
                        }

                        auto phySource = logSource + std::to_string(leafNodes);
                        auto csvSource =
                            createCSVSourceType(logSource, phySource, std::string(TEST_DATA_DIRECTORY) + "window.csv");
                        testHarness.attachWorkerWithCSVSourceToWorkerWithId(csvSource, WorkerId(parent));

                        nlohmann::json request;
                        request["topologyNodeId"] = nodeId;
                        request["logicalSource"] = logSource;
                        request["physicalSource"] = phySource;
                        request["fieldName"] = "key";
                        request["value"] = std::to_string(value);
                        distributionList.emplace_back(request);
                        NES_DEBUG("DistributedNemoJoinIntegrationTest: Adding statistics for {}", request.dump());
                        NES_DEBUG("DistributedNemoJoinIntegrationTest: Adding CSV source:{} to {} for node:{}",
                                  logSource,
                                  parent,
                                  nodeId);
                    } else {
                        testHarness.attachWorkerToWorkerWithId(WorkerId(parent));
                        NES_DEBUG("DistributedNemoJoinIntegrationTest: Adding worker {} to worker with ID:{}", nodeId, parent);
                    }
                    nodes.emplace_back(nodeId);
                    newParents.emplace_back(nodeId);
                }
            }
            parents = newParents;
        }
        testHarness.validate().setupTopology(crdFunctor, distributionList);
        return testHarness;
    }
};

TEST_F(DistributedNemoJoinIntegrationTest, testThreeLevelsTopologyTopDown) {
    uint64_t layers = 3;
    uint64_t nodesPerNode = 4;
    uint64_t leafNodesPerNode = 2;
    std::function<void(CoordinatorConfigurationPtr)> crdFunctor = [](const CoordinatorConfigurationPtr& config) {
        config->optimizer.joinOptimizationMode.setValue(Optimizer::DistributedJoinOptimizationMode::NEMO);
    };
    Query query = Query::from(sourceNameLeft)
                      .joinWith(Query::from(sourceNameRight))
                      .where(Attribute("value") == Attribute("value"))
                      .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)));

    // create flat topology with 1 coordinator and 4 sources
    auto testHarness = createTestHarness(query, crdFunctor, layers, nodesPerNode, leafNodesPerNode);
    // 26 join keys
    uint64_t expectedTuples = 26 * nodesPerNode * leafNodesPerNode;

    auto actualOutput = testHarness.runQuery(expectedTuples).getOutput();
    auto outputString = actualOutput[0].toString(testHarness.getOutputSchema(), true);
    NES_DEBUG("DistributedNemoJoinIntegrationTest: Output\n{}", outputString);
    EXPECT_EQ(TestUtils::countTuples(actualOutput), expectedTuples);

    TopologyPtr topology = testHarness.getTopology();
    QueryPlanPtr queryPlan = testHarness.getQueryPlan();
    NES_DEBUG("DistributedNemoJoinIntegrationTest: Executed with topology \n{}", topology->toString());
    NES_INFO("DistributedNemoJoinIntegrationTest: Executed with plan \n{}", queryPlan->toString());
    EXPECT_EQ(nodesPerNode, countOccurrences("Join", queryPlan->toString()));
}

TEST_F(DistributedNemoJoinIntegrationTest, DISABLED_TCPSourceReadCSVData) {
    uint64_t socketBufferSize = 320;
    uint64_t expectedProcessedBuffers = 20000 * 2;
    uint64_t expectedTuples = expectedProcessedBuffers * 100 - 1;
    //uint64_t expectedProcessedBuffers = 3 * expectedTuples + 2;
    uint64_t workerBufferSize = 4096;

    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->worker.numberOfBuffersInGlobalBufferManager = 4096000;
    coordinatorConfig->worker.numberOfBuffersPerWorker = 128000;

    NES_INFO("TCPSourceIntegrationTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("TCPSourceIntegrationTest: Coordinator started successfully");

    auto tcpSchema = Schema::create()
                         ->addField("id", BasicType::UINT64)
                         ->addField("sequence", BasicType::UINT64)
                         ->addField("eventTime", BasicType::UINT64)
                         ->addField("ingestionTime", BasicType::UINT64);

    crd->getSourceCatalog()->addLogicalSource("tcpStream_left", tcpSchema);
    crd->getSourceCatalog()->addLogicalSource("tcpStream_right", tcpSchema);
    NES_DEBUG("TCPSourceIntegrationTest: Added tcpLogicalSource to coordinator.")

    NES_DEBUG("TCPSourceIntegrationTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;

    TCPSourceTypePtr sourceConfig1 = TCPSourceType::create("tcpStream_left", "tcpStream_1");
    sourceConfig1->setSocketPort(3000);
    sourceConfig1->setSocketHost("127.0.0.1");
    sourceConfig1->setSocketDomainViaString("AF_INET");
    sourceConfig1->setSocketTypeViaString("SOCK_STREAM");
    sourceConfig1->setFlushIntervalMS(5000);
    sourceConfig1->setInputFormat(Configurations::InputFormat::NES_BINARY);
    sourceConfig1->setPersistentTcpSource(true);
    sourceConfig1->setDecideMessageSize(Configurations::TCPDecideMessageSize::USER_SPECIFIED_BUFFER_SIZE);
    sourceConfig1->setSocketBufferSize(socketBufferSize);

    workerConfig1->physicalSourceTypes.add(sourceConfig1);
    workerConfig1->bufferSizeInBytes = workerBufferSize;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    NES_INFO("TCPSourceIntegrationTest: Worker1 started successfully");

    NES_DEBUG("TCPSourceIntegrationTest: Start worker 1");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = *rpcCoordinatorPort;

    TCPSourceTypePtr sourceConfig2 = TCPSourceType::create("tcpStream_right", "tcpStream_2");
    sourceConfig2->setSocketPort(3000);
    sourceConfig2->setSocketHost("127.0.0.1");
    sourceConfig2->setSocketDomainViaString("AF_INET");
    sourceConfig2->setSocketTypeViaString("SOCK_STREAM");
    sourceConfig2->setFlushIntervalMS(5000);
    sourceConfig2->setInputFormat(Configurations::InputFormat::NES_BINARY);
    sourceConfig2->setPersistentTcpSource(true);
    sourceConfig2->setDecideMessageSize(Configurations::TCPDecideMessageSize::USER_SPECIFIED_BUFFER_SIZE);
    sourceConfig2->setSocketBufferSize(socketBufferSize);

    workerConfig2->physicalSourceTypes.add(sourceConfig2);
    workerConfig2->bufferSizeInBytes = workerBufferSize;
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart2);
    NES_INFO("TCPSourceIntegrationTest: Worker2 started successfully");

    std::string filePath = getTestResourceFolder() / "tcpSourceTest.csv";
    remove(filePath.c_str());

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();

    //register query 1
    Query query1 = Query::from("tcpStream_left")
                       .joinWith(Query::from("tcpStream_right"))
                       .where(Attribute("sequence") == Attribute("sequence"))
                       .window(TumblingWindow::of(EventTime(Attribute("sequence")), Milliseconds(1)))
                       //.byKey(Attribute("id"))
                       //.apply(Sum(Attribute("eventTime")))
                       .sink(FileSinkDescriptor::create(filePath, "CSV_FORMAT", "APPEND"));
    QueryId queryId1 = requestHandlerService->validateAndQueueAddQueryRequest(query1.getQueryPlan(),
                                                                              Optimizer::PlacementStrategy::TopDown);// Deploy
    EXPECT_NE(queryId1, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId1, queryCatalog));

    // Wait till buffers are processed by the shared query plan
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd,
                                                  queryId1,
                                                  globalQueryPlan,
                                                  expectedProcessedBuffers,
                                                  false,
                                                  std::chrono::seconds(10000)));

    NES_INFO("QueryDeploymentTest: Remove query");
    requestHandlerService->validateAndQueueStopQueryRequest(queryId1);// hard stop
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId1, queryCatalog));

    std::ifstream ifs(filePath.c_str());
    ASSERT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
    NES_ERROR("TCPSourceIntegrationTest: content={}", content);
    uint64_t lineCnt = Util::countLines(content);
    ASSERT_EQ(expectedTuples + 1, lineCnt);

    bool retStopWrk1 = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(false);
    ASSERT_TRUE(retStopWrk2);

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

}// namespace NES
