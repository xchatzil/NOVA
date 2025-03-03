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
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/LambdaSourceType.hpp>
#include <GRPC/CoordinatorRPCClient.hpp>
#include <Mobility/WorkerMobilityHandler.hpp>
#include <Network/NetworkSink.hpp>
#include <Network/PartitionManager.hpp>
#include <Operators/LogicalOperators/Network/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <RequestProcessor/StorageHandles/SerialStorageHandler.hpp>
#include <RequestProcessor/StorageHandles/StorageDataStructures.hpp>
#include <Runtime/Execution/ExecutableQueryPlan.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Services/RequestHandlerService.hpp>
#include <Util/TestUtils.hpp>
#include <Util/TopologyLinkInformation.hpp>
#include <atomic>
#include <gtest/gtest.h>

namespace NES {

class QueryRedeploymentIntegrationTest : public Testing::BaseIntegrationTest, public testing::WithParamInterface<uint32_t> {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("QueryRedeploymentIntegrationTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Set up QueryRedeploymentIntegrationTest test class");
    }

    /**
     * @brief wait until the topology contains the expected number of nodes so we can rely on these nodes being present for
     * the rest of the test
     * @param timeoutSeconds time to wait before aborting
     * @param nodes expected number of nodes
     * @param topology  the topology object to query
     * @return true if expected number of nodes was reached. false in case of timeout before number was reached
     */
    static bool waitForNodes(int timeoutSeconds, size_t nodes, TopologyPtr topology) {
        size_t numberOfNodes = 0;
        for (int i = 0; i < timeoutSeconds; ++i) {
            auto topoString = topology->toString();
            numberOfNodes = std::count(topoString.begin(), topoString.end(), '\n');
            numberOfNodes -= 1;
            if (numberOfNodes == nodes) {
                break;
            }
        }
        return numberOfNodes == nodes;
    }
};

constexpr std::chrono::duration<int64_t, std::milli> defaultTimeoutInSec = std::chrono::seconds(TestUtils::defaultTimeout);
constexpr uint32_t DATA_CHANNEL_RETRY_TIMES = 10;
//todo #4441: increase number of event channel retries and add test for successful connection
constexpr uint32_t EVENT_CHANNEL_RETRY_TIMES = 1;
constexpr uint32_t DEFAULT_NUMBER_OF_ORIGINS = 0;
constexpr std::chrono::milliseconds WAIT_TIME = std::chrono::milliseconds(1000);

/**
 * @brief This tests the asynchronous connection establishment, where the sink buffers incoming tuples while waiting for the
 * network channel to become available
 */
//FIXME: 4968
TEST_P(QueryRedeploymentIntegrationTest, DISABLED_testAsyncConnectingSink) {
    const uint64_t numBuffersToProduceBeforeCount = 20;
    const uint64_t numBuffersToProduceAfterCount = 20;
    const uint64_t numBuffersToProduce = numBuffersToProduceBeforeCount + numBuffersToProduceAfterCount;
    uint64_t tuplesPerBuffer = 10;
    uint8_t bytesPerTuple = sizeof(uint64_t);
    NES_INFO(" start coordinator");
    std::string testFile = getTestResourceFolder() / "sequence_with_buffering_out.csv";
    remove(testFile.c_str());

    std::string compareStringBeforeCount;
    std::ostringstream ossBeforeCount;
    ossBeforeCount << "seq$value:INTEGER(64 bits)" << std::endl;
    for (uint64_t i = 0; i < numBuffersToProduceBeforeCount * tuplesPerBuffer; ++i) {
        ossBeforeCount << std::to_string(i) << std::endl;
        compareStringBeforeCount = ossBeforeCount.str();
    }

    std::string compareString;
    std::ostringstream oss;
    oss << "seq$value:INTEGER(64 bits)" << std::endl;
    for (uint64_t i = 0; i < numBuffersToProduce * tuplesPerBuffer; ++i) {
        oss << std::to_string(i) << std::endl;
        compareString = oss.str();
    }

    NES_INFO("rest port = {}", *restPort);

    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort.setValue(*rpcCoordinatorPort);
    coordinatorConfig->restPort.setValue(*restPort);
    coordinatorConfig->worker.connectSourceEventChannelsAsync.setValue(true);

    NES_INFO("start coordinator")
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0UL);
    NES_INFO("coordinator started successfully");

    TopologyPtr topology = crd->getTopology();
    ASSERT_TRUE(waitForNodes(5, 1, topology));

    crd->getSourceCatalog()->addLogicalSource("seq", Schema::create()->addField(createField("value", BasicType::UINT64)));

    NES_INFO("start worker 1");
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    wrkConf1->coordinatorPort.setValue(*rpcCoordinatorPort);
    wrkConf1->numWorkerThreads.setValue(GetParam());
    wrkConf1->connectSinksAsync.setValue(true);
    wrkConf1->connectSourceEventChannelsAsync.setValue(true);
    wrkConf1->bufferSizeInBytes.setValue(tuplesPerBuffer * bytesPerTuple);

    std::atomic<uint64_t> bufferCount = 0;
    std::atomic<bool> countReached = false;
    auto lambdaSourceFunction = [&countReached, &bufferCount](NES::Runtime::TupleBuffer& buffer,
                                                              uint64_t numberOfTuplesToProduce) {
        struct Record {
            uint64_t value;
        };
        auto currentCount = ++bufferCount;
        if (currentCount > numBuffersToProduceBeforeCount) {
            //after sending the specified amount of tuples, wait until some tuples actually arrived
            while (!countReached)
                ;
        }
        auto valCount = (currentCount - 1) * (numberOfTuplesToProduce);
        auto* records = buffer.getBuffer<Record>();
        for (auto u = 0u; u < numberOfTuplesToProduce; ++u) {
            records[u].value = valCount + u;
        }
    };
    auto gatheringValue = 10;
    auto lambdaSourceType = LambdaSourceType::create("seq",
                                                     "test_stream",
                                                     std::move(lambdaSourceFunction),
                                                     numBuffersToProduce,
                                                     gatheringValue,
                                                     GatheringMode::INTERVAL_MODE);
    wrkConf1->physicalSourceTypes.add(lambdaSourceType);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    ASSERT_TRUE(waitForNodes(5, 2, topology));

    QueryId queryId = crd->getRequestHandlerService()->validateAndQueueAddQueryRequest(
        R"(Query::from("seq").sink(FileSinkDescriptor::create(")" + testFile + R"(", "CSV_FORMAT", "APPEND"));)",
        Optimizer::PlacementStrategy::BottomUp);

    NES_INFO("Query ID: {}", queryId);
    ASSERT_NE(queryId, INVALID_QUERY_ID);

    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(compareStringBeforeCount, testFile));
    countReached = true;
    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(compareString, testFile));

    int response = remove(testFile.c_str());
    ASSERT_TRUE(response == 0);

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

/**
 * @brief This tests multiple iterations of inserting VersionDrain events to trigger the reconfiguration of a network sink to point to a new source.
 */
//FIXME: 4968
TEST_P(QueryRedeploymentIntegrationTest, DISABLED_testMultiplePlannedReconnects) {
    const uint64_t numberOfReconnectsToPerform = 3;
    const uint64_t numBuffersToProduceBeforeReconnect = 10;
    const uint64_t numBuffersToProduceWhileBuffering = 10;
    const uint64_t numBuffersToProduceAfterReconnect = 10;
    const uint64_t buffersToProducePerReconnectCycle =
        (numBuffersToProduceBeforeReconnect + numBuffersToProduceAfterReconnect + numBuffersToProduceWhileBuffering);
    const uint64_t totalBuffersToProduce = numberOfReconnectsToPerform * buffersToProducePerReconnectCycle;
    const uint64_t gatheringValue = 10;
    const std::chrono::seconds waitTime(10);
    uint64_t tuplesPerBuffer = 10;
    uint8_t bytesPerTuple = sizeof(uint64_t);
    NES_INFO(" start coordinator");
    std::string testFile = getTestResourceFolder() / "sequence_with_buffering_out.csv";
    remove(testFile.c_str());

    std::atomic<uint64_t> bufferCount = 0;
    std::atomic<uint64_t> actualReconnects = 0;
    std::atomic<bool> waitForReconfig = false;
    std::atomic<bool> waitForReconnect = false;
    std::atomic<bool> waitForFinalCount = false;
    auto lambdaSourceFunction = [&bufferCount, &waitForReconfig, &waitForReconnect, &waitForFinalCount, &actualReconnects](
                                    NES::Runtime::TupleBuffer& buffer,
                                    uint64_t numberOfTuplesToProduce) {
        struct Record {
            uint64_t value;
        };
        auto currentCount = ++bufferCount;
        if (currentCount > numBuffersToProduceBeforeReconnect + (actualReconnects * buffersToProducePerReconnectCycle)) {
            //after sending the specified amount of tuples, wait until the reconfiguration has been triggered, subsequent tuples will be buffered
            while (!waitForReconfig)
                ;
        }
        if (currentCount > numBuffersToProduceBeforeReconnect + numBuffersToProduceWhileBuffering
                + (actualReconnects * buffersToProducePerReconnectCycle)) {
            //after writing some tuples into the buffer, give signal to start the new operators to finish the reconnect, tuples will be unbuffered to new destination
            waitForReconnect = true;
        }
        if (currentCount > numBuffersToProduceBeforeReconnect + numBuffersToProduceAfterReconnect
                + numBuffersToProduceWhileBuffering + (actualReconnects * buffersToProducePerReconnectCycle)) {
            while (!waitForFinalCount)
                ;
        }
        auto valCount = (currentCount - 1) * (numberOfTuplesToProduce);
        auto* records = buffer.getBuffer<Record>();
        for (auto u = 0u; u < numberOfTuplesToProduce; ++u) {
            records[u].value = valCount + u;
        }
    };
    auto lambdaSourceType = LambdaSourceType::create("seq",
                                                     "test_stream",
                                                     std::move(lambdaSourceFunction),
                                                     totalBuffersToProduce,
                                                     gatheringValue,
                                                     GatheringMode::INTERVAL_MODE);

    NES_INFO("rest port = {}", *restPort);

    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort.setValue(*rpcCoordinatorPort);
    coordinatorConfig->restPort.setValue(*restPort);
    auto crdWorkerDataPort = getAvailablePort();
    coordinatorConfig->worker.dataPort = *crdWorkerDataPort;
    coordinatorConfig->worker.connectSourceEventChannelsAsync.setValue(true);

    NES_INFO("start coordinator")
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0UL);
    NES_INFO("coordinator started successfully");

    TopologyPtr topology = crd->getTopology();
    ASSERT_TRUE(waitForNodes(5, 1, topology));

    auto schema = Schema::create()->addField(createField("value", BasicType::UINT64));
    crd->getSourceCatalog()->addLogicalSource("seq", schema);

    NES_INFO("start worker 1");
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    wrkConf1->coordinatorPort.setValue(*rpcCoordinatorPort);
    wrkConf1->numWorkerThreads.setValue(GetParam());
    wrkConf1->connectSinksAsync.setValue(true);
    wrkConf1->connectSourceEventChannelsAsync.setValue(true);
    wrkConf1->bufferSizeInBytes.setValue(tuplesPerBuffer * bytesPerTuple);

    wrkConf1->physicalSourceTypes.add(lambdaSourceType);

    auto wrk1DataPort = getAvailablePort();
    wrkConf1->dataPort = *wrk1DataPort;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    ASSERT_TRUE(waitForNodes(5, 2, topology));

    NES_INFO("start worker 2");
    WorkerConfigurationPtr wrkConf2 = WorkerConfiguration::create();
    wrkConf2->coordinatorPort.setValue(*rpcCoordinatorPort);
    auto wrk2DataPort = getAvailablePort();
    wrkConf2->dataPort = *wrk2DataPort;
    wrkConf2->numWorkerThreads.setValue(GetParam());
    wrkConf2->connectSinksAsync.setValue(true);
    wrkConf2->connectSourceEventChannelsAsync.setValue(true);
    wrkConf2->bufferSizeInBytes.setValue(tuplesPerBuffer * bytesPerTuple);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(wrkConf2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart2);
    ASSERT_TRUE(waitForNodes(5, 3, topology));

    wrk1->replaceParent(crd->getNesWorker()->getWorkerId(), wrk2->getWorkerId());

    //start query
    QueryId queryId = crd->getRequestHandlerService()->validateAndQueueAddQueryRequest(
        R"(Query::from("seq").sink(FileSinkDescriptor::create(")" + testFile + R"(", "CSV_FORMAT", "APPEND"));)",
        Optimizer::PlacementStrategy::BottomUp);
    auto networkSinkWrk3Id = 31;
    auto networkSrcWrk3Id = 32;

    auto sinkLocationWrk1 = NES::Network::NodeLocation(wrk1->getWorkerId(), "localhost", *wrk1DataPort);
    auto networkSourceCrdLocation =
        NES::Network::NodeLocation(crd->getNesWorker()->getWorkerId(), "localhost", *crdWorkerDataPort);

    std::vector<NesWorkerPtr> reconnectParents;

    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, crd->getQueryCatalog()));
    SharedQueryId sharedQueryId = crd->getGlobalQueryPlan()->getSharedQueryId(queryId);
    ASSERT_NE(sharedQueryId, INVALID_SHARED_QUERY_ID);
    //reconfiguration
    auto subPlanIdWrk3 = 30;
    ASSERT_EQ(wrk2->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).size(), 1);
    auto oldSubplanId = wrk2->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front();
    auto wrk2Source = wrk2->getNodeEngine()->getExecutableQueryPlan(oldSubplanId)->getSources().front();
    Network::NesPartition currentWrk1TargetPartition(sharedQueryId,
                                                     wrk2Source->getOperatorId(),
                                                     PartitionId(0),
                                                     SubpartitionId(0));

    ASSERT_EQ(crd->getNesWorker()->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).size(), 1);
    auto coordinatorSubplanId = crd->getNesWorker()->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front();
    auto coordinatorSource =
        crd->getNesWorker()->getNodeEngine()->getExecutableQueryPlan(coordinatorSubplanId)->getSources().front();
    Network::NesPartition networkSourceCrdPartition(sharedQueryId,
                                                    coordinatorSource->getOperatorId(),
                                                    PartitionId(0),
                                                    SubpartitionId(0));

    auto oldWorker = wrk2;
    while (actualReconnects < numberOfReconnectsToPerform) {
        subPlanIdWrk3++;

        //wait for data to be written
        std::string compareStringBefore;
        std::ostringstream oss;
        oss << "seq$value:INTEGER(64 bits)" << std::endl;
        for (uint64_t i = 0; i < (numBuffersToProduceBeforeReconnect
                                  + (actualReconnects
                                     * (numBuffersToProduceBeforeReconnect + numBuffersToProduceAfterReconnect
                                        + numBuffersToProduceWhileBuffering)))
                 * tuplesPerBuffer;
             ++i) {
            oss << std::to_string(i) << std::endl;
        }
        compareStringBefore = oss.str();
        ASSERT_TRUE(TestUtils::checkOutputOrTimeout(compareStringBefore, testFile));
        waitForFinalCount = false;
        NES_INFO("start reconnect parent {}", actualReconnects);
        WorkerConfigurationPtr wrkConf3 = WorkerConfiguration::create();
        wrkConf3->coordinatorPort.setValue(*rpcCoordinatorPort);
        auto wrk3DataPort = getAvailablePort();
        wrkConf3->dataPort = *wrk3DataPort;
        wrkConf3->numWorkerThreads.setValue(GetParam());
        wrkConf3->connectSinksAsync.setValue(true);
        wrkConf3->connectSourceEventChannelsAsync.setValue(true);
        wrkConf3->bufferSizeInBytes.setValue(tuplesPerBuffer * bytesPerTuple);
        NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(wrkConf3));
        bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
        ASSERT_TRUE(retStart3);
        ASSERT_TRUE(waitForNodes(5, 4 + actualReconnects, topology));
        reconnectParents.push_back(wrk3);

        std::string compareStringAfter;
        std::ostringstream ossAfter;
        ossAfter << "seq$value:INTEGER(64 bits)" << std::endl;
        for (uint64_t i = 0;
             i < (numBuffersToProduceBeforeReconnect + numBuffersToProduceAfterReconnect + numBuffersToProduceWhileBuffering)
                 * tuplesPerBuffer * (actualReconnects + 1);
             ++i) {
            ossAfter << std::to_string(i) << std::endl;
        }
        compareStringAfter = ossAfter.str();

        int noOfMigratingPlans = 0;
        int noOfCompletedMigrations = 0;
        int noOfRunningPlans = 0;

        RequestProcessor::StorageDataStructures storageDataStructures(coordinatorConfig,
                                                                      topology,
                                                                      crd->getGlobalExecutionPlan(),
                                                                      crd->getGlobalQueryPlan(),
                                                                      crd->getQueryCatalog(),
                                                                      crd->getSourceCatalog(),
                                                                      crd->getUDFCatalog(),
                                                                      crd->getStatisticProbeHandler());
        auto storageHandler = RequestProcessor::SerialStorageHandler::create(storageDataStructures);
        std::vector<TopologyLinkInformation> removedLinks = {{wrk1->getWorkerId(), oldWorker->getWorkerId()}};
        std::vector<TopologyLinkInformation> addedLinks = {{wrk1->getWorkerId(), wrk3->getWorkerId()}};
        std::string coordinatorAddress =
            coordinatorConfig->coordinatorHost.getValue() + ":" + std::to_string(*rpcCoordinatorPort);
        auto coordinatorRPCClient = CoordinatorRPCClient::create(coordinatorAddress);
        coordinatorRPCClient->relocateTopologyNode(removedLinks, addedLinks);

        //notify lambda source that reconfig happened and make it release more tuples into the buffer
        waitForFinalCount = false;
        waitForReconfig = true;
        //wait for tuples in order to make sure that the buffer is actually tested
        while (!waitForReconnect) {
        }

        //verify that the old partition gets unregistered
        auto timeoutInSec = std::chrono::seconds(TestUtils::defaultTimeout);
        auto start_timestamp = std::chrono::system_clock::now();
        while (wrk1->getNodeEngine()->getPartitionManager()->getProducerRegistrationStatus(currentWrk1TargetPartition)
               == Network::PartitionRegistrationStatus::Registered) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            NES_DEBUG("Partition {} has not yet been unregistered", currentWrk1TargetPartition);
            if (std::chrono::system_clock::now() > start_timestamp + timeoutInSec) {
                FAIL();
            }
        }
        ASSERT_NE(wrk1->getNodeEngine()->getPartitionManager()->getProducerRegistrationStatus(currentWrk1TargetPartition),
                  Network::PartitionRegistrationStatus::Registered);
        auto lockedExecutionNode = crd->getGlobalExecutionPlan()->getLockedExecutionNode(wrk3->getWorkerId());
        auto networkSourceWrk3Partition =
            std::dynamic_pointer_cast<Network::NetworkSourceDescriptor>(lockedExecutionNode->operator*()
                                                                            ->getAllDecomposedQueryPlans(sharedQueryId)
                                                                            .front()
                                                                            ->getSourceOperators()
                                                                            .front()
                                                                            ->getSourceDescriptor())
                ->getNesPartition();
        ASSERT_EQ(wrk1->getNodeEngine()->getPartitionManager()->getProducerRegistrationStatus(networkSourceWrk3Partition),
                  Network::PartitionRegistrationStatus::Registered);
        EXPECT_NE(oldWorker->getNodeEngine()->getPartitionManager()->getConsumerRegistrationStatus(currentWrk1TargetPartition),
                  Network::PartitionRegistrationStatus::Registered);
        currentWrk1TargetPartition = networkSourceWrk3Partition;

        //verify that query has been undeployed from old parent
        while (oldWorker->getNodeEngine()->getQueryStatus(sharedQueryId)
               != Runtime::Execution::ExecutableQueryPlanStatus::Finished) {
            NES_DEBUG("Query has not yet stopped on worker {}", oldWorker->getWorkerId());
            std::this_thread::sleep_for(std::chrono::milliseconds(2000));
            if (std::chrono::system_clock::now() > start_timestamp + timeoutInSec) {
                FAIL();
            }
        }
        ASSERT_EQ(oldWorker->getNodeEngine()->getQueryStatus(sharedQueryId),
                  Runtime::Execution::ExecutableQueryPlanStatus::Finished);

        oldWorker = wrk3;
        EXPECT_EQ(oldWorker->getNodeEngine()->getPartitionManager()->getConsumerRegistrationStatus(currentWrk1TargetPartition),
                  Network::PartitionRegistrationStatus::Registered);
        oldSubplanId = oldWorker->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front();

        //check that query has left migrating state and is running again
        ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, crd->getQueryCatalog()));
        auto entries = crd->getQueryCatalog()->getQueryEntriesWithStatus("MIGRATING");
        ASSERT_TRUE(entries.empty());

        //check that all tuples arrived
        ASSERT_TRUE(TestUtils::checkOutputOrTimeout(compareStringAfter, testFile));

        waitForReconfig = false;
        waitForReconnect = false;
        actualReconnects++;
        waitForFinalCount = true;
    }

    //send the last tuples, after which the lambda source shuts down
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, wrk1));

    auto lastReconnectParent = reconnectParents.back();
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, lastReconnectParent));
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, wrk2));
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, crd->getNesWorker()));
    cout << "stopping worker" << endl;
    bool retStopLastParent = lastReconnectParent->stop(false);
    ASSERT_TRUE(retStopLastParent);
    reconnectParents.pop_back();
    for (auto parent : reconnectParents) {
        NES_DEBUG("stopping parent node")
        ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, parent));
        cout << "stopping worker" << endl;
        bool stopParent = parent->stop(false);
        ASSERT_TRUE(stopParent);
    }

    int response = remove(testFile.c_str());
    ASSERT_TRUE(response == 0);

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    cout << "stopping worker" << endl;
    bool retStopWrk2 = wrk2->stop(false);
    ASSERT_TRUE(retStopWrk2);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

INSTANTIATE_TEST_CASE_P(QueryRedeploymentIntegrationTestParam, QueryRedeploymentIntegrationTest, ::testing::Values(1, 4));
}// namespace NES
