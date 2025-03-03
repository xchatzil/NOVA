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
#include <gtest/gtest.h>

#include <API/TestSchemas.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Services/RequestHandlerService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <iostream>

using namespace std;

namespace NES {

using namespace Configurations;
class MultipleWindowsTest : public Testing::BaseIntegrationTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MultipleWindowsTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup MultipleWindowsTest test class.");
    }
};

TEST_F(MultipleWindowsTest, testTwoCentralTumblingWindows) {
    auto coordinatorConfig = CoordinatorConfiguration::createDefault();
    auto workerConfig = WorkerConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    workerConfig->coordinatorPort = *rpcCoordinatorPort;

    NES_INFO("MultipleWindowsTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");
    //register logical source
    const auto testSchema = TestSchemas::getSchemaTemplate("id_val_time_u64");
    crd->getSourceCatalog()->addLogicalSource("window", testSchema);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    workerConfig1->numberOfSlots = (12);
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create("window", "test_stream");
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType1->setNumberOfBuffersToProduce(3);
    workerConfig1->physicalSourceTypes.add(csvSourceType1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath = getTestResourceFolder() / "testDeployOneWorkerCentralWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    NES_INFO("MultipleWindowsTest: Submit query");
    string query = R"(Query::from("window")
        .filter(Attribute("id") < 15)
        .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1))).byKey(Attribute("id")).apply(Sum(Attribute("value")))
        .filter(Attribute("id") < 10)
        .window(TumblingWindow::of(EventTime(Attribute("start")), Seconds(2))).byKey(Attribute("id")).apply(Sum(Attribute("value")))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId = requestHandlerService->validateAndQueueAddQueryRequest(query, Optimizer::PlacementStrategy::BottomUp);
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    string expectedContent =
        "window$start:INTEGER(64 bits),window$end:INTEGER(64 bits),window$id:INTEGER(64 bits),window$value:INTEGER(64 bits)\n"
        "0,2000,1,1\n"
        "0,2000,4,1\n"
        "2000,4000,1,8\n";
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("MultipleWindowsTest: Remove query");

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("MultipleWindowsTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("MultipleWindowsTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("MultipleWindowsTest: Test finished");
}

TEST_F(MultipleWindowsTest, testTwoDistributedTumblingWindows) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->worker.numberOfSlots = 12;
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");
    //register logical source
    auto testSchema = TestSchemas::getSchemaTemplate("id_val_time_u64");
    crd->getSourceCatalog()->addLogicalSource("window", testSchema);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    workerConfig1->numberOfSlots = (12);
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create("window", "test_stream");
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType1->setNumberOfBuffersToProduce(3);
    workerConfig1->physicalSourceTypes.add(csvSourceType1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = port;
    workerConfig2->numberOfSlots = (12);
    CSVSourceTypePtr csvSourceType2 = CSVSourceType::create("window", "test_stream");
    csvSourceType2->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType2->setNumberOfBuffersToProduce(3);
    workerConfig2->physicalSourceTypes.add(csvSourceType2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("WindowDeploymentTest: Worker 2 started successfully");

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath = getTestResourceFolder() / "testDeployOneWorkerCentralWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    NES_INFO("MultipleWindowsTest: Submit query");
    string query = R"(Query::from("window")
        .filter(Attribute("id") < 15)
        .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1))).byKey(Attribute("id")).apply(Sum(Attribute("value")))
        .filter(Attribute("id") < 10)
        .window(TumblingWindow::of(EventTime(Attribute("start")), Seconds(2))).byKey(Attribute("id")).apply(Sum(Attribute("value")))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId = requestHandlerService->validateAndQueueAddQueryRequest(query, Optimizer::PlacementStrategy::BottomUp);
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    string expectedContent =
        "window$start:INTEGER(64 bits),window$end:INTEGER(64 bits),window$id:INTEGER(64 bits),window$value:INTEGER(64 bits)\n"
        "0,2000,1,2\n"
        "0,2000,4,2\n"
        "2000,4000,1,16\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("MultipleWindowsTest: Remove query");

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("MultipleWindowsTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("MultipleWindowsTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("MultipleWindowsTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("MultipleWindowsTest: Test finished");
}

/**
 * @brief test central sliding window and event time
 */
TEST_F(MultipleWindowsTest, testTwoCentralSlidingWindowEventTime) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->worker.numberOfSlots = 12;
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");
    //register logical source
    const auto testSchema = TestSchemas::getSchemaTemplate("id_val_time_u64");
    crd->getSourceCatalog()->addLogicalSource("window", testSchema);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    workerConfig1->coordinatorPort = port;
    workerConfig1->numberOfSlots = (12);
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create("window", "test_stream");
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(0);
    csvSourceType1->setNumberOfBuffersToProduce(1);
    workerConfig1->physicalSourceTypes.add(csvSourceType1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath = getTestResourceFolder() / "outputLog.out";
    remove(outputFilePath.c_str());

    NES_INFO("MultipleWindowsTest: Submit query");
    string query =
        "Query::from(\"window\")"
        ".window(SlidingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(5),Seconds(5)))"
        ".byKey(Attribute(\"id\")).apply(Sum(Attribute(\"value\")))"
        ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"start\"), Milliseconds(0), Milliseconds()))"
        ".window(SlidingWindow::of(EventTime(Attribute(\"start\")),Seconds(10),Seconds(5))) "
        ".byKey(Attribute(\"id\")).apply(Sum(Attribute(\"value\")))"
        ".sink(FileSinkDescriptor::create(\""
        + outputFilePath + R"(","CSV_FORMAT", "APPEND"));)";

    QueryId queryId = requestHandlerService->validateAndQueueAddQueryRequest(query, Optimizer::PlacementStrategy::BottomUp);
    NES_DEBUG("wait start");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    string expectedContent =
        "window$start:INTEGER(64 bits),window$end:INTEGER(64 bits),window$id:INTEGER(64 bits),window$value:INTEGER(64 bits)\n"
        "0,10000,1,51\n"
        "0,10000,4,1\n"
        "0,10000,11,5\n"
        "0,10000,12,1\n"
        "0,10000,16,2\n"
        "5000,15000,1,95\n"
        "10000,20000,1,145\n"
        "15000,25000,1,126\n"
        "20000,30000,1,41\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("MultipleWindowsTest: Remove query");

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("MultipleWindowsTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("MultipleWindowsTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("MultipleWindowsTest: Test finished");
}

/**
 * @brief test central sliding window and event time
 */
TEST_F(MultipleWindowsTest, testTwoDistributedSlidingWindowEventTime) {
    auto coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");
    //register logical source
    auto testSchema = TestSchemas::getSchemaTemplate("id_val_time_u64");
    crd->getSourceCatalog()->addLogicalSource("window", testSchema);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    workerConfig1->coordinatorPort = port;
    workerConfig1->numberOfSlots = (12);
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create("window", "test_stream");
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(0);
    csvSourceType1->setNumberOfBuffersToProduce(1);
    workerConfig1->physicalSourceTypes.add(csvSourceType1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = port;
    workerConfig2->numberOfSlots = (12);
    CSVSourceTypePtr csvSourceType2 = CSVSourceType::create("window", "test_stream");
    csvSourceType2->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(0);
    csvSourceType2->setNumberOfBuffersToProduce(1);
    workerConfig2->physicalSourceTypes.add(csvSourceType2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("WindowDeploymentTest: Worker 2 started successfully");

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath = getTestResourceFolder() / "outputLog.out";
    remove(outputFilePath.c_str());

    NES_INFO("MultipleWindowsTest: Submit query");
    string query = "Query::from(\"window\")"
                   ".window(SlidingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(5),Seconds(5))).byKey(Attribute(\"id\")"
                   ").apply(Sum(Attribute(\"value\")))"
                   ".window(SlidingWindow::of(EventTime(Attribute(\"end\")),Seconds(10),Seconds(5))).byKey(Attribute(\"id\")). "
                   "apply(Sum(Attribute(\"value\")))"
                   ".sink(FileSinkDescriptor::create(\""
        + outputFilePath + R"(","CSV_FORMAT", "APPEND"));)";

    QueryId queryId = requestHandlerService->validateAndQueueAddQueryRequest(query, Optimizer::PlacementStrategy::BottomUp);
    NES_DEBUG("wait start");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    string expectedContent =
        "window$start:INTEGER(64 bits),window$end:INTEGER(64 bits),window$id:INTEGER(64 bits),window$value:INTEGER(64 bits)\n"
        "5000,15000,1,102\n"
        "5000,15000,12,2\n"
        "5000,15000,4,2\n"
        "5000,15000,11,10\n"
        "5000,15000,16,4\n"
        "10000,20000,1,190\n"
        "15000,25000,1,290\n"
        "20000,30000,1,252\n"
        "25000,35000,1,82\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("MultipleWindowsTest: Remove query");

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("MultipleWindowsTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("MultipleWindowsTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("MultipleWindowsTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("MultipleWindowsTest: Test finished");
}

TEST_F(MultipleWindowsTest, testTwoCentralTumblingAndSlidingWindows) {
    auto coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");
    //register logical source
    auto testSchema = TestSchemas::getSchemaTemplate("id_val_time_u64");
    crd->getSourceCatalog()->addLogicalSource("window", testSchema);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    workerConfig1->numberOfSlots = (12);
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create("window", "test_stream");
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(10);
    csvSourceType1->setNumberOfBuffersToProduce(3);
    workerConfig1->physicalSourceTypes.add(csvSourceType1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath = getTestResourceFolder() / "testDeployOneWorkerCentralWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    NES_INFO("MultipleWindowsTest: Submit query");
    string query = R"(Query::from("window")
        .filter(Attribute("id") < 15)
        .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(2))).byKey(Attribute("id")).apply(Sum(Attribute("value")))
        .filter(Attribute("id") < 10)
        .assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute("start"), Milliseconds(0), Milliseconds()))
        .window(SlidingWindow::of(EventTime(Attribute("start")),Seconds(1),Milliseconds(500))).byKey(Attribute("id")).apply(Sum(Attribute("value")))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId = requestHandlerService->validateAndQueueAddQueryRequest(query, Optimizer::PlacementStrategy::BottomUp);
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    string expectedContent =
        "window$start:INTEGER(64 bits),window$end:INTEGER(64 bits),window$id:INTEGER(64 bits),window$value:INTEGER(64 bits)\n"
        "0,1000,1,1\n"
        "0,1000,4,1\n"
        "2000,3000,1,11\n"
        "4000,5000,1,9\n"
        "6000,7000,1,13\n"
        "8000,9000,1,17\n"
        "10000,11000,1,21\n"
        "12000,13000,1,25\n"
        "14000,15000,1,29\n"
        "16000,17000,1,33\n"
        "18000,19000,1,37\n"
        "20000,21000,1,41\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("MultipleWindowsTest: Remove query");

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("MultipleWindowsTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("MultipleWindowsTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("MultipleWindowsTest: Test finished");
}

TEST_F(MultipleWindowsTest, testTwoDistributedTumblingAndSlidingWindows) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");
    //register logical source
    auto testSchema = TestSchemas::getSchemaTemplate("id_val_time_u64");
    crd->getSourceCatalog()->addLogicalSource("window", testSchema);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    workerConfig1->coordinatorPort = port;
    workerConfig1->numberOfSlots = (12);
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create("window", "test_stream");
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(5);
    csvSourceType1->setNumberOfBuffersToProduce(3);
    workerConfig1->physicalSourceTypes.add(csvSourceType1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = port;
    workerConfig2->numberOfSlots = (12);
    CSVSourceTypePtr csvSourceType2 = CSVSourceType::create("window", "test_stream");
    csvSourceType2->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(5);
    csvSourceType2->setNumberOfBuffersToProduce(3);
    workerConfig2->physicalSourceTypes.add(csvSourceType2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("WindowDeploymentTest: Worker 2 started successfully");

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath = getTestResourceFolder() / "testDeployOneWorkerCentralWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    NES_INFO("MultipleWindowsTest: Submit query");
    string query = R"(Query::from("window")
        .window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500))).byKey(Attribute("id")).apply(Sum(Attribute("value")))
        .assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute("start"), Milliseconds(0), Milliseconds()))
        .window(TumblingWindow::of(EventTime(Attribute("start")), Seconds(2))).byKey(Attribute("id")).apply(Sum(Attribute("value")))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId = requestHandlerService->validateAndQueueAddQueryRequest(query, Optimizer::PlacementStrategy::BottomUp);
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    string expectedContent =
        "window$start:INTEGER(64 bits),window$end:INTEGER(64 bits),window$id:INTEGER(64 bits),window$value:INTEGER(64 bits)\n"
        "0,2000,1,6\n"
        "0,2000,12,2\n"
        "0,2000,4,2\n"
        "0,2000,11,4\n"
        "0,2000,16,4\n"
        "2000,4000,1,48\n"
        "2000,4000,11,16\n"
        "2000,4000,16,4\n"
        "4000,6000,1,40\n"
        "6000,8000,1,56\n"
        "8000,10000,1,16\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("MultipleWindowsTest: Remove query");

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("MultipleWindowsTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("MultipleWindowsTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("MultipleWindowsTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("MultipleWindowsTest: Test finished");
}

/**
 * @brief Test all three windows in a row
 */
TEST_F(MultipleWindowsTest, testThreeDifferentWindows) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");
    //register logical source
    auto testSchema = TestSchemas::getSchemaTemplate("id_val_time_u64");
    crd->getSourceCatalog()->addLogicalSource("window", testSchema);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    workerConfig1->numberOfSlots = (12);
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create("window", "test_stream");
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(10);
    csvSourceType1->setNumberOfBuffersToProduce(3);
    workerConfig1->physicalSourceTypes.add(csvSourceType1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testOutput.out";
    remove(outputFilePath.c_str());

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();

    NES_DEBUG("MultipleWindowsTest: Submit query");

    NES_DEBUG("MultipleWindowsTest: Submit query");
    string query = R"(Query::from("window")
        .filter(Attribute("id") < 15)
        .window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500))).byKey(Attribute("id")).apply(Sum(Attribute("value")))
        .assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute("start"), Milliseconds(0), Milliseconds()))
        .window(TumblingWindow::of(EventTime(Attribute("start")), Seconds(1))).byKey(Attribute("id")).apply(Sum(Attribute("value")))
        .filter(Attribute("id") < 10)
        .assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute("start"), Milliseconds(0), Milliseconds()))
        .window(TumblingWindow::of(EventTime(Attribute("start")), Seconds(2))).apply(Sum(Attribute("value")))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId = requestHandlerService->validateAndQueueAddQueryRequest(query, Optimizer::PlacementStrategy::BottomUp);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    string expectedContent = "window$start:INTEGER(64 bits),window$end:INTEGER(64 bits),window$value:INTEGER(64 bits)\n"
                             "0,2000,4\n"
                             "2000,4000,24\n"
                             "4000,6000,20\n"
                             "6000,8000,28\n"
                             "8000,10000,36\n"
                             "10000,12000,44\n"
                             "12000,14000,52\n"
                             "14000,16000,60\n"
                             "16000,18000,68\n"
                             "18000,20000,76\n"
                             "20000,22000,62\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("MultipleWindowsTest: Remove query");

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("MultipleWindowsTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("MultipleWindowsTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("MultipleWindowsTest: Test finished");
}

/**
 * @brief This tests just outputs the default source for a hierarchy with one relay which also produces data by itself
 * Topology:
    PhysicalNode[id=1, ip=127.0.0.1, resourceCapacity=12, usedResource=0] => Join 2
    |--PhysicalNode[id=2, ip=127.0.0.1, resourceCapacity=1, usedResource=0] => Join 1
    |  |--PhysicalNode[id=6, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=5, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=4, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
 */
TEST_F(MultipleWindowsTest, DISABLED_testSeparatedWindow) {
    /**
     * @Ankit this should lso not happen, node 2 has a slot count of 3 but 5 operators are deployed
     * ExecutionNode(id:1, ip:127.0.0.1, topologyId:1)
| QuerySubPlan(queryId:1, querySubPlanId:4)
|  SINK(6)
|    SOURCE(17,)
|--ExecutionNode(id:2, ip:127.0.0.1, topologyId:2)
|  | QuerySubPlan(queryId:1, querySubPlanId:3)
|  |  SINK(18)
|  |    CENTRALWINDOW(9)
|  |      WATERMARKASSIGNER(4)
|  |        WindowComputationOperator(10)
|  |          SOURCE(13,)
|  |          SOURCE(15,)
|  |--ExecutionNode(id:3, ip:127.0.0.1, topologyId:3)
|  |  | QuerySubPlan(queryId:1, querySubPlanId:1)
|  |  |  SINK(14)
|  |  |    SliceCreationOperator(11)
|  |  |      WATERMARKASSIGNER(2)
|  |  |        SOURCE(1,window)
|  |--ExecutionNode(id:4, ip:127.0.0.1, topologyId:4)
|  |  | QuerySubPlan(queryId:1, querySubPlanId:2)
|  |  |  SINK(16)
|  |  |    SliceCreationOperator(12)
|  |  |      WATERMARKASSIGNER(7)
|  |  |        SOURCE(8,window)
     */
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");
    //register logical source
    auto testSchema = TestSchemas::getSchemaTemplate("id_val_time_u64");
    crd->getSourceCatalog()->addLogicalSource("window", testSchema);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    workerConfig1->coordinatorPort = port;
    workerConfig1->numberOfSlots = (3);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = port;
    workerConfig2->numberOfSlots = (12);
    CSVSourceTypePtr csvSourceType2 = CSVSourceType::create("window", "test_stream");
    csvSourceType2->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType2->setGatheringInterval(1);
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType2->setNumberOfBuffersToProduce(2);
    csvSourceType2->setSkipHeader(false);
    workerConfig2->physicalSourceTypes.add(csvSourceType2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    wrk2->replaceParent(WorkerId(1), WorkerId(2));
    NES_INFO("WindowDeploymentTest: Worker 2 started successfully");

    NES_INFO("MultipleWindowsTest: Start worker 3");

    WorkerConfigurationPtr workerConfig3 = WorkerConfiguration::create();
    workerConfig3->coordinatorPort = port;
    workerConfig3->numberOfSlots = (12);
    CSVSourceTypePtr csvSourceType3 = CSVSourceType::create("window", "test_stream");
    csvSourceType3->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType3->setGatheringInterval(1);
    csvSourceType3->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType3->setNumberOfBuffersToProduce(2);
    csvSourceType3->setSkipHeader(false);
    workerConfig3->physicalSourceTypes.add(csvSourceType3);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(workerConfig3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    wrk3->replaceParent(WorkerId(1), WorkerId(2));
    NES_INFO("MultipleWindowsTest: Worker3 started SUCCESSFULLY");

    std::string outputFilePath = getTestResourceFolder() / "testTwoJoinsWithDifferentStreamTumblingWindowDistributed.out";
    remove(outputFilePath.c_str());

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();

    NES_INFO("MultipleWindowsTest: Submit query");

    string query = R"(Query::from("window")
        .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1))).byKey(Attribute("id")).apply(Sum(Attribute("value")))
        .window(TumblingWindow::of(EventTime(Attribute("start")), Seconds(2))).byKey(Attribute("id")).apply(Sum(Attribute("value")))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId = requestHandlerService->validateAndQueueAddQueryRequest(query, Optimizer::PlacementStrategy::BottomUp);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    string expectedContent = "window1window2window3$start:INTEGER(64 bits),window1window2window3$end:INTEGER(64 "
                             "bits),window1window2window3$key:INTEGER(64 bits),window1window2$"
                             "start:INTEGER(64 bits),window1window2$end:INTEGER(64 bits),window1window2$key:INTEGER(64 "
                             "bits),window1$win1:INTEGER(64 bits),window1$id1:INTEGER(64 bits),window1$"
                             "timestamp:INTEGER(64 bits),window2$win2:INTEGER(64 bits),window2$id2:INTEGER(64 "
                             "bits),window2$timestamp:INTEGER(64 bits),window3$win3:INTEGER(64 bits),window3$id3:"
                             "INTEGER(64 bits),window3$timestamp:INTEGER(64 bits)\n"
                             "1000,2000,4,1000,2000,4,1,4,1002,3,4,1102,4,4,1001\n"
                             "1000,2000,4,1000,2000,4,1,4,1002,3,4,1112,4,4,1001\n"
                             "1000,2000,12,1000,2000,12,1,12,1001,5,12,1011,1,12,1300\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("MultipleWindowsTest: Remove query");

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("MultipleWindowsTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("MultipleWindowsTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("MultipleWindowsTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_DEBUG("MultipleWindowsTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("MultipleWindowsTest: Test finished");
}

/**
 * @brief This tests just outputs the default source for a hierarchy with one relay which also produces data by itself
 * Topology:
    PhysicalNode[id=1, ip=127.0.0.1, resourceCapacity=12, usedResource=0] => Join 2
    |--PhysicalNode[id=2, ip=127.0.0.1, resourceCapacity=1, usedResource=0] => Join 1
    |  |--PhysicalNode[id=6, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=5, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=4, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
 */
TEST_F(MultipleWindowsTest, DISABLED_testNotVaildQuery) {
    /**
     * @Ankit this plan should not happen, it spearates the slicer from the source
     * ExecutionNode(id:1, ip:127.0.0.1, topologyId:1)
| QuerySubPlan(queryId:1, querySubPlanId:5)
|  SINK(8)
|    CENTRALWINDOW(12)
|      WATERMARKASSIGNER(6)
|        FILTER(5)
|          WindowComputationOperator(13)
|            SOURCE(18,)
|            SOURCE(22,)
|--ExecutionNode(id:2, ip:127.0.0.1, topologyId:2)
|  | QuerySubPlan(queryId:1, querySubPlanId:2)
|  |  SINK(19)
|  |    SliceCreationOperator(14)
|  |      SOURCE(16,)
|  | QuerySubPlan(queryId:1, querySubPlanId:4)
|  |  SINK(23)
|  |    SliceCreationOperator(15)
|  |      SOURCE(20,)
|  |--ExecutionNode(id:3, ip:127.0.0.1, topologyId:3)
|  |  | QuerySubPlan(queryId:1, querySubPlanId:1)
|  |  |  SINK(17)
|  |  |    WATERMARKASSIGNER(3)
|  |  |      FILTER(2)
|  |  |        SOURCE(1,window)
|  |--ExecutionNode(id:4, ip:127.0.0.1, topologyId:4)
|  |  | QuerySubPlan(queryId:1, querySubPlanId:3)
|  |  |  SINK(21)
|  |  |    WATERMARKASSIGNER(9)
|  |  |      FILTER(11)
|  |  |        SOURCE(10,window)
     */
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");
    //register logical source
    auto testSchema = TestSchemas::getSchemaTemplate("id_val_time_u64");
    crd->getSourceCatalog()->addLogicalSource("window", testSchema);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    workerConfig1->numberOfSlots = (3);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = port;
    workerConfig2->numberOfSlots = (12);
    CSVSourceTypePtr csvSourceType2 = CSVSourceType::create("window", "test_stream");
    csvSourceType2->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType2->setGatheringInterval(1);
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType2->setNumberOfBuffersToProduce(2);
    csvSourceType2->setSkipHeader(false);
    workerConfig2->physicalSourceTypes.add(csvSourceType2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    wrk2->replaceParent(WorkerId(1), WorkerId(2));
    NES_INFO("WindowDeploymentTest: Worker 2 started successfully");

    NES_INFO("MultipleWindowsTest: Start worker 3");

    WorkerConfigurationPtr workerConfig3 = WorkerConfiguration::create();
    workerConfig3->coordinatorPort = port;
    workerConfig3->numberOfSlots = (12);
    CSVSourceTypePtr csvSourceType3 = CSVSourceType::create("window", "test_stream");
    csvSourceType3->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType3->setGatheringInterval(1);
    csvSourceType3->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType3->setNumberOfBuffersToProduce(2);
    csvSourceType3->setSkipHeader(false);
    workerConfig3->physicalSourceTypes.add(csvSourceType3);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(workerConfig3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    wrk3->replaceParent(WorkerId(1), WorkerId(2));
    NES_INFO("MultipleWindowsTest: Worker3 started SUCCESSFULLY");

    std::string outputFilePath = getTestResourceFolder() / "testTwoJoinsWithDifferentStreamTumblingWindowDistributed.out";
    remove(outputFilePath.c_str());

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();

    NES_INFO("MultipleWindowsTest: Submit query");

    string query = R"(Query::from("window")
        .filter(Attribute("id") < 15)
        .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1))).byKey(Attribute("id")).apply(Sum(Attribute("value")))
        .filter(Attribute("id") < 10)
        .window(TumblingWindow::of(EventTime(Attribute("start")), Seconds(2))).byKey(Attribute("id")).apply(Sum(Attribute("value")))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId = requestHandlerService->validateAndQueueAddQueryRequest(query, Optimizer::PlacementStrategy::BottomUp);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    string expectedContent =
        "window1window2window3$start:INTEGER,window1window2window3$end:INTEGER,window1window2window3$key:INTEGER,window1window2$"
        "start:INTEGER,window1window2$end:INTEGER,window1window2$key:INTEGER,window1$win1:INTEGER,window1$id1:INTEGER,window1$"
        "timestamp:INTEGER,window2$win2:INTEGER,window2$id2:INTEGER,window2$timestamp:INTEGER,window3$win3:INTEGER,window3$id3:"
        "INTEGER,window3$timestamp:INTEGER\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1102,4,4,1001\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1112,4,4,1001\n"
        "1000,2000,12,1000,2000,12,1,12,1001,5,12,1011,1,12,1300\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("MultipleWindowsTest: Remove query");

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("MultipleWindowsTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("MultipleWindowsTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("MultipleWindowsTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_DEBUG("MultipleWindowsTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("MultipleWindowsTest: Test finished");
}

}// namespace NES
