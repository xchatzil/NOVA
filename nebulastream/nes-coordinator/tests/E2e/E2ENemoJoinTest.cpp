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
#define _TURN_OFF_PLATFORM_STRING// for cpprest/details/basic_types.h
#include <BaseIntegrationTest.hpp>
#include <Identifiers/NESStrongTypeJson.hpp>
#include <Util/Common.hpp>
#include <Util/Core.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <cstdio>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <sstream>
#include <string>
#include <unistd.h>

using namespace std;
namespace NES {

class E2ENemoJoinTest : public Testing::BaseIntegrationTest {
  public:
    uint16_t timeoutSetupMs = 100;
    std::chrono::seconds timeoutResultSec = std::chrono::seconds(120);

    uint64_t bufferSizeInBytes = 4096;
    uint64_t numberOfBuffersInGlobalBufferManager = 80960;
    uint64_t numberOfBuffersPerWorker = 20960;

    uint64_t buffers = 700;//700;
    uint64_t tuplesPerBuffer = 100;

    uint64_t windowSizes[3] = {1, 2, 10};

    uint64_t gatheringIntervalMs = 100;
    uint64_t ingRatePerSec = 1000 / gatheringIntervalMs * tuplesPerBuffer;

    uint64_t expectedBuffers = (tuplesPerBuffer * buffers) + (4 * buffers);

    uint64_t expectedTuples = 2 * buffers * tuplesPerBuffer + 1;

    std::string rootDir;//= getTestResourceFolder().string() + "/";
    std::string basePathOutput = rootDir + std::to_string(ingRatePerSec) + "_";
    std::string left1Path =
        std::filesystem::path(TEST_DATA_DIRECTORY) / "JoinData/normalized/small/left/left1_bmp280_29791_full.csv";
    std::string left2Path =
        std::filesystem::path(TEST_DATA_DIRECTORY) / "JoinData/normalized/small/left/left2_bmp280_29792_full.csv";
    std::string left3Path =
        std::filesystem::path(TEST_DATA_DIRECTORY) / "JoinData/normalized/small/left/left3_bmp280_29793_full.csv";
    std::string left4Path =
        std::filesystem::path(TEST_DATA_DIRECTORY) / "JoinData/normalized/small/left/left4_bmp280_29794_full.csv";

    std::string right1Path =
        std::filesystem::path(TEST_DATA_DIRECTORY) / "JoinData/normalized/small/right/right1_dht22_65817_full.csv";
    std::string right2Path =
        std::filesystem::path(TEST_DATA_DIRECTORY) / "JoinData/normalized/small/right/right2_dht22_65818_full.csv";
    std::string right3Path =
        std::filesystem::path(TEST_DATA_DIRECTORY) / "JoinData/normalized/small/right/right3_dht22_65819_full.csv";
    std::string right4Path =
        std::filesystem::path(TEST_DATA_DIRECTORY) / "JoinData/normalized/small/right/right4_dht22_65820_full.csv";

    static void SetUpTestCase() {
        NES::Logger::setupLogging("E2ENemoJoinTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup E2e test class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down E2ENemoJoinTest test class."); }
};

TEST_F(E2ENemoJoinTest, testJoinQueryDefaultW1) {
    // stress on nodes 4, 5, 6, 7
    std::string joinType = "--optimizer.distributedJoinOptimizationMode=NONE";

    // the query
    auto windowSize = windowSizes[0];
    std::string outputFilePath =
        basePathOutput + std::to_string(windowSize) + "_" + joinType.substr(joinType.find('=') + 1) + ".out";
    remove(outputFilePath.c_str());
    std::stringstream querySs;
    querySs << "{\"userQuery\" : ";
    querySs << "\"Query::from(\\\"left\\\").joinWith(Query::from(\\\"right\\\")).where(Attribute(\\\"location\\\") == Attribute("
               "\\\"location\\\"))"
               ".window(TumblingWindow::of(EventTime(Attribute(\\\"timestamp\\\")), Milliseconds(";
    querySs << windowSize;
    querySs << ")))"
               ".sink(FileSinkDescriptor::create(\\\"";
    querySs << outputFilePath;
    querySs << R"(\", \"CSV_FORMAT\", \"APPEND\", true)";
    querySs << R"());","placement" : "BottomUp"})";
    querySs << endl;

    uint64_t healthWaitTime = 100;
    NES_INFO("start coordinator");
    auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(*rpcCoordinatorPort),
                                                    TestUtils::restPort(*restPort),
                                                    TestUtils::bufferSizeInBytes(bufferSizeInBytes, "worker."),
                                                    TestUtils::numGlobalBuffers(numberOfBuffersInGlobalBufferManager, "worker."),
                                                    TestUtils::numBuffersPerWorker(numberOfBuffersPerWorker, "worker."),
                                                    joinType,// same result with and without
                                                    TestUtils::enableDebug()});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 0));

    std::stringstream schemaLeft;
    schemaLeft << "{\"logicalSourceName\" : \"left\",\"schema\" : \"Schema::create()"
                  "->addField(createField(\\\"sensorId\\\", BasicType::UINT32))"
                  "->addField(createField(\\\"location\\\", BasicType::UINT32))"
                  "->addField(createField(\\\"timestamp\\\", BasicType::UINT64))"
                  "->addField(createField(\\\"temperature\\\", BasicType::FLOAT64));\"}";
    schemaLeft << endl;
    NES_INFO("schema left submit={}", schemaLeft.str());
    ASSERT_TRUE(TestUtils::addLogicalSource(schemaLeft.str(), std::to_string(*restPort)));

    std::stringstream schemaRight;
    schemaRight << "{\"logicalSourceName\" : \"right\",\"schema\" : \"Schema::create()"
                   "->addField(createField(\\\"sensorId\\\", BasicType::UINT32))"
                   "->addField(createField(\\\"location\\\", BasicType::UINT32))"
                   "->addField(createField(\\\"timestamp\\\", BasicType::UINT64))"
                   "->addField(createField(\\\"humidity\\\", BasicType::FLOAT64));\"}";
    schemaRight << endl;
    NES_INFO("schema left submit={}", schemaRight.str());
    ASSERT_TRUE(TestUtils::addLogicalSource(schemaRight.str(), std::to_string(*restPort)));

    auto networkingWorker1 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                                     TestUtils::dataPort(0),
                                                     TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                                     TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                     TestUtils::numGlobalBuffers(numberOfBuffersInGlobalBufferManager),
                                                     TestUtils::numBuffersPerWorker(numberOfBuffersPerWorker),
                                                     TestUtils::workerHealthCheckWaitTime(healthWaitTime)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 1));

    auto networkingWorker2 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                                     TestUtils::dataPort(0),
                                                     TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                                     TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                     TestUtils::numGlobalBuffers(numberOfBuffersInGlobalBufferManager),
                                                     TestUtils::numBuffersPerWorker(numberOfBuffersPerWorker),
                                                     TestUtils::workerHealthCheckWaitTime(healthWaitTime)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 2));

    auto joinWorker1 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                               TestUtils::dataPort(0),
                                               TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                               TestUtils::parentId(2),
                                               TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                               TestUtils::numGlobalBuffers(numberOfBuffersInGlobalBufferManager),
                                               TestUtils::numBuffersPerWorker(numberOfBuffersPerWorker),
                                               TestUtils::workerHealthCheckWaitTime(healthWaitTime)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 3));

    auto joinWorker2 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                               TestUtils::dataPort(0),
                                               TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                               TestUtils::parentId(2),
                                               TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                               TestUtils::numGlobalBuffers(numberOfBuffersInGlobalBufferManager),
                                               TestUtils::numBuffersPerWorker(numberOfBuffersPerWorker),
                                               TestUtils::workerHealthCheckWaitTime(healthWaitTime)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 4));

    auto joinWorker3 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                               TestUtils::dataPort(0),
                                               TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                               TestUtils::parentId(3),
                                               TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                               TestUtils::numGlobalBuffers(numberOfBuffersInGlobalBufferManager),
                                               TestUtils::numBuffersPerWorker(numberOfBuffersPerWorker),
                                               TestUtils::workerHealthCheckWaitTime(healthWaitTime)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 5));

    auto joinWorker4 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                               TestUtils::dataPort(0),
                                               TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                               TestUtils::parentId(3),
                                               TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                               TestUtils::numGlobalBuffers(numberOfBuffersInGlobalBufferManager),
                                               TestUtils::numBuffersPerWorker(numberOfBuffersPerWorker),
                                               TestUtils::workerHealthCheckWaitTime(healthWaitTime)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 6));

    auto leftJoinSource1 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                                   TestUtils::dataPort(0),
                                                   TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                                   TestUtils::parentId(4),
                                                   TestUtils::sourceType(SourceType::CSV_SOURCE),
                                                   TestUtils::csvSourceFilePath(left1Path),
                                                   TestUtils::physicalSourceName("test_stream_left_1"),
                                                   TestUtils::logicalSourceName("left"),
                                                   TestUtils::numberOfBuffersToProduce(buffers),
                                                   TestUtils::numberOfTuplesToProducePerBuffer(tuplesPerBuffer),
                                                   TestUtils::sourceGatheringInterval(gatheringIntervalMs),
                                                   TestUtils::workerHealthCheckWaitTime(healthWaitTime),
                                                   TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                   TestUtils::enableDebug()});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 7));

    auto rightJoinSource1 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                                    TestUtils::dataPort(0),
                                                    TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                                    TestUtils::parentId(4),
                                                    TestUtils::sourceType(SourceType::CSV_SOURCE),
                                                    TestUtils::csvSourceFilePath(right1Path),
                                                    TestUtils::physicalSourceName("test_stream_right_1"),
                                                    TestUtils::logicalSourceName("right"),
                                                    TestUtils::numberOfBuffersToProduce(buffers),
                                                    TestUtils::numberOfTuplesToProducePerBuffer(tuplesPerBuffer),
                                                    TestUtils::sourceGatheringInterval(gatheringIntervalMs),
                                                    TestUtils::workerHealthCheckWaitTime(healthWaitTime),
                                                    TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                    TestUtils::enableDebug()});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 8));

    auto leftJoinSource2 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                                   TestUtils::dataPort(0),
                                                   TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                                   TestUtils::parentId(5),
                                                   TestUtils::sourceType(SourceType::CSV_SOURCE),
                                                   TestUtils::csvSourceFilePath(left2Path),
                                                   TestUtils::physicalSourceName("test_stream_left_2"),
                                                   TestUtils::logicalSourceName("left"),
                                                   TestUtils::numberOfBuffersToProduce(buffers),
                                                   TestUtils::numberOfTuplesToProducePerBuffer(tuplesPerBuffer),
                                                   TestUtils::sourceGatheringInterval(gatheringIntervalMs),
                                                   TestUtils::workerHealthCheckWaitTime(healthWaitTime),
                                                   TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                   TestUtils::enableDebug()});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 9));

    auto rightJoinSource2 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                                    TestUtils::dataPort(0),
                                                    TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                                    TestUtils::parentId(5),
                                                    TestUtils::sourceType(SourceType::CSV_SOURCE),
                                                    TestUtils::csvSourceFilePath(right2Path),
                                                    TestUtils::physicalSourceName("test_stream_right_2"),
                                                    TestUtils::logicalSourceName("right"),
                                                    TestUtils::numberOfBuffersToProduce(buffers),
                                                    TestUtils::numberOfTuplesToProducePerBuffer(tuplesPerBuffer),
                                                    TestUtils::sourceGatheringInterval(gatheringIntervalMs),
                                                    TestUtils::workerHealthCheckWaitTime(healthWaitTime),
                                                    TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                    TestUtils::enableDebug()});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 10));

    auto leftJoinSource3 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                                   TestUtils::dataPort(0),
                                                   TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                                   TestUtils::parentId(6),
                                                   TestUtils::sourceType(SourceType::CSV_SOURCE),
                                                   TestUtils::csvSourceFilePath(left3Path),
                                                   TestUtils::physicalSourceName("test_stream_left_3"),
                                                   TestUtils::logicalSourceName("left"),
                                                   TestUtils::numberOfBuffersToProduce(buffers),
                                                   TestUtils::numberOfTuplesToProducePerBuffer(tuplesPerBuffer),
                                                   TestUtils::sourceGatheringInterval(gatheringIntervalMs),
                                                   TestUtils::workerHealthCheckWaitTime(healthWaitTime),
                                                   TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                   TestUtils::enableDebug()});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 11));

    auto rightJoinSource3 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                                    TestUtils::dataPort(0),
                                                    TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                                    TestUtils::parentId(6),
                                                    TestUtils::sourceType(SourceType::CSV_SOURCE),
                                                    TestUtils::csvSourceFilePath(right3Path),
                                                    TestUtils::physicalSourceName("test_stream_right_3"),
                                                    TestUtils::logicalSourceName("right"),
                                                    TestUtils::numberOfBuffersToProduce(buffers),
                                                    TestUtils::numberOfTuplesToProducePerBuffer(tuplesPerBuffer),
                                                    TestUtils::sourceGatheringInterval(gatheringIntervalMs),
                                                    TestUtils::workerHealthCheckWaitTime(healthWaitTime),
                                                    TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                    TestUtils::enableDebug()});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 12));

    auto leftJoinSource4 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                                   TestUtils::dataPort(0),
                                                   TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                                   TestUtils::parentId(7),
                                                   TestUtils::sourceType(SourceType::CSV_SOURCE),
                                                   TestUtils::csvSourceFilePath(left4Path),
                                                   TestUtils::physicalSourceName("test_stream_left_4"),
                                                   TestUtils::logicalSourceName("left"),
                                                   TestUtils::numberOfBuffersToProduce(buffers),
                                                   TestUtils::numberOfTuplesToProducePerBuffer(tuplesPerBuffer),
                                                   TestUtils::sourceGatheringInterval(gatheringIntervalMs),
                                                   TestUtils::workerHealthCheckWaitTime(healthWaitTime),
                                                   TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                   TestUtils::enableDebug()});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 13));

    auto rightJoinSource4 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                                    TestUtils::dataPort(0),
                                                    TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                                    TestUtils::parentId(7),
                                                    TestUtils::sourceType(SourceType::CSV_SOURCE),
                                                    TestUtils::csvSourceFilePath(right4Path),
                                                    TestUtils::physicalSourceName("test_stream_right_4"),
                                                    TestUtils::logicalSourceName("right"),
                                                    TestUtils::numberOfBuffersToProduce(buffers),
                                                    TestUtils::numberOfTuplesToProducePerBuffer(tuplesPerBuffer),
                                                    TestUtils::sourceGatheringInterval(gatheringIntervalMs),
                                                    TestUtils::workerHealthCheckWaitTime(healthWaitTime),
                                                    TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                    TestUtils::enableDebug()});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 14));

    auto topology = TestUtils::getTopology(*restPort);
    NES_INFO("The final topology:\n{}", topology.dump());

    // deploy values for statistics
    auto startNode = 8;
    auto startStream = 1;
    auto numJoins = 4;

    for (auto i = startStream; i <= numJoins; i++) {
        nlohmann::json request;
        request["topologyNodeId"] = startNode++;
        request["logicalSource"] = "left";
        request["physicalSource"] = "test_stream_left_" + std::to_string(i);
        request["fieldName"] = "location";
        request["value"] = std::to_string(i);
        auto jsonString = request.dump();
        NES_INFO("Json string={}", jsonString);
        auto success = TestUtils::addSourceStatistics(request.dump(), std::to_string(*restPort));
        ASSERT_TRUE(success["success"]);

        request["topologyNodeId"] = startNode++;
        request["logicalSource"] = "right";
        request["physicalSource"] = "test_stream_right_" + std::to_string(i);
        request["fieldName"] = "location";
        request["value"] = std::to_string(i);
        jsonString = request.dump();
        NES_INFO("Json string={}", jsonString);

        success = TestUtils::addSourceStatistics(request.dump(), std::to_string(*restPort));
        ASSERT_TRUE(success["success"]);
    }

    NES_INFO("query string submit={}", querySs.str());
    nlohmann::json json_return = TestUtils::startQueryViaRest(querySs.str(), std::to_string(*restPort));
    QueryId queryId = json_return.at("queryId").get<QueryId>();

    NES_INFO("try to acc return");
    NES_INFO("Query ID: {}", queryId);
    ASSERT_NE(queryId, INVALID_QUERY_ID);

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, expectedBuffers, std::to_string(*restPort), timeoutResultSec));
    nlohmann::json jsonPlan = TestUtils::getExecutionPlan(queryId, std::to_string(*restPort));

    std::ifstream ifs(outputFilePath.c_str());
    ASSERT_TRUE(ifs.good());

    uint64_t lineCnt = Util::countLines(ifs);
    NES_INFO("json execution plan:{}", jsonPlan.dump());
    EXPECT_EQ(expectedTuples, lineCnt);
}

TEST_F(E2ENemoJoinTest, testJoinQueryNovaW1) {
    // stress on nodes 4, 5, 6, 7
    std::string joinType = TestUtils::enableNemoJoin();

    // the query
    auto windowSize = windowSizes[0];
    std::string outputFilePath = basePathOutput + std::to_string(windowSize) + "_NOVA.out";
    remove(outputFilePath.c_str());
    std::stringstream querySs;
    querySs << "{\"userQuery\" : ";
    querySs << "\"Query::from(\\\"left\\\").joinWith(Query::from(\\\"right\\\")).where(Attribute(\\\"location\\\") == Attribute("
               "\\\"location\\\"))"
               ".window(TumblingWindow::of(EventTime(Attribute(\\\"timestamp\\\")), Milliseconds(";
    querySs << windowSize;
    querySs << ")))"
               ".sink(FileSinkDescriptor::create(\\\"";
    querySs << outputFilePath;
    querySs << R"(\", \"CSV_FORMAT\", \"APPEND\", true)";
    querySs << R"());","placement" : "BottomUp"})";
    querySs << endl;

    uint64_t healthWaitTime = 100;
    NES_INFO("start coordinator");
    auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(*rpcCoordinatorPort),
                                                    TestUtils::restPort(*restPort),
                                                    TestUtils::bufferSizeInBytes(bufferSizeInBytes, "worker."),
                                                    TestUtils::numGlobalBuffers(numberOfBuffersInGlobalBufferManager, "worker."),
                                                    TestUtils::numBuffersPerWorker(numberOfBuffersPerWorker, "worker."),
                                                    joinType,// same result with and without
                                                    TestUtils::enableDebug()});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 0));

    std::stringstream schemaLeft;
    schemaLeft << "{\"logicalSourceName\" : \"left\",\"schema\" : \"Schema::create()"
                  "->addField(createField(\\\"sensorId\\\", BasicType::UINT32))"
                  "->addField(createField(\\\"location\\\", BasicType::UINT32))"
                  "->addField(createField(\\\"timestamp\\\", BasicType::UINT64))"
                  "->addField(createField(\\\"temperature\\\", BasicType::FLOAT64));\"}";
    schemaLeft << endl;
    NES_INFO("schema left submit={}", schemaLeft.str());
    ASSERT_TRUE(TestUtils::addLogicalSource(schemaLeft.str(), std::to_string(*restPort)));

    std::stringstream schemaRight;
    schemaRight << "{\"logicalSourceName\" : \"right\",\"schema\" : \"Schema::create()"
                   "->addField(createField(\\\"sensorId\\\", BasicType::UINT32))"
                   "->addField(createField(\\\"location\\\", BasicType::UINT32))"
                   "->addField(createField(\\\"timestamp\\\", BasicType::UINT64))"
                   "->addField(createField(\\\"humidity\\\", BasicType::FLOAT64));\"}";
    schemaRight << endl;
    NES_INFO("schema left submit={}", schemaRight.str());
    ASSERT_TRUE(TestUtils::addLogicalSource(schemaRight.str(), std::to_string(*restPort)));

    auto networkingWorker1 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                                     TestUtils::dataPort(0),
                                                     TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                                     TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                     TestUtils::numGlobalBuffers(numberOfBuffersInGlobalBufferManager),
                                                     TestUtils::numBuffersPerWorker(numberOfBuffersPerWorker),
                                                     TestUtils::workerHealthCheckWaitTime(healthWaitTime)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 1));

    auto networkingWorker2 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                                     TestUtils::dataPort(0),
                                                     TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                                     TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                     TestUtils::numGlobalBuffers(numberOfBuffersInGlobalBufferManager),
                                                     TestUtils::numBuffersPerWorker(numberOfBuffersPerWorker),
                                                     TestUtils::workerHealthCheckWaitTime(healthWaitTime)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 2));

    auto joinWorker1 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                               TestUtils::dataPort(0),
                                               TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                               TestUtils::parentId(2),
                                               TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                               TestUtils::numGlobalBuffers(numberOfBuffersInGlobalBufferManager),
                                               TestUtils::numBuffersPerWorker(numberOfBuffersPerWorker),
                                               TestUtils::workerHealthCheckWaitTime(healthWaitTime)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 3));

    auto joinWorker2 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                               TestUtils::dataPort(0),
                                               TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                               TestUtils::parentId(2),
                                               TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                               TestUtils::numGlobalBuffers(numberOfBuffersInGlobalBufferManager),
                                               TestUtils::numBuffersPerWorker(numberOfBuffersPerWorker),
                                               TestUtils::workerHealthCheckWaitTime(healthWaitTime)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 4));

    auto joinWorker3 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                               TestUtils::dataPort(0),
                                               TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                               TestUtils::parentId(3),
                                               TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                               TestUtils::numGlobalBuffers(numberOfBuffersInGlobalBufferManager),
                                               TestUtils::numBuffersPerWorker(numberOfBuffersPerWorker),
                                               TestUtils::workerHealthCheckWaitTime(healthWaitTime)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 5));

    auto joinWorker4 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                               TestUtils::dataPort(0),
                                               TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                               TestUtils::parentId(3),
                                               TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                               TestUtils::numGlobalBuffers(numberOfBuffersInGlobalBufferManager),
                                               TestUtils::numBuffersPerWorker(numberOfBuffersPerWorker),
                                               TestUtils::workerHealthCheckWaitTime(healthWaitTime)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 6));

    auto leftJoinSource1 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                                   TestUtils::dataPort(0),
                                                   TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                                   TestUtils::parentId(4),
                                                   TestUtils::sourceType(SourceType::CSV_SOURCE),
                                                   TestUtils::csvSourceFilePath(left1Path),
                                                   TestUtils::physicalSourceName("test_stream_left_1"),
                                                   TestUtils::logicalSourceName("left"),
                                                   TestUtils::numberOfBuffersToProduce(buffers),
                                                   TestUtils::numberOfTuplesToProducePerBuffer(tuplesPerBuffer),
                                                   TestUtils::sourceGatheringInterval(gatheringIntervalMs),
                                                   TestUtils::workerHealthCheckWaitTime(healthWaitTime),
                                                   TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                   TestUtils::enableDebug()});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 7));

    auto rightJoinSource1 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                                    TestUtils::dataPort(0),
                                                    TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                                    TestUtils::parentId(4),
                                                    TestUtils::sourceType(SourceType::CSV_SOURCE),
                                                    TestUtils::csvSourceFilePath(right1Path),
                                                    TestUtils::physicalSourceName("test_stream_right_1"),
                                                    TestUtils::logicalSourceName("right"),
                                                    TestUtils::numberOfBuffersToProduce(buffers),
                                                    TestUtils::numberOfTuplesToProducePerBuffer(tuplesPerBuffer),
                                                    TestUtils::sourceGatheringInterval(gatheringIntervalMs),
                                                    TestUtils::workerHealthCheckWaitTime(healthWaitTime),
                                                    TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                    TestUtils::enableDebug()});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 8));

    auto leftJoinSource2 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                                   TestUtils::dataPort(0),
                                                   TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                                   TestUtils::parentId(5),
                                                   TestUtils::sourceType(SourceType::CSV_SOURCE),
                                                   TestUtils::csvSourceFilePath(left2Path),
                                                   TestUtils::physicalSourceName("test_stream_left_2"),
                                                   TestUtils::logicalSourceName("left"),
                                                   TestUtils::numberOfBuffersToProduce(buffers),
                                                   TestUtils::numberOfTuplesToProducePerBuffer(tuplesPerBuffer),
                                                   TestUtils::sourceGatheringInterval(gatheringIntervalMs),
                                                   TestUtils::workerHealthCheckWaitTime(healthWaitTime),
                                                   TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                   TestUtils::enableDebug()});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 9));

    auto rightJoinSource2 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                                    TestUtils::dataPort(0),
                                                    TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                                    TestUtils::parentId(5),
                                                    TestUtils::sourceType(SourceType::CSV_SOURCE),
                                                    TestUtils::csvSourceFilePath(right2Path),
                                                    TestUtils::physicalSourceName("test_stream_right_2"),
                                                    TestUtils::logicalSourceName("right"),
                                                    TestUtils::numberOfBuffersToProduce(buffers),
                                                    TestUtils::numberOfTuplesToProducePerBuffer(tuplesPerBuffer),
                                                    TestUtils::sourceGatheringInterval(gatheringIntervalMs),
                                                    TestUtils::workerHealthCheckWaitTime(healthWaitTime),
                                                    TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                    TestUtils::enableDebug()});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 10));

    auto leftJoinSource3 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                                   TestUtils::dataPort(0),
                                                   TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                                   TestUtils::parentId(6),
                                                   TestUtils::sourceType(SourceType::CSV_SOURCE),
                                                   TestUtils::csvSourceFilePath(left3Path),
                                                   TestUtils::physicalSourceName("test_stream_left_3"),
                                                   TestUtils::logicalSourceName("left"),
                                                   TestUtils::numberOfBuffersToProduce(buffers),
                                                   TestUtils::numberOfTuplesToProducePerBuffer(tuplesPerBuffer),
                                                   TestUtils::sourceGatheringInterval(gatheringIntervalMs),
                                                   TestUtils::workerHealthCheckWaitTime(healthWaitTime),
                                                   TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                   TestUtils::enableDebug()});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 11));

    auto rightJoinSource3 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                                    TestUtils::dataPort(0),
                                                    TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                                    TestUtils::parentId(6),
                                                    TestUtils::sourceType(SourceType::CSV_SOURCE),
                                                    TestUtils::csvSourceFilePath(right3Path),
                                                    TestUtils::physicalSourceName("test_stream_right_3"),
                                                    TestUtils::logicalSourceName("right"),
                                                    TestUtils::numberOfBuffersToProduce(buffers),
                                                    TestUtils::numberOfTuplesToProducePerBuffer(tuplesPerBuffer),
                                                    TestUtils::sourceGatheringInterval(gatheringIntervalMs),
                                                    TestUtils::workerHealthCheckWaitTime(healthWaitTime),
                                                    TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                    TestUtils::enableDebug()});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 12));

    auto leftJoinSource4 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                                   TestUtils::dataPort(0),
                                                   TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                                   TestUtils::parentId(7),
                                                   TestUtils::sourceType(SourceType::CSV_SOURCE),
                                                   TestUtils::csvSourceFilePath(left4Path),
                                                   TestUtils::physicalSourceName("test_stream_left_4"),
                                                   TestUtils::logicalSourceName("left"),
                                                   TestUtils::numberOfBuffersToProduce(buffers),
                                                   TestUtils::numberOfTuplesToProducePerBuffer(tuplesPerBuffer),
                                                   TestUtils::sourceGatheringInterval(gatheringIntervalMs),
                                                   TestUtils::workerHealthCheckWaitTime(healthWaitTime),
                                                   TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                   TestUtils::enableDebug()});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 13));

    auto rightJoinSource4 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                                    TestUtils::dataPort(0),
                                                    TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                                    TestUtils::parentId(7),
                                                    TestUtils::sourceType(SourceType::CSV_SOURCE),
                                                    TestUtils::csvSourceFilePath(right4Path),
                                                    TestUtils::physicalSourceName("test_stream_right_4"),
                                                    TestUtils::logicalSourceName("right"),
                                                    TestUtils::numberOfBuffersToProduce(buffers),
                                                    TestUtils::numberOfTuplesToProducePerBuffer(tuplesPerBuffer),
                                                    TestUtils::sourceGatheringInterval(gatheringIntervalMs),
                                                    TestUtils::workerHealthCheckWaitTime(healthWaitTime),
                                                    TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                    TestUtils::enableDebug()});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 14));

    auto topology = TestUtils::getTopology(*restPort);
    NES_INFO("The final topology:\n{}", topology.dump());

    // deploy values for statistics
    auto startNode = 8;
    auto startStream = 1;
    auto numJoins = 4;
    auto val = 1;

    for (auto i = startStream; i <= numJoins; i++) {
        nlohmann::json request;
        request["topologyNodeId"] = startNode++;
        request["logicalSource"] = "left";
        request["physicalSource"] = "test_stream_left_" + std::to_string(i);
        request["fieldName"] = "location";
        request["value"] = std::to_string(val);
        auto jsonString = request.dump();
        NES_INFO("Json string={}", jsonString);
        auto success = TestUtils::addSourceStatistics(request.dump(), std::to_string(*restPort));
        ASSERT_TRUE(success["success"]);

        request["topologyNodeId"] = startNode++;
        request["logicalSource"] = "right";
        request["physicalSource"] = "test_stream_right_" + std::to_string(i);
        request["fieldName"] = "location";
        request["value"] = std::to_string(val);
        jsonString = request.dump();
        NES_INFO("Json string={}", jsonString);

        success = TestUtils::addSourceStatistics(request.dump(), std::to_string(*restPort));
        ASSERT_TRUE(success["success"]);

        if ((i) % 2 == 0) {
            val++;
        }
    }

    NES_INFO("query string submit={}", querySs.str());
    nlohmann::json json_return = TestUtils::startQueryViaRest(querySs.str(), std::to_string(*restPort));
    QueryId queryId = json_return.at("queryId").get<QueryId>();

    NES_INFO("try to acc return");
    NES_INFO("Query ID: {}", queryId);
    ASSERT_NE(queryId, INVALID_QUERY_ID);

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, expectedBuffers, std::to_string(*restPort), timeoutResultSec));
    nlohmann::json jsonPlan = TestUtils::getExecutionPlan(queryId, std::to_string(*restPort));

    std::ifstream ifs(outputFilePath.c_str());
    ASSERT_TRUE(ifs.good());

    uint64_t lineCnt = Util::countLines(ifs);
    NES_INFO("json execution plan:{}", jsonPlan.dump());
    EXPECT_EQ(expectedTuples, lineCnt);
}

TEST_F(E2ENemoJoinTest, testJoinQueryTreeW1) {
    // stress on nodes 4, 5, 6, 7
    std::string joinType = TestUtils::enableNemoJoin();

    // the query
    auto windowSize = windowSizes[0];
    std::string outputFilePath = basePathOutput + std::to_string(windowSize) + "_TREE.out";
    remove(outputFilePath.c_str());
    std::stringstream querySs;
    querySs << "{\"userQuery\" : ";
    querySs << "\"Query::from(\\\"left\\\").joinWith(Query::from(\\\"right\\\")).where(Attribute(\\\"location\\\") == Attribute("
               "\\\"location\\\"))"
               ".window(TumblingWindow::of(EventTime(Attribute(\\\"timestamp\\\")), Milliseconds(";
    querySs << windowSize;
    querySs << ")))"
               ".sink(FileSinkDescriptor::create(\\\"";
    querySs << outputFilePath;
    querySs << R"(\", \"CSV_FORMAT\", \"APPEND\", true)";
    querySs << R"());","placement" : "BottomUp"})";
    querySs << endl;

    uint64_t healthWaitTime = 100;
    NES_INFO("start coordinator");
    auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(*rpcCoordinatorPort),
                                                    TestUtils::restPort(*restPort),
                                                    TestUtils::bufferSizeInBytes(bufferSizeInBytes, "worker."),
                                                    TestUtils::numGlobalBuffers(numberOfBuffersInGlobalBufferManager, "worker."),
                                                    TestUtils::numBuffersPerWorker(numberOfBuffersPerWorker, "worker."),
                                                    joinType,// same result with and without
                                                    TestUtils::enableDebug()});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 0));

    std::stringstream schemaLeft;
    schemaLeft << "{\"logicalSourceName\" : \"left\",\"schema\" : \"Schema::create()"
                  "->addField(createField(\\\"sensorId\\\", BasicType::UINT32))"
                  "->addField(createField(\\\"location\\\", BasicType::UINT32))"
                  "->addField(createField(\\\"timestamp\\\", BasicType::UINT64))"
                  "->addField(createField(\\\"temperature\\\", BasicType::FLOAT64));\"}";
    schemaLeft << endl;
    NES_INFO("schema left submit={}", schemaLeft.str());
    ASSERT_TRUE(TestUtils::addLogicalSource(schemaLeft.str(), std::to_string(*restPort)));

    std::stringstream schemaRight;
    schemaRight << "{\"logicalSourceName\" : \"right\",\"schema\" : \"Schema::create()"
                   "->addField(createField(\\\"sensorId\\\", BasicType::UINT32))"
                   "->addField(createField(\\\"location\\\", BasicType::UINT32))"
                   "->addField(createField(\\\"timestamp\\\", BasicType::UINT64))"
                   "->addField(createField(\\\"humidity\\\", BasicType::FLOAT64));\"}";
    schemaRight << endl;
    NES_INFO("schema left submit={}", schemaRight.str());
    ASSERT_TRUE(TestUtils::addLogicalSource(schemaRight.str(), std::to_string(*restPort)));

    auto networkingWorker1 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                                     TestUtils::dataPort(0),
                                                     TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                                     TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                     TestUtils::numGlobalBuffers(numberOfBuffersInGlobalBufferManager),
                                                     TestUtils::numBuffersPerWorker(numberOfBuffersPerWorker),
                                                     TestUtils::workerHealthCheckWaitTime(healthWaitTime)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 1));

    auto networkingWorker2 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                                     TestUtils::dataPort(0),
                                                     TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                                     TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                     TestUtils::numGlobalBuffers(numberOfBuffersInGlobalBufferManager),
                                                     TestUtils::numBuffersPerWorker(numberOfBuffersPerWorker),
                                                     TestUtils::workerHealthCheckWaitTime(healthWaitTime)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 2));

    auto joinWorker1 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                               TestUtils::dataPort(0),
                                               TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                               TestUtils::parentId(2),
                                               TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                               TestUtils::numGlobalBuffers(numberOfBuffersInGlobalBufferManager),
                                               TestUtils::numBuffersPerWorker(numberOfBuffersPerWorker),
                                               TestUtils::workerHealthCheckWaitTime(healthWaitTime)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 3));

    auto joinWorker2 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                               TestUtils::dataPort(0),
                                               TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                               TestUtils::parentId(2),
                                               TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                               TestUtils::numGlobalBuffers(numberOfBuffersInGlobalBufferManager),
                                               TestUtils::numBuffersPerWorker(numberOfBuffersPerWorker),
                                               TestUtils::workerHealthCheckWaitTime(healthWaitTime)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 4));

    auto joinWorker3 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                               TestUtils::dataPort(0),
                                               TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                               TestUtils::parentId(3),
                                               TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                               TestUtils::numGlobalBuffers(numberOfBuffersInGlobalBufferManager),
                                               TestUtils::numBuffersPerWorker(numberOfBuffersPerWorker),
                                               TestUtils::workerHealthCheckWaitTime(healthWaitTime)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 5));

    auto joinWorker4 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                               TestUtils::dataPort(0),
                                               TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                               TestUtils::parentId(3),
                                               TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                               TestUtils::numGlobalBuffers(numberOfBuffersInGlobalBufferManager),
                                               TestUtils::numBuffersPerWorker(numberOfBuffersPerWorker),
                                               TestUtils::workerHealthCheckWaitTime(healthWaitTime)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 6));

    auto leftJoinSource1 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                                   TestUtils::dataPort(0),
                                                   TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                                   TestUtils::parentId(4),
                                                   TestUtils::sourceType(SourceType::CSV_SOURCE),
                                                   TestUtils::csvSourceFilePath(left1Path),
                                                   TestUtils::physicalSourceName("test_stream_left_1"),
                                                   TestUtils::logicalSourceName("left"),
                                                   TestUtils::numberOfBuffersToProduce(buffers),
                                                   TestUtils::numberOfTuplesToProducePerBuffer(tuplesPerBuffer),
                                                   TestUtils::sourceGatheringInterval(gatheringIntervalMs),
                                                   TestUtils::workerHealthCheckWaitTime(healthWaitTime),
                                                   TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                   TestUtils::enableDebug()});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 7));

    auto rightJoinSource1 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                                    TestUtils::dataPort(0),
                                                    TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                                    TestUtils::parentId(4),
                                                    TestUtils::sourceType(SourceType::CSV_SOURCE),
                                                    TestUtils::csvSourceFilePath(right1Path),
                                                    TestUtils::physicalSourceName("test_stream_right_1"),
                                                    TestUtils::logicalSourceName("right"),
                                                    TestUtils::numberOfBuffersToProduce(buffers),
                                                    TestUtils::numberOfTuplesToProducePerBuffer(tuplesPerBuffer),
                                                    TestUtils::sourceGatheringInterval(gatheringIntervalMs),
                                                    TestUtils::workerHealthCheckWaitTime(healthWaitTime),
                                                    TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                    TestUtils::enableDebug()});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 8));

    auto leftJoinSource2 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                                   TestUtils::dataPort(0),
                                                   TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                                   TestUtils::parentId(5),
                                                   TestUtils::sourceType(SourceType::CSV_SOURCE),
                                                   TestUtils::csvSourceFilePath(left2Path),
                                                   TestUtils::physicalSourceName("test_stream_left_2"),
                                                   TestUtils::logicalSourceName("left"),
                                                   TestUtils::numberOfBuffersToProduce(buffers),
                                                   TestUtils::numberOfTuplesToProducePerBuffer(tuplesPerBuffer),
                                                   TestUtils::sourceGatheringInterval(gatheringIntervalMs),
                                                   TestUtils::workerHealthCheckWaitTime(healthWaitTime),
                                                   TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                   TestUtils::enableDebug()});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 9));

    auto rightJoinSource2 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                                    TestUtils::dataPort(0),
                                                    TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                                    TestUtils::parentId(5),
                                                    TestUtils::sourceType(SourceType::CSV_SOURCE),
                                                    TestUtils::csvSourceFilePath(right2Path),
                                                    TestUtils::physicalSourceName("test_stream_right_2"),
                                                    TestUtils::logicalSourceName("right"),
                                                    TestUtils::numberOfBuffersToProduce(buffers),
                                                    TestUtils::numberOfTuplesToProducePerBuffer(tuplesPerBuffer),
                                                    TestUtils::sourceGatheringInterval(gatheringIntervalMs),
                                                    TestUtils::workerHealthCheckWaitTime(healthWaitTime),
                                                    TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                    TestUtils::enableDebug()});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 10));

    auto leftJoinSource3 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                                   TestUtils::dataPort(0),
                                                   TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                                   TestUtils::parentId(6),
                                                   TestUtils::sourceType(SourceType::CSV_SOURCE),
                                                   TestUtils::csvSourceFilePath(left3Path),
                                                   TestUtils::physicalSourceName("test_stream_left_3"),
                                                   TestUtils::logicalSourceName("left"),
                                                   TestUtils::numberOfBuffersToProduce(buffers),
                                                   TestUtils::numberOfTuplesToProducePerBuffer(tuplesPerBuffer),
                                                   TestUtils::sourceGatheringInterval(gatheringIntervalMs),
                                                   TestUtils::workerHealthCheckWaitTime(healthWaitTime),
                                                   TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                   TestUtils::enableDebug()});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 11));

    auto rightJoinSource3 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                                    TestUtils::dataPort(0),
                                                    TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                                    TestUtils::parentId(6),
                                                    TestUtils::sourceType(SourceType::CSV_SOURCE),
                                                    TestUtils::csvSourceFilePath(right3Path),
                                                    TestUtils::physicalSourceName("test_stream_right_3"),
                                                    TestUtils::logicalSourceName("right"),
                                                    TestUtils::numberOfBuffersToProduce(buffers),
                                                    TestUtils::numberOfTuplesToProducePerBuffer(tuplesPerBuffer),
                                                    TestUtils::sourceGatheringInterval(gatheringIntervalMs),
                                                    TestUtils::workerHealthCheckWaitTime(healthWaitTime),
                                                    TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                    TestUtils::enableDebug()});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 12));

    auto leftJoinSource4 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                                   TestUtils::dataPort(0),
                                                   TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                                   TestUtils::parentId(7),
                                                   TestUtils::sourceType(SourceType::CSV_SOURCE),
                                                   TestUtils::csvSourceFilePath(left4Path),
                                                   TestUtils::physicalSourceName("test_stream_left_4"),
                                                   TestUtils::logicalSourceName("left"),
                                                   TestUtils::numberOfBuffersToProduce(buffers),
                                                   TestUtils::numberOfTuplesToProducePerBuffer(tuplesPerBuffer),
                                                   TestUtils::sourceGatheringInterval(gatheringIntervalMs),
                                                   TestUtils::workerHealthCheckWaitTime(healthWaitTime),
                                                   TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                   TestUtils::enableDebug()});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 13));

    auto rightJoinSource4 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                                    TestUtils::dataPort(0),
                                                    TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                                    TestUtils::parentId(7),
                                                    TestUtils::sourceType(SourceType::CSV_SOURCE),
                                                    TestUtils::csvSourceFilePath(right4Path),
                                                    TestUtils::physicalSourceName("test_stream_right_4"),
                                                    TestUtils::logicalSourceName("right"),
                                                    TestUtils::numberOfBuffersToProduce(buffers),
                                                    TestUtils::numberOfTuplesToProducePerBuffer(tuplesPerBuffer),
                                                    TestUtils::sourceGatheringInterval(gatheringIntervalMs),
                                                    TestUtils::workerHealthCheckWaitTime(healthWaitTime),
                                                    TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                    TestUtils::enableDebug()});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 14));

    auto topology = TestUtils::getTopology(*restPort);
    NES_INFO("The final topology:\n{}", topology.dump());

    // deploy values for statistics
    auto startNode = 8;
    auto startStream = 1;
    auto numJoins = 4;

    for (auto i = startStream; i <= numJoins; i++) {
        nlohmann::json request;
        request["topologyNodeId"] = startNode++;
        request["logicalSource"] = "left";
        request["physicalSource"] = "test_stream_left_" + std::to_string(i);
        request["fieldName"] = "location";
        request["value"] = std::to_string(i);
        auto jsonString = request.dump();
        NES_INFO("Json string={}", jsonString);
        auto success = TestUtils::addSourceStatistics(request.dump(), std::to_string(*restPort));
        ASSERT_TRUE(success["success"]);

        request["topologyNodeId"] = startNode++;
        request["logicalSource"] = "right";
        request["physicalSource"] = "test_stream_right_" + std::to_string(i);
        request["fieldName"] = "location";
        request["value"] = std::to_string(i);
        jsonString = request.dump();
        NES_INFO("Json string={}", jsonString);

        success = TestUtils::addSourceStatistics(request.dump(), std::to_string(*restPort));
        ASSERT_TRUE(success["success"]);
    }

    NES_INFO("query string submit={}", querySs.str());
    nlohmann::json json_return = TestUtils::startQueryViaRest(querySs.str(), std::to_string(*restPort));
    QueryId queryId = json_return.at("queryId").get<QueryId>();

    NES_INFO("try to acc return");
    NES_INFO("Query ID: {}", queryId);
    ASSERT_NE(queryId, INVALID_QUERY_ID);

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, expectedBuffers, std::to_string(*restPort), timeoutResultSec));
    nlohmann::json jsonPlan = TestUtils::getExecutionPlan(queryId, std::to_string(*restPort));

    std::ifstream ifs(outputFilePath.c_str());
    ASSERT_TRUE(ifs.good());

    uint64_t lineCnt = Util::countLines(ifs);
    NES_INFO("json execution plan:{}", jsonPlan.dump());
    EXPECT_EQ(expectedTuples, lineCnt);
}

TEST_F(E2ENemoJoinTest, testJoinQuerySourcesW1) {
    // stress on nodes 4, 5, 6, 7
    std::string joinType = TestUtils::enableNemoJoin();

    // the query
    auto windowSize = windowSizes[0];
    std::string outputFilePath = basePathOutput + std::to_string(windowSize) + "_SOURCES.out";
    remove(outputFilePath.c_str());
    std::stringstream querySs;
    querySs << "{\"userQuery\" : ";
    querySs << "\"Query::from(\\\"left\\\").joinWith(Query::from(\\\"right\\\")).where(Attribute(\\\"location\\\") == Attribute("
               "\\\"location\\\"))"
               ".window(TumblingWindow::of(EventTime(Attribute(\\\"timestamp\\\")), Milliseconds(";
    querySs << windowSize;
    querySs << ")))"
               ".sink(FileSinkDescriptor::create(\\\"";
    querySs << outputFilePath;
    querySs << R"(\", \"CSV_FORMAT\", \"APPEND\", true)";
    querySs << R"());","placement" : "TopDown"})";
    querySs << endl;

    uint64_t healthWaitTime = 100;
    NES_INFO("start coordinator");
    auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(*rpcCoordinatorPort),
                                                    TestUtils::restPort(*restPort),
                                                    TestUtils::bufferSizeInBytes(bufferSizeInBytes, "worker."),
                                                    TestUtils::numGlobalBuffers(numberOfBuffersInGlobalBufferManager, "worker."),
                                                    TestUtils::numBuffersPerWorker(numberOfBuffersPerWorker, "worker."),
                                                    joinType,// same result with and without
                                                    TestUtils::enableDebug()});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 0));

    std::stringstream schemaLeft;
    schemaLeft << "{\"logicalSourceName\" : \"left\",\"schema\" : \"Schema::create()"
                  "->addField(createField(\\\"sensorId\\\", BasicType::UINT32))"
                  "->addField(createField(\\\"location\\\", BasicType::UINT32))"
                  "->addField(createField(\\\"timestamp\\\", BasicType::UINT64))"
                  "->addField(createField(\\\"temperature\\\", BasicType::FLOAT64));\"}";
    schemaLeft << endl;
    NES_INFO("schema left submit={}", schemaLeft.str());
    ASSERT_TRUE(TestUtils::addLogicalSource(schemaLeft.str(), std::to_string(*restPort)));

    std::stringstream schemaRight;
    schemaRight << "{\"logicalSourceName\" : \"right\",\"schema\" : \"Schema::create()"
                   "->addField(createField(\\\"sensorId\\\", BasicType::UINT32))"
                   "->addField(createField(\\\"location\\\", BasicType::UINT32))"
                   "->addField(createField(\\\"timestamp\\\", BasicType::UINT64))"
                   "->addField(createField(\\\"humidity\\\", BasicType::FLOAT64));\"}";
    schemaRight << endl;
    NES_INFO("schema left submit={}", schemaRight.str());
    ASSERT_TRUE(TestUtils::addLogicalSource(schemaRight.str(), std::to_string(*restPort)));

    auto networkingWorker1 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                                     TestUtils::dataPort(0),
                                                     TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                                     TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                     TestUtils::numGlobalBuffers(numberOfBuffersInGlobalBufferManager),
                                                     TestUtils::numBuffersPerWorker(numberOfBuffersPerWorker),
                                                     TestUtils::workerHealthCheckWaitTime(healthWaitTime)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 1));

    auto networkingWorker2 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                                     TestUtils::dataPort(0),
                                                     TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                                     TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                     TestUtils::numGlobalBuffers(numberOfBuffersInGlobalBufferManager),
                                                     TestUtils::numBuffersPerWorker(numberOfBuffersPerWorker),
                                                     TestUtils::workerHealthCheckWaitTime(healthWaitTime)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 2));

    auto networkingWorker3 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                                     TestUtils::dataPort(0),
                                                     TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                                     TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                     TestUtils::numGlobalBuffers(numberOfBuffersInGlobalBufferManager),
                                                     TestUtils::numBuffersPerWorker(numberOfBuffersPerWorker),
                                                     TestUtils::workerHealthCheckWaitTime(healthWaitTime)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 3));

    auto networkingWorker4 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                                     TestUtils::dataPort(0),
                                                     TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                                     TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                     TestUtils::numGlobalBuffers(numberOfBuffersInGlobalBufferManager),
                                                     TestUtils::numBuffersPerWorker(numberOfBuffersPerWorker),
                                                     TestUtils::workerHealthCheckWaitTime(healthWaitTime)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 4));

    auto leftJoinSource1W1 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                                     TestUtils::dataPort(0),
                                                     TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                                     TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                     TestUtils::numGlobalBuffers(numberOfBuffersInGlobalBufferManager),
                                                     TestUtils::numBuffersPerWorker(numberOfBuffersPerWorker),
                                                     TestUtils::parentId(2),
                                                     TestUtils::sourceType(SourceType::CSV_SOURCE),
                                                     TestUtils::csvSourceFilePath(left1Path),
                                                     TestUtils::physicalSourceName("test_stream_left_1_p"),
                                                     TestUtils::logicalSourceName("left"),
                                                     TestUtils::numberOfBuffersToProduce(buffers),
                                                     TestUtils::numberOfTuplesToProducePerBuffer(tuplesPerBuffer),
                                                     TestUtils::sourceGatheringInterval(gatheringIntervalMs),
                                                     TestUtils::workerHealthCheckWaitTime(healthWaitTime),
                                                     TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                     TestUtils::enableDebug()});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 5));

    auto leftJoinSource2W2 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                                     TestUtils::dataPort(0),
                                                     TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                                     TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                     TestUtils::numGlobalBuffers(numberOfBuffersInGlobalBufferManager),
                                                     TestUtils::numBuffersPerWorker(numberOfBuffersPerWorker),
                                                     TestUtils::parentId(3),
                                                     TestUtils::sourceType(SourceType::CSV_SOURCE),
                                                     TestUtils::csvSourceFilePath(left2Path),
                                                     TestUtils::physicalSourceName("test_stream_left_2_p"),
                                                     TestUtils::logicalSourceName("left"),
                                                     TestUtils::numberOfBuffersToProduce(buffers),
                                                     TestUtils::numberOfTuplesToProducePerBuffer(tuplesPerBuffer),
                                                     TestUtils::sourceGatheringInterval(gatheringIntervalMs),
                                                     TestUtils::workerHealthCheckWaitTime(healthWaitTime),
                                                     TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                     TestUtils::enableDebug()});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 6));

    auto leftJoinSource3W3 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                                     TestUtils::dataPort(0),
                                                     TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                                     TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                     TestUtils::numGlobalBuffers(numberOfBuffersInGlobalBufferManager),
                                                     TestUtils::numBuffersPerWorker(numberOfBuffersPerWorker),
                                                     TestUtils::parentId(4),
                                                     TestUtils::sourceType(SourceType::CSV_SOURCE),
                                                     TestUtils::csvSourceFilePath(left3Path),
                                                     TestUtils::physicalSourceName("test_stream_left_3_p"),
                                                     TestUtils::logicalSourceName("left"),
                                                     TestUtils::numberOfBuffersToProduce(buffers),
                                                     TestUtils::numberOfTuplesToProducePerBuffer(tuplesPerBuffer),
                                                     TestUtils::sourceGatheringInterval(gatheringIntervalMs),
                                                     TestUtils::workerHealthCheckWaitTime(healthWaitTime),
                                                     TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                     TestUtils::enableDebug()});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 7));

    auto leftJoinSource4W4 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                                     TestUtils::dataPort(0),
                                                     TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                                     TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                     TestUtils::numGlobalBuffers(numberOfBuffersInGlobalBufferManager),
                                                     TestUtils::numBuffersPerWorker(numberOfBuffersPerWorker),
                                                     TestUtils::parentId(5),
                                                     TestUtils::sourceType(SourceType::CSV_SOURCE),
                                                     TestUtils::csvSourceFilePath(left4Path),
                                                     TestUtils::physicalSourceName("test_stream_left_4_p"),
                                                     TestUtils::logicalSourceName("left"),
                                                     TestUtils::numberOfBuffersToProduce(buffers),
                                                     TestUtils::numberOfTuplesToProducePerBuffer(tuplesPerBuffer),
                                                     TestUtils::sourceGatheringInterval(gatheringIntervalMs),
                                                     TestUtils::workerHealthCheckWaitTime(healthWaitTime),
                                                     TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                     TestUtils::enableDebug()});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 8));

    auto rightJoinSource1 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                                    TestUtils::dataPort(0),
                                                    TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                                    TestUtils::parentId(6),
                                                    TestUtils::sourceType(SourceType::CSV_SOURCE),
                                                    TestUtils::csvSourceFilePath(right1Path),
                                                    TestUtils::physicalSourceName("test_stream_right_1"),
                                                    TestUtils::logicalSourceName("right"),
                                                    TestUtils::numberOfBuffersToProduce(buffers),
                                                    TestUtils::numberOfTuplesToProducePerBuffer(tuplesPerBuffer),
                                                    TestUtils::sourceGatheringInterval(gatheringIntervalMs),
                                                    TestUtils::workerHealthCheckWaitTime(healthWaitTime),
                                                    TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                    TestUtils::enableDebug()});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 9));

    auto rightJoinSource2 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                                    TestUtils::dataPort(0),
                                                    TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                                    TestUtils::parentId(7),
                                                    TestUtils::sourceType(SourceType::CSV_SOURCE),
                                                    TestUtils::csvSourceFilePath(right2Path),
                                                    TestUtils::physicalSourceName("test_stream_right_2"),
                                                    TestUtils::logicalSourceName("right"),
                                                    TestUtils::numberOfBuffersToProduce(buffers),
                                                    TestUtils::numberOfTuplesToProducePerBuffer(tuplesPerBuffer),
                                                    TestUtils::sourceGatheringInterval(gatheringIntervalMs),
                                                    TestUtils::workerHealthCheckWaitTime(healthWaitTime),
                                                    TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                    TestUtils::enableDebug()});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 10));

    auto rightJoinSource3 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                                    TestUtils::dataPort(0),
                                                    TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                                    TestUtils::parentId(8),
                                                    TestUtils::sourceType(SourceType::CSV_SOURCE),
                                                    TestUtils::csvSourceFilePath(right3Path),
                                                    TestUtils::physicalSourceName("test_stream_right_3"),
                                                    TestUtils::logicalSourceName("right"),
                                                    TestUtils::numberOfBuffersToProduce(buffers),
                                                    TestUtils::numberOfTuplesToProducePerBuffer(tuplesPerBuffer),
                                                    TestUtils::sourceGatheringInterval(gatheringIntervalMs),
                                                    TestUtils::workerHealthCheckWaitTime(healthWaitTime),
                                                    TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                    TestUtils::enableDebug()});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 11));

    auto rightJoinSource4 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                                    TestUtils::dataPort(0),
                                                    TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                                    TestUtils::parentId(9),
                                                    TestUtils::sourceType(SourceType::CSV_SOURCE),
                                                    TestUtils::csvSourceFilePath(right4Path),
                                                    TestUtils::physicalSourceName("test_stream_right_4"),
                                                    TestUtils::logicalSourceName("right"),
                                                    TestUtils::numberOfBuffersToProduce(buffers),
                                                    TestUtils::numberOfTuplesToProducePerBuffer(tuplesPerBuffer),
                                                    TestUtils::sourceGatheringInterval(gatheringIntervalMs),
                                                    TestUtils::workerHealthCheckWaitTime(healthWaitTime),
                                                    TestUtils::bufferSizeInBytes(bufferSizeInBytes),
                                                    TestUtils::enableDebug()});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeoutSetupMs, 12));

    auto topology = TestUtils::getTopology(*restPort);
    NES_INFO("The final topology:\n{}", topology.dump());

    // deploy values for statistics
    auto startNode = 6;
    auto startStream = 1;
    auto numJoins = 4;

    for (auto i = startStream; i <= numJoins; i++) {
        nlohmann::json request;
        request["topologyNodeId"] = startNode++;
        request["logicalSource"] = "left";
        request["physicalSource"] = "test_stream_left_" + std::to_string(i) + "_p";
        request["fieldName"] = "location";
        request["value"] = std::to_string(i);
        auto jsonString = request.dump();
        NES_INFO("Json string={}", jsonString);
        auto success = TestUtils::addSourceStatistics(request.dump(), std::to_string(*restPort));
        ASSERT_TRUE(success["success"]);

        request["topologyNodeId"] = startNode + 3;
        request["logicalSource"] = "right";
        request["physicalSource"] = "test_stream_right_" + std::to_string(i);
        request["fieldName"] = "location";
        request["value"] = std::to_string(i);
        jsonString = request.dump();
        NES_INFO("Json string={}", jsonString);

        success = TestUtils::addSourceStatistics(request.dump(), std::to_string(*restPort));
        ASSERT_TRUE(success["success"]);
    }

    NES_INFO("query string submit={}", querySs.str());
    nlohmann::json json_return = TestUtils::startQueryViaRest(querySs.str(), std::to_string(*restPort));
    QueryId queryId = json_return.at("queryId").get<QueryId>();

    NES_INFO("try to acc return");
    NES_INFO("Query ID: {}", queryId);
    ASSERT_NE(queryId, INVALID_QUERY_ID);

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, expectedBuffers, std::to_string(*restPort), timeoutResultSec));
    nlohmann::json jsonPlan = TestUtils::getExecutionPlan(queryId, std::to_string(*restPort));

    std::ifstream ifs(outputFilePath.c_str());
    ASSERT_TRUE(ifs.good());

    uint64_t lineCnt = Util::countLines(ifs);
    NES_INFO("json execution plan:{}", jsonPlan.dump());
    EXPECT_EQ(expectedTuples, lineCnt);
}

}// namespace NES
