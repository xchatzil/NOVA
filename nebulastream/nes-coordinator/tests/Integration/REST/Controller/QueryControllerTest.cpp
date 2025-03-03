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
#include <API/Query.hpp>
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Identifiers/NESStrongTypeJson.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Services/QueryParsingService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <cpr/cpr.h>
#include <gtest/gtest.h>
#include <memory>
#include <nlohmann/json.hpp>

namespace NES {
class QueryControllerTest : public Testing::BaseIntegrationTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("QueryControllerTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup QueryControllerTest test class.");
    }

    void SetUp() override {
        Testing::BaseIntegrationTest::SetUp();
        //Increment the query id so that the shared query and query plan ids are different before the start of each test
        PlanIdGenerator::getNextQueryId();
    }

    static void TearDownTestCase() { NES_INFO("Tear down QueryControllerTest test class."); }

    void startCoordinator() {
        NES_INFO("QueryControllerTest: Start coordinator");
        coordinatorConfig = CoordinatorConfiguration::createDefault();
        coordinatorConfig->rpcPort = *rpcCoordinatorPort;
        coordinatorConfig->restPort = *restPort;

        coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
        ASSERT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
        NES_INFO("QueryControllerTest: Coordinator started successfully");
    }

    void stopCoordinator() {
        bool stopCrd = coordinator->stopCoordinator(true);
        ASSERT_TRUE(stopCrd);
    }

    NesCoordinatorPtr coordinator;
    CoordinatorConfigurationPtr coordinatorConfig;
};

//Check if submitting a POST request without defining 'userQuery' returns 400
TEST_F(QueryControllerTest, testSubmitQueryNoUserQuery) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));

    nlohmann::json request;
    request["faultTolerance"] = "AT_MOST_ONCE";
    request["lineage"] = "IN_MEMORY";
    auto future = cpr::PostAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/query/execute-query"},
                                 cpr::Header{{"Content-Type", "application/json"}},
                                 cpr::Body{request.dump()});
    future.wait();
    auto response = future.get();
    EXPECT_EQ(response.status_code, 400l);
    nlohmann::json res;
    EXPECT_NO_THROW(res = nlohmann::json::parse(response.text));
    std::string errorMessage = res["message"].get<std::string>();
    EXPECT_TRUE(errorMessage.find("Incorrect or missing key word for user query, use 'userQuery'") != std::string::npos);
    stopCoordinator();
}

//Check if submitting a POST request without defining 'placement' returns 400
TEST_F(QueryControllerTest, testSubmitQueryNoPlacement) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    nlohmann::json request;
    request["userQuery"] =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()); )";
    auto future = cpr::PostAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/query/execute-query"},
                                 cpr::Header{{"Content-Type", "application/json"}},
                                 cpr::Body{request.dump()});
    future.wait();
    auto response = future.get();
    EXPECT_EQ(response.status_code, 400l);
    nlohmann::json res;
    EXPECT_NO_THROW(res = nlohmann::json::parse(response.text));
    NES_DEBUG("{}", res.dump());
    std::string errorMessage = res["message"].get<std::string>();
    EXPECT_TRUE(errorMessage.find("No placement strategy specified. Specify a placement strategy using 'placement'.")
                != std::string::npos);
    stopCoordinator();
}

//Check if submitting a POST request with an unsupported 'placement' strategy returns 400
TEST_F(QueryControllerTest, testSubmitQueryInvalidPlacement) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    nlohmann::json request;
    request["userQuery"] =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()); )";
    request["placement"] = "FAST";
    auto future = cpr::PostAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/query/execute-query"},
                                 cpr::Header{{"Content-Type", "application/json"}},
                                 cpr::Body{request.dump()});
    future.wait();
    auto response = future.get();
    EXPECT_EQ(response.status_code, 400l);
    nlohmann::json res;
    EXPECT_NO_THROW(res = nlohmann::json::parse(response.text));
    NES_DEBUG("{}", res.dump());
    std::string errorMessage = res["message"].get<std::string>();
    EXPECT_TRUE(errorMessage.find("Invalid Placement Strategy: ") != std::string::npos);
    stopCoordinator();
}

//Check if submitting a proper query returns 200
TEST_F(QueryControllerTest, testSubmitValidQuery) {
    NES_INFO("TestsForOatppEndpoints: Start coordinator");
    coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;

    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->coordinatorPort = *rpcCoordinatorPort;
    workerConfiguration->queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    workerConfiguration->physicalSourceTypes.add(DefaultSourceType::create("default_logical", "default_physical"));
    coordinatorConfig->worker = *(workerConfiguration);
    coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
    ASSERT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    NES_INFO("QueryControllerTest: Coordinator started successfully");
    bool success = TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5);
    ASSERT_TRUE(success);
    nlohmann::json request;
    request["userQuery"] =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()); )";
    request["placement"] = "BottomUp";
    request["faultTolerance"] = "AT_MOST_ONCE";
    request["lineage"] = "IN_MEMORY";
    auto future = cpr::PostAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/query/execute-query"},
                                 cpr::Header{{"Content-Type", "application/json"}},
                                 cpr::Body{request.dump()});
    future.wait();
    auto response = future.get();
    EXPECT_EQ(response.status_code, 202l);
    nlohmann::json res;
    EXPECT_NO_THROW(res = nlohmann::json::parse(response.text));
    auto queryId = res["queryId"];
    EXPECT_TRUE(TestUtils::checkRunningOrTimeout(queryId, std::to_string(coordinatorConfig->restPort.getValue())));
    stopCoordinator();
}

//Check if getting an execution-plan returns as expected
TEST_F(QueryControllerTest, testGetExecutionPlan) {
    NES_INFO("TestsForOatppEndpoints: Start coordinator");
    coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;

    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->coordinatorPort = *rpcCoordinatorPort;
    workerConfiguration->queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    workerConfiguration->physicalSourceTypes.add(DefaultSourceType::create("default_logical", "default_physical"));
    coordinatorConfig->worker = *(workerConfiguration);
    coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
    ASSERT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    NES_INFO("QueryControllerTest: Coordinator started successfully");
    auto sourceCatalog = coordinator->getSourceCatalog();
    bool success = TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5);

    ASSERT_TRUE(success);
    nlohmann::json request;
    request["userQuery"] =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()); )";
    request["placement"] = "BottomUp";
    request["faultTolerance"] = "AT_MOST_ONCE";
    request["lineage"] = "IN_MEMORY";
    auto f1 = cpr::PostAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/query/execute-query"},
                             cpr::Header{{"Content-Type", "application/json"}},
                             cpr::Body{request.dump()});
    f1.wait();
    auto r1 = f1.get();
    EXPECT_EQ(r1.status_code, 202l);

    nlohmann::json response1;
    ASSERT_NO_THROW(response1 = nlohmann::json::parse(r1.text));
    auto queryId = response1["queryId"].get<QueryId>();
    NES_DEBUG("{}", queryId);
    auto started = TestUtils::waitForQueryToStart(queryId, coordinator->getQueryCatalog());
    ASSERT_TRUE(started);
    auto f2 = cpr::GetAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/query/execution-plan"},
                            cpr::Parameters{{"queryId", queryId.toString()}});
    f2.wait();
    auto r2 = f2.get();
    EXPECT_EQ(r2.status_code, 200l);
    nlohmann::json response2;
    ASSERT_NO_THROW(response2 = nlohmann::json::parse(r2.text));
    EXPECT_EQ(response2.size(), 1);
    for (auto executionNode : response2["ExecutionNodes"]) {
        const auto& rootWorkerNodeIds = coordinator->getTopology()->getRootWorkerNodeIds();
        const auto& workerId = executionNode["WorkerId"].get<WorkerId>();
        auto foundInRootWorkerId = std::find(rootWorkerNodeIds.begin(), rootWorkerNodeIds.end(), workerId);
        EXPECT_NE(foundInRootWorkerId, rootWorkerNodeIds.end());
        EXPECT_TRUE(executionNode["ScheduledDecomposedQueries"].size() != 0);
    }

    auto f3 = cpr::GetAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/query/execution-plan"},
                            cpr::Parameters{{"queryId", std::to_string(0)}});
    f3.wait();
    auto r3 = f3.get();
    EXPECT_EQ(r3.status_code, 404l);
    nlohmann::json response3;
    ASSERT_NO_THROW(response3 = nlohmann::json::parse(r3.text));
    NES_DEBUG("{}", response3.dump());
    EXPECT_EQ(response3["message"], "No query with given ID: 0");
    EXPECT_TRUE(TestUtils::checkRunningOrTimeout(queryId, std::to_string(coordinatorConfig->restPort.getValue())));
    stopCoordinator();
}

//Check if getting an execution-plan with invalid query ID returns a 404
TEST_F(QueryControllerTest, testGetExecutionPlanNoSuchQueryId) {
    NES_INFO("TestsForOatppEndpoints: Start coordinator");
    coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;

    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->coordinatorPort = *rpcCoordinatorPort;
    workerConfiguration->queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    workerConfiguration->physicalSourceTypes.add(DefaultSourceType::create("default_logical", "default_physical"));
    coordinatorConfig->worker = *(workerConfiguration);
    coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
    ASSERT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    NES_INFO("QueryControllerTest: Coordinator started successfully");
    auto sourceCatalog = coordinator->getSourceCatalog();
    bool success = TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5);

    ASSERT_TRUE(success);
    nlohmann::json request;
    request["userQuery"] =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()); )";
    request["placement"] = "BottomUp";
    request["faultTolerance"] = "AT_MOST_ONCE";
    request["lineage"] = "IN_MEMORY";
    auto f1 = cpr::PostAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/query/execute-query"},
                             cpr::Header{{"Content-Type", "application/json"}},
                             cpr::Body{request.dump()});
    f1.wait();
    auto r1 = f1.get();
    EXPECT_EQ(r1.status_code, 202l);

    nlohmann::json response1;
    ASSERT_NO_THROW(response1 = nlohmann::json::parse(r1.text));
    auto queryId = response1["queryId"].get<QueryId>();
    NES_DEBUG("{}", queryId);
    auto started = TestUtils::waitForQueryToStart(queryId, coordinator->getQueryCatalog());
    ASSERT_TRUE(started);
    auto f2 = cpr::GetAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/query/execution-plan"},
                            cpr::Parameters{{"queryId", std::to_string(0)}});
    f2.wait();
    auto r2 = f2.get();
    EXPECT_EQ(r2.status_code, 404l);
    nlohmann::json response2;
    EXPECT_NO_THROW(response2 = nlohmann::json::parse(r2.text));
    EXPECT_EQ(response2["message"], "No query with given ID: 0");
    EXPECT_TRUE(TestUtils::checkRunningOrTimeout(queryId, std::to_string(coordinatorConfig->restPort.getValue())));
    stopCoordinator();
}

//Check if getting a query-plan returns as expected
TEST_F(QueryControllerTest, testGetQueryPlan) {
    NES_INFO("TestsForOatppEndpoints: Start coordinator");
    coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;

    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->coordinatorPort = *rpcCoordinatorPort;
    workerConfiguration->physicalSourceTypes.add(DefaultSourceType::create("default_logical", "default_physical"));
    workerConfiguration->queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    coordinatorConfig->worker = *(workerConfiguration);
    coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
    ASSERT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    NES_INFO("QueryControllerTest: Coordinator started successfully");

    auto sourceCatalog = coordinator->getSourceCatalog();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));

    nlohmann::json request;
    request["userQuery"] =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()); )";
    request["placement"] = "BottomUp";
    request["faultTolerance"] = "AT_MOST_ONCE";
    request["lineage"] = "IN_MEMORY";
    auto f1 = cpr::PostAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/query/execute-query"},
                             cpr::Header{{"Content-Type", "application/json"}},
                             cpr::Body{request.dump()});
    f1.wait();
    auto r1 = f1.get();
    EXPECT_EQ(r1.status_code, 202l);

    nlohmann::json response1;
    ASSERT_NO_THROW(response1 = nlohmann::json::parse(r1.text));
    auto queryId = response1["queryId"].get<QueryId>();
    NES_DEBUG("{}", queryId);
    auto started = TestUtils::waitForQueryToStart(queryId, coordinator->getQueryCatalog());
    EXPECT_TRUE(started);

    auto f2 = cpr::GetAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/query/query-plan"},
                            cpr::Parameters{{"queryId", queryId.toString()}});
    f2.wait();
    auto r2 = f2.get();
    EXPECT_EQ(r2.status_code, 200l);
    nlohmann::json response2;
    EXPECT_NO_THROW(response2 = nlohmann::json::parse(r2.text));
    for (auto edge : response2["edges"]) {
        EXPECT_TRUE(edge.contains("source") && edge.contains("target"));
    }
    for (auto node : response2["nodes"]) {
        EXPECT_TRUE(node.contains("id") && node.contains("name") && node.contains("nodeType"));
    }
    EXPECT_TRUE(TestUtils::checkRunningOrTimeout(queryId, std::to_string(coordinatorConfig->restPort.getValue())));
    stopCoordinator();
}

//Check if getting a query-plan with invalid query ID returns a 404
TEST_F(QueryControllerTest, testGetQueryPlanNoSuchQueryId) {
    NES_INFO("TestsForOatppEndpoints: Start coordinator");
    coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;

    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->coordinatorPort = *rpcCoordinatorPort;
    workerConfiguration->queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    workerConfiguration->physicalSourceTypes.add(DefaultSourceType::create("default_logical", "default_physical"));
    coordinatorConfig->worker = *(workerConfiguration);
    coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
    EXPECT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    NES_INFO("QueryControllerTest: Coordinator started successfully");
    auto sourceCatalog = coordinator->getSourceCatalog();
    EXPECT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));

    nlohmann::json request;
    request["userQuery"] =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()); )";
    request["placement"] = "BottomUp";
    request["faultTolerance"] = "AT_MOST_ONCE";
    request["lineage"] = "IN_MEMORY";
    auto f1 = cpr::PostAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/query/execute-query"},
                             cpr::Header{{"Content-Type", "application/json"}},
                             cpr::Body{request.dump()});
    f1.wait();
    auto r1 = f1.get();
    EXPECT_EQ(r1.status_code, 202l);

    nlohmann::json response1;
    EXPECT_NO_THROW(response1 = nlohmann::json::parse(r1.text));
    auto queryId = response1["queryId"].get<QueryId>();
    NES_DEBUG("{}", queryId);
    auto started = TestUtils::waitForQueryToStart(queryId, coordinator->getQueryCatalog());
    EXPECT_TRUE(started);
    auto f2 = cpr::GetAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/query/query-plan"},
                            cpr::Parameters{{"queryId", std::to_string(0)}});
    f2.wait();
    auto r2 = f2.get();
    EXPECT_EQ(r2.status_code, 404l);
    nlohmann::json response2 = nlohmann::json::parse(r2.text);
    NES_DEBUG("{}", response2.dump());
    EXPECT_EQ(response2["message"], "No query with given ID: 0");
    EXPECT_TRUE(TestUtils::checkRunningOrTimeout(queryId, std::to_string(coordinatorConfig->restPort.getValue())));
    stopCoordinator();
}
}// namespace NES
