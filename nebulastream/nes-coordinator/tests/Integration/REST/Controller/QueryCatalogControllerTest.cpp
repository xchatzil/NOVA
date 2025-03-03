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
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Identifiers/NESStrongTypeJson.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Services/QueryParsingService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <cpr/cpr.h>
#include <gtest/gtest.h>
#include <memory>
#include <nlohmann/json.hpp>

namespace NES {
class QueryCatalogControllerTest : public Testing::BaseIntegrationTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("QueryCatalogControllerTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup QueryCatalogControllerTest test class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down QueryCatalogControllerTest test class."); }

    void startCoordinator() {
        NES_INFO("QueryCatalogControllerTest: Start coordinator");
        coordinatorConfig = CoordinatorConfiguration::createDefault();
        coordinatorConfig->rpcPort = *rpcCoordinatorPort;
        coordinatorConfig->restPort = *restPort;

        coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
        ASSERT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
        NES_INFO("QueryCatalogControllerTest: Coordinator started successfully");
    }

    void stopCoordinator() {
        bool stopCrd = coordinator->stopCoordinator(true);
        ASSERT_TRUE(stopCrd);
    }

    NesCoordinatorPtr coordinator;
    CoordinatorConfigurationPtr coordinatorConfig;
};

// Test that allRegisteredQueries first returns an empty json when no queries are registered and then a non-empty one after a query has been registered
TEST_F(QueryCatalogControllerTest, testGetRequestAllRegistedQueries) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));

    cpr::AsyncResponse future1 =
        cpr::GetAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/queryCatalog/allRegisteredQueries"});
    future1.wait();
    auto r = future1.get();
    EXPECT_EQ(r.status_code, 200l);
    nlohmann::json jsonResponse;
    ASSERT_NO_THROW(jsonResponse = nlohmann::json::parse(r.text));

    auto query = Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create());
    auto queryCatalog = coordinator->getQueryCatalog();
    const QueryPlanPtr queryPlan = query.getQueryPlan();
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);
    queryCatalog->createQueryCatalogEntry("query string",
                                          queryPlan,
                                          Optimizer::PlacementStrategy::BottomUp,
                                          QueryState::REGISTERED);
    cpr::AsyncResponse future2 =
        cpr::GetAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/queryCatalog/allRegisteredQueries"});
    future2.wait();
    auto response2 = future2.get();
    EXPECT_EQ(response2.status_code, 200l);
    nlohmann::json jsonResponse2;
    ASSERT_NO_THROW(jsonResponse2 = nlohmann::json::parse(response2.text));
    ASSERT_TRUE(!jsonResponse2.empty());
    stopCoordinator();
}

// Test queries endpoint: 400 if no status provided, otherwise 200. Depending on if a query is registered or not either an empty json body or non-empty
TEST_F(QueryCatalogControllerTest, testGetQueriesWithSpecificStatus) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));

    // When making a request for a query without specifying a specific status
    cpr::AsyncResponse future1 = cpr::GetAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/queryCatalog/queries"});
    future1.wait();
    auto r1 = future1.get();
    // return a 400 BAD REQUEST due to missing query parameters
    EXPECT_EQ(r1.status_code, 400l);

    // When including the status
    cpr::AsyncResponse future2 = cpr::GetAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/queryCatalog/queries"},
                                               cpr::Parameters{{"status", "REGISTERED"}});

    future2.wait();
    auto r2 = future2.get();
    // return 200 OK
    EXPECT_EQ(r2.status_code, 200l);
    // and an empty json
    nlohmann::json jsonResponse;
    ASSERT_NO_THROW(jsonResponse = nlohmann::json::parse(r2.text));
    ASSERT_TRUE(jsonResponse.empty());

    // create a query to add to query catalog service
    auto query = Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create());
    auto queryCatalog = coordinator->getQueryCatalog();
    const QueryPlanPtr queryPlan = query.getQueryPlan();
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);
    queryCatalog->createQueryCatalogEntry("queryString",
                                          queryPlan,
                                          Optimizer::PlacementStrategy::BottomUp,
                                          QueryState::REGISTERED);

    // when making a request for a query with a specific status after having submitted a query
    cpr::AsyncResponse future3 = cpr::GetAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/queryCatalog/queries"},
                                               cpr::Parameters{{"status", "REGISTERED"}});

    future3.wait();
    auto r3 = future3.get();
    //return 200 OK
    EXPECT_EQ(r3.status_code, 200l);
    // and a non-empty json
    nlohmann::json jsonResponse2;
    ASSERT_NO_THROW(jsonResponse2 = nlohmann::json::parse(r3.text));
    ASSERT_TRUE(!jsonResponse2.empty());
    stopCoordinator();
}

//Test status endpoint correctly returns status of a query
TEST_F(QueryCatalogControllerTest, testGetRequestStatusOfQuery) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));

    // when sending a request to the status endpoint without specifying a 'queryId' query parameter
    cpr::AsyncResponse f1 = cpr::GetAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/queryCatalog/status"});
    f1.wait();
    auto r1 = f1.get();
    // return 400 BAD REQUEST
    EXPECT_EQ(r1.status_code, 400l);

    // when sending a request to the status endpoint with 'queryId' supplied but no such query registered
    cpr::AsyncResponse f2 = cpr::GetAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/queryCatalog/status"},
                                          cpr::Parameters{{"queryId", "1"}});
    f2.wait();
    auto r2 = f2.get();
    //return 400 NO CONTENT
    EXPECT_EQ(r2.status_code, 404l);

    //create a query and submit i to the queryCatalogService
    auto query = Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create());
    auto queryCatalog = coordinator->getQueryCatalog();
    const QueryPlanPtr queryPlan = query.getQueryPlan();
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);
    queryCatalog->createQueryCatalogEntry("queryString",
                                          queryPlan,
                                          Optimizer::PlacementStrategy::BottomUp,
                                          QueryState::REGISTERED);

    // when sending a request to the status endpoint with 'queryId' supplied and a query with specified id registered
    cpr::AsyncResponse f3 = cpr::GetAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/queryCatalog/status"},
                                          cpr::Parameters{{"queryId", queryId.toString()}});
    f3.wait();
    auto r3 = f3.get();
    //return 200 OK
    EXPECT_EQ(r3.status_code, 200l);
    // and response body contains key: status and value: REGISTERED
    nlohmann::json jsonResponse;
    ASSERT_NO_THROW(jsonResponse = nlohmann::json::parse(r3.text));
    ASSERT_TRUE(jsonResponse["status"] == "REGISTERED");
    ASSERT_TRUE(jsonResponse["queryId"].get<QueryId>() == queryId);
    stopCoordinator();
}
}//namespace NES
