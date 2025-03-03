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
#ifdef ENABLE_OPC_BUILD
#include <API/Schema.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <cstring>
#include <future>
#include <iostream>
#include <open62541/plugin/pki_default.h>
#include <open62541/server.h>
#include <open62541/server_config_default.h>
#include <string>
#include <thread>

const std::string& url = "opc.tcp://localhost:4840";
//static const UA_NodeId baseDataVariableType = {0, UA_NODEIDTYPE_NUMERIC, {UA_NS0ID_BASEDATAVARIABLETYPE}};
static volatile UA_Boolean running = true;
static UA_Server* server = UA_Server_new();

namespace NES {

class OPCSourceTest : public Testing::BaseIntegrationTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("OPCSourceTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("OPCSOURCETEST::SetUpTestCase()");
    }

    void SetUp() {
        Testing::BaseIntegrationTest::SetUp();
        NES_DEBUG("OPCSOURCETEST::SetUp() OPCSourceTest cases set up.");
        test_schema = Schema::create()->addField("var", BasicType::UINT32);
        auto workerConfigurations = Configurations::WorkerConfiguration::create();
        auto defaultSourceType = DefaultSourceType::create("logical", "physical");
        workerConfigurations->physicalSourceTypes.add(defaultSourceType);
        nodeEngine = Runtime::NodeEngineBuilder::create(workerConfigurations)
                         .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                         .build();
        bufferManager = nodeEngine->getBufferManager();
        queryManager = nodeEngine->getQueryManager();
        buffer_size = bufferManager->getBufferSize();
        ASSERT_GT(buffer_size, 0ULL);
    }

    /* Will be called after a test is executed. */
    void TearDown() {
        Testing::BaseIntegrationTest::TearDown();
        ASSERT_TRUE(nodeEngine->stop());
        NES_DEBUG("OPCSOURCETEST::TearDown() Tear down OPCSourceTest");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_DEBUG("OPCSOURCETEST::TearDownTestCases() Tear down OPCSourceTest test class."); }

    static void addVariable(UA_Server* server) {
        /* Define the attribute of the myInteger variable node */
        UA_VariableAttributes attr = UA_VariableAttributes_default;
        UA_Int32 myInteger = 42;
        UA_Variant_setScalar(&attr.value, &myInteger, &UA_TYPES[UA_TYPES_INT32]);
        attr.description = UA_LOCALIZEDTEXT((char*) "en-US", (char*) "the answer");
        attr.displayName = UA_LOCALIZEDTEXT((char*) "en-US", (char*) "the answer");
        attr.dataType = UA_TYPES[UA_TYPES_INT32].typeId;
        attr.accessLevel = UA_ACCESSLEVELMASK_READ | UA_ACCESSLEVELMASK_WRITE;

        /* Add the variable node to the information model */
        UA_NodeId myIntegerNodeId = UA_NODEID_STRING(1, (char*) "the.answer");
        UA_QualifiedName myIntegerName = UA_QUALIFIEDNAME(1, (char*) "the answer");
        UA_NodeId parentNodeId = UA_NODEID_NUMERIC(0, UA_NS0ID_OBJECTSFOLDER);
        UA_NodeId parentReferenceNodeId = UA_NODEID_NUMERIC(0, UA_NS0ID_ORGANIZES);
        UA_Server_addVariableNode(server,
                                  myIntegerNodeId,
                                  parentNodeId,
                                  parentReferenceNodeId,
                                  myIntegerName,
                                  UA_NODEID_NUMERIC(0, UA_NS0ID_BASEDATAVARIABLETYPE),
                                  attr,
                                  NULL,
                                  NULL);
    }

    static void writeVariable(UA_Server* server) {
        UA_NodeId myIntegerNodeId = UA_NODEID_STRING(1, (char*) "the.answer");

        /* Write a different integer value */
        UA_Int32 myInteger = 43;
        UA_Variant myVar;
        UA_Variant_init(&myVar);
        UA_Variant_setScalar(&myVar, &myInteger, &UA_TYPES[UA_TYPES_INT32]);
        UA_Server_writeValue(server, myIntegerNodeId, myVar);

        /* Set the status code of the value to an error code. The function
         * UA_Server_write provides access to the raw service. The above
         * UA_Server_writeValue is syntactic sugar for writing a specific node
         * attribute with the write service. */
        UA_WriteValue wv;
        UA_WriteValue_init(&wv);
        wv.nodeId = myIntegerNodeId;
        wv.attributeId = UA_ATTRIBUTEID_VALUE;
        wv.value.status = UA_STATUSCODE_BADNOTCONNECTED;
        wv.value.hasStatus = true;
        UA_Server_write(server, &wv);

        /* Reset the variable to a good statuscode with a value */
        wv.value.hasStatus = false;
        wv.value.value = myVar;
        wv.value.hasValue = true;
        UA_Server_write(server, &wv);
    }

    static void startServer(std::promise<bool>& p) {
        UA_ServerConfig_setDefault(UA_Server_getConfig(server));
        addVariable(server);
        writeVariable(server);
        p.set_value(true);
        UA_StatusCode retval = UA_Server_run(server, &running);
        NES_DEBUG(" retval is={}", retval);
        UA_Server_delete(server);
    }

    static void stopServer() { running = false; }

    Runtime::NodeEnginePtr nodeEngine{nullptr};
    Runtime::BufferManagerPtr bufferManager;
    Runtime::QueryManagerPtr queryManager;
    SchemaPtr test_schema;
    uint64_t buffer_size;

  protected:
    UA_NodeId nodeId = UA_NODEID_STRING(1, (char*) "the.answer");
    const std::string user = "";
    const std::string password = "";
};

/**
 * Tests basic set up of OPC source
 */
TEST_F(OPCSourceTest, OPCSourceInit) {

    auto opcSource =
        createOPCSource(test_schema, bufferManager, queryManager, url, nodeId, user, password, 1, 12, "physicalSource", {});

    SUCCEED();
}

/**
 * Test if schema, OPC server url, and node index are the same
 */
TEST_F(OPCSourceTest, OPCSourcePrint) {

    auto opcSource =
        createOPCSource(test_schema, bufferManager, queryManager, url, nodeId, user, password, 1, 12, "physicalSource", {});

    std::string expected =
        "OPC_SOURCE(SCHEMA(var:INTEGER ), URL= opc.tcp://localhost:4840, NODE_INDEX= 1, NODE_IDENTIFIER= the.answer. ";

    EXPECT_EQ(opcSource->toString(), expected);

    NES_DEBUG("{}", opcSource->toString());

    SUCCEED();
}

/**
 * Tests if obtained value is valid.
 */

TEST_F(OPCSourceTest, OPCSourceValue) {
    std::promise<bool> p;
    std::thread t1([&p]() {
        startServer(p);
    });
    t1.detach();
    auto test_schema = Schema::create()->addField("var", BasicType::UINT32);
    auto opcSource =
        createOPCSource(test_schema, bufferManager, queryManager, url, nodeId, user, password, 1, 12, "physicalSource", {});
    opcSource->open();

    p.get_future().get();
    opcSource->open();
    auto tuple_buffer = opcSource->receiveData();
    EXPECT_TRUE(tuple_buffer.has_value());
    uint64_t value = 0;
    auto* tuple = (uint32_t*) tuple_buffer->getBuffer();
    value = *tuple;
    uint64_t expected = 43;
    NES_DEBUG("OPCSOURCETEST::TEST_F(OPCSourceTest, OPCSourceValue) expected value is: {}. Received value is: {}",
              expected,
              value);
    EXPECT_EQ(value, expected);
    tuple_buffer->release();
    stopServer();
}
}// namespace NES
#endif
